import psycopg2
import pandas as pd
import requests
import numpy as np
import json
import streamlit as st   # ✅ so we can use st.secrets
from datetime import datetime, timedelta, timezone
import pytz
from myapp_utils.db_connection import get_connection

SHOP_DOMAIN = st.secrets.get("shopify", {}).get("store_url")
SHOP_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
SHOPIFY_STORE_URL = st.secrets.get("shopify", {}).get("store_url")
ACCESS_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
API_VERSION  = st.secrets.get("shopify", {}).get("api_version")
DEFAULT_DAYS  = st.secrets.get("shopify", {}).get("default_days")
HAS_REAL_SHOPIFY = bool(SHOP_DOMAIN and SHOP_TOKEN)

# ---------------------------
# Watermark for Incremental update
# ---------------------------

def get_shop_timezone():
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/shop.json"
    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    shop_data = response.json()["shop"]
    return shop_data.get("iana_timezone")
    
# def get_last_watermark(table_name):
    
    # conn = get_connection()
    
    # with conn.cursor() as cur:
        # cur.execute("SELECT last_watermark FROM dev.etl_watermarks WHERE table_name = %s", (table_name,))
        # result = cur.fetchone()
        # return result[0] if result else "N/A" #datetime(2000,1,1, tzinfo=timezone.utc)
        
def get_last_watermark(p_users_stores_id:int, p_table_name: str):
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT * FROM dev.fn_get_last_etl_watermark(%s,%s);",[p_users_stores_id, p_table_name])
        result = cur.fetchone()
        return result[0]
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Error fetching last watermark for {p_table_name} ", e)
        
    finally:
        cur.close()
        conn.close()


def update_last_watermark(p_users_stores_id:str, p_table_name: str, p_load_type: str, last_watermark_date: str):
    conn = get_connection()
    cur = conn.cursor()
        
    try:
        cur.execute("CALL dev.usp_update_etl_watermark(%s,%s,%s,%s);",[p_users_stores_id, p_table_name, p_load_type, last_watermark_date])
        conn.commit()
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Error updating watermark for {p_table_name}->{p_load_type} ", e)
        
    finally:
        cur.close()
        conn.close()


# def update_watermark(table_name, new_watermark):
    
    # conn = get_connection()
    
    # shop_tz = get_shop_timezone()
    # tz = pytz.timezone(shop_tz)
    # shop_now = datetime.now(tz) #new_watermark
    
    # with conn.cursor() as cur:
        # cur.execute("""
            # INSERT INTO dev.etl_watermarks (table_name, last_watermark)
            # VALUES (%s, %s)
            # ON CONFLICT (table_name)
            # DO UPDATE SET last_watermark = EXCLUDED.last_watermark;
        # """, (table_name, shop_now))
    # conn.commit()

#def update_watermark(table_name: str):
    
#    """Update ETL watermark using shop's local time."""
#    shop_tz = get_shop_timezone()
#    tz = pytz.timezone(shop_tz)
#    shop_now = datetime.now(tz)

#    sql = """
#    INSERT INTO dev.etl_watermarks(table_name, last_watermark)
#    VALUES (%s, %s)
#    ON CONFLICT (table_name) DO UPDATE
#    SET last_watermark = EXCLUDED.last_watermark;
#    """
#    conn = get_connection()
#    with conn.cursor() as cur:
#        cur.execute(sql, (table_name, shop_now))
#    conn.commit()
#    return shop_now
    
# ---------------------------
# Utility: DF → JSON
# ---------------------------
#def df_to_json(df: pd.DataFrame) -> str:
#    """Convert DataFrame to JSON string for PostgreSQL."""
#    return df.to_json(orient="records", date_format="iso")

def df_to_json(df: pd.DataFrame, id_col: str = "id") -> str:
    """
    Safely convert a DataFrame to JSON string in records format.
    Skips rows where the specified id column is null.
    Ensures any dict/list columns are properly serialized.
    Casts id column to integer if numeric-like.
    """
    df_copy = df.copy()

    # Ensure id column exists
    if id_col not in df_copy.columns:
        raise ValueError(f"Column '{id_col}' not found in dataframe")

    # Drop null IDs
    df_copy = df_copy[df_copy[id_col].notnull()]

    # Cast ID column to int if numeric
    if np.issubdtype(df_copy[id_col].dtype, np.number):
        df_copy[id_col] = df_copy[id_col].astype("Int64")   # pandas nullable int
        df_copy[id_col] = df_copy[id_col].apply(lambda x: int(x) if pd.notnull(x) else None)

    # Convert dict/list columns to JSON strings
    for col in df_copy.columns:
        if df_copy[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_copy[col] = df_copy[col].apply(json.dumps)

    #return json.loads(df_copy.to_json(orient="records"))
    return df_copy.to_json(orient="records") 
    
# ---------------------------
# Class DB
# ---------------------------
class Db_Upsert:
    # ---------------------------
    # Customers Loader
    # ---------------------------
    @staticmethod
    def add_store(shop_name:str, enc_api_password:str, enc_api_key:str, user_id:int):
        conn = get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("CALL dev.usp_insert_store_and_initialize_etl(%s,%s,%s,%s);",[shop_name, enc_api_password,enc_api_key, user_id])
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            print("❌ Error adding new store ", e)
        finally:
            cur.close()
            conn.close()
    
    
    @staticmethod
    def load_customers(df: pd.DataFrame, mode: str,users_stores_id:int):
        # print("***** DEBUG :: Start <load_customers> Function")
        #print("***** DEBUG :: Columns ::", df.columns.tolist())
        #print("***** DEBUG :: Data types ::\n", df.dtypes)
        # print("***** DEBUG :: Mode ::", mode)

        if df.empty:
            print("No customers to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            # Convert entire DataFrame to JSON string
            json_data = df_to_json(df, id_col="id")
            
            
            if mode == "Full":
                cur.execute("CALL dev.usp_insert_customer(%s,%s);", [json_data,users_stores_id])

            else:
                cur.execute("CALL dev.usp_upsert_customer(%s,%s);", [json_data,users_stores_id])

            max_updated = pd.to_datetime(df["updated_at"]).max()
            update_last_watermark(users_stores_id, "customers", mode, max_updated)
            conn.commit()
            

        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading customers:")
            st.exception(e)
            st.stop()

        finally:
            cur.close()
            conn.close()

    # ---------------------------
    # Orders Loader
    # ---------------------------
    @staticmethod
    def load_orders(df: pd.DataFrame, mode: str,users_stores_id:int):
        # print("***** DEBUG :: Start <load_orders> Function")
        # print("***** DEBUG :: Mode ::", mode)

        if df.empty:
            print("No orders to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            json_data = df_to_json(df)
            if mode == "Full":
                
                cur.execute("CALL dev.usp_insert_orders(%s,%s);", [json_data,users_stores_id])

            else:
                cur.execute("CALL dev.usp_upsert_orders(%s,%s);", [json_data,users_stores_id])
            
            max_updated = pd.to_datetime(df["updated_at"]).max()
            update_last_watermark(users_stores_id, "orders", mode, max_updated)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading orders:", e)
            st.exception(e)
            st.stop()
            
        finally:
            cur.close()
            conn.close()

    # ---------------------------
    # Prodcut Loader
    # ---------------------------
    @staticmethod
    def load_products(df: pd.DataFrame, mode: str,users_stores_id:int):

        if df.empty:
            print("No product to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            json_data = df_to_json(df)
            if mode == "Full":
                
                cur.execute("CALL dev.usp_insert_products(%s,%s);", [json_data,users_stores_id])
                
            else:
                cur.execute("CALL dev.usp_upsert_products(%s,%s);", [json_data,users_stores_id])
                
            max_updated = pd.to_datetime(df["updated_at"]).max()
            update_last_watermark(users_stores_id, "products", mode, max_updated)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading products:")
            st.exception(e)
            st.stop()
            
        finally:
            cur.close()
            conn.close()
            
    # ---------------------------
    # Refunds Loader
    # ---------------------------
    @staticmethod
    def load_refunds(df: pd.DataFrame, mode: str,users_stores_id:int):

        if df.empty:
            print("No refund to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            json_data = df_to_json(df)
            tz = pytz.timezone('Asia/Kolkata')
            if mode == "Full":
                
                cur.execute("CALL dev.usp_upsert_refunds(%s,%s);", [json_data,users_stores_id])
                current_time = datetime.now(tz)
                
            else:
                cur.execute("CALL dev.usp_upsert_refunds(%s,%s);", [json_data,users_stores_id])
                current_time = datetime.now(tz)

            max_updated = current_time.strftime("%Y-%m-%d %H:%M:%S.%f %z")
            update_last_watermark(users_stores_id, "refunds", mode, max_updated)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading refund:")
            st.exception(e)
            st.stop()
            
        finally:
            cur.close()
            conn.close()

    # ---------------------------
    # Inventory Items Loader
    # ---------------------------
    @staticmethod
    def load_inventory_items(df: pd.DataFrame, mode: str,users_stores_id:int):

        if df.empty:
            print("No inventory items to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            json_data = df_to_json(df)
            if mode == "Full":
                
                cur.execute("CALL dev.usp_insert_inventory_items(%s,%s);", [json_data,users_stores_id])
                
            else:
                cur.execute("CALL dev.usp_upsert_inventory_items(%s,%s);", [json_data,users_stores_id])
                

            max_updated = pd.to_datetime(df["updated_at"]).max()
            update_last_watermark(users_stores_id, "inventory_items", mode, max_updated)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading inventory items:")
            st.exception(e)
            st.stop()
            
        finally:
            cur.close()
            conn.close()
            
    # ---------------------------
    # Varinet Loader
    # ---------------------------
    @staticmethod
    def load_varients(df: pd.DataFrame, mode: str,users_stores_id:int):

        if df.empty:
            print("No varients to process.")
            return

        conn = get_connection()
        cur = conn.cursor()

        try:
            json_data = df_to_json(df)
            if mode == "Full":
                
                cur.execute("CALL dev.usp_insert_product_variants(%s,%s);", [json_data,users_stores_id])
            else:
                cur.execute("CALL dev.usp_upsert_product_variants(%s,%s);", [json_data,users_stores_id])
    
            max_updated = pd.to_datetime(df["updated_at"]).max()
            update_last_watermark(users_stores_id, "variants", mode, max_updated)
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            st.error("❌ Error loading variants")
            st.exception(e)
            st.stop()
            
        finally:
            cur.close()
            conn.close()