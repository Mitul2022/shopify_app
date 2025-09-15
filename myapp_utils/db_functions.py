import os
import time
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import requests
import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import time
from datetime import datetime, timedelta, timezone
from db_loader import Db_Upsert, get_last_watermark, update_last_watermark
from urllib.parse import urlencode
import json
#from analytics_functions import kpi_cards, build_customer_ltv
from shopify_functions import fetch_shopify_data
from db_connection import get_connection
# ---------------------------
# Read secrets / config
# ---------------------------
SHOP_DOMAIN = st.secrets.get("shopify", {}).get("store_url")
SHOP_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
SHOPIFY_STORE_URL = st.secrets.get("shopify", {}).get("store_url")
ACCESS_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
API_VERSION  = st.secrets.get("shopify", {}).get("api_version")
DEFAULT_DAYS  = st.secrets.get("shopify", {}).get("default_days")
HAS_REAL_SHOPIFY = bool(SHOP_DOMAIN and SHOP_TOKEN)

def fetch_kpi_summary(users_stores_id, start, end):
    query = "SELECT * FROM dev.fn_get_kpi_summary_by_store_id(%s,%s,%s);"
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (users_stores_id,start, end,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            # if not df.empty:
                # return df.iloc[0].to_dict()  # ✅ Return first row as dict
            # else:
                # return {}  # Empty dict if no rows
    
    return df

def get_sales_over_time(users_stores_id, start, end):
    query = "SELECT * FROM dev.fn_get_sales_over_time_by_store_id(%s,%s,%s);"
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (users_stores_id,start, end,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            # if not df.empty:
                # return df.iloc[0].to_dict()  # ✅ Return first row as dict
            # else:
                # return {}  # Empty dict if no rows
    
    return df
    
def get_table_stats(users_stores_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM dev.usp_get_table_count_by_userid(%s);", (users_stores_id,))
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        if not df.empty:
            return df.iloc[0].to_dict()  # ✅ Return first row as dict
        else:
            return {}  # Empty dict if no rows
            
def full_load(table_name):
    """Simulate full load (truncate + reload all data)."""
    time.sleep(1)  # replace with API/db logic
    return f"🔄 Full load completed for {table_name}."


def incremental_load(table_name, last_sync_time):
    """Simulate incremental load (fetch records updated since last sync)."""
    time.sleep(1)  # replace with API/db logic
    return f"🔄 Incremental load completed for {table_name} (since {last_sync_time})."


# 🔹 Main ETL Orchestrator
def data_loading(load_config, progress_placeholders):
    for t, mode in load_config.items():
        status = progress_placeholders[t]["status"]
        bar = progress_placeholders[t]["bar"]

        status.info(f"⏳ Starting {mode} load for {t}...")
        prog = bar.progress(0)

        # Simulate progress
        for pct in range(1, 101, 10):
            time.sleep(0.1)
            prog.progress(pct)

        # Run actual load
        if mode == "Full":
            msg = full_load(t)
        else:
            msg = incremental_load(t, last_sync_log[t])

        # Update status
        status.success(f"✅ {msg} at {datetime.now().strftime('%H:%M:%S')}")
        bar.empty()

        # Update last sync log
        last_sync_log[t] = datetime.now()
        
def format_sync_time1(value):
    """Handle both datetime and string values safely."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    elif isinstance(value, str):
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %I:%M %p")
            return parsed.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return value  # fallback, show as-is
    else:
        return "N/A"

def format_sync_time(dt):
    if not dt:
        return "N/A"
    return dt.strftime("%Y-%m-%d %I:%M:%S %p")  

# ---------------------------
# Caching & Sync
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60*5)  # auto-refresh every 5 min
def load_shopify_data(days: int):
    if not HAS_REAL_SHOPIFY:
        # Fallback demo data (so the app still works without creds)
        days_idx = pd.date_range(end=datetime.today(), periods=days)
        orders = np.random.randint(20, 100, size=len(days_idx))
        revenue = orders * np.random.randint(50, 150, size=len(days_idx))
        customers = np.random.randint(15, 60, size=len(days_idx))
        df_orders = pd.DataFrame({"created_at": days_idx, "total": revenue})
        df_top = pd.DataFrame({
            "product": ["Product A","Product B","Product C","Product D","Product E"],
            "units": np.random.randint(50,200,5),
            "revenue": np.random.randint(1000,5000,5)
        })
        df_customers = pd.DataFrame({
            "created_at": days_idx,
            "name": [f"Customer {i}" for i in range(len(days_idx))],
            "orders": np.random.randint(1, 5, len(days_idx)),
            "total_spent": np.random.randint(50,500, len(days_idx))
        })
        return df_orders, df_top, df_customers

    # Real Shopify
    #df_cust   = fetch_customers(days)
    #df_orders = fetch_orders(days)
    #df_top    = fetch_top_products(days)
    
    # Real Shopify
    df_cust   = fetch_shopify_data('customers',days)
    df_orders = fetch_shopify_data('orders',days)
    df_top    = fetch_shopify_data('products',days)
    
    
    return df_orders, df_top, df_cust

def trigger_manual_sync(days: int):
    # Bust cache and reload fresh
    load_shopify_data.clear()
    return load_shopify_data(days)


def get_data_db(table_name):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"Select * from dev.{table_name}")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=colnames)
        
def get_data_db_parameters(table_name,date_col,from_date,to_date,users_stores_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"Select * from dev.{table_name} where cast({date_col} as date) between cast('{from_date}' as date) and cast('{to_date}' as date) and users_stores_id = {users_stores_id};")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=colnames)