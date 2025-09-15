import requests
import pandas as pd
from urllib.parse import urlencode
import streamlit as st
from db_loader import Db_Upsert, get_last_watermark, update_last_watermark
from db_connection import get_user_stores_by_store_id
from datetime import datetime, timedelta, timezone
import re
from cryptography.fernet import Fernet
# from login_page import decrypt_code,encryp_code

# SHOP_DOMAIN = st.secrets.get("shopify", {}).get("store_url")
SHOP_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
SHOPIFY_STORE_URL = st.secrets.get("shopify", {}).get("store_url")
# ACCESS_TOKEN  = st.secrets.get("shopify", {}).get("access_token")
# API_VERSION  = st.secrets.get("shopify", {}).get("api_version")
DEFAULT_DAYS  = st.secrets.get("shopify", {}).get("default_days")
ENCRYPTION_KEY = st.secrets.get("shopify", {}).get("encryption_key")
# HAS_REAL_SHOPIFY = bool(SHOP_DOMAIN and SHOP_TOKEN)
    # ACCESS_TOKEN=decrypt_code(ACCESS_pass)




def fetch_shopify_data(resource: str, users_stores_id: int, extra_params: dict = None) -> pd.DataFrame:
    """Generic function to fetch Shopify data (customers, orders, products).
    Added extra parameter for inventory items"""
  
    stores_data=get_user_stores_by_store_id(users_stores_id)
    print(stores_data)
    SHOP_DOMAIN=stores_data["shop_name"]
    api_password=stores_data["api_password"]
    
    fernet = Fernet(ENCRYPTION_KEY)
    ACCESS_TOKEN = fernet.decrypt(api_password.encode()).decode()
    print(f"DEBUG: PASSWORD {ACCESS_TOKEN}")
    API_VERSION="2025-07"
    
    #from datetime import datetime, timezone, timedelta
    watermark_resource_map = {
        "orders": "orders",
        "refunds": "refunds",
        "inventory_items": "inventory_items",
        "products": "products",
        "variants": "variants"
    }

    
    #conn = get_connection()
    base_resource = (
        "refunds" if "refunds" in resource 
        else "variants" if "variants" in resource 
        else resource.split("/")[0]
    )

    # last_watermark = get_last_watermark(resource)
    last_watermark_dt = get_last_watermark(users_stores_id,watermark_resource_map.get(base_resource, resource))
    
    # if isinstance(last_watermark, str):
        # try:
        # last_watermark = datetime.fromisoformat(last_watermark.replace("Z", "+00:00"))
        # except ValueError:
            # last_watermark = datetime.now(timezone.utc) - days

    # since = (datetime.now(timezone.utc) - days).isoformat()
    
    base_url = f"https://{SHOP_DOMAIN}.myshopify.com/admin/api/{API_VERSION}/{resource}.json"
    print("Base URL",base_url)
    

    headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    # params = {"limit": 250, "created_at_min": since}
    # params = {"created_at_min": since}
    # params = {"updated_at_min": last_watermark.isoformat(), "limit": 250, "created_at_min": since}
    # params = {"created_at_min": from_date_str, "created_at_max":to_date_str ,"limit": 250}
    
    print("User Id:",users_stores_id)
    print("Resource:",resource)
    
    # last_watermark_dt = get_last_watermark(users_stores_id, resource)
    print("DEBUG",last_watermark_dt)
    params = {"updated_at_min": last_watermark_dt.isoformat(), "limit": 250}
    
    if resource == "orders":
        params["status"] = "any"
    # else:
        # params = {"updated_at_min": last_watermark.isoformat(), "limit": 250, "created_at_min": since}
    # params = {"limit": 250}
    if extra_params:
         params.update(extra_params)

    all_data = []
    
    url = f"{base_url}?{urlencode(params)}"
    print("Final URL",url)

    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()   # ✅ must be resp.json()
        # all_data = pd.json_normalize(data.get(resource, []))  # generic flatten
       
        num_rows = len(data.get("orders", []))

       
        # Shopify REST API uses the resource name as key
        # items = data.get(resource, [])
        key = resource.split("/")[-1] if "/" in resource else resource
        items = data.get(key, [])
        all_data.extend(items)

        # handle pagination
        link = resp.headers.get("Link")
        if link:
            match = re.search(r'<([^>]+)>; rel="next"', link)
            url = match.group(1) if match else None        
        else:
            url = None
            
        # print(data)
        # print(all_data)
        # print(f"Fetched {len(all_data)} items for {resource}")
        

    return pd.DataFrame(all_data)

# def fetch_shopify_data(resource: str, from_date : str, to_date : str, users_stores_id: int, extra_params: dict = None) -> pd.DataFrame:
    """Generic function to fetch Shopify data (customers, orders, products).
    Added extra parameter for inventory items"""
  
    # stores_data=get_user_stores_by_store_id(users_stores_id)
    # print(stores_data)
    # SHOP_DOMAIN=stores_data["shop_name"]
    # api_password=stores_data["api_password"]
    
    # fernet = Fernet(ENCRYPTION_KEY)
    # ACCESS_TOKEN = fernet.decrypt(api_password.encode()).decode()
    # print(f"DEBUG: PASSWORD {ACCESS_TOKEN}")
    # API_VERSION="2025-07"
    
    # from datetime import datetime, timezone, timedelta
    # watermark_resource_map = {
        # "orders": "orders",
        # "refunds": "refunds",
        # "inventory_items": "inventory_items",
        # "products": "products",
        # "variants": "variants"
    # }

    
    #conn = get_connection()
    # base_resource = (
        # "refunds" if "refunds" in resource 
        # else "variants" if "variants" in resource 
        # else resource.split("/")[0]
    # )

    # last_watermark = get_last_watermark(resource)
    # last_watermark = get_last_watermark(watermark_resource_map.get(base_resource, resource))
    
    # if isinstance(last_watermark, str):
        # try:
        # last_watermark = datetime.fromisoformat(last_watermark.replace("Z", "+00:00"))
        # except ValueError:
            # last_watermark = datetime.now(timezone.utc) - days

    # since = (datetime.now(timezone.utc) - days).isoformat()
    # print("SINCE:",since)
    
    # base_url = f"https://{SHOP_DOMAIN}.myshopify.com/admin/api/{API_VERSION}/{resource}.json"
    # print("Base URL",base_url)
    
    # min_date=str(from_date)+"T00:00:00-00:00"
    # max_date=str(to_date)+"T23:59:59-00:00"
    
    # from_date_str = from_date.isoformat() + "T00:00:00Z"
    # to_date_str = to_date.isoformat() + "T23:59:59Z"

    # headers = {"X-Shopify-Access-Token": ACCESS_TOKEN}
    # params = {"limit": 250, "created_at_min": since}
    # params = {"created_at_min": since}
    # params = {"updated_at_min": last_watermark.isoformat(), "limit": 250, "created_at_min": since}
    # params = {"created_at_min": from_date_str, "created_at_max":to_date_str ,"limit": 250}
    # params = {
        # "created_at_min": f"{from_date}T00:00:00+00:00",
        # "created_at_max": f"{to_date}T23:59:59+00:00",
        # "limit": 250
    # }
    
    # last_watermark_dt = get_last_watermark(users_stores_id, resource)
    # params = {"updated_at_min": last_watermark_dt.isoformat(), "limit": 250}
    
    # if resource == "orders":
        # params["status"] = "any"
    # else:
        # params = {"updated_at_min": last_watermark.isoformat(), "limit": 250, "created_at_min": since}
    # params = {"limit": 250}
    # if extra_params:
         # params.update(extra_params)

    # all_data = []
    
    # url = f"{base_url}?{urlencode(params)}"
    # print("Final URL",url)

    # while url:
        # resp = requests.get(url, headers=headers)
        # resp.raise_for_status()
        # data = resp.json()   # ✅ must be resp.json()
        # all_data = pd.json_normalize(data.get(resource, []))  # generic flatten
       
        # num_rows = len(data.get("orders", []))

       
        # Shopify REST API uses the resource name as key
        # items = data.get(resource, [])
        # key = resource.split("/")[-1] if "/" in resource else resource
        # items = data.get(key, [])
        # all_data.extend(items)

        # handle pagination
        # link = resp.headers.get("Link")
        # if link:
            # match = re.search(r'<([^>]+)>; rel="next"', link)
            # url = match.group(1) if match else None        
        # else:
            # url = None
            
        # print(data)
        # print(all_data)
        # print(f"Fetched {len(all_data)} items for {resource}")
        

    # return pd.DataFrame(all_data)
