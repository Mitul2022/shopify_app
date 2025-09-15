import os
import time
import pyotp
import qrcode
import io
from cryptography.fernet import Fernet
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import requests
import pandas as pd
import numpy as np
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import time
from datetime import datetime, timedelta, timezone,date
from urllib.parse import urlencode
import json
from streamlit_extras.stylable_container import stylable_container
from login_page import login_connections
from db_functions import get_data_db_parameters, get_data_db,full_load, incremental_load, data_loading, format_sync_time, load_shopify_data, trigger_manual_sync, get_table_stats, fetch_kpi_summary, get_sales_over_time
from db_connection import get_jsonb_data, get_connection, run_query, get_user, get_user_stores, get_store_data, get_user_stores_by_store_id
from db_loader import Db_Upsert, get_last_watermark, update_last_watermark
from other_functions import time_ago, process_table,filter_days_difference
from analytics_functions import creating_revenue, order_products_barchart, kpi_cards, build_customer_ltv
from shopify_functions import fetch_shopify_data
from datefilter1 import get_date_range

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(page_title="Shopify Connector App", layout="wide")

if "show_logout_dialog" not in st.session_state:
    st.session_state.show_logout_dialog = False

# ---------------------------
# login Authentication
# ---------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None
if "user" not in st.session_state:
    st.session_state.user = None
if "show_otp" not in st.session_state:
    st.session_state.show_otp = False
if "email" not in st.session_state:
    st.session_state.email=None
if "str_id" not in st.session_state:
    st.session_state.str_id=None    
if "str_id_ana" not in st.session_state:
    st.session_state.str_id_ana=None    
if "start_date" not in st.session_state:
    st.session_state.start_date=None    
if "end_date" not in st.session_state:
    st.session_state.end_date=None    

if "sh_name" not in st.session_state:
    st.session_state.sh_name = ""
if "api_pwd" not in st.session_state:
    st.session_state.api_pwd = ""
if "api_k" not in st.session_state:
    st.session_state.api_k = ""

st.markdown("<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap' rel='stylesheet'>",unsafe_allow_html=True,)



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
ENCRYPTION_KEY = st.secrets.get("shopify", {}).get("encryption_key")
 

# ---------------------------
# login Page
# ---------------------------
if not st.session_state.authenticated:
    st.markdown("""<style>.block-container {padding-top: 2rem;padding-bottom: 5rem;}div[data-testid="stTabs"] {padding-top: 0rem;margin-top: 0rem;}</style>""",
    unsafe_allow_html=True)


    st.markdown("""<style>body {background-color: #f0f2f5; font-size:18px  /* Light grey modern background */}
    .stTabs [role="tablist"] {justify-content: center;}</style>""",
        unsafe_allow_html=True
    )
    st.markdown("""
    <div style="
        text-align:center; 
        padding:10px; 
        background: #00809D; 
        border-radius:12px; 
        color:#FBFBFF;">
        <h3>Shopify Connector App</h3>
    </div>
    """, unsafe_allow_html=True)
    tabs = st.tabs([":key: Login", ":closed_lock_with_key: Sign Up"])

    with tabs[0]:
        with stylable_container(
            key="login_container",
            css_styles="""{background: #ffffff; /* White for strong contrast */border-radius: 15px;padding: 2rem;max-width: 580px;margin: 3rem auto;
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); /* Stronger shadow */transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
            st.markdown("<h3 style='text-align:center;'>Login to Your Account</h3>", unsafe_allow_html=True)
            email= st.text_input("Email",placeholder="Enter your email")
            # username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")

            if st.button(":key: Login", width='stretch'):
                if not email or not password:
                    st.warning("Please enter both email and password.")
                    st.stop()

                user = get_user(email)
                #print("DEBUG:",user)
                user_n=user["username"]
                # if not user_n:
                    # st.error("Invalid Username")
                    # st.stop()

                db_ps = user["password_hash"]
                #print("DEBUG PASSWORD:",db_ps)
                pwd_ok = False
                if db_ps:
                    if login_connections.check_password(password, db_ps):
                        pwd_ok = True
                        #print("DEBUG PWD_OK:",pwd_ok)
                if not pwd_ok:
                    st.error("Invalid Password")
                    st.stop()

                eml=user["email"]
                if not eml:
                    st.error("Invalid email")
                    st.stop()
                    
                # ---- OTP Step ----
                st.session_state["awaiting_otp"] = True
                st.session_state["auth_user"] = user
                st.success("✅ Password correct.")
                #st.info("Please enter OTP from Google Authenticator App.")
                #st.write(user)
                
                #st.session_state.authenticated = True
                #st.session_state.show_otp = True
                st.session_state.user = user
                st.session_state.username = user_n
                #st.rerun()                
                # OTP Step
            
            if st.session_state.get("awaiting_otp"):
                otp = st.text_input("Enter 6-digit OTP from Google Authenticator App", max_chars=6, key="otp_input")
                #print("otp new : ",otp)
                if st.button("Verify OTP", key="btn_verify_otp"):
                    user = st.session_state["auth_user"]
                    if not user["totp_secret"]:
                        st.error("❌ No TOTP secret found. Please re-register.")
                        st.stop()

                    totp = pyotp.TOTP(user["totp_secret"])
                    if totp.verify(otp):
                        st.success("🎉 Login successful!")
                        st.session_state["authenticated"] = True
                        st.session_state["awaiting_otp"] = False
                        st.session_state["page"] = "welcome"
                        st.rerun()
                        #home_page()
                    else:
                        st.error("❌ Invalid OTP")
                    
                    
                    
    # ------------------- SIGN UP TAB -------------------
    with tabs[1]:
        with stylable_container(
            key="signup_container",
            css_styles="""{background: #ffffff; /* White for strong contrast */border-radius: 15px;padding: 2rem;max-width: 580px;margin: 3rem auto;
                box-shadow: 0 8px 24px rgba(0,0,0,0.4); /* Stronger shadow */transition: transform 0.2s ease, box-shadow 0.2s ease;}
                .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
            st.markdown("<h3 style='text-align:center;'>Create a New Account</h3>", unsafe_allow_html=True)
            email= st.text_input("Email",placeholder="email")
            username = st.text_input("Username",placeholder="username")
            password = st.text_input("Password", type="password",placeholder="password")
            confirmed_pwd = st.text_input("Confirm Password", type="password",placeholder="confirm password")

            if st.button("Create Account", width='stretch'):
                if not (email and username and password and confirmed_pwd):
                    st.warning("Please fill out all details")
                    st.stop()

                if password != confirmed_pwd:
                    st.warning("Passwords do not match")
                    st.stop()

                exists = run_query("SELECT email FROM users WHERE email = %s;", (email,), fetch=True)
                if exists:
                    st.error("Email already exists. Either LogIn or user another one")
                    st.stop()

                sign_hashed = login_connections.hash_password(password)
                # print(f"DEBUG:{sign_hashed}")
                secret = pyotp.random_base32()
                try:
                    run_query(
                        "INSERT INTO users(email,username,password_hash,totp_secret) Values(%s,%s,%s,%s);",
                        (email,username, sign_hashed,secret)
                    )
                except Exception as e:
                    st.error(f"Error In Inserting In Database {e}")
                    st.stop()

                totp = pyotp.TOTP(secret)
                uri = totp.provisioning_uri(name=username, issuer_name="Shopify App")
                qr = qrcode.make(uri)
                buf = io.BytesIO()
                qr.save(buf, format="PNG")

                st.success("✅ Account created successfully!")
                st.info("Scan the below QR code using Google Authenticator App to active 2-Factor Authentication")
                st.info("Once 2-Factor Authentication is configured you can Login with your email.")
                st.image(buf.getvalue())
                user = get_user(email)
                #st.session_state.user = user
                #st.session_state.username = username
                #st.session_state.email = email
                
                #with st.spinner("Redirecting to Home..."):
                #    time.sleep(7)
                #st.session_state.authenticated = True
                #st.rerun()

# ---------------------------
# Sidebar (buttons with icons)
# ---------------------------
else:
    with st.sidebar:
        username=st.session_state.username
        st.info(f"**Welcome {username}!!!** ")
        # st.markdown("""<div style="text-align:center;background-color: #F3F4F6;padding: 4px;border-radius: 10px;box-shadow: 0px 4px 10px rgba(0,0,0,0.05);width: 100%">
        # <h4 style="font-family:'Poppins',sans-serif;font-size: 18px;font-weight: bold;color: #000;">Navigation Panel</h4></div>""",unsafe_allow_html=True)
        page = option_menu(
            menu_title= None,
            options=["Home","Add Stores", "Data Loading", "Active Stores", "Analytics"],
            icons=["house","shop", "database", "link", "bar-chart-line"],
            # options=["Home","Add Stores", "Data Loading", "Active Stores", "Analytics", "New Analytics"],
            # icons=["house","shop", "database", "link", "bar-chart-line",  "pie-chart"],
            # options=["Home","Add Stores", "Data Loading", "Connected Stores", "Sync Jobs", "Settings", "Analytics"],
            # icons=["house", "building-up","database", "shop", "clock-history", "gear", "bar-chart-line"],
            default_index=0,
            styles={
                "container": {"padding":"10px","background-color":"#F3F4F6"},
                "icon": {"color": "#000", "font-size": "20px","font-family":"sans-serif"},
                "nav-link": {"font-size":"16px","font-family":"'Poppins', sans-serif","margin":"5px","border-radius":"10px","padding":"10px","color":"#000"},
                "nav-link-selected": {"background-color":"#00809D","color":"#FFFFFF","font-weight":"bold","font-family": "'Poppins', sans-serif"},
            }
        )
        
        st.markdown("---")
        if st.sidebar.button("🔒 Logout ", key="btn_logout"):
            login_connections.logout_func()

    # Ensure we have data loaded
    # df_orders, df_top, df_cust = load_shopify_data(days_window)
    # st.session_state["last_sync"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
# ---------------------------
# PAGE: Home
# ---------------------------
    if page == "Home":
        st.markdown("""<div style="text-align:center; padding:20px; background: #00809D; border-radius:12px; color:white;">
        <h1>🛍️ Shopify Connector App</h1><p style="font-size:18px;">Generate <b>accurate insights</b> and <b>beautiful visuals</b> in minutes!</p></div>""", 
        unsafe_allow_html=True)
        
        #st.subheader(f":tada: Hey there! Wellcome to our Shopify App!! :tada:")
        #st.write("Our goal is to **streamline your workflow** and make data handling effortless! 🚀")
        st.write("")
        st.write("👉 To get started, follow these steps:")
        s = st.expander("📋 **Quick Start Instructions**")
        with s:
            st.markdown("""
            1. **Navigation Menu**:  
               - You will find **Add Stores**, **Data Loading**, **Active Stores** and **Analytics**.  
               
            2. **Add Stores**:  
                - You will need to add your store first under **Add Stores** page, to access rest of the features and pages.  
                 
            3. **Data Loading**:  
               - Click on **Data Loading** to start data ingestion from Shopify API.  
               - Select:
                 - ✅ Desired Store
                 - ✅ Dataset to load
                 - ✅ Load type (**Full** or **Incremental**)
                     - Full: It will remove the exisiting data and re-load data from Shopify store. 
                     - Incremental: It will load only newly inserted/updated from Shopify store.
                             
            4. **Analytics**:
               - Once the data is ingested, you can explore **insights** and **visualizations** on this page. 🎨 
               
            5. **Log Out**:  
               - Use the **Log Out** button to securely end your session.  
               
            """)
            
    # ---------------------------
    # PAGE: Add Stores
    # ---------------------------
    elif page == "Add Stores":
        
        # st.markdown("""
        # <div style="text-align:center; padding:7px; background:linear-gradient(135deg, #5CA4A9, #38A3A5, #57CC99); border-radius:12px; color:white;">
            # <h2>➕ Connect a New Store</h2>
            # <p>Add and authenticate your Shopify store securely.</p>
        # </div>
        # """, unsafe_allow_html=True)
        
        with stylable_container(key="header",css_styles="""{background: #00809D; border-radius: 12px; padding: 2rem; color:white; text-align:center
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
                _, c1,c2 = st.columns([1,2,1])
                with c1:
                    st.title("Connect a New Store")
        
        
        with stylable_container(
            key="add_store_container",
            css_styles="""{background: #ffffff; border-radius: 15px; padding: 2rem;max-width: 580px;margin: 2rem auto;
            box-shadow: 0 8px 24px rgba(0,0,0,0.09); box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
            #st.markdown("<h4 style='text-align:center;'>Add Store</h4>", unsafe_allow_html=True)
            
            shop_name = st.text_input("Shop_name", placeholder="Enter your shopname",key="sh_name")
            api_key = st.text_input("API Key",placeholder="Enter your api",type="password",key="api_k")
            api_password = st.text_input("API Password",placeholder="Enter your token",type="password",key="api_pwd")
            
            

            if st.button("🛠 Test Connection & Add Store", width='stretch'):
                if not (shop_name and api_key and api_password):
                    st.warning("Please fill out all details")
                    st.stop()
                user1=st.session_state.user
                print(user1)
                user_id=user1["user_id"]
                
                store_name=get_user_stores(user_id)
                if store_name:
                    for store in store_name:
                        if store["shop_name"]==shop_name:
                            sh_name= store["shop_name"]
                            st.info(f"{sh_name} already exists")
                            st.stop()
                
                
                with st.spinner("Testing Shopify Connection..."):
                    time.sleep(1)
                    if login_connections.test_connection(shop_name, api_key, api_password):
                        # api_pwd_hashed = login_connections.hash_password(api_password)
                        # api_key_hashed = login_connections.hash_password(api_key)
                        # key = Fernet.generate_key()
                        
                        # api_pwd_hashed = encryp_code(cipher,api_password)
                        # api_key_hashed = encryp_code(cipher,api_key)
                        
                        fernet = Fernet(ENCRYPTION_KEY)
                        enc_api_key = fernet.encrypt(api_key.encode()).decode()
                        enc_api_password = fernet.encrypt(api_password.encode()).decode()
 
                        
                        try:
                            # new_id = run_query(
                                # """
                                # INSERT INTO dev.stores (shop_name, api_key, api_password, is_active)
                                # VALUES (%s, %s, %s, %s)
                                # RETURNING store_id;
                                # """,
                                # (shop_name, enc_api_password, enc_api_key,True)
                            # )
                            # print("DEBUG",new_id)
                            # print("DEBUG_1",user_id)
                            # run_query(
                                # """
                                # INSERT INTO dev.users_stores_mapping (store_id, user_id)
                                # VALUES (%s, %s)
                                # """,
                                # (new_id, user_id)
                            # )
                            Db_Upsert.add_store(shop_name, enc_api_password, enc_api_key, user_id)
                        except Exception as e:
                           st.error(f"Could Not Insert Into Database! {e}")
                           st.stop()
                        st.success(f"✅ Account {shop_name} added successfully!")
                    else:
                        st.write(f"❌ Connection to Shopify failed. Check your credentials.")
                        st.stop()
                        
    # ---------------------------
    # PAGE: Dashboard
    # ---------------------------
    # elif page == "Dashboard":
        # st.title("Dashboard")
        # total_orders = 500 #len(df_orders) if not df_orders.empty else 0
        # total_revenue = 10000 #float(df_orders["total"].sum()) if "total" in df_orders else 0.0
        # active_customers = 50 #df_cust["email"].nunique() if "email" in df_cust else df_cust.shape[0]
        # last_sync = time_ago(datetime.now())
        # kpi_cards({
            # "Total Orders": f"{total_orders:,}",
            # "Revenue": f"${total_revenue:,.0f}",
            # "Active Customers": f"{active_customers:,}",
            # "Last Sync": last_sync,
        # })
        # st.markdown("---")

        # Orders over time (daily)
        #if not df_orders.empty:
        #    df_daily = df_orders.copy()
        #    df_daily["date"] = pd.to_datetime(df_daily["created_at"]).dt.date
        #    daily = df_daily.groupby("date", as_index=False)["total"].sum()
        #    fig = px.line(daily, x="date", y="total", markers=True, title="Revenue by Day")
        #    st.plotly_chart(fig, width='stretch')
        #else:
        #    st.info("No orders found for selected window.")

    # ---------------------------
    # PAGE: Connected Stores (placeholder single store)
    # ---------------------------
    # elif page == "Connected Stores":
        # user1=st.session_state.user
        # user_id=user1["user_id"]
                
        # stores_data=get_user_stores(user_id)
        # if stores_data:
            # st.title("Connected Stores")
            # st.markdown("""<div style="text-align:center; padding:10px; background: #00809D; border-radius:12px; color:#FBFBFF;"><h3>All Shopify Stores</h3></div>
            # """, unsafe_allow_html=True)
            # st.title("All Shopify Stores")
            # users=st.session_state.user
            # u_id=users["user_id"]

            # st.markdown(
                # f"""
                # <div style="padding:20px;border-radius:15px;box-shadow:0 4px 8px rgba(0,0,0,0.1);margin-bottom:20px;">
                    # <h4>{SHOP_DOMAIN or 'Demo Store'}</h4>
                    # <p>Status: <span style="color:{'green' if HAS_REAL_SHOPIFY else 'orange'}">
                    # {'Active' if HAS_REAL_SHOPIFY else 'Demo Mode'}</span></p>
                    # <p>Last Sync: {st.session_state.get('last_sync','auto (≤5 min ago)')}</p>
                # </div>
                # """,
                # unsafe_allow_html=True
            # )
            
            # store_data=get_user_stores(u_id)
            
            # for idx, store in enumerate(store_data):
                # with store_cols[idx % 2]:
                # st.markdown(
                    # f"""    
                    # <div style="padding:20px;background-color:#FFFFF; border-radius:15px; box-shadow:0 4px 8px rgba(0,0,0,0.1); margin-bottom:20px;">
                        # <h4>{store['shop_name']}</h4>
                        # <p>Status: <span style="color:{'green' if store['is_active'] else 'red'}">{"Active" if store['is_active'] else "Inactive"}</span></p>
     
                    # </div>
                    # """,
                    # unsafe_allow_html=True
                # )
        # else:
            # st.markdown("""<div style="background-color:#FFF4E5;border:2px solid #FFA726;border-radius:12px;padding:20px;text-align:center;
            # box-shadow:0px 4px 12px rgba(0,0,0,0.1);font-family: 'Poppins', sans-serif;">
            # <h3>⚠️ No Stores Found</h3><p>You need to add a store before continuing.</p>""",unsafe_allow_html=True)
            
    elif page == "Active Stores":
        
        user = st.session_state.user
        user_id = user["user_id"]

        #st.title("All Shopify Stores")
        
        with stylable_container(key="header",css_styles="""{background: #00809D; border-radius: 12px; padding: 2rem; color:white; text-align:center
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
                _, c1,c2 = st.columns([1,2,1])
                with c1:
                    st.title("Active Shopify Stores")

        stores_data = get_user_stores(user_id)

        if stores_data:
            for idx, store in enumerate(stores_data):
                store_id = store['users_stores_id']
                stats = get_table_stats(store_id)
                store_number = idx + 1

                # One container per store
                with st.container():
                    st.markdown(f"""
                        <div style="padding:20px; margin-bottom:10px; border-radius:15px; background-color:#f4f4f4;">
                            <h3>Store-{store_number}: {store['shop_name']}</h3>
                            <p>Status: <span style="color:{'green' if store['is_active'] else 'red'};">
                                {'🟢 Active' if store['is_active'] else '🔴 Inactive'}</span></p>
                            <!--<p>Domain: <strong>{store.get('shop_domain', 'N/A')}</strong></p>-->
                            <!--<p>Last Sync: {store.get('last_sync', 'Unknown')}</p>-->
                        </div>
                    """, unsafe_allow_html=True)

                    if stats:
                        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
                        with kpi1:
                            st.markdown(f"""
                                <div style="padding:15px; background-color:#fff; border-radius:10px; text-align:center; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
                                    <h5 style="margin:0;">🛒 Sync Orders</h5>
                                    <h3 style="color:#4CAF50; margin:0;">{stats['total_orders']}</h3>
                                </div>
                            """, unsafe_allow_html=True)
                        with kpi2:
                            st.markdown(f"""
                                <div style="padding:15px; background-color:#fff; border-radius:10px; text-align:center; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
                                    <h5 style="margin:0;">👥 Sync Customers</h5>
                                    <h3 style="color:#2196F3; margin:0;">{stats['total_customers']}</h3>
                                </div>
                            """, unsafe_allow_html=True)
                        with kpi3:
                            st.markdown(f"""
                                <div style="padding:15px; background-color:#fff; border-radius:10px; text-align:center; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
                                    <h5 style="margin:0;">📦 Sync Products</h5>
                                    <h3 style="color:#9C27B0; margin:0;">{stats['total_products']}</h3>
                                </div>
                            """, unsafe_allow_html=True)
                        with kpi4:
                            st.markdown(f"""
                                <div style="padding:15px; background-color:#fff; border-radius:10px; text-align:center; box-shadow:0 2px 5px rgba(0,0,0,0.05);">
                                    <h5 style="margin:0;">💰 Sync Refunds</h5>
                                    <h3 style="color:#FF9800; margin:0;">{stats['total_refunds']}</h3>
                                </div>
                            """, unsafe_allow_html=True)

                    else:
                        st.info("No table stats available for this store.")
                    
                    st.markdown("---")  # Divider between stores
        else:
            st.warning("No Stores Found!")    

    # ---------------------------
    # PAGE: Sync Jobs (simple view)
    # ---------------------------
    # elif page == "Sync Jobs":
        # st.title("Sync Jobs")
        # st.info("Showing recent sync activity. (For production, persist jobs in a DB/queue.)")
        # st.table(pd.DataFrame([
            # {"Job": "Manual Sync", "Status": "Success", "Run At": st.session_state.get("last_sync","N/A")},
            # {"Job": "Auto Cache Refresh (5m)", "Status": "Scheduled", "Run At": "every 5 minutes"},
        # ]))

    # ---------------------------
    # PAGE: Data Loading
    # ---------------------------
    elif page == "Data Loading":
        user1=st.session_state.user
        user_id=user1["user_id"]
                
        stores_data=get_user_stores(user_id)
        if stores_data:
            last_sync_log = {
            "Orders": datetime(2025, 8, 28, 12, 30),     # datetime
            "Customers": "2025-08-28 10:15 AM",          # string
            "Products": datetime(2025, 8, 27, 21, 45),   # datetime
            "Transactions": "2025-08-26 11:00 AM",       # string
            "Fulfillments": datetime(2025, 8, 25, 14, 20)
            }

            tables = list(last_sync_log.keys())
            
            # Example Shopify tables
            all_tables = [
                {"name": "Orders", "db_name":"orders"},
                {"name": "Customers", "db_name":"customers"},
                {"name": "Products", "db_name":"products"},
                {"name": "Inventory Items", "db_name":"inventory_items"},
                {"name": "Refunds", "db_name":"refunds"},
                {"name": "Product Varient", "db_name":"variants"}
            ]
            

            # st.set_page_config(layout="wide")
            # c1,c2 = st.columns([2,1])
            # with c1:
                
            with stylable_container(key="header",css_styles="""{background: #00809D; border-radius: 12px; padding: 2rem; color:white; text-align:center
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
                _, c1,c2 = st.columns([1,2,1])
                with c1:
                    st.title("Shopify Data Ingestion")
            
            #st.title("📊 Shopify Data Load Dashboard")
            st.markdown("""
            Select which **tables** you want to process, and for each, choose **Full Load** or **Incremental Load**. By default, all tables will be selected.  
            
            """)
            
            # with c2:
                # filter_options = [
                # "Today", "Yesterday", "Last 7 days", "Last 30 days",
                # "Last 60 days", "Last 90 days", "Last 365 days",
                # "Last week", "Last month", "Last quarter",
                # "Week to date", "Last year", "Month to date",
                # "Quarter to date", "Year to date", "Custom Dates"
                # ] 
                
                # choice = st.selectbox("📅 Select Date Range", filter_options, index=2) 
                # if choice == "Custom Dates":
                    # start = st.date_input("Start Date", date.today() - timedelta(days=7))
                    # end = st.date_input("End Date", date.today())
                # else:
                    # start, end = get_date_range(choice)
            # st.success(f"✅ Filter applied: **{choice}** → {start} to {end}")
            # day_diff=filter_days_difference(start,end)
            
            # print("DAY DIFF",day_diff)
            
            
            users=st.session_state.user
            u_id=users["user_id"]
            print("debug- userid:",u_id)
            store_data=get_user_stores(u_id)
            
            store_names = [store["shop_name"] for store in store_data]
            
            selected_store = st.selectbox("**Selected Store**", options=store_names)
            
            for store in store_data:
                if store["shop_name"]==selected_store:
                    str_id=store["users_stores_id"]
                    print(f"ID {str_id}")
                    st.session_state.str_id=str_id
                
            
            # Store configs
            load_config = {}
            progress_placeholders = {}

            # Layout: 2 cards per row
            cols = st.columns(2)

            # Build cards for selected tables
            for i, table in enumerate(all_tables):
                col = cols[i % 2]
                table_name = table["db_name"]  # ✅ Extract name
                last_sync_value =  get_last_watermark(str_id, table_name)  #last_sync_log.get(table_name, table["last_watermark"])
                
                last_sync_display = "N/A" if str(last_sync_value) == "1900-01-01 05:21:10+05:21:10" else format_sync_time(last_sync_value)

                with col:
                    with st.container():
                        st.markdown(
                            f"""
                            <div style="padding:20px; border-radius:15px;
                                        box-shadow:0 4px 8px rgba(0,0,0,0.3);
                                        margin-bottom:20px; background-color:white;">
                                <h3 style="margin:0;">{table_name}</h3>
                                <p style="color:gray; margin:0;">
                                    Last Sync: {last_sync_display}
                                </p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        # ✅ Checkbox to include/exclude this table
                        include_table = st.checkbox(f"Include {table_name}", value=True, key=f"{table_name}_chk")

                        if include_table:
                            # ✅ Toggle for Full / Incremental
                            mode = st.toggle(f"{table_name} → Full Load?", key=f"{table_name}_toggle")
                            load_config[table_name] = "Full" if mode else "Incremental"

                            # ✅ Placeholder for progress tracking
                            progress_placeholders[table_name] = {
                                "bar": st.empty(),
                                "status": st.empty()
                            }
                        
              
                
            # 🚀 Start Button
            st.markdown("---")
            str_id=st.session_state.str_id
            stores_data=get_user_stores_by_store_id(str_id)
            shop_domain=stores_data["shop_name"]
            if st.button("🚀 Start Data Load"):
                st.success("ETL Process Started...")
                total_count = 0
                
                
                for table, mode in load_config.items():
                    #st.write(f"⏳ Processing {table} ({mode})...") 
                    #st.info(table)
                    #st.info(mode)
                    
                    try:
                        str_id=st.session_state.str_id
                        if table == "customers":
                            
                            df_cust = fetch_shopify_data("customers",str_id) 
                            if not df_cust.empty:
                                total_count = len(df_cust)
                                process_table("Customers", mode, total_count)
                                Db_Upsert.load_customers(df_cust,mode,str_id)
                            else:
                                st.write("No new/updated customers")
                            
                        elif table == "orders":
                            df_orders = fetch_shopify_data("orders",str_id)
                            if not df_orders.empty: 
                                total_count = len(df_orders)
                                process_table("orders", mode, total_count)
                                Db_Upsert.load_orders(df_orders, mode,str_id)
                                st.session_state.orders=df_orders
                            else:
                                st.write("No new/updated orders")

                        elif table == "products":
                            df_products = fetch_shopify_data("products",str_id) 
                            if not df_products.empty:
                                total_count = len(df_products)
                                process_table("products", mode, total_count)
                                Db_Upsert.load_products(df_products, mode,str_id)
                                st.session_state.products=df_products
                            else:
                                st.write("No new/updated products")
                            
                        elif table == "refunds":
                            if "orders" in st.session_state:
                                df_orders = st.session_state.get("orders")
                            else:
                                df_orders = fetch_shopify_data("orders",str_id)
                            if not df_orders.empty:
                                order_ids = df_orders["id"]
                                
                                refund_frames = []
                                progress = st.progress(0,text=f" Refunds Processing")
                                for i, oid in enumerate(order_ids):
                                    refund_url = f"orders/{oid}/refunds"
                                    df_refunds = fetch_shopify_data(refund_url,str_id)
                                    
                                    if not df_refunds.empty:
                                        df_refunds["order_id"] = oid
                                        refund_frames.append(df_refunds)
                                    ref_result=(i + 1) / len(order_ids)
                                    progress.progress(ref_result,f"Refunds Processing")
                                df_all_refunds = pd.concat(refund_frames, ignore_index=True, copy=False) if refund_frames else pd.DataFrame()

                                total_count = len(df_all_refunds)
                                process_table("refunds", mode, total_count)
                                Db_Upsert.load_refunds(df_all_refunds, mode, str_id)
                            else:
                                st.write("No new/updated orders for refunds")
                        
                        elif table == "inventory_items":
                            if "products" in st.session_state:
                                df_products = st.session_state.get("products")
                                
                            else: 
                                df_products=fetch_shopify_data("products",str_id)
                            
                            ids = []
                            if "variants" in df_products.columns:
                                for variants_raw in df_products["variants"]:
                                    if isinstance(variants_raw, str):
                                        variants_list = ast.literal_eval(variants_raw)
                                    else:
                                        variants_list = variants_raw
                                        
                                    for v in variants_list:
                                        if isinstance(v, dict) and "inventory_item_id" in v:
                                            ids.append(v["inventory_item_id"])

                            if ids:
                                ids_join=list(set(ids))
                                ids_str = ",".join(map(str, ids))
                                df_its = fetch_shopify_data("inventory_items",str_id,extra_params={"ids": ids_str})
                                total_count = len(df_its)
                                process_table("inventory items", mode, total_count)
                                Db_Upsert.load_inventory_items(df_its, mode, str_id)
                            else:
                                st.write("No new/updated products to get inventory items")
                            
                        
                        elif table == "variants":
                            if "products" in st.session_state:
                                df_products = st.session_state.get("products")
                            else: 
                                df_products=fetch_shopify_data("products",str_id)
                            if not df_products.empty:
                                pro_ids=df_products["id"]
                                variants_frames = []
                                progress = st.progress(0, text=f" Variants Processing")
                                for i, pid in enumerate(pro_ids):
                                    refund_url = f"products/{pid}/variants"
                                    df_variants = fetch_shopify_data(refund_url,str_id)
                                    if not df_variants.empty:
                                        df_variants["product_id"] = pid
                                        variants_frames.append(df_variants)
                                    prog_result=(i + 1) / len(pro_ids)
                                    progress.progress(prog_result,f"Variants Processing")
                                df_all_variants = pd.concat(variants_frames, ignore_index=True, copy=False) if variants_frames else pd.DataFrame()
                                
                                total_count = len(df_all_variants)
                                process_table("variants", mode, total_count)
                                Db_Upsert.load_varients(df_all_variants, mode,str_id)
                            else:
                                st.write("No new/updated products to get variants")
                            
                        st.success(f"✅ {mode} load: processed {total_count} {table}.")
                        # st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Failed to load {table}: {e}")
                        st.exception(e)
        else:
            st.markdown("""<div style="background-color:#FFF4E5;border:2px solid #FFA726;border-radius:12px;padding:20px;text-align:center;
            box-shadow:0px 4px 12px rgba(0,0,0,0.1);font-family: 'Poppins', sans-serif;">
            <h3>⚠️ No Stores Found</h3><p>You need to add a store before continuing.</p>""",unsafe_allow_html=True)

        
    # ---------------------------
    # PAGE: Logs (simple)
    # ---------------------------
    # elif page == "Logs":
        # st.title("Logs")
        # st.info("Add structured logs here (API calls, rate limits).")

    # ---------------------------
    # PAGE: Settings
    # ---------------------------
    # elif page == "Settings":
        # st.title("Settings")
        # st.caption("Set your Shopify credentials in .streamlit/secrets.toml")
        # st.json({
            # "store_domain": SHOP_DOMAIN or "<not set>",
            # "api_version": API_VERSION,
            # "scopes_needed": ["read_orders","read_products","read_customers"],
            # "cache_ttl_minutes": 5
        # })

    # ---------------------------
    # PAGE: Analytics
    # ---------------------------
    elif page == "Analytics":
        user1=st.session_state.user
        user_id=user1["user_id"]
                
        stores_data=get_user_stores(user_id)
        if stores_data:
            st.markdown("""
                <style> 
                .block-container {padding-top: 1rem;padding-bottom: 5rem;padding-left: 0rem; padding-right: 0rem;}
                div[data-testid="stTabs"] {padding-top: 0rem;# margin-top: 0rem;}
                .dash-card {background: #ffffff;border: 1px solid rgba(0,0,0,0.06);border-radius: 16px;padding: 1.2rem 1.2rem 1rem 1.2rem;
                box-shadow: 0 2px 14px rgba(0,0,0,0.04);margin-bottom: 1rem;}
                .dash-title { font-size: 1.15rem; font-weight: 600; margin-bottom: 0.4rem; }
                .dash-subtle { color: #6b7280; font-size: 0.9rem; }
                </style>""",
                unsafe_allow_html=True
            )
            st.markdown("""
                <style>
                body {
                    background-color: #f0f2f5;
                    font-size: 18px;
                }
                .stTabs [role="tablist"] {
                    display: flex;
                    justify-content: center;
                }
                </style>
                """, unsafe_allow_html=True)
            # tab=st.tabs(["Analytics","Summary"])
            # with tab[0]:
            
            
            with stylable_container(key="header",css_styles="""{background: #00809D; border-radius: 12px; padding: 2rem; color:white; text-align:center
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
                _, c1,c2 = st.columns([1,2,1])
                with c1:
                    st.title("Analytics & Insights")
                with c2:
                    filter_options = [
                    "Today", "Yesterday", "Last 7 days", "Last 30 days",
                    "Last 60 days", "Last 90 days", "Last 365 days",
                    "Last week", "Last month", "Last quarter",
                    "Week to date", "Last year", "Month to date",
                    "Quarter to date", "Year to date", "Custom Dates"
                    ] 
                    
                    choice = st.selectbox("📅 Select Date Range", filter_options, index=2) 
                    if choice == "Custom Dates":
                        start = st.date_input("Start Date", date.today() - timedelta(days=7))
                        end = st.date_input("End Date", date.today())
                    else:
                        start, end = get_date_range(choice)
                    st.session_state.start_date=start
                    st.session_state.end_date=end
                    
                # st.success(f"✅ Filter applied: **{choice}** → {start} to {end}")

            
            #getting data from db
            users=st.session_state.user
            u_id=users["user_id"]
            store_data=get_user_stores(u_id)
            store_names = [store["shop_name"] for store in store_data]
            
            selected_store = st.selectbox("**Selected Store**", options=store_names)
            
            for store in store_data:
                if store["shop_name"]==selected_store:
                    str_id_ana=store["users_stores_id"]
                    print(f"ID {str_id_ana}")
                    st.session_state.str_id_ana=str_id_ana
                    
           
            #Filter Data Changes
            str_id_ana=st.session_state.str_id_ana
            #df_orders=get_data_db_parameters("orders","processed_at",start,end,str_id_ana)
            df_orders_kpi=fetch_kpi_summary(str_id_ana, start, end)
            df_orders=get_data_db_parameters("orders","processed_at",start,end,str_id_ana)
            df_products=get_data_db_parameters("products","created_at",start,end,str_id_ana)
            df_products_charts=get_data_db("products")
            
                
            if not df_orders_kpi.empty:
                # Extract values from the first row
                total_sales = df_orders_kpi.at[0, "total_sales"]
                total_orders = df_orders_kpi.at[0, "total_orders"]
                avg_order_val = df_orders_kpi.at[0, "avg_order_val"]
                refund_rate = df_orders_kpi.at[0, "refund_rate"] * 100
                total_customers = df_orders_kpi.at[0, "total_customers"]
            else:
                st.info("No record found for selected date range.")
                st.stop()
            
            quantity=get_jsonb_data("line_items","quantity","int","orders",str_id_ana,"processed_at",start,end)
            quantity=pd.to_numeric(quantity)
            product_id=get_jsonb_data("line_items","product_id","bigint","orders",str_id_ana,"processed_at",start,end)
            
            
            #df_orders["total_price"]=pd.to_numeric(df_orders["total_price"],errors="coerce")
            
            # KPIs
            # total_orders = len(df_orders) if not df_orders.empty else 0
            # total_revenue = float(df_orders["total_price"].sum()) if "total_price" in df_orders else 0.0
            # unique_customers = df_orders["email"].nunique() if "email" in df_orders else 0
            # aov = (total_revenue / total_orders) if total_orders else 0.0
            
            kpi_cards({
                "Orders": f"{total_orders:,}",
                "Revenue": f"₹{total_sales:,.0f}",
                "Customers": f"{total_customers:,}",
                "AOV": f"₹{avg_order_val:,.2f}",
                "Refund Rate": f"{refund_rate:,.2f}%"
            })
            
            # kpi_cards({
                # "Orders": f"{total_orders:,}",
                # "Revenue": f"${total_revenue:,.0f}",
                # "Customers": f"{unique_customers:,}",
                # "AOV": f"${aov:,.2f}"
            # })

            # tab=st.tabs(["Overall","Summary"])
            # with tab[0]:
            # Sales Trend
           
            if not df_orders.empty:
                df_orders["date"] = pd.to_datetime(df_orders["created_at"]).dt.date
                daily = df_orders.groupby("date", as_index=False).agg(
                    orders=("id","count"),
                    revenue=("total_price","sum")
                )
                daily["revenue"] = pd.to_numeric(daily["revenue"], errors="coerce")

                fig1 = px.line(daily, x="date", y=["orders","revenue"], markers=True, title="Orders & Revenue Over Time")
                st.plotly_chart(fig1, width='stretch')
            else:
                st.info("No orders to chart.")

            # Top Products
            # with stylable_container(
                # key="chart_container",
                # css_styles="""{background: #E6EBE0;border-radius: 5px;padding: 2rem;margin: 3rem auto;}
                # .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
            c1,c2=st.columns(2)
            with c1:
                if not df_products.empty and not df_orders.empty:
                    charts=order_products_barchart(df_products_charts)
                    fig2 = px.bar(charts, x="products", y="revenue", color="quantity",
                                  title="Top Products by Revenue (last window)")
                    st.plotly_chart(fig2, width='stretch')
                else:
                    st.info("No product line items found in this window.")

            with c2:
                if not df_products.empty and not df_orders.empty:
                    fig3 = px.pie(charts.head(8), names="products", values="revenue", title="Revenue Distribution")
                    st.plotly_chart(fig3, width='stretch')
                else:
                    st.info("No product line items found in this window.")
        
            # Simple insights
            st.markdown("---")
            st.subheader("Insights")
            if not df_products.empty:
                df_row = creating_revenue()
                top_row=df_row.sort_values("revenue",ascending=False)
                top_row=top_row.iloc[0]
                low_row=df_row.sort_values("revenue",ascending=True)
                low_row=low_row.iloc[0]
                st.success(f"• **{top_row['product_id']}** leads revenue (₹{top_row['revenue']}). Consider featuring it in campaigns.")
                st.warning(f"• **{low_row['product_id']}** downs revenue (₹{low_row['revenue']}). Improve its marketing .")
            if not df_orders.empty:
                last7 = df_orders[df_orders["created_at"] >= (datetime.now(timezone.utc) - timedelta(days=7))]
                st.info(f"• Last 7 days revenue: ₹{last7['total_price'].sum():,.0f}")

            # with tab[1]:
                # st.subheader("Summary")
                # edited_df=st.data_editor(df_orders, num_rows="dynamic", width='stretch')
                # edited_df=st.data_editor(df_orders, num_rows="dynamic", width='stretch', column_config={"total_price": st.column_config.NumberColumn("Total Price", format="$%.2f")})
                # selected_rows = edited_df.loc[edited_df["_selected_rows"]] if "_selected_rows" in edited_df.columns else edited_df
                # total_price_sum = selected_rows["total_price"].sum() if not selected_rows.empty else 0.0
                # st.metric("Total Revenue", f"${total_price_sum:,.2f}")
        else:
            st.markdown("""<div style="background-color:#FFF4E5;border:2px solid #FFA726;border-radius:12px;padding:20px;text-align:center;
            box-shadow:0px 4px 12px rgba(0,0,0,0.1);font-family: 'Poppins', sans-serif;">
            <h3>⚠️ No Stores Found</h3><p>You need to add a store before continuing.</p>""",unsafe_allow_html=True)

    elif page=="New Analytics":
        
        import streamlit as st
        import pandas as pd
        import plotly.express as px
        import numpy as np
        
        user1=st.session_state.user
        user_id=user1["user_id"]
                
        stores_data=get_user_stores(user_id)
        if stores_data:
            st.markdown("""
                <style> 
                .block-container {padding-top: 1rem;padding-bottom: 5rem;padding-left: 0rem; padding-right: 0rem;}
                div[data-testid="stTabs"] {padding-top: 0rem;# margin-top: 0rem;}
                .dash-card {background: #ffffff;border: 1px solid rgba(0,0,0,0.06);border-radius: 16px;padding: 1.2rem 1.2rem 1rem 1.2rem;
                box-shadow: 0 2px 14px rgba(0,0,0,0.04);margin-bottom: 1rem;}
                .dash-title { font-size: 1.15rem; font-weight: 600; margin-bottom: 0.4rem; }
                .dash-subtle { color: #6b7280; font-size: 0.9rem; }
                </style>""",
                unsafe_allow_html=True
            )
            st.markdown("""
                <style>
                body {
                    background-color: #f0f2f5;
                    font-size: 18px;
                }
                .stTabs [role="tablist"] {
                    display: flex;
                    justify-content: center;
                }
                </style>
                """, unsafe_allow_html=True)
            # tab=st.tabs(["Analytics","Summary"])
            # with tab[0]:
            
            
            with stylable_container(key="header",css_styles="""{background: #00809D; border-radius: 12px; padding: 2rem; color:white; text-align:center
            box-shadow: 0 8px 24px rgba(0,0,0,0.4); transition: transform 0.2s ease, box-shadow 0.2s ease;}
            .container:hover {transform: translateY(-5px);box-shadow: 0 12px 28px rgba(0,0,0,0.25);}"""):
                _, c1,c2 = st.columns([1,2,1])
                with c1:
                    st.title("Analytics & Insights")
                with c2:
                    filter_options = [
                    "Today", "Yesterday", "Last 7 days", "Last 30 days",
                    "Last 60 days", "Last 90 days", "Last 365 days",
                    "Last week", "Last month", "Last quarter",
                    "Week to date", "Last year", "Month to date",
                    "Quarter to date", "Year to date", "Custom Dates"
                    ] 
                    
                    choice = st.selectbox("📅 Select Date Range", filter_options, index=2) 
                    if choice == "Custom Dates":
                        start = st.date_input("Start Date", date.today() - timedelta(days=7))
                        end = st.date_input("End Date", date.today())
                    else:
                        start, end = get_date_range(choice)
                    st.session_state.start_date=start
                    st.session_state.end_date=end
                    
                # st.success(f"✅ Filter applied: **{choice}** → {start} to {end}")

            
            #getting data from db
            users=st.session_state.user
            u_id=users["user_id"]
            store_data=get_user_stores(u_id)
            store_names = [store["shop_name"] for store in store_data]
            
            selected_store = st.selectbox("Select Store", options=store_names)
            
            for store in store_data:
                if store["shop_name"]==selected_store:
                    users_stores_id=store["users_stores_id"]
                    #print(f"ID {users_stores_id}")
                    #st.session_state.str_id_ana=str_id_ana

        # -----------------------------
        # Page config
        # -----------------------------
        # st.set_page_config(
            # page_title="Shopify Dashboard",
            # page_icon="🛍️",
            # layout="wide"
        # )

        #st.title("🛍️ Shopify Store Dashboard")

        # -----------------------------
        # Filters
        # -----------------------------
        # with st.sidebar:
            # st.header("Filters")
            # date_range = st.date_input("Date Range")
            # region = st.selectbox("Region", ["All", "North", "South", "East", "West"])
            # channel = st.multiselect("Sales Channel", ["Online", "POS", "Facebook", "Instagram"])
            # st.markdown("---")
            # st.write("Use filters to customize insights")

        # -----------------------------
        # KPI Cards
        # -----------------------------
        
        
        try:
            df = fetch_kpi_summary(users_stores_id, start, end)
            #print(df)
            #str_id_ana=store["users_stores_id"]
            
            if df.empty:
                st.warning("No data found for this Store ID.")
            else:
                #st.subheader(f"KPI Summary for Store ID {store_id}")
                #st.dataframe(df)
                
                # Extract values from the first row
                total_sales = df.at[0, "total_sales"]
                total_orders = df.at[0, "total_orders"]
                avg_order_val = df.at[0, "avg_order_val"]
                refund_rate = df.at[0, "refund_rate"]
                total_customers = df.at[0, "total_customers"]

                # Display metrics in columns
                col1, col2, col3, col4, col5 = st.columns(5)

                col1.metric("**Total Sales**", f"₹{total_sales:,.2f}")
                col2.metric("**Orders**", int(total_orders))
                col3.metric("**Average Order Value**", f"₹{avg_order_val:,.2f}")
                col4.metric("**Customers**", int(total_customers))
                col5.metric("**Refund Rate**", f"{float(refund_rate)*100:.2f}%")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")

        st.markdown("---")
        

        # -----------------------------
        # Tabs for sections
        # -----------------------------
        # tab_sales, tab_products, tab_customers, tab_ops = st.tabs(
            # ["📈 Sales Insights", "📦 Product Performance", "👥 Customers", "🚚 Operations"]
        # )
        
        tab_sales, = st.tabs(
            ["📈 Sales Insights"]
        )
        

        # -----------------------------
        # SALES TAB
        # -----------------------------
        with tab_sales:
            st.subheader("Sales Overview")
            #col1, col2 = st.columns([2, 1])

            # Sample data
            # dates = pd.date_range("2025-09-01", periods=10)
            # sales = np.random.randint(8000, 15000, size=10)
            # df_sales = pd.DataFrame({"Date": dates, "Sales": sales})
            
            df_sales = get_sales_over_time(users_stores_id, start, end)
            df_sales["sales_date"] = pd.to_datetime(df_sales["sales_date"])
            df_sales.rename(columns={
                "sales_date": "Date",
                "total_sales": "Sales"
            }, inplace=True)
            df_sales["Date"] = df_sales["Date"].dt.date

            fig_line = px.line(df_sales, x="Date", y="Sales", title="Total Sales Over Time")
            st.plotly_chart(fig_line, use_container_width=True, key="sales_over_time")
            #col1.plotly_chart(fig_line, use_container_width=True, key="sales_over_time")

            # channels = ["Online", "POS", "Facebook", "Instagram"]
            # vals = [50000, 20000, 15000, 10000]
            # fig_pie = px.pie(names=channels, values=vals, title="Sales by Channel")
            # col2.plotly_chart(fig_pie, use_container_width=True, key="sales_channel")

        # -----------------------------
        # PRODUCTS TAB
        # -----------------------------
        # with tab_products:
            # st.subheader("Top Products & Inventory")
            # col1, col2 = st.columns([2, 1])

            # top_products = pd.DataFrame({
                # "Product": [f"Product {i}" for i in range(1, 8)],
                # "Revenue": np.random.randint(5000, 30000, 7),
                # "Stock": np.random.randint(5, 50, 7)
            # })

            # fig_bar = px.bar(top_products, x="Revenue", y="Product",
                             # orientation="h", title="Top Products by Revenue")
            # col1.plotly_chart(fig_bar, use_container_width=True, key="product_revenue")

            # st.table(top_products[["Product", "Stock"]].rename(columns={"Stock": "Qty on Hand"}))

        # -----------------------------
        # CUSTOMERS TAB
        # -----------------------------
        # with tab_customers:
            # st.subheader("Customer Insights")
            # col1, col2 = st.columns([2, 1])

            #LTV example
            # ltv = pd.DataFrame({
                # "Customer": [f"C{i}" for i in range(1, 11)],
                # "LifetimeValue": np.random.randint(1000, 15000, 10)
            # })
            # fig_ltv = px.histogram(ltv, x="LifetimeValue",
                                   # nbins=10, title="Lifetime Value Distribution")
            # col1.plotly_chart(fig_ltv, use_container_width=True, key="ltv_hist")

            # st.write("Top 5 Customers by LTV")
            # st.dataframe(ltv.sort_values("LifetimeValue", ascending=False).head())

        # -----------------------------
        # OPERATIONS TAB
        # -----------------------------
        # with tab_ops:
            # st.subheader("Fulfillment & Shipping")
            # col1, col2 = st.columns([1, 1])

            # shipping = pd.DataFrame({
                # "Carrier": ["DHL", "FedEx", "UPS", "BlueDart"],
                # "Orders": [120, 90, 150, 80],
                # "AvgTime": [2.1, 3.0, 1.8, 2.5]
            # })

            # fig_bar = px.bar(shipping, x="Carrier", y="Orders", title="Orders by Carrier")
            # col1.plotly_chart(fig_bar, use_container_width=True, key="carrier_orders")

            # fig_scatter = px.scatter(shipping, x="AvgTime", y="Orders",
                                     # size="Orders", title="Shipping Time vs Orders")
            # col2.plotly_chart(fig_scatter, use_container_width=True, key="shipping_scatter")




