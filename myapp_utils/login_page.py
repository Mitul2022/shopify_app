import pandas as pd
import streamlit as st
from streamlit_extras.stylable_container import stylable_container
import bcrypt
import requests
import time
import pyotp
import qrcode
import io
from cryptography.fernet import Fernet

# ------------------- Session State -------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "username" not in st.session_state:
    st.session_state.username = None

# ------------------- Helper Functions -------------------
class login_connections():
    @staticmethod
    def hash_password(password):
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
       
    @staticmethod
    def check_password(password, db_pwd):
        return bcrypt.checkpw(password.encode(), db_pwd.encode())
        
    @staticmethod
    def test_connection(shop_name, api_key, api_password):
        url = f"https://{shop_name}.myshopify.com/admin/api/2025-07/shop.json"
        try:
            headers = {"X-Shopify-Access-Token": api_password}
            response = requests.get(url, headers=headers, timeout=10)

            # response = requests.get(url, auth=(api_key, api_password))
            return response.status_code == 200
        except Exception as e:
            print("Error during Shopify connection:", e)
            return False
    @staticmethod        
    @st.dialog("Logout")
    def logout_func():
        st.info("Are you sure you want to log out :question:")
        c1,c2,c3,c4=st.columns([1,2,2,1])
        if c2.button("Yes",width="stretch"):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.toast("Logged out successfully")
            st.rerun()
        if c3.button("No",width="stretch"):
            st.session_state.authenticated = True
            st.rerun()
            

# cipher = Fernet(key)
# def encryp_code(cipher,password):
    # try:
        # return cipher.encrypt(password.encode()).decode()
    # except Exception as e:
        # print("Error during encryption:", e)
# def decrypt_code(cipher,password):
    # try:
        # return cipher.decrypt(password.encode()).decode()
    # except Exception as e:
        # print("Error during encryption:", e)



