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
from analytics_functions import kpi_cards, build_customer_ltv

# ---------------------------
# Extra Functions
# ---------------------------
def time_ago(dt):
    if dt is None:
        return "Never"

    diff = datetime.now() - dt
    seconds = diff.total_seconds()

    if seconds < 60:
        return f"{int(seconds)} seconds ago"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        return f"{minutes} minute{'s' if minutes > 1 else ''} ago"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        return f"{hours} hour{'s' if hours > 1 else ''} ago"
    else:
        days = int(seconds // 86400)
        return f"{days} day{'s' if days > 1 else ''} ago"
        
# ---------------------------
# Process Bar
# ---------------------------

def process_table(table: str, mode: str, total_steps: int):
    #st.write(f"⏳ Processing {table} ({mode})...")

    # create a placeholder for progress bar
    progress_bar = st.progress(0, text=f"Loading {table} ({mode})")

    for step in range(total_steps):
        time.sleep(0.5)  # simulate work (API call, DB insert, etc.)
        progress = int((step + 1) / total_steps * 100)
        progress_bar.progress(progress, text=f"{table} ({mode}) - {progress}% complete")

    #st.success(f"✅ Finished processing {table} ({mode})")

def filter_days_difference(date_to,date_from):
    date_diff=date_from-date_to
    return date_diff + timedelta(days=1)
