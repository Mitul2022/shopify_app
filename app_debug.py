# app_debug.py — Step-by-step Streamlit ETL debugger
# Save this file and run:  streamlit run app_debug.py --logger.level=debug

import sys
import os
import time
import json
import platform
import traceback

import streamlit as st
import pandas as pd
import numpy as np
import psycopg2

st.set_page_config(page_title="ETL Debug Dashboard", layout="wide")

# -------------------------------
# Logging helpers
# -------------------------------

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    st.session_state.setdefault("logs", [])
    line = f"[{ts}] {msg}"
    st.session_state["logs"].append(line)
    print(line)

from contextlib import contextmanager

@contextmanager
def step(name: str):
    start = time.time()
    log(f"▶️ {name} ...")
    with st.spinner(f"{name}..."):
        try:
            yield
        except Exception as e:
            dur = time.time() - start
            log(f"❌ {name} failed after {dur:.2f}s: {e}")
            st.exception(e)
            raise
        else:
            dur = time.time() - start
            log(f"✅ {name} done in {dur:.2f}s")
            st.success(f"{name} done in {dur:.2f}s")

def show_logs():
    st.code("\n".join(st.session_state.get("logs", [])) or "No logs yet", language="text")

# -------------------------------
# DB helpers
# -------------------------------

def get_connection():
    cfg = st.secrets["postgres"]
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),  # safe default
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=8,  # seconds — prevents infinite hang
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
    )

# -------------------------------
# Data helpers
# -------------------------------

def df_to_json(df: pd.DataFrame, id_col: str = "id") -> str:
    """Convert DataFrame to JSON (records) with robust ID + JSON handling.
    - Ensures id_col exists
    - Drops rows with null/invalid IDs
    - Coerces IDs like "6340973690954.0" → 6340973690954
    - Serializes dict/list columns
    - Normalizes datetime columns to ISO strings
    """
    df_copy = df.copy()

    if id_col not in df_copy.columns:
        raise ValueError(f"Column '{id_col}' not found. Columns: {df_copy.columns.tolist()}")

    def to_bigint(v):
        if v is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        try:
            if isinstance(v, str):
                s = v.strip()
                if s == "":
                    return None
                return int(float(s))
            return int(v)
        except Exception:
            # invalid ID – treat as null so it gets dropped
            return None

    df_copy[id_col] = df_copy[id_col].apply(to_bigint)
    before = len(df_copy)
    df_copy = df_copy[df_copy[id_col].notnull()]
    dropped = before - len(df_copy)
    if dropped:
        log(f"ℹ️ Dropped {dropped} row(s) with null/invalid {id_col}")

    # Serialize dict/list columns
    for col in df_copy.columns:
        if df_copy[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_copy[col] = df_copy[col].apply(json.dumps)

    # Normalize datetimes to strings (ISO-ish)
    for col, dtype in df_copy.dtypes.items():
        if str(dtype).startswith("datetime64"):
            df_copy[col] = df_copy[col].astype("datetime64[ns]").dt.tz_localize(None).astype(str)

    return df_copy.to_json(orient="records")


def load_customers(df: pd.DataFrame, mode: str, id_col: str = "customer_id"):
    log(f"load_customers(): starting with {len(df)} row(s), mode={mode}")
    log(f"dtypes:\n{df.dtypes}")

    with step("Convert DataFrame → JSON"):
        json_data = df_to_json(df, id_col=id_col)
        log(f"JSON size: {len(json_data):,} bytes")

    with step("DB connect"):
        conn = get_connection()
        cur = conn.cursor()

    try:
        if mode == "Full":
            with step("CALL dev.usp_bulk_insert_customers"):
                cur.execute("CALL dev.usp_insert_customers(%s);", [json_data])
        else:
            with step("CALL dev.usp_upsert_customer"):
                cur.execute("CALL dev.usp_upsert_customer(%s);", [json_data])
        with step("COMMIT"):
            conn.commit()
        log("Stored procedure completed successfully.")
    except Exception:
        with step("ROLLBACK"):
            conn.rollback()
        log("Error during load; rolled back transaction.")
        raise
    finally:
        cur.close()
        conn.close()
        log("DB connection closed.")

# -------------------------------
# UI
# -------------------------------

st.title("🧪 ETL Debug Dashboard (Streamlit)")

with st.sidebar:
    st.header("Controls")
    id_col = st.text_input("ID column", value="customer_id")
    mode = st.radio("Load mode", ["Incremental", "Full"], index=0)
    use_mock = st.checkbox(
        "Use mock data", True,
        help="Uncheck to call your fetch_customers() if available in your project."
    )
    st.button("Clear logs", on_click=lambda: st.session_state.update({"logs": []}))
    st.caption("Tip: Run with --logger.level=debug and watch the terminal too.")

with st.expander("Environment"):
    st.write({
        "python": sys.version,
        "platform": platform.platform(),
        "streamlit": st.__version__,
        "pandas": pd.__version__,
        "psycopg2": getattr(psycopg2, "__version__", "unknown"),
    })

# --- Quick DB test ---
run_db = st.button("Test DB connection")
if run_db:
    try:
        with step("DB connect"):
            conn = get_connection()
        with step("DB ping (SELECT 1)"):
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        conn.close()
        log("DB connection OK.")
    except Exception as e:
        log("DB connection failed.")
        st.exception(e)

# --- JSON conversion test ---
run_json = st.button("Test JSON conversion")
if run_json:
    with step("Build sample DataFrame"):
        df_sample = pd.DataFrame({
            id_col: [6340973690954.0, "