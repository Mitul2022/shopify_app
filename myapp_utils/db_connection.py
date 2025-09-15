import streamlit as st
import psycopg2
import toml

# ---------------------------
# DB Connection (from secrets.toml)
# ---------------------------
    
def get_connection():
    cfg = st.secrets["postgres"]
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),  # safe access with default
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"]
    )

# ---------------------------
# run query 
# ---------------------------
def run_query(query, params=None, fetch=False, many=False):
    con = get_connection()
    con.autocommit = True
    try:
        with con:
            with con.cursor() as cur:
                cur.execute("set search_path to dev;")
                if params is not None and not isinstance(params, (list, tuple)):
                    params = (params,)
                if many:
                    cur.executemany(query, params)
                else:
                    cur.execute(query, params)

                if "RETURNING" in query.upper():
                    return cur.fetchone()[0]
                elif fetch:
                    return cur.fetchall()
    finally:
        con.close()

# class query_auth():
# def run_query(query, params=None, fetch=False, many=False):
    # con=get_connection()
    # con.autocommit = True
    # try:
        # with con:
            # with con.cursor() as cur:
                # cur.execute("set search_path to dev;")
                # if params is not None and not isinstance(params, (list,tuple)):
                    # params=(params,)
                # if many:
                    # cur.executemany(query,params)
                # else:
                    # cur.execute(query,params)
                # if fetch:
                    # return cur.fetchall()
    # finally:
        # con.close()

# ---------------------------
# Credencials  
# ---------------------------
def get_user(email):
    row=run_query(
        "SELECT userid, username, password_hash, totp_secret,email FROM dev.users WHERE email = %s;",
        (email,),fetch=True
    )
    if not row:
        return None
    rec=row[0]
    if isinstance(rec,dict):
        return rec
    else:
        userid,username,ps_hash,totp_secret,email,=rec
        return {
            "user_id":userid,
            "username":username,
            "password_hash":ps_hash,
            "totp_secret":totp_secret,
            "email":email
        }

# ---------------------------
# Credencials  
# ---------------------------
def get_user_stores(userid):
    row=run_query(
        "SELECT * FROM dev.usp_get_store_by_userid(%s);",
        (userid,),fetch=True
    )
    if not row:
        return None
    else:
        result = []
        for r in row:
            shop_name,api_key,api_password,is_active,updated_at,users_stores_id,user_id=r
            result.append( {
                
                "shop_name":shop_name,
                "api_key":api_key,
                "api_password":api_password,
                "is_active":is_active,
                "updated_at":updated_at,
                "users_stores_id":users_stores_id,
                "user_id":user_id
            })
        return result

# ---------------------------
# Store APIS URL  
# ---------------------------
def get_user_stores_by_store_id(users_stores_id):
    row=run_query(
        "select * from dev.usp_get_store_by_users_stores_id(%s);",
        (users_stores_id,),fetch=True
    )
    if not row:
        return None
    rec=row[0]
    if isinstance(rec,dict):
        return row
    else:
        shop_name,api_key,api_password,is_active,updated_at,users_stores_id,user_id=rec
        return {
            "shop_name":shop_name,
            "api_key":api_key,
            "api_password":api_password,
            "is_active":is_active,
            "updated_at":updated_at,
            "users_stores_id":users_stores_id,
            "user_id":user_id
            }


# ---------------------------
# DB-Units  
# ---------------------------
def get_jsonb_data(column_name, key_name, data_type, table_name,users_stores_id,date_col,from_date,to_date):
    query = f"""
        SELECT (jsonb_array_elements(( {column_name} #>> '{{}}')::jsonb) ->> %s)::{data_type} AS {key_name}
        FROM {table_name} where users_stores_id = {users_stores_id} and cast({date_col} as date) between cast('{from_date}' as date) and cast('{to_date}' as date);
    """
    rows = run_query(query, (key_name,), fetch=True)
    if not rows:
        return None
    return [r[0] for r in rows]


# ---------------------------
# DB-Units  
# ---------------------------
def get_store_data():
    rows = run_query(
        """
        SELECT usm.users_stores_id, s.store_id, s.shop_name, usm.user_id
        FROM dev.users u
        JOIN dev.users_stores_mapping usm ON u.userid = usm.user_id
        JOIN dev.stores s ON s.store_id = usm.store_id
        WHERE s.is_active = true AND usm.user_id = 12;
        """,
        fetch=True
    )
    if not rows:
        return None
    
    return [
        {
            "users_stores_id": r[0],
            "store_id": r[1],
            "shop_name": r[2],
            "user_id": r[3]
        }
        for r in rows
    ]
