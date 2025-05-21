import duckdb
import streamlit as st

DB_PATH = "qode_engine_data.db"

@st.cache_resource
def get_db_connections():
    disk_conn = duckdb.connect(DB_PATH)
    disk_conn.execute("PRAGMA memory_limit='2GB'")
    
    return disk_conn
