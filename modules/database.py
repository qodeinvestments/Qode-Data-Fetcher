import duckdb
import streamlit as st

DB_PATH = "qode_engine_data.db"
DB_IN_MEMORY = ":memory:"

@st.cache_resource
def get_db_connections():
    disk_conn = duckdb.connect(DB_PATH)
    memory_conn = duckdb.connect(DB_IN_MEMORY)
    
    for conn in [disk_conn, memory_conn]:
        conn.execute("PRAGMA memory_limit='2GB'")
        conn.execute("PRAGMA threads=4")
    
    return disk_conn, memory_conn
