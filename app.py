import os
import streamlit as st
from modules.auth import init_auth, show_login
from modules.database import get_db_connections
from modules.query_engine import QueryEngine
from modules.utils import SystemMonitor
from modules.ui import show_sidebar, render_main_interface
from modules.db_info import render_db_info

st.set_page_config(
    page_title="Qode Data Fetcher",
    page_icon="Q",
    layout="wide"
)

def main():
    os.makedirs("query_history", exist_ok=True)
    os.makedirs("query_logs", exist_ok=True)
    
    init_auth()
    
    disk_conn = get_db_connections()
    
    query_engine = QueryEngine(disk_conn)
    
    if not st.session_state.get('logged_in', False):
        show_login()
    else:
        show_sidebar(query_engine)

        page = st.session_state.get('current_page', 'main')
        
        if page == 'main':
            render_main_interface(query_engine)
        elif page == 'db_info':
            render_db_info(disk_conn)
        
        SystemMonitor.show_metrics(disk_conn)

if __name__ == "__main__":
    main()