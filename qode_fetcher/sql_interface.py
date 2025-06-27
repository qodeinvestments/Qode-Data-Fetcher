import streamlit as st
from time_series_builder import time_series_query_builder
from advanced_editor import advanced_query_editor, admin_query_editor
from option_chain_viewer import option_chain_viewer
from greeks_calculator import greeks_calculator
from auth import get_current_user

def sql_query_interface(query_engine):
    st.header("SQL Query Interface")
    user_info = get_current_user()
    first_name = None
    if user_info:
        first_name = user_info.get('first_name')

    is_admin = first_name == 'Jay'
    
    
    if is_admin:
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Time Series Query Builder", "Historical Option Chain", "Greeks Calculator", "SQL Editor", "Admin SQL Editor"])
    else:
        tab1, tab2, tab3, tab4 = st.tabs([
            "Time Series Query Builder", "Historical Option Chain", "Greeks Calculator", "SQL Editor"])
    
    with tab1:
        time_series_query_builder(query_engine)
    
    with tab2:
        option_chain_viewer(query_engine)
    
    with tab3:
        greeks_calculator()
        
    with tab4:
        advanced_query_editor(query_engine)
    
    if is_admin:
        with tab5:
            admin_query_editor(query_engine)