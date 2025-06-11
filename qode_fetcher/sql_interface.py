import streamlit as st
from time_series_builder import time_series_query_builder
from advanced_editor import advanced_query_editor
from option_chain_viewer import option_chain_viewer
from greeks_calculator import greeks_calculator

def sql_query_interface(query_engine):
    st.header("SQL Query Interface")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Time Series Query Builder", "Historical Option Chain", "Greeks Calculator", "SQL Editor"])
    
    with tab1:
        time_series_query_builder(query_engine)
    
    with tab2:
        option_chain_viewer(query_engine)
    
    with tab3:
        greeks_calculator()
        
    with tab4:
        advanced_query_editor(query_engine)