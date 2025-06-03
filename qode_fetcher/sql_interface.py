import streamlit as st
from time_series_builder import time_series_query_builder
from advanced_editor import advanced_query_editor
from option_chain_viewer import option_chain_viewer

def sql_query_interface(query_engine):
    st.header("SQL Query Interface")
    
    tab1, tab2, tab3 = st.tabs(["Time Series Query Builder", "Option Chain Viewer", "Advanced SQL Editor"])
    
    with tab1:
        time_series_query_builder(query_engine)
    
    with tab2:
        option_chain_viewer(query_engine)
    
    with tab3:
        advanced_query_editor(query_engine)