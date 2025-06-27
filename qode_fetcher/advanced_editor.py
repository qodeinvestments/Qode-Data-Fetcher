import streamlit as st
from chart_renderer import has_candlestick_columns, has_line_chart_columns, render_appropriate_chart
from sample_queries import get_sample_queries
from data_utils import event_days_filter_ui
import pandas as pd

def advanced_query_editor(query_engine):
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
        border-bottom: 3px solid #3b82f6;
        padding-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">SQL Editor</div>', unsafe_allow_html=True)
    
    sample_queries = get_sample_queries()
    
    selected_sample = st.selectbox("Sample Queries (optional):", [""] + list(sample_queries.keys()))
    
    if selected_sample:
        default_query = sample_queries[selected_sample]
    else:
        default_query = ""
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=default_query,
        height=150,
        help="Only SELECT queries are allowed for security."
    )
    
    filter_option, filtered_event_days = event_days_filter_ui(key1="sql select", key2='sql multi')
    event_dates = [e['date'] for e in filtered_event_days]

    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="adv_execute")
        
    if execute_button and sql_query.strip():
        execute_advanced_query(query_engine, sql_query, filter_option, event_dates)

def execute_advanced_query(query_engine, query, filter_option, event_dates):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query)
        
        if not error and "timestamp" in result.columns:
            result['date'] = pd.to_datetime(result['timestamp']).dt.date.astype(str)
            if filter_option == "Exclude Event Days":
                result = result[~result['date'].isin(event_dates)]
            elif filter_option == "Only Event Days":
                result = result[result['date'].isin(event_dates)]
                
            result = result.drop(columns=["date"])
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, "adv_query_results.csv")
                
                with col2:
                    json_data = result.to_json(orient='records')
                    st.download_button("Download as JSON", json_data, "adv_query_results.json")
                    
                with col3:
                    parquet_file = result.to_parquet()
                    st.download_button("Download as Parquet", parquet_file, "adv_query_results.parquet")
                    
                with col4:
                    gzip_file = result.to_csv(compression='gzip')
                    st.download_button("Download as Gzip CSV", gzip_file, "adv_query_results.csv.gz", mime="application/gzip")
                    
                st.dataframe(result)

                if has_candlestick_columns(result) or has_line_chart_columns(result):
                    render_appropriate_chart(result)
                
            else:
                st.info("Query returned no results")

def admin_query_editor(query_engine):
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
        border-bottom: 3px solid #f59e42;
        padding-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Admin SQL Editor</div>', unsafe_allow_html=True)
    
    sample_queries = get_sample_queries()
    
    selected_sample = st.selectbox("Sample Queries (optional):", [""] + list(sample_queries.keys()), key="admin_sample_query")
    
    if selected_sample:
        default_query = sample_queries[selected_sample]
    else:
        default_query = ""
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=default_query,
        height=150,
        help=None,
        key="admin_sql_query_area"
    )
    
    filter_option, filtered_event_days = event_days_filter_ui(key1="admin sql select", key2='admin sql multi')
    event_dates = [e['date'] for e in filtered_event_days]

    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="admin_execute")
        
    if execute_button and sql_query.strip():
        execute_admin_query(query_engine, sql_query, filter_option, event_dates)

def execute_admin_query(query_engine, query, filter_option, event_dates):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query, is_admin=True)
        
        if not error and isinstance(result, pd.DataFrame) and "timestamp" in result.columns:
            result['date'] = pd.to_datetime(result['timestamp']).dt.date.astype(str)
            if filter_option == "Exclude Event Days":
                result = result[~result['date'].isin(event_dates)]
            elif filter_option == "Only Event Days":
                result = result[result['date'].isin(event_dates)]
                
            result = result.drop(columns=["date"])
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, "admin_query_results.csv")
                
                with col2:
                    json_data = result.to_json(orient='records')
                    st.download_button("Download as JSON", json_data, "admin_query_results.json")
                    
                with col3:
                    parquet_file = result.to_parquet()
                    st.download_button("Download as Parquet", parquet_file, "admin_query_results.parquet")
                    
                with col4:
                    gzip_file = result.to_csv(compression='gzip')
                    st.download_button("Download as Gzip CSV", gzip_file, "admin_query_results.csv.gz", mime="application/gzip")
                    
                st.dataframe(result)

                if has_candlestick_columns(result) or has_line_chart_columns(result):
                    render_appropriate_chart(result)
                
            else:
                st.info("Query returned no results")