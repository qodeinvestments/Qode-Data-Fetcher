import streamlit as st
from chart_renderer import has_candlestick_columns, has_line_chart_columns, render_appropriate_chart
from sample_queries import get_sample_queries

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
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary", key="adv_execute")
    
    if execute_button and sql_query.strip():
        execute_advanced_query(query_engine, sql_query)

def execute_advanced_query(query_engine, query):
    with st.spinner("Executing query..."):
        result, exec_time, error = query_engine.execute_query(query)
        
        if error:
            st.error(f"Query Error: {error}")
        else:
            st.success(f"Query executed successfully in {exec_time:.2f} seconds")
            
            if len(result) > 0:
                st.write(f"**Results: {len(result)} rows**")
                st.dataframe(result)
                
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
                    
                
                if has_candlestick_columns(result) or has_line_chart_columns(result):
                    render_appropriate_chart(result)
                
            else:
                st.info("Query returned no results")