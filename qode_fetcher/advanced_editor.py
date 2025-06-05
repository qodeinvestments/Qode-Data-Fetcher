import streamlit as st
from chart_renderer import has_candlestick_columns, render_candlestick_chart
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
                         
                col1, col2 = st.columns(2)
                with col1:
                    csv = result.to_csv(index=False)
                    st.download_button("Download as CSV", csv, "adv_query_results.csv")
                
                if has_candlestick_columns(result):
                    st.subheader("Candlestick Preview")
                    render_candlestick_chart(result)
            else:
                st.info("Query returned no results")