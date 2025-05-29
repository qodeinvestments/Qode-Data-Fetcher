import streamlit as st

def sql_query_interface(query_engine):
    st.header("SQL Query Interface")
    
    st.subheader("Query Editor")
    
    sample_queries = {
        "Show all tables": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'market_data' ORDER BY table_name",
        "Index data sample": "SELECT * FROM market_data.NSE_Index_NIFTY LIMIT 10",
        "Options data sample": "SELECT * FROM information_schema.tables WHERE table_name LIKE '%options%' LIMIT 5"
    }
    
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
        execute_button = st.button("Execute Query", type="primary")
    
    if execute_button and sql_query.strip():
        with st.spinner("Executing query..."):
            result, exec_time, error = query_engine.execute_query(sql_query)
            
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
                        st.download_button("Download as CSV", csv, "query_results.csv")
                else:
                    st.info("Query returned no results")