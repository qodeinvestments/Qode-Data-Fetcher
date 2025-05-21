import streamlit as st
import plotly.graph_objects as go
from modules.auth import logout

def show_sidebar(query_engine):
    st.sidebar.image("./logo.jpg", width=100)
    
    with st.sidebar.container():
        cols = st.columns([3, 2])
        
        with cols[0]:
            st.markdown(f"### Welcome, {st.session_state['user_name']}")
            st.write(f"User ID: `{st.session_state['user_id']}`")
        
        with cols[1]:
            if st.button("Logout", use_container_width=True):
                logout()
                st.rerun()
    
    st.sidebar.header("Navigation")
    nav_cols = st.sidebar.columns(2)
    
    with nav_cols[0]:
        if st.button("Data Query", key="nav_main", use_container_width=True):
            st.session_state['current_page'] = 'main'
            st.rerun()
            
    with nav_cols[1]:
        if st.button("DB Info", key="nav_db_info", use_container_width=True):
            st.session_state['current_page'] = 'db_info'
            st.rerun()
    
    with st.sidebar.expander("Your Recent Queries", expanded=False):
        user_queries = query_engine.get_user_queries(limit=10)
        
        if not user_queries:
            st.info("No queries yet. Start exploring data!")
        else:
            if st.button("Delete All Queries", key="delete_all_queries", type="primary"):
                try:
                    import shutil
                    for query_info in user_queries:
                        shutil.rmtree(query_info['path'])
                    st.success("All queries deleted")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error deleting all queries: {str(e)}")
        
        for query_info in user_queries:
            st.markdown(f"**{query_info['name']}**")
            st.text(query_info['query'][:100] + ('...' if len(query_info['query']) > 100 else ''))
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if query_info['has_results']:
                    download_options = ["CSV", "Excel", "Parquet"]
                    download_format = st.selectbox(
                        "Format", 
                        download_options, 
                        key=f"fmt_{query_info['folder']}"
                    )
                    
                    if download_format == "CSV":
                        with open(f"{query_info['path']}/results.csv", "rb") as f:
                            st.download_button(
                                "Download", f, 
                                file_name=f"{query_info['name']}.csv",
                                mime="text/csv",
                                key=f"dl_csv_{query_info['folder']}"
                            )
                    elif download_format == "Excel":
                        with open(f"{query_info['path']}/results.xlsx", "rb") as f:
                            st.download_button(
                                "Download", f, 
                                file_name=f"{query_info['name']}.xlsx",
                                mime="application/vnd.ms-excel",
                                key=f"dl_xlsx_{query_info['folder']}"
                            )
                    else:  # Parquet
                        with open(f"{query_info['path']}/results.parquet", "rb") as f:
                            st.download_button(
                                "Download", f, 
                                file_name=f"{query_info['name']}.parquet",
                                mime="application/octet-stream",
                                key=f"dl_parquet_{query_info['folder']}"
                            )
            
            with col2:
                if st.button("Load", key=f"load_{query_info['folder']}"):
                    try:
                        with open(f"{query_info['path']}/query.sql", "r") as f:
                            sql = f.read()
                        with open(f"{query_info['path']}/input.txt", "r") as f:
                            nl = f.read()
                        
                        st.session_state['natural_query'] = nl
                        st.session_state['sql_query'] = sql
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to load query: {str(e)}")
            
            with col3:
                if st.button("Delete", key=f"del_{query_info['folder']}"):
                    try:
                        import shutil
                        shutil.rmtree(query_info['path'])
                        st.success("Deleted")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
def render_main_interface(query_engine):
    st.header("Qode Data Fetcher")
    
    st.markdown("""
    This platform enables users to query financial market data using structured query language or natural language.
    Simply describe what data you need, and the system will generate and execute the query for you.
    """)
    
    with st.expander("Getting Started", expanded=False):
        st.markdown("""
        ### How to use this platform
        
        1. **Enter your query** in natural language (e.g., "Show NIFTY50 index data for the last 5 days")
        2. Click **Generate SQL** to convert it to a database query 
        3. Review the SQL (edit if needed)
        4. Click **Run Query** to execute and view results
        5. Export or visualize the data as needed
        
        **Pro Tips:**
        - Specify precise time ranges and instruments
        - For complex analysis, break into multiple queries
        - Save important queries for future reference
        """)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Natural Language")
        
        natural_query = st.text_area(
            "Enter your query in natural language:", 
            value=st.session_state.get('natural_query', ''),
            placeholder="Example: Show NIFTY50 index data for the last trading day with 15-minute intervals",
            key="natural_query_input",
            height=150,
        )
            
        generate_sql = st.button("Generate SQL", use_container_width=True)
    
    if generate_sql and natural_query:
        with st.spinner("Converting to SQL..."):
            sql_query = query_engine.nl_to_sql(natural_query)
            if sql_query:
                st.session_state['sql_query'] = sql_query
                st.session_state['natural_query'] = natural_query
            else:
                st.error("Failed to generate SQL query")
    
    with col2:
        st.subheader("SQL Query")
        sql_query = st.text_area(
            "SQL query:", 
            value=st.session_state.get('sql_query', ''),
            placeholder="SQL query will appear here after generation",
            height=150,
            key="sql_query_input"
        )
        
        run_query = st.button("Run Query", use_container_width=True, disabled=not sql_query)
    
    if run_query and sql_query:
        with st.spinner("Executing query..."):
            result = query_engine.execute_query(sql_query)
            
            if len(result) == 2:
                results, execution_time = result
                error_message = None
            else:
                results, execution_time, error_message = result
            
            if results is not None:
                st.success(f"✅ Query executed successfully in {execution_time:.4f} seconds")
                
                st.dataframe(results, use_container_width=True)
                
                query_name = st.text_input("Query Name:", value=f"Query_{execution_time:.2f}s", 
                                         placeholder="Enter a name for this query")
                
                save_query = st.button("Save Query", use_container_width=True)
                
                if save_query:
                    query_id, query_dir = query_engine.save_query(natural_query, sql_query, results, query_name)
                    st.success(f"Query saved as '{query_name}'")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    csv = results.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download CSV",
                        csv,
                        f"query_results_{query_name if 'query_name' in locals() else 'data'}.csv",
                        "text/csv",
                        key='download-csv'
                    )
                
                with col2:
                    excel_buffer = query_engine.dataframe_to_excel(results)
                    st.download_button(
                        "Download Excel",
                        excel_buffer,
                        f"query_results_{query_name if 'query_name' in locals() else 'data'}.xlsx",
                        "application/vnd.ms-excel",
                        key='download-excel'
                    )
                    
                with col3:
                    parquet_buffer = query_engine.dataframe_to_parquet(results)
                    st.download_button(
                        "Download Parquet",
                        parquet_buffer,
                        f"query_results_{query_name if 'query_name' in locals() else 'data'}.parquet",
                        "application/octet-stream",
                        key='download-parquet'
                    )
            else:
                st.error(f"Query failed after {execution_time:.4f} seconds")
                if error_message:
                    with st.expander("Error details"):
                        st.code(error_message)