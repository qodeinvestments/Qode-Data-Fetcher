import streamlit as st
from modules.auth import logout
import os

def get_saved_queries(limit=10):
    user_id = st.session_state.get('user_id', 'default_user')
    user_folder = f"query_history/{user_id}"
    
    if not os.path.exists(user_folder):
        return []
    
    queries = []
    for folder_name in os.listdir(user_folder):
        folder_path = os.path.join(user_folder, folder_name)
        if os.path.isdir(folder_path):
            query_info = {
                'name': folder_name,
                'folder': folder_name,
                'path': folder_path,
                'has_results': os.path.exists(os.path.join(folder_path, 'results.csv'))
            }
            
            try:
                sql_file = os.path.join(folder_path, f"{folder_name}_query.sql")
                
                if os.path.exists(sql_file):
                    with open(sql_file, 'r') as f:
                        query_info['query'] = f.read()
                elif os.path.exists(os.path.join(folder_path, 'query.sql')):
                    with open(os.path.join(folder_path, 'query.sql'), 'r') as f:
                        query_info['query'] = f.read()
                else:
                    query_info['query'] = "No query found"
                    
            except Exception as e:
                query_info['query'] = f"Error reading query: {str(e)}"
            
            queries.append(query_info)
    
    queries.sort(key=lambda x: os.path.getmtime(x['path']), reverse=True)
    return queries[:limit]

def get_saved_queries_count():
    user_id = st.session_state.get('user_id', 'default_user')
    user_folder = f"query_history/{user_id}"
    
    if not os.path.exists(user_folder):
        return 0
    
    return len([d for d in os.listdir(user_folder) if os.path.isdir(os.path.join(user_folder, d))])

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
            
def render_main_interface(query_engine):
    st.header("Qode Data Fetcher")

    sql_query = st.text_area(
        "SQL query:", 
        value=st.session_state.get('sql_query', ''),
        placeholder="SQL query will appear here after generation",
        height=150,
        key="sql_query_input"
    )
    
    col_run, col_clear_sql = st.columns([3, 1])
    with col_run:
        run_query = st.button("Run Query", use_container_width=True, disabled=not sql_query, type="primary")
    with col_clear_sql:
        if st.button("🧹", help="Clear SQL", key="clear_sql"):
            st.session_state['sql_query'] = ''
            st.rerun()
    
    if run_query and sql_query:
        with st.spinner("Executing query..."):
            result = query_engine.execute_query(sql_query)
            
            if len(result) == 2:
                results, execution_time = result
                error_message = None
            else:
                results, execution_time, error_message = result
            
            if results is not None:
                st.success(f"Query executed successfully in {execution_time:.4f} seconds | {len(results)} rows returned")
                
                tab1, tab2 = st.tabs(["Results", "Quick Stats"])
                
                with tab1:
                    st.dataframe(results, use_container_width=True)
                
                with tab2:
                    if not results.empty:
                        col_info1, col_info2, col_info3 = st.columns(3)
                        with col_info1:
                            st.metric("Total Rows", len(results))
                        with col_info2:
                            st.metric("Total Columns", len(results.columns))
                        with col_info3:
                            memory_usage = results.memory_usage(deep=True).sum() / 1024 / 1024
                            st.metric("Memory Usage", f"{memory_usage:.2f} MB")
                        
                        numeric_cols = results.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.subheader("Numeric Column Summary")
                            st.dataframe(results[numeric_cols].describe(), use_container_width=True)
                
                st.markdown("---")


                st.markdown("### Download Options")
                col1, col2, col3 = st.columns(3)
                with col1:
                    csv = results.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📄 Download CSV",
                        csv,
                        f"query_results.csv",
                        "text/csv",
                        key='download-csv',
                        use_container_width=True
                    )
                    
                with col3:
                    if hasattr(query_engine, 'dataframe_to_parquet'):
                        parquet_buffer = query_engine.dataframe_to_parquet(results)
                        st.download_button(
                            "🗜️ Download Parquet",
                            parquet_buffer,
                            f"query_results.parquet",
                            "application/octet-stream",
                            key='download-parquet',
                            use_container_width=True
                        )
            else:
                st.error(f"Query failed after {execution_time:.4f} seconds")
                if error_message:
                    with st.expander("Error details"):
                        st.code(error_message, language="sql")