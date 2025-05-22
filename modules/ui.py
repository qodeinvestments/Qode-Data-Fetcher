import streamlit as st
from modules.auth import logout
import datetime
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
                nl_file = os.path.join(folder_path, f"{folder_name}_natural_language.txt")
                
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
    
    st.sidebar.markdown("---")
    
    query_count = len(query_engine.get_user_queries(limit=100))
    with st.sidebar.expander(f"Your Recent Queries ({query_count})", expanded=False):
        user_queries = query_engine.get_user_queries(limit=10)
        
        if not user_queries:
            st.info("No queries yet. Start exploring data!")
        else:
            col_del, col_clear = st.columns(2)
            with col_del:
                if st.button("🗑️ Clear All", key="delete_all_queries", help="Delete all saved queries"):
                    try:
                        import shutil
                        for query_info in user_queries:
                            shutil.rmtree(query_info['path'])
                        st.success("All queries deleted")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error deleting all queries: {str(e)}")
            
            with col_clear:
                if st.button("🔄 Refresh", key="refresh_queries", help="Refresh query list"):
                    st.rerun()
        
        for i, query_info in enumerate(user_queries):
            with st.container():
                st.markdown(f"**{query_info['name']}**")
                
                truncated_query = query_info['query'][:80] + ('...' if len(query_info['query']) > 80 else '')
                st.caption(truncated_query)
                
                col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                
                with col1:
                    if query_info['has_results']:
                        download_format = st.selectbox(
                            "📥", 
                            ["CSV", "Excel", "Parquet"], 
                            key=f"fmt_{query_info['folder']}",
                            label_visibility="collapsed",
                            help="Download format"
                        )
                        
                        if download_format == "CSV":
                            with open(f"{query_info['path']}/results.csv", "rb") as f:
                                st.download_button(
                                    "⬇️", f, 
                                    file_name=f"{query_info['name']}.csv",
                                    mime="text/csv",
                                    key=f"dl_csv_{query_info['folder']}",
                                    help="Download CSV"
                                )
                        # elif download_format == "Excel":
                        #     with open(f"{query_info['path']}/results.xlsx", "rb") as f:
                        #         st.download_button(
                        #             "⬇️", f, 
                        #             file_name=f"{query_info['name']}.xlsx",
                        #             mime="application/vnd.ms-excel",
                        #             key=f"dl_xlsx_{query_info['folder']}",
                        #             help="Download Excel"
                        #         )
                        else:
                            with open(f"{query_info['path']}/results.parquet", "rb") as f:
                                st.download_button(
                                    "⬇️", f, 
                                    file_name=f"{query_info['name']}.parquet",
                                    mime="application/octet-stream",
                                    key=f"dl_parquet_{query_info['folder']}",
                                    help="Download Parquet"
                                )
                
                with col2:
                    if st.button("📂", key=f"load_{query_info['folder']}", help="Load query"):
                        try:
                            with open(f"{query_info['path']}/query.sql", "r") as f:
                                sql = f.read()
                            with open(f"{query_info['path']}/input.txt", "r") as f:
                                nl = f.read()
                            
                            st.session_state['natural_query'] = nl
                            st.session_state['sql_query'] = sql
                            st.success(f"Loaded: {query_info['name']}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to load query: {str(e)}")
                
                with col3:
                    if st.button("👁️", key=f"view_{query_info['folder']}", help="Preview results"):
                        if query_info['has_results']:
                            st.session_state[f'preview_{query_info["folder"]}'] = not st.session_state.get(f'preview_{query_info["folder"]}', False)
                            st.rerun()
                
                with col4:
                    if st.button("🗑️", key=f"del_{query_info['folder']}", help="Delete query"):
                        try:
                            import shutil
                            shutil.rmtree(query_info['path'])
                            st.success("Deleted")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                
                if st.session_state.get(f'preview_{query_info["folder"]}', False) and query_info['has_results']:
                    try:
                        import pandas as pd
                        df = pd.read_csv(f"{query_info['path']}/results.csv")
                        st.dataframe(df.head(5), use_container_width=True, height=150)
                    except Exception as e:
                        st.error(f"Preview error: {str(e)}")
                
                if i < len(user_queries) - 1:
                    st.markdown("---")

def render_main_interface(query_engine):
    st.header("🔍 Qode Data Fetcher")
    
    st.markdown("""
    This platform enables users to query financial market data using structured query language or natural language.
    Simply describe what data you need, and the system will generate and execute the query for you.
    """)
    
    with st.expander("🚀 Getting Started", expanded=False):
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
        
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    query_name = st.text_input(
        "Query Name:", 
        value=f"Query_{current_time}", 
        placeholder="Enter a name for this query",
        key="query_name_input"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("💬 Natural Language")
        
        natural_query = st.text_area(
            "Enter your query in natural language:", 
            value=st.session_state.get('natural_query', ''),
            placeholder="Example: Show NIFTY50 index data for the last trading day with 15-minute intervals",
            key="natural_query_input",
            height=150,
        )
        
        col_gen, col_clear = st.columns([3, 1])
        with col_gen:
            generate_sql = st.button("🔄 Generate SQL", use_container_width=True)
        with col_clear:
            if st.button("🧹", help="Clear input", key="clear_nl"):
                st.session_state['natural_query'] = ''
                st.rerun()
    
    if generate_sql and natural_query:
        with st.spinner("Converting to SQL..."):
            sql_query = query_engine.nl_to_sql(natural_query)
            if sql_query:
                st.session_state['sql_query'] = sql_query
                st.session_state['natural_query'] = natural_query
                st.success("✅ SQL generated successfully!")
            else:
                st.error("❌ Failed to generate SQL query")
    
    with col2:
        st.subheader("⚡ SQL Query")
        sql_query = st.text_area(
            "SQL query:", 
            value=st.session_state.get('sql_query', ''),
            placeholder="SQL query will appear here after generation",
            height=150,
            key="sql_query_input"
        )
        
        col_run, col_clear_sql = st.columns([3, 1])
        with col_run:
            run_query = st.button("▶️ Run Query", use_container_width=True, disabled=not sql_query, type="primary")
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
                st.success(f"✅ Query executed successfully in {execution_time:.4f} seconds | {len(results)} rows returned")
                
                tab1, tab2 = st.tabs(["📊 Results", "📈 Quick Stats"])
                
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

                if query_name:
                    try:
                        user_id = st.session_state.get('user_id', 'default_user')
                        base_path = f"query_history/{user_id}"
                        query_folder_path = f"{base_path}/{query_name}"

                        os.makedirs(query_folder_path, exist_ok=True)
                        
                        with open(f"{query_folder_path}/{query_name}_query.sql", "w") as f:
                            f.write(sql_query)
                        
                        with open(f"{query_folder_path}/{query_name}_natural_language.txt", "w") as f:
                            f.write(natural_query)
                        
                        st.success(f"✅ Query saved as '{query_name}' in query_history/{user_id}/{query_name}/")
                        st.balloons()
                    except Exception as e:
                        st.error(f"❌ Failed to save query: {str(e)}")
                
                st.markdown("### 📥 Download Options")
                col1, col2, col3 = st.columns(3)
                with col1:
                    csv = results.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📄 Download CSV",
                        csv,
                        f"query_results_{query_name if query_name else 'data'}.csv",
                        "text/csv",
                        key='download-csv',
                        use_container_width=True
                    )
                
                # with col2:
                #     if hasattr(query_engine, 'dataframe_to_excel'):
                #         excel_buffer = query_engine.dataframe_to_excel(results)
                #         st.download_button(
                #             "📊 Download Excel",
                #             excel_buffer,
                #             f"query_results_{query_name if query_name else 'data'}.xlsx",
                #             "application/vnd.ms-excel",
                #             key='download-excel',
                #             use_container_width=True
                #         )
                    
                with col3:
                    if hasattr(query_engine, 'dataframe_to_parquet'):
                        parquet_buffer = query_engine.dataframe_to_parquet(results)
                        st.download_button(
                            "🗜️ Download Parquet",
                            parquet_buffer,
                            f"query_results_{query_name if query_name else 'data'}.parquet",
                            "application/octet-stream",
                            key='download-parquet',
                            use_container_width=True
                        )
            else:
                st.error(f"❌ Query failed after {execution_time:.4f} seconds")
                if error_message:
                    with st.expander("🔍 Error details"):
                        st.code(error_message, language="sql")