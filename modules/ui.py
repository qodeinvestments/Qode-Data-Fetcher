import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
from modules.auth import logout
from modules.database import get_schema_info

def show_sidebar(query_engine):
    st.sidebar.image("./logo.jpg", width=100)
    
    with st.sidebar.container():
        cols = st.columns([3, 2])
        
        with cols[0]:
            st.markdown(f"### Welcome, {st.session_state['user_name']}")
            st.markdown(f"User ID: `{st.session_state['user_id']}`")
        

        with cols[1]:
            if st.button("Logout", use_container_width=True):
                logout()
                st.rerun()
    
    st.sidebar.header("Database Options")
    use_in_memory = st.sidebar.checkbox("Use In-Memory Database", value=False, 
                                      help="Faster but doesn't persist data")
    st.session_state['use_in_memory'] = use_in_memory
    
    with st.sidebar.expander("Your Recent Queries", expanded=False):
        user_queries = query_engine.get_user_queries(limit=10)
        
        if not user_queries:
            st.info("No queries yet. Start exploring data!")
        
        for query_info in user_queries:
            st.markdown(f"**{query_info['folder']}**")
            st.text(query_info['query'][:100] + ('...' if len(query_info['query']) > 100 else ''))
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if query_info['has_results']:
                    with open(f"{query_info['path']}/results.xlsx", "rb") as f:
                        st.download_button(
                            "Results", f, 
                            file_name=f"results_{query_info['folder']}.xlsx",
                            mime="application/vnd.ms-excel",
                            key=f"dl_{query_info['folder']}"
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
                    except:
                        st.error("Failed to load query")
            
            with col3:
                if st.button("Delete", key=f"del_{query_info['folder']}"):
                    try:
                        import shutil
                        shutil.rmtree(query_info['path'])
                        st.success("Deleted")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
    
    with st.sidebar.expander("Database Schema", expanded=False):
        schema_info = get_schema_info(query_engine.disk_conn)
        if schema_info is not None:
            st.dataframe(schema_info, use_container_width=True)
        else:
            st.write("Schema information not available")
    
    with st.sidebar.expander("Example Queries", expanded=True):
        examples = [
            "Show NIFTY50 index data for the last trading day",
            "Get highest and lowest prices for Reliance Futures in the past week",
            "Compare daily closing prices of HDFC Bank options with strike price 1600 for both CE and PE",
            "Calculate 5-minute returns for TCS stock over the last 3 trading sessions",
            "Show the top 5 highest volume intervals for NIFTY Bank options"
        ]
        
        for i, example in enumerate(examples):
            if st.button(f"Try Example {i+1}", key=f"ex_{i}"):
                st.session_state['natural_query'] = example
                st.rerun()

def render_main_interface(query_engine):
    st.header("Qode Data Fetcher")
    
    st.markdown("""
    This platform enables quant traders to query financial market data using natural language.
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
    
    natural_query = st.text_area(
        "Enter your query in natural language:", 
        value=st.session_state.get('natural_query', ''),
        placeholder="Example: Show NIFTY50 index data for the last trading day with 15-minute intervals",
        key="natural_query_input"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        generate_sql = st.button("Generate SQL", use_container_width=True)
    
    if generate_sql and natural_query:
        with st.spinner("Converting to SQL..."):
            sql_query = query_engine.nl_to_sql(natural_query)
            if sql_query:
                st.session_state['sql_query'] = sql_query
                st.session_state['natural_query'] = natural_query
            else:
                st.error("Failed to generate SQL query")
    
    st.subheader("SQL Query")
    sql_query = st.text_area(
        "SQL query:", 
        value=st.session_state.get('sql_query', ''),
        placeholder="SQL query will appear here after generation",
        height=150,
        key="sql_query_input"
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        run_query = st.button("Run Query", use_container_width=True, disabled=not sql_query)
    
    if run_query and sql_query:
        with st.spinner("Executing query..."):
            results, execution_time = query_engine.execute_query(
                sql_query, st.session_state.get('use_in_memory', False)
            )
            
            if results is not None:
                query_id, query_dir = query_engine.save_query(natural_query, sql_query, results)
                
                st.success(f"✅ Query executed successfully in {execution_time:.4f} seconds (ID: {query_id})")
                
                tab1, tab2, tab3 = st.tabs(["Results", "Profiling", "Visualization"])
                
                with tab1:
                    st.dataframe(results, use_container_width=True)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        csv = results.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            "Download CSV",
                            csv,
                            f"query_results_{query_id}.csv",
                            "text/csv",
                            key='download-csv'
                        )
                    
                    with col2:
                        excel_file = results.to_excel(index=False)
                        st.download_button(
                            "Download Excel",
                            excel_file,
                            f"query_results_{query_id}.xlsx",
                            "application/vnd.ms-excel",
                            key='download-excel'
                        )
                
                # with tab2:
                #     st.dataframe(profiling_info, use_container_width=True)
                
                with tab3:
                    try:
                        create_visualization(results)
                    except Exception as e:
                        st.warning("Could not create visualization automatically")
                        st.error(str(e))
            else:
                st.error(f"Query failed after {execution_time:.4f} seconds")
                # st.code(profiling_info, language="python")

def create_visualization(df):
    if df.empty or len(df) < 2:
        st.info("Not enough data points for visualization")
        return
    
    time_cols = [col for col in df.columns if any(x in col.lower() for x in ['time', 'date', 'timestamp'])]
    
    if not time_cols:
        st.bar_chart(df)
        return
    
    time_col = time_cols[0]
    df = df.sort_values(time_col)
    
    price_cols = [col for col in df.columns if any(x == col.lower() for x in ['o', 'h', 'l', 'c', 'open', 'high', 'low', 'close'])]
    vol_cols = [col for col in df.columns if any(x == col.lower() for x in ['v', 'volume'])]
    oi_cols = [col for col in df.columns if any(x in col.lower() for x in ['oi', 'open_interest'])]
    
    if len(price_cols) >= 4:
        ohlc_mapping = {}
        for col_type, patterns in [
            ('open', ['o', 'open']), 
            ('high', ['h', 'high']), 
            ('low', ['l', 'low']), 
            ('close', ['c', 'close'])
        ]:
            for pattern in patterns:
                matching_cols = [col for col in price_cols if col.lower() == pattern]
                if matching_cols:
                    ohlc_mapping[col_type] = matching_cols[0]
                    break
        
        if len(ohlc_mapping) == 4:
            fig = go.Figure(data=[go.Candlestick(
                x=df[time_col],
                open=df[ohlc_mapping['open']], 
                high=df[ohlc_mapping['high']],
                low=df[ohlc_mapping['low']], 
                close=df[ohlc_mapping['close']],
                name='OHLC'
            )])
            
            if vol_cols:
                fig.add_trace(go.Bar(
                    x=df[time_col],
                    y=df[vol_cols[0]],
                    name='Volume',
                    yaxis='y2',
                    marker_color='rgba(0, 0, 255, 0.3)'
                ))
                
                fig.update_layout(
                    yaxis2=dict(
                        title="Volume",
                        overlaying="y",
                        side="right",
                        showgrid=False
                    )
                )
            
            fig.update_layout(
                title='Market Data Visualization',
                xaxis_title='Time',
                yaxis_title='Price',
                height=500,
                xaxis_rangeslider_visible=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            return
    
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if time_col in numeric_cols:
        numeric_cols.remove(time_col)
    
    if numeric_cols:
        st.line_chart(df, x=time_col, y=numeric_cols[:3])