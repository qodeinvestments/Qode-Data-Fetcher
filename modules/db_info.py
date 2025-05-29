import streamlit as st
import pandas as pd

def render_db_info(conn):
    st.header("Database Information")
    
    st.markdown("""
    This page provides detailed information about the market data stored in the database.
    Explore tables, structure, and statistics to better understand available data.
    """)
    
    with st.spinner("Loading database information..."):
        tables = conn.execute("""
            SELECT database_name, schema_name, table_name, estimated_size, column_count
            FROM duckdb_tables()
            WHERE schema_name = 'market_data'
            ORDER BY table_name
            """).fetchdf()
    
    table_list = tables['table_name'].tolist()
    
    if not table_list:
        st.warning("⚠️ No tables found in the database.")
        return
    
    exchanges = set()
    instruments = set()
    underlyings = set()
    
    for table in table_list:
        parts = table.split('_')
        exchanges.add(parts[0])
        instruments.add(parts[1])
        underlyings.add(parts[2])
    
    st.markdown("### Database Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Tables", len(table_list))
    
    with col2:
        st.metric("Exchanges", len(exchanges))
    
    with col3:
        st.metric("Instruments", len(instruments))
    
    with col4:
        st.metric("Underlyings", len(underlyings))
    
    tab1, tab2 = st.tabs(["All Tables", "Table Explorer"])
    
    with tab1:
        st.markdown("### Database Tables")
        
        col_search, col_filter = st.columns([2, 1])
        with col_search:
            search_term = st.text_input("Search tables:", placeholder="Filter by table name...", key="table_search")

        filtered_tables = tables.copy()
        
        if search_term:
            filtered_tables = filtered_tables[filtered_tables['table_name'].str.contains(search_term, case=False)]
        
        st.dataframe(
            filtered_tables,
            use_container_width=True,
            column_config={
                "estimated_size": st.column_config.NumberColumn(
                    "Estimated Size",
                    format="%d bytes"
                ),
                "column_count": st.column_config.NumberColumn(
                    "Columns",
                    format="%d"
                )
            }
        )
    
    with tab2:
        st.markdown("### Table Structure Explorer")
        
        selected_table = st.selectbox(
            "Select a table to explore:",
            table_list,
            key="table_selector"
        )
        
        if selected_table:
            st.markdown("#### Sample Data (First 10 rows)")
            try:
                sample_data = conn.execute(f"SELECT * FROM market_data.{selected_table} LIMIT 10").fetchdf()
                if not sample_data.empty:
                    st.dataframe(sample_data, use_container_width=True, height=300)
                else:
                    st.info("No data available in this table")
            except Exception as e:
                st.error(f"Error fetching sample data: {str(e)}")
            
            try:
                row_count = conn.execute(f"SELECT COUNT(*) as count FROM market_data.{selected_table}").fetchdf()['count'][0]
                col_count = len(conn.execute(f"DESCRIBE market_data.{selected_table}").fetchdf())
                
                col_stats1, col_stats2 = st.columns(2)
                with col_stats1:
                    st.metric("Total Rows", f"{row_count:,}")
                with col_stats2:
                    st.metric("Total Columns", col_count)
            except Exception as e:
                st.error(f"Error fetching table statistics: {str(e)}")