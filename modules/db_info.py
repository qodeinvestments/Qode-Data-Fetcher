import streamlit as st
import pandas as pd

def render_db_info(conn):
    st.header("🗄️ Database Information")
    
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
    
    st.markdown("### 📊 Database Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📋 Total Tables", len(table_list))
    
    with col2:
        st.metric("🏛️ Exchanges", len(exchanges))
    
    with col3:
        st.metric("📈 Instruments", len(instruments))
    
    with col4:
        st.metric("💰 Underlyings", len(underlyings))
    
    tab1, tab2, tab3, tab4 = st.tabs(["📋 All Tables", "🔍 Table Explorer", "📊 Statistics", "🏷️ Categories"])
    
    with tab1:
        st.markdown("### Database Tables")
        
        col_search, col_filter = st.columns([2, 1])
        with col_search:
            search_term = st.text_input("🔍 Search tables:", placeholder="Filter by table name...", key="table_search")

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
        st.markdown("### 🔍 Table Structure Explorer")
        
        selected_table = st.selectbox(
            "Select a table to explore:",
            table_list,
            key="table_selector"
        )
        
        if selected_table:
            col_info, col_sample = st.columns([1, 1])
            
            with col_info:
                st.markdown(f"#### Table: `{selected_table}`")
                try:
                    schema_info = conn.execute(f"DESCRIBE market_data.{selected_table}").fetchdf()
                    st.markdown("**Column Information:**")
                    st.dataframe(schema_info, use_container_width=True, height=300)
                except Exception as e:
                    st.error(f"Error fetching schema: {str(e)}")
            
            with col_sample:
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
                    st.metric("📊 Total Rows", f"{row_count:,}")
                with col_stats2:
                    st.metric("📋 Total Columns", col_count)
            except Exception as e:
                st.error(f"Error fetching table statistics: {str(e)}")
    
    with tab3:
        st.markdown("### 📊 Database Statistics")
        
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.markdown("#### Tables by Size")
            size_data = tables.copy()
            size_data['size_mb'] = size_data['estimated_size'] / (1024 * 1024)
            size_data = size_data.sort_values('size_mb', ascending=True).tail(10)
            
            st.bar_chart(
                size_data.set_index('table_name')['size_mb'],
                height=300
            )
        
        with col_chart2:
            st.markdown("#### Column Count Distribution")
            col_dist = tables['column_count'].value_counts().sort_index()
            st.bar_chart(col_dist, height=300)
        
        st.markdown("#### Summary Statistics")
        summary_stats = pd.DataFrame({
            'Metric': [
                'Total Size (MB)',
                'Average Size (MB)',
                'Total Columns',
                'Average Columns per Table',
                'Largest Table Size (MB)',
                'Smallest Table Size (MB)'
            ],
            'Value': [
                f"{tables['estimated_size'].sum() / (1024 * 1024):.2f}",
                f"{tables['estimated_size'].mean() / (1024 * 1024):.2f}",
                f"{tables['column_count'].sum():,}",
                f"{tables['column_count'].mean():.1f}",
                f"{tables['estimated_size'].max() / (1024 * 1024):.2f}",
                f"{tables['estimated_size'].min() / (1024 * 1024):.2f}"
            ]
        })
        st.dataframe(summary_stats, use_container_width=True, hide_index=True)
    
    with tab4:
        st.markdown("### 🏷️ Data Categories")
        
        col_cat1, col_cat2, col_cat3 = st.columns(3)
        
        with col_cat1:
            st.markdown("#### 🏛️ Exchanges")
            for exchange in sorted(exchanges):
                exchange_tables = [t for t in table_list if t.startswith(f"{exchange}_")]
                st.markdown(f"**{exchange.upper()}** ({len(exchange_tables)} tables)")
                with st.expander(f"View {exchange} tables"):
                    for table in sorted(exchange_tables):
                        st.text(f"• {table}")
        
        with col_cat2:
            st.markdown("#### 📈 Instruments")
            for instrument in sorted(instruments):
                instrument_tables = [t for t in table_list if f"_{instrument}_" in t]
                st.markdown(f"**{instrument.upper()}** ({len(instrument_tables)} tables)")
                with st.expander(f"View {instrument} tables"):
                    for table in sorted(instrument_tables):
                        st.text(f"• {table}")
        
        with col_cat3:
            st.markdown("#### 💰 Underlyings")
            for underlying in sorted(underlyings):
                underlying_tables = [t for t in table_list if t.endswith(f"_{underlying}")]
                st.markdown(f"**{underlying.upper()}** ({len(underlying_tables)} tables)")
                with st.expander(f"View {underlying} tables"):
                    for table in sorted(underlying_tables):
                        st.text(f"• {table}")
    
    st.markdown("---")
    
    col_export1, col_export2, col_refresh = st.columns([1, 1, 1])
    
    with col_export1:
        tables_csv = tables.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📄 Export Table List (CSV)",
            tables_csv,
            "database_tables.csv",
            "text/csv",
            use_container_width=True
        )

    with col_refresh:
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()