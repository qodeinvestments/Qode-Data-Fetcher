import streamlit as st

def render_db_info(conn):
    st.header("Database Information")
    
    st.markdown("""
    This page provides detailed information about the market data stored in the database.
    Explore tables, structure, and statistics to better understand available data.
    """)
    
    tables = conn.execute("""
        SELECT database_name, schema_name, table_name, estimated_size, column_count
        FROM duckdb_tables()
        WHERE schema_name = 'market_data'
        ORDER BY table_name
        """).fetchdf()
    
    table_list = tables['table_name'].tolist()
    
    if not table_list:
        st.warning("No tables found in the database.")
        return
    
    exchanges = set()
    instruments = set()
    underlyings = set()
    
    for table in table_list:
        parts = table.split('_')
        exchanges.add(parts[0])
        instruments.add(parts[1])
        underlyings.add(parts[2])
        
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Tables", len(table_list))
    
    with col2:
        st.metric("Exchanges", len(exchanges))
    
    with col3:
        st.metric("Instruments", len(instruments))
    
    with col4:
        st.metric("Underlyings", len(underlyings))
        
    st.dataframe(tables)