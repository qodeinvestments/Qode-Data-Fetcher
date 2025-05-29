import streamlit as st
from database import get_database_connection, get_database_info
from query_engine import QueryEngine
from sql_interface import sql_query_interface
from data_explorer import data_explorer

st.set_page_config(
    page_title="Qode Data Fetcher",
    layout="wide",
    initial_sidebar_state="expanded"
)

conn = get_database_connection()
query_engine = QueryEngine(conn)

def main():
    st.title("Qode Data Fetcher")
    st.markdown("---")
    
    if conn is None:
        st.error("Unable to connect to database. Please check your connection.")
        return
    
    st.sidebar.title("Navigation")
    
    tab_selection = st.sidebar.radio(
        "Select Interface:",
        ["SQL Query Interface", "Data Explorer"]
    )
    
    if tab_selection == "SQL Query Interface":
        sql_query_interface(query_engine)
    else:
        data_explorer(query_engine)
    
    db_size, db_modified = get_database_info()
    if db_size > 0:
        st.sidebar.metric("Database Size", f"{db_size:.2f} GB")
        if db_modified:
            st.sidebar.write(f"Last Updated: {db_modified.strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()