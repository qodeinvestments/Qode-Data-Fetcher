import streamlit as st
from streamlit_option_menu import option_menu
from database import get_database_connection, get_database_info
from query_engine import QueryEngine
from sql_interface import sql_query_interface
from data_explorer import data_explorer
from auth import require_authentication, show_user_info_sidebar

conn = get_database_connection()
query_engine = QueryEngine(conn)

st.set_page_config(
    page_title="Qode Data Fetcher",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    if not require_authentication():
        return
    
    if conn is None:
        st.error("Unable to connect to database. Please check your connection.")
        return

    with st.sidebar:
        st.title("Qode Data Fetcher")
        st.markdown("---")
        
        selected = option_menu(
            menu_title=None,
            options=["SQL Query Interface", "Data Explorer"],
            icons=["code-slash", "database"],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={
                "container": {"padding": "5!important"},
                "icon": {"color": "white", "font-size": "20px"},
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "0px",
                    "--hover-color": "#8D8D8D"
                },
                "nav-link-selected": {"background-color": "#ff4b4b"},
            }
        )
        
        st.markdown("---")
        db_size, db_modified = get_database_info()
        if db_size > 0:
            st.metric("Database Size", f"{db_size:.2f} GB")
            if db_modified:
                st.write(f"Last Updated: {db_modified.strftime('%Y-%m-%d %H:%M')}")
        
        show_user_info_sidebar()
    
    if selected == "SQL Query Interface":
        sql_query_interface(query_engine)
    elif selected == "Data Explorer":
        data_explorer(query_engine)

if __name__ == "__main__":
    main()