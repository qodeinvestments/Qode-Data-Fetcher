import streamlit as st
import pandas as pd
import time
import re

class QueryEngine:
    def __init__(self, disk_conn):
        self.disk_conn = disk_conn

    def _is_read_only_query(self, query):
        query = re.sub(r'--.*?(\n|$)|/\*.*?\*/', '', query, flags=re.DOTALL)
        modify_pattern = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|UPSERT|MERGE|COPY|GRANT|REVOKE)\b'
        return not re.search(modify_pattern, query, re.IGNORECASE)

    def execute_query(self, sql_query):
        if not self._is_read_only_query(sql_query):
            return None, 0, "ERROR: Only SELECT queries are allowed."

        start_time = time.time()
        try:
            result = self.disk_conn.execute(sql_query).fetchdf()
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            if result.empty:
                result = pd.DataFrame(columns=["No results found"])
            
            execution_time = time.time() - start_time
            return result, execution_time, None
        except Exception as e:
            execution_time = time.time() - start_time
            return None, execution_time, str(e)

    def get_available_tables(self):
        try:
            query = """
            SELECT table_name, table_schema 
            FROM information_schema.tables 
            WHERE table_schema = 'market_data'
            ORDER BY table_name
            """
            result = self.disk_conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return pd.DataFrame()

    def get_table_schema(self, table_name):
        try:
            query = f"DESCRIBE SELECT * FROM {table_name} LIMIT 0"
            result = self.disk_conn.execute(query).fetchdf()
            return result
        except Exception as e:
            st.error(f"Error fetching schema for {table_name}: {e}")
            return pd.DataFrame()