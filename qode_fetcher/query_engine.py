import streamlit as st
import pandas as pd
import time
import re
import json
from datetime import datetime

class QueryEngine:
    def __init__(self, disk_conn):
        self.disk_conn = disk_conn
        self.log_file = 'query_logs.jsonl'

    def _log_query(self, user_email, query, status, execution_time, error=None):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "user": user_email,
            "query": query.strip(),
            "status": status,
            "execution_time": execution_time,
            "error": error
        }
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')

    def _is_read_only_query(self, query):
        query = re.sub(r'--.*?(\n|$)|/\*.*?\*/', '', query, flags=re.DOTALL)
        modify_pattern = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|UPSERT|MERGE|COPY|GRANT|REVOKE)\b'
        return not re.search(modify_pattern, query, re.IGNORECASE)

    def execute_query(self, sql_query):
        user_email = st.session_state.get('email', 'unknown')
        
        if not self._is_read_only_query(sql_query):
            error_msg = "ERROR: Only SELECT queries are allowed."
            self._log_query(user_email, sql_query, "FAILED", 0, error_msg)
            return None, 0, error_msg

        start_time = time.time()
        try:
            result = self.disk_conn.execute(sql_query).fetchdf()
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            if result.empty:
                result = pd.DataFrame(columns=["No results found"])
                        
            execution_time = time.time() - start_time
            self._log_query(user_email, sql_query, "SUCCESS", execution_time)
            return result, execution_time, None
        except Exception as e:
            execution_time = time.time() - start_time
            error_str = str(e)
            self._log_query(user_email, sql_query, "ERROR", execution_time, error_str)
            return None, execution_time, error_str

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