import streamlit as st
import pandas as pd
import duckdb
import datetime
import os

def get_database_connection():
    try:
        conn = duckdb.connect("~/../mnt/disk2/qode_edw.db", read_only=True)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")

def get_database_info():
    try:
        db_path = "qode_edw.db"
        if os.path.exists(db_path):
            size_bytes = os.path.getsize(db_path)
            size_gb = size_bytes / (1024**3)
            modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(db_path))
            return size_gb, modified_time
        return 0, None
    except:
        return 0, None

def get_table_metadata(conn, table_name):
    try:
        if conn is None:
            return None
            
        full_table_name = f"market_data.{table_name}"
        
        count_query = f"SELECT COUNT(*) as total_rows FROM {full_table_name}"
        count_result = conn.execute(count_query).fetchdf()
        total_rows = int(count_result.iloc[0]['total_rows']) if len(count_result) > 0 else 0
        
        schema_query = f"DESCRIBE SELECT * FROM {full_table_name} LIMIT 0"
        schema_result = conn.execute(schema_query).fetchdf()
        total_columns = len(schema_result)
        
        timestamp_query = f"""
        SELECT 
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp
        FROM {full_table_name}
        WHERE timestamp IS NOT NULL
        """
        timestamp_result = conn.execute(timestamp_query).fetchdf()
        earliest_ts = timestamp_result.iloc[0]['earliest_timestamp'] if len(timestamp_result) > 0 else None
        latest_ts = timestamp_result.iloc[0]['latest_timestamp'] if len(timestamp_result) > 0 else None
        
        frequency = "Unknown"
        try:
            freq_query = f"""
            WITH ordered_data AS (
                SELECT timestamp,
                       LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM {full_table_name}
                WHERE timestamp IS NOT NULL
                ORDER BY timestamp
                LIMIT 100
            ),
            time_diffs AS (
                SELECT EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as diff_seconds
                FROM ordered_data
                WHERE prev_timestamp IS NOT NULL
            )
            SELECT ROUND(AVG(diff_seconds)) as avg_interval_seconds
            FROM time_diffs
            """
            
            freq_result = conn.execute(freq_query).fetchdf()
            if len(freq_result) > 0 and freq_result.iloc[0]['avg_interval_seconds'] is not None:
                avg_seconds = int(freq_result.iloc[0]['avg_interval_seconds'])
                if avg_seconds == 60:
                    frequency = "1min"
                elif avg_seconds == 300:
                    frequency = "5min"
                elif avg_seconds == 900:
                    frequency = "15min"
                elif avg_seconds == 3600:
                    frequency = "1hour"
                elif avg_seconds == 86400:
                    frequency = "1day"
                elif avg_seconds < 60:
                    frequency = f"{avg_seconds}s"
                else:
                    frequency = f"{avg_seconds}s"
        except Exception as freq_error:
            frequency = "Unknown"
        
        return {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'earliest_timestamp': earliest_ts,
            'latest_timestamp': latest_ts,
            'frequency': frequency
        }
    except Exception as e:
        st.error(f"Error getting metadata for {table_name}: {e}")
        return None

def get_table_sample_data(conn, table_name):
    try:
        if conn is None:
            return pd.DataFrame(), pd.DataFrame()
            
        full_table_name = f"market_data.{table_name}"
        
        first_10_query = f"SELECT * FROM {full_table_name} ORDER BY timestamp ASC LIMIT 10"
        first_10 = conn.execute(first_10_query).fetchdf()
        
        last_10_query = f"SELECT * FROM {full_table_name} ORDER BY timestamp DESC LIMIT 10"
        last_10 = conn.execute(last_10_query).fetchdf()
        
        return first_10, last_10
    except Exception as e:
        st.error(f"Error fetching sample data for {table_name}: {e}")
        return pd.DataFrame(), pd.DataFrame()