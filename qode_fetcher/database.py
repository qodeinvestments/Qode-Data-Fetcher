import streamlit as st
import pandas as pd
import duckdb
import datetime
import os
from typing import List, Dict, Optional, Tuple

CHUNK_SIZE = 100000

def get_database_connection():
    try:
        conn = duckdb.connect("./qode_edw.db")
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute("PRAGMA profiling_output='profile.json'")
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")

def get_database_info():
    try:
        db_path = "./qode_edw.db"
        if os.path.exists(db_path):
            size_bytes = os.path.getsize(db_path)
            size_gb = size_bytes / (1024**3)
            modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(db_path))
            return size_gb, modified_time
        return 0, None
    except:
        return 0, None

def get_all_tables(conn) -> List[str]:
    """Get all table names from the market_data schema with caching"""
    try:
        if conn is None:
            return []
        
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        ORDER BY table_name
        """
        result = conn.execute(query).fetchdf()
        return result['table_name'].tolist()
    except Exception as e:
        st.error(f"Error fetching table names: {e}")
        return []

def fuzzy_search_tables(all_tables: List[str], search_term: str, limit: int = 50) -> List[str]:
    """
    Perform fuzzy search on table names with multiple strategies for better matching
    """
    if not search_term or not all_tables:
        return all_tables[:limit]
    
    search_term = search_term.upper().strip()
    
    exact_matches = []
    starts_with_matches = []
    contains_matches = []
    fuzzy_matches = []
    
    for table in all_tables:
        table_upper = table.upper()
        
        if table_upper == search_term:
            exact_matches.append(table)
        elif table_upper.startswith(search_term):
            starts_with_matches.append(table)
        elif search_term in table_upper:
            contains_matches.append(table)
        else:
            search_parts = search_term.replace('_', ' ').split()
            if len(search_parts) > 1:
                if all(part in table_upper for part in search_parts):
                    fuzzy_matches.append(table)
            else:
                match_score = sum(1 for char in search_term if char in table_upper)
                if match_score >= len(search_term) * 0.7:
                    fuzzy_matches.append(table)
    
    combined_results = exact_matches + starts_with_matches + contains_matches + fuzzy_matches
    
    seen = set()
    unique_results = []
    for table in combined_results:
        if table not in seen:
            seen.add(table)
            unique_results.append(table)
            if len(unique_results) >= limit:
                break
    
    return unique_results

def search_tables_by_pattern(all_tables: List[str], exchange: str = "", 
                           instrument: str = "", underlying: str = "", limit: int = 50) -> List[str]:
    """
    Search tables by specific pattern components
    """
    filtered_tables = all_tables
    
    if exchange:
        exchange_upper = exchange.upper()
        filtered_tables = [t for t in filtered_tables if t.upper().startswith(exchange_upper)]
    
    if instrument:
        instrument_upper = instrument.upper()
        filtered_tables = [t for t in filtered_tables if instrument_upper in t.upper()]
    
    if underlying:
        underlying_upper = underlying.upper()
        filtered_tables = [t for t in filtered_tables if underlying_upper in t.upper()]
    
    return filtered_tables[:limit]

def _get_valid_table_name(conn, table_name: str) -> Optional[str]:
    schemas_to_try = ["market_data", "qode_edw", ""]
    
    for schema in schemas_to_try:
        full_name = f"{schema}.{table_name}" if schema else table_name
        try:
            conn.execute(f"SELECT 1 FROM {full_name} LIMIT 1").fetchdf()
            return full_name
        except:
            continue
    return None

def _has_timestamp_column(conn, full_table_name: str) -> bool:
    try:
        schema_result = conn.execute(f"DESCRIBE SELECT * FROM {full_table_name} LIMIT 0").fetchdf()
        return 'timestamp' in [col.lower() for col in schema_result['column_name'].tolist()]
    except:
        return False

def _get_basic_metadata(conn, full_table_name: str) -> Dict:
    try:
        count_result = conn.execute(f"SELECT COUNT(*) as total_rows FROM {full_table_name}").fetchdf()
        total_rows = int(count_result.iloc[0]['total_rows']) if len(count_result) > 0 else 0
        
        schema_result = conn.execute(f"DESCRIBE SELECT * FROM {full_table_name} LIMIT 0").fetchdf()
        total_columns = len(schema_result)
        
        return {'total_rows': total_rows, 'total_columns': total_columns}
    except Exception as e:
        return {'total_rows': 0, 'total_columns': 0}

def _get_timestamp_metadata(conn, full_table_name: str) -> Dict:
    try:
        timestamp_result = conn.execute(f"""
        SELECT 
            MIN(timestamp) as earliest_timestamp,
            MAX(timestamp) as latest_timestamp
        FROM {full_table_name}
        WHERE timestamp IS NOT NULL
        """).fetchdf()
        
        earliest_ts = timestamp_result.iloc[0]['earliest_timestamp'] if len(timestamp_result) > 0 else None
        latest_ts = timestamp_result.iloc[0]['latest_timestamp'] if len(timestamp_result) > 0 else None
        
        return {'earliest_timestamp': earliest_ts, 'latest_timestamp': latest_ts}
    except:
        return {'earliest_timestamp': None, 'latest_timestamp': None}

def _calculate_frequency(conn, full_table_name: str) -> Tuple[str, Optional[int]]:
    try:
        freq_result = conn.execute(f"""
        WITH ordered_data AS (
            SELECT timestamp,
                   LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
            FROM {full_table_name}
            WHERE timestamp IS NOT NULL
            ORDER BY timestamp
            LIMIT 100
        ),
        time_diffs AS (
            SELECT 
                EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as diff_seconds
            FROM ordered_data
            WHERE prev_timestamp IS NOT NULL
        ),
        adjusted_diffs AS (
            SELECT 
                CASE 
                    WHEN diff_seconds = 172800 
                         AND EXTRACT(DOW FROM prev_timestamp) = 5 
                         AND EXTRACT(DOW FROM timestamp) = 1
                    THEN 86400
                    ELSE diff_seconds
                END as adj_diff_seconds
            FROM time_diffs
        )
        SELECT ROUND(AVG(adj_diff_seconds)) as avg_interval_seconds
        FROM adjusted_diffs
        """).fetchdf()
        
        if len(freq_result) > 0 and freq_result.iloc[0]['avg_interval_seconds'] is not None:
            avg_seconds = int(freq_result.iloc[0]['avg_interval_seconds'])
            
            frequency_map = {
                60: "1min",
                300: "5min", 
                900: "15min",
                3600: "1hour",
                86400: "1day"
            }
            
            if avg_seconds in frequency_map:
                return frequency_map[avg_seconds], avg_seconds
            elif avg_seconds < 60:
                return f"{avg_seconds}s", avg_seconds
            elif avg_seconds < 3600:
                mins = avg_seconds / 60
                return f"{mins:.2f} mins", avg_seconds
            else:
                days = avg_seconds / 86400
                return f"{days:.2f} days", avg_seconds
                
        return "Unknown", None
    except:
        return "Unknown", None

def _calculate_missing_timestamps(earliest_ts, latest_ts, total_rows: int, avg_seconds: Optional[int]) -> Optional[int]:
    if not all([earliest_ts, latest_ts, avg_seconds]) or avg_seconds <= 0:
        return None
    
    try:
        total_expected = int((latest_ts - earliest_ts).total_seconds() // avg_seconds) + 1
        missing = total_expected - total_rows
        return max(0, missing)
    except:
        return None

def get_table_metadata(conn, table_name: str) -> Optional[Dict]:
    if conn is None:
        return None
    
    full_table_name = _get_valid_table_name(conn, table_name)
    if not full_table_name:
        st.error(f"Table {table_name} not found in any schema")
        return None
    
    metadata = _get_basic_metadata(conn, full_table_name)
    
    if _has_timestamp_column(conn, full_table_name):
        timestamp_meta = _get_timestamp_metadata(conn, full_table_name)
        frequency, avg_seconds = _calculate_frequency(conn, full_table_name)
        missing_timestamps = _calculate_missing_timestamps(
            timestamp_meta['earliest_timestamp'],
            timestamp_meta['latest_timestamp'], 
            metadata['total_rows'],
            avg_seconds
        )
        
        metadata.update(timestamp_meta)
        metadata['frequency'] = frequency
        metadata['missing_timestamps'] = missing_timestamps
    else:
        metadata.update({
            'earliest_timestamp': None,
            'latest_timestamp': None,
            'frequency': "No timestamp column",
            'missing_timestamps': None
        })
    
    return metadata

def get_table_sample_data(conn, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if conn is None:
        return pd.DataFrame(), pd.DataFrame()
    
    full_table_name = _get_valid_table_name(conn, table_name)
    if not full_table_name:
        st.error(f"Table {table_name} not found in any schema")
        return pd.DataFrame(), pd.DataFrame()
    
    try:
        has_timestamp = _has_timestamp_column(conn, full_table_name)
        
        if has_timestamp:
            first_10 = conn.execute(f"SELECT * FROM {full_table_name} ORDER BY timestamp ASC LIMIT 10").fetchdf()
            last_10 = conn.execute(f"SELECT * FROM {full_table_name} ORDER BY timestamp DESC LIMIT 10").fetchdf()
        else:
            first_10 = conn.execute(f"SELECT * FROM {full_table_name} LIMIT 10").fetchdf()
            last_10 = conn.execute(f"SELECT * FROM {full_table_name} ORDER BY ROWID DESC LIMIT 10").fetchdf()
        
        return first_10, last_10
        
    except Exception as e:
        st.error(f"Error fetching sample data for {table_name}: {e}")
        return pd.DataFrame(), pd.DataFrame()

def get_existing_table_schema(conn, table_name):
    try:
        full_table_name = _get_valid_table_name(conn, table_name)
        if not full_table_name:
            return None
        
        query = f"DESCRIBE {full_table_name}"
        result = conn.execute(query).fetchdf()
        return result
    except Exception as e:
        st.error(f"Failed to retrieve table schema for '{table_name}': {str(e)}")
        return None

def validate_table_compatibility(conn, table_name, new_df):
    try:
        full_table_name = _get_valid_table_name(conn, table_name)
        if not full_table_name:
            return [f"Table {table_name} not found"], set(), set(), set()
            
        existing_schema = conn.execute(f"DESCRIBE {full_table_name}").fetchdf()
        existing_columns = set(existing_schema['column_name'].tolist())
        new_columns = set(new_df.columns.tolist())
        
        missing_columns = existing_columns - new_columns
        extra_columns = new_columns - existing_columns
        
        compatibility_issues = []
        
        if missing_columns:
            compatibility_issues.append(f"Missing columns in new data: {list(missing_columns)}")
        
        if extra_columns:
            compatibility_issues.append(f"Extra columns in new data (will be ignored): {list(extra_columns)}")
        
        common_columns = existing_columns.intersection(new_columns)
        for col in common_columns:
            existing_type = existing_schema[existing_schema['column_name'] == col]['column_type'].iloc[0]
            new_type = str(new_df[col].dtype)
            
            type_compatible = False
            if 'VARCHAR' in existing_type.upper() or 'TEXT' in existing_type.upper():
                type_compatible = True
            elif 'INTEGER' in existing_type.upper() or 'BIGINT' in existing_type.upper():
                type_compatible = new_df[col].dtype in ['int32', 'int64', 'Int32', 'Int64']
            elif 'DOUBLE' in existing_type.upper() or 'FLOAT' in existing_type.upper():
                type_compatible = new_df[col].dtype in ['float32', 'float64', 'int32', 'int64']
            elif 'TIMESTAMP' in existing_type.upper() or 'DATE' in existing_type.upper():
                type_compatible = pd.api.types.is_datetime64_any_dtype(new_df[col])
            
            if not type_compatible:
                compatibility_issues.append(f"Type mismatch for column '{col}': existing={existing_type}, new={new_type}")
        
        return compatibility_issues, missing_columns, extra_columns, common_columns
        
    except Exception as e:
        return [f"Error validating compatibility: {str(e)}"], set(), set(), set()

def prepare_dataframe_for_append(new_df, existing_columns, missing_columns, extra_columns):
    aligned_df = new_df.copy()
    
    for col in missing_columns:
        aligned_df[col] = None
    
    aligned_df = aligned_df.reindex(columns=list(existing_columns))
    
    return aligned_df

def append_data_to_table(conn, df: pd.DataFrame, table_name: str) -> bool:
    try:
        full_table_name = _get_valid_table_name(conn, table_name)
        if not full_table_name:
            st.error(f"Table {table_name} not found")
            return False
            
        total_rows = len(df)
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        chunks_processed = 0
        total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        conn.execute("BEGIN TRANSACTION")
        
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk = df.iloc[i:i + CHUNK_SIZE]
            
            conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM chunk")
            
            chunks_processed += 1
            progress = chunks_processed / total_chunks
            progress_bar.progress(progress)
            status_text.text(f"Appending chunk {chunks_processed}/{total_chunks}")
        
        conn.execute("COMMIT")
        
        progress_bar.progress(1.0)
        status_text.text("Data appended successfully!")
        
        return True
        
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        st.error(f"Error during data append: {str(e)}")
        return False

def download_table_data(conn, table_name):
    try:
        full_table_name = _get_valid_table_name(conn, table_name)
        if not full_table_name:
            st.error(f"Table {table_name} not found")
            return None
            
        query = f"SELECT * FROM {full_table_name}"
        df = conn.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Failed to download table data: {str(e)}")
        return None
    
def ingest_data_to_duckdb(df: pd.DataFrame, table_name: str, file_info: Dict, conn) -> bool:
    temp_parquet_path = "./temp.parquet"
    
    try:
        total_rows = len(df)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("Saving data to temporary parquet file...")
        df.to_parquet(temp_parquet_path, index=False)
        progress_bar.progress(0.2)
        
        status_text.text("Preparing database transaction...")
        conn.execute("BEGIN TRANSACTION")
        
        status_text.text("Creating table structure...")
        first_chunk = df.head(1)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM first_chunk WHERE 1=0")
        progress_bar.progress(0.4)
        
        status_text.text("Inserting data from parquet file...")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{temp_parquet_path}')")
        progress_bar.progress(0.8)
        
        status_text.text("Committing transaction to database...")
        conn.execute("COMMIT")
        
        status_text.text("Verifying table creation and data integrity...")
        verification_result = conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchdf()
        actual_rows = verification_result.iloc[0]['row_count']
        
        if actual_rows != total_rows:
            raise Exception(f"Data integrity check failed: expected {total_rows} rows, got {actual_rows}")
        
        progress_bar.progress(1.0)
        status_text.text(f"✅ Successfully ingested {total_rows:,} rows into '{table_name}'")
        
        return True
        
    except Exception as e:
        status_text.text("❌ Rolling back transaction due to error...")
        try:
            conn.execute("ROLLBACK")
        except:
            pass
        st.error(f"Error during data ingestion: {str(e)}")
        return False
    finally:
        try:
            if os.path.exists(temp_parquet_path):
                os.remove(temp_parquet_path)
        except:
            pass