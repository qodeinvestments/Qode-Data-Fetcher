import os
import io
import json
import time
import uuid
import datetime
import traceback
import pandas as pd
import streamlit as st
import re
from concurrent.futures import ThreadPoolExecutor

class QueryEngine:
    def __init__(self, disk_conn):
        self.disk_conn = disk_conn

    def nl_to_sql(self, prompt):
        system_prompt = """### Task
Generate a SQL query for DuckDB that answers the following question.
IMPORTANT: Only generate read-only SELECT queries. Do not generate any queries that modify data.

### Database Schema
The database has tables such as:
- options_nifty_20240101_18000_C
- index_nifty50
- futures_banknifty_20240101

Each has columns:
- timestamp, o, h, l, c, v, oi

### Question
""" + prompt + "\n\n### SQL Query\n"

        inputs = self.tokenizer(system_prompt, return_tensors="pt")
        
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=256,
            temperature=0.2,
            top_p=0.95,
            do_sample=False,
            pad_token_id=self.tokenizer.eos_token_id,
        )
        
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)

        if "```sql" in decoded:
            decoded = decoded.split("```sql")[1].split("```")[0].strip()
        elif "SELECT" in decoded:
            decoded = decoded.split("SELECT", 1)[1]
            decoded = "SELECT " + decoded.split("###")[0].strip()

        query = decoded.strip()
        if not self._is_read_only_query(query):
            return "ERROR: Only SELECT queries are allowed."
            
        return query

    def _is_read_only_query(self, query):
        query = re.sub(r'--.*?(\n|$)|/\*.*?\*/', '', query, flags=re.DOTALL)
        
        modify_pattern = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|UPSERT|MERGE|COPY|GRANT|REVOKE)\b'
        if re.search(modify_pattern, query, re.IGNORECASE):
            return False
                
        return True

    def execute_query(self, sql_query):
        if not self._is_read_only_query(sql_query):
            error_message = "ERROR: Only SELECT queries are allowed. Data modification operations are blocked."
            return None, 0, error_message

        start_time = time.time()
        
        try:
            result = self.disk_conn.execute(sql_query).fetchdf()
            
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            if result.empty:
                result = pd.DataFrame(columns=["No results found"])
            
            execution_time = time.time() - start_time
            
            self._log_query(sql_query, execution_time, len(result))
            
            return result, execution_time
        except Exception as e:
            error_traceback = traceback.format_exc()
            execution_time = time.time() - start_time
            return None, execution_time, error_traceback

    def _log_query(self, sql_query, execution_time, row_count):
        query_log = {
            "timestamp": datetime.datetime.now().isoformat(),
            "query": sql_query,
            "execution_time": execution_time,
            "row_count": row_count,
            "user_id": st.session_state.get('user_id', 'anonymous')
        }
        
        log_file = f"query_logs/query_log_{datetime.datetime.now().strftime('%Y%m%d')}.jsonl"
        
        with open(log_file, "a") as f:
            f.write(json.dumps(query_log) + "\n")

    def dataframe_to_excel(self, df):
        output = io.BytesIO()
        df.to_excel(output, index=False, sheet_name='Results')
        output.seek(0)
        return output

    def dataframe_to_parquet(self, df):
        output = io.BytesIO()
        df.to_parquet(output, index=False)
        output.seek(0)
        return output

    def save_query(self, natural_query, sql_query, results_df=None, query_name="Query"):
        query_id = str(uuid.uuid4())[:8]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        sanitized_name = "".join([c if c.isalnum() else "_" for c in query_name])
        folder_name = f"{query_id}_{timestamp}_{sanitized_name}"
        query_dir = f"{st.session_state['user_folder']}/{folder_name}"
        
        os.makedirs(query_dir, exist_ok=True)
        
        with open(f"{query_dir}/input.txt", "w") as f:
            f.write(natural_query)
        
        with open(f"{query_dir}/query.sql", "w") as f:
            f.write(sql_query)
            
        with open(f"{query_dir}/metadata.json", "w") as f:
            metadata = {
                "name": query_name,
                "created_at": timestamp,
                "query_id": query_id
            }
            json.dump(metadata, f)
        
        if results_df is not None and not results_df.empty:
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [
                    executor.submit(results_df.to_excel, f"{query_dir}/results.xlsx", index=False),
                    executor.submit(results_df.to_csv, f"{query_dir}/results.csv", index=False),
                    executor.submit(results_df.to_parquet, f"{query_dir}/results.parquet", index=False)
                ]
                for future in futures:
                    future.result()
            
        return query_id, query_dir

    def get_user_queries(self, limit=10):
        if not st.session_state.get('user_folder') or not os.path.exists(st.session_state['user_folder']):
            return []
            
        query_folders = []
        for folder in os.listdir(st.session_state['user_folder']):
            folder_path = f"{st.session_state['user_folder']}/{folder}"
            
            if not os.path.isdir(folder_path):
                continue
                
            try:
                with open(f"{folder_path}/input.txt", "r") as f:
                    natural_query = f.read()
                
                query_name = folder.split("_", 2)[2].replace("_", " ") if len(folder.split("_")) > 2 else folder
                
                if os.path.exists(f"{folder_path}/metadata.json"):
                    with open(f"{folder_path}/metadata.json", "r") as f:
                        metadata = json.load(f)
                        query_name = metadata.get("name", query_name)
                
                has_results = all(
                    os.path.exists(f"{folder_path}/results.{ext}") 
                    for ext in ["xlsx", "csv", "parquet"]
                )
                
                query_folders.append({
                    "folder": folder,
                    "path": folder_path,
                    "query": natural_query,
                    "has_results": has_results,
                    "timestamp": folder.split("_", 2)[1] if "_" in folder else folder,
                    "name": query_name
                })
            except Exception:
                pass
                
        return sorted(query_folders, key=lambda x: x["timestamp"], reverse=True)[:limit]