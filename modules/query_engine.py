import os
import json
import time
import uuid
import datetime
import traceback
import pandas as pd
import streamlit as st
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
import re

class QueryEngine:
    def __init__(self, disk_conn, memory_conn):
        self.disk_conn = disk_conn
        self.memory_conn = memory_conn
        self._initialize_model()

    def _initialize_model(self):
        model_name = "defog/sqlcoder-7b-2"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name, device_map="auto", torch_dtype=torch.float16
        )

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
        """Check if a query is read-only (SELECT only)."""
        query = re.sub(r'--.*?(\n|$)|/\*.*?\*/', '', query, flags=re.DOTALL)
        
        modify_pattern = r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|UPSERT|MERGE|COPY|WITH\s+RECURSIVE|GRANT|REVOKE)\b'
        if re.search(modify_pattern, query, re.IGNORECASE):
            return False
            
        if not re.match(r'^\s*(SELECT|WITH\b(?!\s+RECURSIVE\b))', query, re.IGNORECASE):
            return False
            
        if re.match(r'^\s*WITH\b(?!\s+RECURSIVE\b)', query, re.IGNORECASE):
            after_with = re.sub(r'^\s*WITH\b(?!\s+RECURSIVE\b).*?(\bSELECT\b|$)', '', query, flags=re.DOTALL|re.IGNORECASE)
            if not re.match(r'^\s*SELECT\b', after_with, re.IGNORECASE):
                return False
                
        return True

    def execute_query(self, sql_query, in_memory=False):
        if not self._is_read_only_query(sql_query):
            error_message = "ERROR: Only SELECT queries are allowed. Data modification operations are blocked."
            return None, 0, error_message
            
        conn = self.memory_conn if in_memory else self.disk_conn
        start_time = time.time()
        
        try:
            conn.execute("PRAGMA enable_profiling")
            conn.execute("PRAGMA profiling_mode='detailed'")
            
            result = conn.execute(sql_query).fetchdf()
            
            if not isinstance(result, pd.DataFrame):
                result = pd.DataFrame(result)
            if result.empty:
                result = pd.DataFrame(columns=["No results found"])
            
            execution_time = time.time() - start_time
            
            self._log_query(sql_query, execution_time, in_memory, len(result))
            
            return result, execution_time
        except Exception as e:
            error_traceback = traceback.format_exc()
            execution_time = time.time() - start_time
            return None, execution_time, error_traceback

    def _log_query(self, sql_query, execution_time, in_memory, row_count):
        query_log = {
            "timestamp": datetime.datetime.now().isoformat(),
            "query": sql_query,
            "execution_time": execution_time,
            "in_memory": in_memory,
            "row_count": row_count,
            "user_id": st.session_state.get('user_id', 'anonymous')
        }
        
        log_dir = "query_logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/query_log_{datetime.datetime.now().strftime('%Y%m%d')}.jsonl"
        
        with open(log_file, "a") as f:
            f.write(json.dumps(query_log) + "\n")

    def save_query(self, natural_query, sql_query, results_df=None):
        query_id = str(uuid.uuid4())[:8]
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        query_dir = f"{st.session_state['user_folder']}/{query_id}_{timestamp}"
        os.makedirs(query_dir, exist_ok=True)
        
        with open(f"{query_dir}/input.txt", "w") as f:
            f.write(natural_query)
        
        with open(f"{query_dir}/query.sql", "w") as f:
            f.write(sql_query)
        
        if results_df is not None and not results_df.empty:
            results_df.to_excel(f"{query_dir}/results.xlsx", index=False)
            
        return query_id, query_dir

    def get_user_queries(self, limit=10):
        if not st.session_state.get('user_folder') or not os.path.exists(st.session_state['user_folder']):
            return []
            
        query_folders = []
        for folder in os.listdir(st.session_state['user_folder']):
            folder_path = f"{st.session_state['user_folder']}/{folder}"
            if os.path.isdir(folder_path):
                try:
                    with open(f"{folder_path}/input.txt", "r") as f:
                        natural_query = f.read()
                    
                    has_results = os.path.exists(f"{folder_path}/results.xlsx")
                    
                    query_folders.append({
                        "folder": folder,
                        "path": folder_path,
                        "query": natural_query,
                        "has_results": has_results,
                        "timestamp": folder.split("_", 1)[1] if "_" in folder else folder
                    })
                except Exception:
                    pass
        
        return sorted(query_folders, key=lambda x: x["timestamp"], reverse=True)[:limit]