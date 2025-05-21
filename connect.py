import duckdb

DB_PATH = "qode_engine_data.db"
DATA_DIR = "cold_storage"

db_path = DB_PATH
conn = duckdb.connect(db_path)
print(f"Connected to DuckDB database at {db_path}")