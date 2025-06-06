import duckdb

DB_PATH = "qode_edw.db"
DATA_DIR = "cold_storage"

conn = duckdb.connect(DB_PATH)
print(f"Connected to DuckDB database at {DB_PATH}")

def optimize_existing_database(conn):
    print("Optimizing existing database tables...")
    
    tables = conn.execute("""
    SELECT table_name 
    FROM duckdb_tables()
    WHERE schema_name = 'market_data'
    """).fetchall()
    
    for table in tables:
        table_name = table[0]
        full_table_name = f"market_data.{table_name}"
        
        print(f"Optimizing table: {full_table_name}")
            
        index_name = f"idx_{table_name}_timestamp"
        print(f"Creating index {index_name} on {full_table_name}(timestamp)")
        
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table_name}(timestamp)")

    conn.execute("VACUUM;")
    print("Database vacuumed to reclaim space and optimize storage")

optimize_existing_database(conn)

print("Database optimization complete!")