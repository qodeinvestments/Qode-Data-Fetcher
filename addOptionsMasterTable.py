import duckdb
import re
from datetime import datetime

conn = duckdb.connect('qode_edw.db')

print("Fetching options tables...")
tables_df = conn.execute("""
    SELECT table_name
    FROM duckdb_tables()
    WHERE schema_name = 'market_data'
      AND (table_name LIKE '%_call' OR table_name LIKE '%_put')
""").fetchdf()

print(f"Found {len(tables_df)} options tables")

pattern = re.compile(r'^[^_]+_Options_(?P<underlying>\w+)_(?P<expiry>\d{8})_(?P<strike>\d+)_(?P<option_type>call|put)$')

tables_by_underlying = {}
for tbl in tables_df['table_name']:
    match = pattern.match(tbl)
    if not match:
        print(f"Skipping table {tbl} - doesn't match expected pattern")
        continue
    
    underlying = match.group('underlying')
    tables_by_underlying.setdefault(underlying, []).append({
        'table': tbl,
        'expiry': match.group('expiry'),
        'strike': int(match.group('strike')),
        'option_type': match.group('option_type')
    })

print(f"Found options data for {len(tables_by_underlying)} underlyings: {list(tables_by_underlying.keys())}")

for underlying, tables in tables_by_underlying.items():
    print(f"\nProcessing {underlying} options ({len(tables)} tables)...")
    
    master_table = f"market_data.options_master_{underlying.lower()}"

    conn.execute(f"""
        CREATE TABLE {master_table} (
            timestamp TIMESTAMP,
            symbol VARCHAR,
            strike INTEGER,
            expiry DATE,
            option_type VARCHAR,
            underlying VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            oi BIGINT
        )
    """)
    
    inserted_count = 0
    for t in tables:
        expiry_str = t['expiry']
        expiry = f"{expiry_str[:4]}-{expiry_str[4:6]}-{expiry_str[6:]}"
        strike = t['strike']
        option_type = t['option_type']
        tbl = t['table']
        
        exp_date = datetime.strptime(expiry_str, '%Y%m%d')
        symbol_suffix = 'CE' if option_type == 'call' else 'PE'
        symbol = f"{underlying}{exp_date.strftime('%y%b').upper()}{strike}{symbol_suffix}"
        
        cols = conn.execute(f"""
            PRAGMA table_info('market_data.{tbl}')
        """).fetchdf()['name'].tolist()
        
        column_mapping = {
            'timestamp': 'timestamp',
            'o': 'open', 'open': 'open',
            'h': 'high', 'high': 'high', 
            'l': 'low', 'low': 'low',
            'c': 'close', 'close': 'close',
            'v': 'volume', 'volume': 'volume',
            'oi': 'oi', 'open_interest': 'oi'
        }
        
        select_parts = []
        for db_col, master_col in column_mapping.items():
            if db_col in cols:
                select_parts.append(f"{db_col}")
                break
        else:
            select_parts.append("NULL")
        
        select_clause = f"""
            {select_parts[0] if select_parts else 'timestamp'} as timestamp,
            '{symbol}' as symbol,
            {strike} as strike,
            DATE '{expiry}' as expiry,
            '{option_type}' as option_type,
            '{underlying}' as underlying
        """
        
        ohlcv_columns = ['open', 'high', 'low', 'close', 'volume', 'oi']
        ohlcv_mappings = [
            (['o', 'open'], 'open'),
            (['h', 'high'], 'high'),
            (['l', 'low'], 'low'),
            (['c', 'close'], 'close'),
            (['v', 'volume'], 'volume'),
            (['oi', 'open_interest'], 'oi')
        ]
        
        for possible_cols, target_col in ohlcv_mappings:
            found_col = None
            for col_variant in possible_cols:
                if col_variant in cols:
                    found_col = col_variant
                    break
            
            if found_col:
                select_clause += f", {found_col} as {target_col}"
            else:
                select_clause += f", NULL as {target_col}"
        
        insert_sql = f"""
            INSERT INTO {master_table} 
            SELECT {select_clause}
            FROM market_data.{tbl}
            WHERE timestamp IS NOT NULL
        """
        
        try:
            result = conn.execute(insert_sql)
            row_count = conn.execute(f"SELECT COUNT(*) FROM market_data.{tbl} WHERE timestamp IS NOT NULL").fetchone()[0]
            inserted_count += row_count
            print(f"  ✓ {tbl}: {row_count} rows")
        except Exception as e:
            print(f"  ✗ Error inserting from {tbl}: {e}")
    
    print(f"Sorting {master_table} by timestamp...")
    temp_table = f"{master_table}_temp"
    
    conn.execute(f"""
        CREATE TABLE {temp_table} AS
        SELECT * FROM {master_table}
        ORDER BY timestamp, symbol, strike, expiry
    """)
    
    conn.execute(f"DROP TABLE {master_table}")
    conn.execute(f"ALTER TABLE {temp_table} RENAME TO {master_table.split('.')[-1]}")
    
    final_count = conn.execute(f"SELECT COUNT(*) FROM {master_table}").fetchone()[0]
    unique_symbols = conn.execute(f"SELECT COUNT(DISTINCT symbol) FROM {master_table}").fetchone()[0]
    date_range = conn.execute(f"""
        SELECT 
            MIN(timestamp) as min_date,
            MAX(timestamp) as max_date
        FROM {master_table}
    """).fetchone()
    
    print(f"✅ {underlying} master table created:")
    print(f"   - Total rows: {final_count:,}")
    print(f"   - Unique symbols: {unique_symbols}")
    print(f"   - Date range: {date_range[0]} to {date_range[1]}")

print("\n🎉 All options master tables created successfully!")

print("\nSummary of created master tables:")
master_tables = conn.execute("""
    SELECT table_name
    FROM duckdb_tables()
    WHERE schema_name = 'market_data'
      AND table_name LIKE 'options_master_%'
""").fetchdf()

for table in master_tables['table_name']:
    count = conn.execute(f"SELECT COUNT(*) FROM market_data.{table}").fetchone()[0]
    print(f"  - market_data.{table}: {count:,} rows")

conn.close()