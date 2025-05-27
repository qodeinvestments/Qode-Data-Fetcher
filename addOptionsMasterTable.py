import duckdb
import re

conn = duckdb.connect('qode_engine_data.db')

tables_df = conn.execute("""
    SELECT table_name
    FROM duckdb_tables()
    WHERE schema_name = 'market_data'
      AND (table_name LIKE '%_call' OR table_name LIKE '%_put')
""").fetchdf()

pattern = re.compile(r'^[^_]+_Options_(?P<underlying>\w+)_(?P<expiry>\d{8})_(?P<strike>\d+)_(?P<option_type>call|put)$')

tables_by_underlying = {}

for tbl in tables_df['table_name']:
    match = pattern.match(tbl)
    if not match:
        continue
    underlying = match.group('underlying')
    tables_by_underlying.setdefault(underlying, []).append({
        'table': tbl,
        'expiry': match.group('expiry'),
        'strike': int(match.group('strike')),
        'option_type': match.group('option_type')
    })

for underlying, tables in tables_by_underlying.items():
    master_table = f"market_data.options_master_{underlying.lower()}"

    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {master_table} (
        timestamp TIMESTAMP,
        c DOUBLE,
        oi BIGINT,
        strike INTEGER,
        expiry DATE,
        option_type VARCHAR,
        underlying VARCHAR
    )
    """)

    for t in tables:
        expiry_str = t['expiry']
        expiry = f"{expiry_str[:4]}-{expiry_str[4:6]}-{expiry_str[6:]}"
        strike = t['strike']
        option_type = t['option_type']
        tbl = t['table']

        cols = conn.execute(f"""
            PRAGMA table_info('market_data.{tbl}')
        """).fetchdf()['name'].tolist()

        select_cols = ['timestamp', 'c', 'oi']
        select_cols = [col for col in select_cols if col in cols]

        select_clause = ", ".join(select_cols)
        if select_clause:
            select_clause = select_clause + ", "
        insert_sql = f"""
        INSERT INTO {master_table} (timestamp, c, oi, strike, expiry, option_type, underlying)
        SELECT
            {select_clause}
            {strike} AS strike,
            DATE '{expiry}' AS expiry,
            '{option_type}' AS option_type,
            '{underlying}' AS underlying
        FROM market_data.{tbl}
        WHERE timestamp IS NOT NULL
        """
        try:
            conn.execute(insert_sql)
        except Exception as e:
            print(f"Error inserting from {tbl}: {e}")
