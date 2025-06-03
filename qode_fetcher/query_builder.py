def build_query(table_name, columns, start_datetime, end_datetime, resample_interval, instrument_type):
    if resample_interval == "Raw Data":
        columns_str = ", ".join([f'"{col}"' for col in columns])
        return f"""
SELECT {columns_str}
FROM market_data.{table_name}
WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
ORDER BY timestamp"""
    
    time_bucket_expressions = {
        "1s": "date_trunc('second', timestamp)",
        "1m": "date_trunc('minute', timestamp)", 
        "5m": "date_trunc('minute', timestamp) - INTERVAL (EXTRACT(MINUTE FROM timestamp)::int % 5) MINUTE",
        "15m": "date_trunc('minute', timestamp) - INTERVAL (EXTRACT(MINUTE FROM timestamp)::int % 15) MINUTE",
        "30m": "date_trunc('minute', timestamp) - INTERVAL (EXTRACT(MINUTE FROM timestamp)::int % 30) MINUTE",
        "1h": "date_trunc('hour', timestamp)",
        "1d": "date_trunc('day', timestamp)"
    }
    
    time_bucket_expr = time_bucket_expressions.get(resample_interval, "date_trunc('minute', timestamp)")
    
    agg_columns = []
    group_by_expr = None
    
    for col in columns:
        col_quoted = f'"{col}"'
        if col.lower() == "timestamp":
            group_by_expr = f"{time_bucket_expr} AS {col_quoted}"
            agg_columns.append(group_by_expr)
        elif col.lower() in ["o", "open"]:
            agg_columns.append(f"(array_agg({col_quoted} ORDER BY timestamp))[1] AS {col_quoted}")
        elif col.lower() in ["h", "high"]:
            agg_columns.append(f"MAX({col_quoted}) AS {col_quoted}")
        elif col.lower() in ["l", "low"]:
            agg_columns.append(f"MIN({col_quoted}) AS {col_quoted}")
        elif col.lower() in ["c", "close"]:
            agg_columns.append(f"(array_agg({col_quoted} ORDER BY timestamp DESC))[1] AS {col_quoted}")
        elif col.lower() in ["v", "volume"]:
            agg_columns.append(f"SUM({col_quoted}) AS {col_quoted}")
        elif col.lower() == "oi":
            if instrument_type == "Futures":
                agg_columns.append(f"(array_agg({col_quoted} ORDER BY timestamp DESC))[1] AS {col_quoted}")
            else:
                agg_columns.append(f"AVG({col_quoted}) AS {col_quoted}")
        elif col.lower() == "symbol":
            agg_columns.append(f"(array_agg({col_quoted}))[1] AS {col_quoted}")
        elif col.lower() == "delivery_cycle":
            agg_columns.append(f"(array_agg({col_quoted}))[1] AS {col_quoted}")
        else:
            agg_columns.append(f"AVG({col_quoted}) AS {col_quoted}")

    if group_by_expr is None:
        group_by_expr = f"{time_bucket_expr} AS \"time_bucket\""
        agg_columns.insert(0, group_by_expr)

    agg_columns_str = ",\n    ".join(agg_columns)
    
    resampled_query = f"""
SELECT 
    {agg_columns_str}
FROM market_data.{table_name}
WHERE timestamp BETWEEN '{start_datetime}' AND '{end_datetime}'
GROUP BY {time_bucket_expr}
ORDER BY {time_bucket_expr}"""
    
    return resampled_query