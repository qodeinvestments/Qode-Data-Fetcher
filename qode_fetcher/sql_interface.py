import streamlit as st
from streamlit_lightweight_charts import renderLightweightCharts
import pandas as pd

def sql_query_interface(query_engine):
    st.header("SQL Query Interface")
    
    st.subheader("Query Editor")
    
    sample_queries = {
        "Show all tables": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'market_data' ORDER BY table_name",
        "Index data sample": "SELECT * FROM market_data.NSE_Index_NIFTY LIMIT 10",
        "Generate daily Bollinger Bands (20-day window) and RSI (14-day), with conditions to find potential reversal zones on NIFTY Index from 2023": """WITH resampled AS (
        SELECT
            date_trunc('day', timestamp) AS day,
            AVG(c) AS close
        FROM market_data.NSE_Index_NIFTY
        WHERE timestamp BETWEEN '2023-01-01' AND '2025-04-30'
        GROUP BY day
        ORDER BY day
        ),
        rsi_calc AS (
        SELECT
            day,
            close,
            LAG(close) OVER (ORDER BY day) AS prev_close
        FROM resampled
        ),
        rsi_diff AS (
        SELECT
            day,
            close,
            CASE WHEN close - prev_close > 0 THEN close - prev_close ELSE 0 END AS gain,
            CASE WHEN prev_close - close > 0 THEN prev_close - close ELSE 0 END AS loss
        FROM rsi_calc
        ),
        rsi_smoothed AS (
        SELECT
            day,
            close,
            AVG(gain) OVER w AS avg_gain,
            AVG(loss) OVER w AS avg_loss
        FROM rsi_diff
        WINDOW w AS (ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
        ),
        bollinger AS (
        SELECT
            day,
            close,
            AVG(close) OVER w AS ma20,
            STDDEV(close) OVER w AS std20,
            avg_gain,
            avg_loss
        FROM rsi_smoothed
        WINDOW w AS (ORDER BY day ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
        )
        SELECT
        day,
        close,
        ma20,
        ma20 + 2 * std20 AS upper_band,
        ma20 - 2 * std20 AS lower_band,
        100 - (100 / (1 + avg_gain / NULLIF(avg_loss, 0))) AS rsi_14
        FROM bollinger
        ORDER BY day;""",
                "Intraday 5-min bars with VWAP calculation for BANKNIFTY Futures": """WITH bars AS (
        SELECT
            date_trunc('minute', timestamp) - INTERVAL (EXTRACT(MINUTE FROM timestamp) % 5) MINUTE AS time_bin,
            SUM(c * v) / NULLIF(SUM(v), 0) AS vwap,
            SUM(v) AS volume
        FROM market_data.NSE_Futures_BANKNIFTY
        WHERE timestamp BETWEEN '2024-04-01' AND '2024-04-02'
        GROUP BY time_bin
        )
        SELECT * FROM bars ORDER BY time_bin;""",
                "Compare daily close of NIFTY index and NIFTY futures for divergence detection (spot-premium)": """WITH index_data AS (
        SELECT
            date_trunc('day', timestamp) AS day,
            AVG(c) AS index_close
        FROM market_data.NSE_Index_NIFTY
        GROUP BY day
        ),
        futures_data AS (
        SELECT
            date_trunc('day', timestamp) AS day,
            AVG(c) AS fut_close
        FROM market_data.NSE_Futures_NIFTY
        GROUP BY day
        )
        SELECT
        i.day,
        i.index_close,
        f.fut_close,
        f.fut_close - i.index_close AS premium,
        CASE WHEN f.fut_close - i.index_close > 0 THEN 'Contango' ELSE 'Backwardation' END AS market_state
        FROM index_data i
        JOIN futures_data f ON i.day = f.day
        ORDER BY i.day;"""
    }
    
    selected_sample = st.selectbox("Sample Queries (optional):", [""] + list(sample_queries.keys()))
    
    if selected_sample:
        default_query = sample_queries[selected_sample]
    else:
        default_query = ""
    
    sql_query = st.text_area(
        "Enter SQL Query:",
        value=default_query,
        height=150,
        help="Only SELECT queries are allowed for security."
    )
    
    col1, col2 = st.columns([1, 4])
    with col1:
        execute_button = st.button("Execute Query", type="primary")
    
    if execute_button and sql_query.strip():
        with st.spinner("Executing query..."):
            result, exec_time, error = query_engine.execute_query(sql_query)
            
            if error:
                st.error(f"Query Error: {error}")
            else:
                st.success(f"Query executed successfully in {exec_time:.2f} seconds")
                
                if len(result) > 0:
                    st.write(f"**Results: {len(result)} rows**")
                    st.dataframe(result)
                             
                    col1, col2 = st.columns(2)
                    with col1:
                        csv = result.to_csv(index=False)
                        st.download_button("Download as CSV", csv, "query_results.csv")
                    
                    if has_candlestick_columns(result):
                        st.subheader("Candlestick Preview")
                        render_candlestick_chart(result)
                else:
                    st.info("Query returned no results")

def filter_last_week(df, timestamp_col):
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df_sorted = df.sort_values(timestamp_col, ascending=False)
    
    if len(df_sorted) == 0:
        return df_sorted
    
    latest_date = df_sorted[timestamp_col].iloc[0]
    week_ago = latest_date - pd.Timedelta(days=7)
    
    filtered_df = df_sorted[df_sorted[timestamp_col] >= week_ago]
    
    st.info(f"Displaying {len(filtered_df)} records of max 7 days")
    
    return filtered_df.sort_values(timestamp_col)

def has_candlestick_columns(df):
    required_cols = ['timestamp', 'o', 'h', 'l', 'c']
    return all(col in df.columns.str.lower() for col in required_cols)

def render_candlestick_chart(df):
    df_cols = df.columns.str.lower()
    
    timestamp_col = next((col for col in df.columns if col.lower() == 'timestamp'), None)
    o_col = next((col for col in df.columns if col.lower() == 'o'), None)
    h_col = next((col for col in df.columns if col.lower() == 'h'), None)
    l_col = next((col for col in df.columns if col.lower() == 'l'), None)
    c_col = next((col for col in df.columns if col.lower() == 'c'), None)
    v_col = next((col for col in df.columns if col.lower() == 'v'), None)
    
    if not all([timestamp_col, o_col, h_col, l_col, c_col]):
        st.error("Missing required columns for candlestick chart")
        return
    
    df_filtered = filter_last_week(df, timestamp_col)
    
    chart_data = []
    volume_data = []
    
    for _, row in df_filtered.iterrows():
        timestamp = row[timestamp_col]
        
        if isinstance(timestamp, str):
            try:
                ts = pd.to_datetime(timestamp).timestamp()
            except:
                ts = pd.Timestamp(timestamp).timestamp()
        else:
            ts = pd.Timestamp(timestamp).timestamp()
        
        chart_data.append({
            'time': ts,
            'open': float(row[o_col]),
            'high': float(row[h_col]),
            'low': float(row[l_col]),
            'close': float(row[c_col])
        })
        
        if v_col:
            volume_data.append({
                'time': ts,
                'value': float(row[v_col])
            })
    
    chart_options = {
        "layout": {
            "textColor": "#e0e6f0",
            "background": {"type": "solid", "color": "#01040D"}
        },
        "grid": {
            "vertLines": {"color": "#25304b"},
            "horzLines": {"color": "#25304b"}
        },
        "timeScale": {
            "timeVisible": True,
            "secondsVisible": False,
            "rightOffset": 0,
            "barSpacing": 10,
            "minBarSpacing": 5,
            "lockVisibleTimeRangeOnResize": True
        },
        "crosshair": {
            "mode": 0,
            "vertLine": {"color": "#758696", "style": 0, "width": 1, "labelBackgroundColor": "#131c33"},
            "horzLine": {"color": "#758696", "style": 0, "width": 1, "labelBackgroundColor": "#131c33"}
        }
    }

    series = [{
        "type": "Candlestick",
        "data": chart_data,
        "options": {
            "upColor": "#26a69a",
            "downColor": "#ef5350",
            "borderVisible": False,
            "wickUpColor": "#26a69a",
            "wickDownColor": "#ef5350"
        }
    }]

    if volume_data:
        series.append({
            "type": "Histogram",
            "data": volume_data,
            "options": {
                "color": "#26a69a",
                "priceFormat": {"type": "volume"},
                "priceScaleId": "volume"
            },
            "priceScale": {
                "scaleMargins": {"top": 0.8, "bottom": 0}
            }
        })

    if chart_data:
        times = [item['time'] for item in chart_data]
        min_time = min(times)
        max_time = max(times)
        chart_options["timeScale"]["visibleTimeRange"] = {"from": min_time, "to": max_time}

    renderLightweightCharts([
        {
            "chart": chart_options,
            "series": series
        }
    ], key="candlestick_chart")