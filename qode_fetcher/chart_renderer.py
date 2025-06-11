import streamlit as st
import pandas as pd
from streamlit_lightweight_charts import renderLightweightCharts

def normalize_column_name(df_columns, target_names):
    """Find column that matches any of the target names (case insensitive)"""
    df_cols_lower = [col.lower() for col in df_columns]
    for target in target_names:
        if target.lower() in df_cols_lower:
            return df_columns[df_cols_lower.index(target.lower())]
    return None

def has_candlestick_columns(df):
    """Check if dataframe has required columns for candlestick chart"""
    if df.empty:
        return False
    
    timestamp_variants = ['timestamp', 'time', 'date', 'datetime']
    open_variants = ['open', 'o']
    high_variants = ['high', 'h']
    low_variants = ['low', 'l']
    close_variants = ['close', 'c']
    
    timestamp_col = normalize_column_name(df.columns, timestamp_variants)
    open_col = normalize_column_name(df.columns, open_variants)
    high_col = normalize_column_name(df.columns, high_variants)
    low_col = normalize_column_name(df.columns, low_variants)
    close_col = normalize_column_name(df.columns, close_variants)
    
    return all([timestamp_col, open_col, high_col, low_col, close_col])

def has_line_chart_columns(df):
    """Check if dataframe has required columns for line chart"""
    if df.empty:
        return False
    
    timestamp_variants = ['timestamp', 'time', 'date', 'datetime']
    price_variants = ['ltp', 'close', 'c', 'price', 'value']
    
    timestamp_col = normalize_column_name(df.columns, timestamp_variants)
    price_col = normalize_column_name(df.columns, price_variants)
    
    return timestamp_col is not None and price_col is not None

def filter_last_week(df, timestamp_col):
    """Filter dataframe to show only last 7 days of data"""
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df_sorted = df.sort_values(timestamp_col, ascending=False)
    
    if len(df_sorted) == 0:
        return df_sorted
    
    latest_date = df_sorted[timestamp_col].iloc[0]
    week_ago = latest_date - pd.Timedelta(days=7)
    
    filtered_df = df_sorted[df_sorted[timestamp_col] >= week_ago]
    
    st.info(f"Displaying {len(filtered_df)} records of max 7 days")
    
    return filtered_df.sort_values(timestamp_col)

def render_candlestick_chart(df):
    """Render candlestick chart with flexible column names"""
    timestamp_variants = ['timestamp', 'time', 'date', 'datetime']
    open_variants = ['open', 'o']
    high_variants = ['high', 'h']
    low_variants = ['low', 'l']
    close_variants = ['close', 'c']
    volume_variants = ['volume', 'v', 'vol']

    timestamp_col = normalize_column_name(df.columns, timestamp_variants)
    o_col = normalize_column_name(df.columns, open_variants)
    h_col = normalize_column_name(df.columns, high_variants)
    l_col = normalize_column_name(df.columns, low_variants)
    c_col = normalize_column_name(df.columns, close_variants)
    v_col = normalize_column_name(df.columns, volume_variants)
    
    if not all([timestamp_col, o_col, h_col, l_col, c_col]):
        st.error("Missing required columns for candlestick chart")
        return
    
    df_filtered = filter_last_week(df, timestamp_col)
    
    chart_data = []
    volume_data = []
    
    for _, row in df_filtered.iterrows():
        timestamp = row[timestamp_col]
        
        if any(pd.isna(row[col]) for col in [o_col, h_col, l_col, c_col]):
            continue

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
        
        if v_col and pd.notna(row[v_col]):
            volume_data.append({
                'time': ts,
                'value': float(row[v_col])
            })
    
    chart_options = get_chart_options()
    series = build_candlestick_series(chart_data, volume_data)
    
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

def render_line_chart(df):
    """Render line chart for price data"""
    timestamp_variants = ['timestamp', 'time', 'date', 'datetime']
    price_variants = ['ltp', 'close', 'c', 'price', 'value']
    
    timestamp_col = normalize_column_name(df.columns, timestamp_variants)
    price_col = normalize_column_name(df.columns, price_variants)
    
    if not timestamp_col or not price_col:
        st.error("Missing required columns for line chart (timestamp and price)")
        return
    
    df_filtered = filter_last_week(df, timestamp_col)
    
    chart_data = []
    
    for _, row in df_filtered.iterrows():
        timestamp = row[timestamp_col]
        
        if isinstance(timestamp, str):
            try:
                ts = pd.to_datetime(timestamp).timestamp()
            except:
                ts = pd.Timestamp(timestamp).timestamp()
        else:
            ts = pd.Timestamp(timestamp).timestamp()
        
        if pd.notna(row[price_col]):
            chart_data.append({
                'time': ts,
                'value': float(row[price_col])
            })
    
    chart_options = get_chart_options()
    series = build_line_series(chart_data)
    
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
    ], key="line_chart")

def render_appropriate_chart(df):
    """Automatically choose and render the appropriate chart type based on available columns"""
    if has_candlestick_columns(df):
        st.subheader("Candlestick Chart")
        render_candlestick_chart(df)
    elif has_line_chart_columns(df):
        st.subheader("Price Line Chart")
        render_line_chart(df)
    else:
        st.info("Chart visualization not available - missing required columns for candlestick (timestamp, open, high, low, close) or line chart (timestamp, price/ltp/close)")

def get_chart_options():
    """Get common chart styling options"""
    return {
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

def build_candlestick_series(chart_data, volume_data):
    """Build candlestick chart series with optional volume"""
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
    
    return series

def build_line_series(chart_data):
    """Build line chart series"""
    return [{
        "type": "Line",
        "data": chart_data,
        "options": {
            "color": "#2196F3",
            "lineWidth": 2,
            "crosshairMarkerVisible": True,
            "crosshairMarkerRadius": 6,
            "crosshairMarkerBorderColor": "#ffffff",
            "crosshairMarkerBackgroundColor": "#2196F3",
            "lineType": 0
        }
    }]