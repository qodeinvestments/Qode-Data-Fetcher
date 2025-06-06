import streamlit as st
import pandas as pd
from streamlit_lightweight_charts import renderLightweightCharts

def has_candlestick_columns(df):
    required_cols = ['timestamp', 'o', 'h', 'l', 'c']
    return all(col in df.columns.str.lower() for col in required_cols)

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
    
    chart_options = get_chart_options()
    series = build_chart_series(chart_data, volume_data)
    
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

def get_chart_options():
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

def build_chart_series(chart_data, volume_data):
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