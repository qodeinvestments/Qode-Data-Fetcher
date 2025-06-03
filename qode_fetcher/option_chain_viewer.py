import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from data_utils import get_underlyings

def option_chain_viewer(query_engine):
    st.title("Option Chain Viewer")
    
    col1, col2, col3, col4 = st.columns(4)
    
    exchange_options = ["NSE", "BSE"]
    
    with col1:
        selected_exchange = st.selectbox("Exchange:", exchange_options, key="oc_exchange")
    
    with col2:
        underlying_options = get_underlyings(query_engine, selected_exchange, "Index")
        if underlying_options:
            selected_underlying = st.selectbox("Underlying:", underlying_options, key="oc_underlying")
        else:
            st.warning(f"No index data available for {selected_exchange}")
            return
    
    with col3:
        selected_date = st.date_input(
            "Date:",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            key="oc_date"
        )
    
    with col4:
        selected_time = st.time_input(
            "Time:",
            value=datetime.strptime("15:30:00", "%H:%M:%S").time(),
            key="oc_time"
        )
    
    selected_datetime = datetime.combine(selected_date, selected_time)
    
    st.markdown("---")
    st.markdown("### Filters & Settings")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        expiry_filter = st.selectbox(
            "Expiry Filter:",
            ["All", "Current Week", "Current Month", "Next Month"],
            key="oc_expiry"
        )
    
    with col2:
        option_type_filter = st.selectbox(
            "Option Type:",
            ["All", "Call", "Put"],
            key="oc_type"
        )
    
    with col3:
        strike_range = st.slider(
            "Strike Range (% from spot):",
            min_value=1,
            max_value=50,
            value=1,
            step=5,
            key="oc_range"
        )
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        fetch_button = st.button("Fetch Option Chain", type="primary", key="oc_fetch")
    
    with col2:
        show_charts = st.checkbox("Show Charts", value=True, key="show_charts")
    
    if fetch_button:
        fetch_enhanced_option_chain(
            query_engine,
            selected_exchange,
            selected_underlying,
            selected_datetime,
            expiry_filter,
            option_type_filter,
            strike_range,
            show_charts
        )

def get_spot_price(query_engine, exchange, underlying, target_datetime):
    underlying_table = f"{exchange}_Index_{underlying}"
    
    query = f"""
    SELECT o, h, l, c
    FROM market_data.{underlying_table}
    WHERE timestamp = '{target_datetime}'
    ORDER BY timestamp DESC
    LIMIT 1
    """
    
    result, _, error = query_engine.execute_query(query)
    
    if error or len(result) == 0:
        return None
    
    return float(result.iloc[0]['c'])

def build_option_chain_query(exchange, underlying, target_datetime, expiry_filter, option_type_filter, spot_price, strike_range):
    base_query = f"""
    SELECT 
        timestamp,
        expiry,
        symbol,
        strike,
        option_type,
        underlying,
        open,
        high,
        low,
        close,
        volume,
        open_interest
    FROM market_data.{exchange}_Options_{underlying}_Master WHERE timestamp = '{target_datetime}'
    """
    
    if expiry_filter != "All":
        if expiry_filter == "Current Week":
            base_query += f" AND expiry >= '{target_datetime.date()}' AND expiry <= '{(target_datetime + timedelta(days=7)).date()}'"
        elif expiry_filter == "Current Month":
            base_query += f" AND expiry >= '{target_datetime.date()}' AND expiry <= '{(target_datetime + timedelta(days=30)).date()}'"
        elif expiry_filter == "Next Month":
            base_query += f" AND expiry >= '{(target_datetime + timedelta(days=30)).date()}' AND expiry <= '{(target_datetime + timedelta(days=60)).date()}'"
    
    if option_type_filter != "All":
        base_query += f" AND option_type = '{option_type_filter.lower()}'"
    
    if spot_price and strike_range:
        lower_bound = spot_price * (1 - strike_range / 100)
        upper_bound = spot_price * (1 + strike_range / 100)
        base_query += f" AND strike BETWEEN {lower_bound} AND {upper_bound}"
    
    final_query = f"""
    WITH latest_data AS (
        {base_query}
    ),
    ranked_data AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY expiry, strike, option_type ORDER BY timestamp DESC) as rn
        FROM latest_data
    )
    SELECT 
        expiry,
        symbol,
        strike,
        option_type,
        underlying,
        open,
        high,
        low,
        close,
        volume,
        open_interest,
        timestamp
    FROM ranked_data
    WHERE rn = 1
    ORDER BY expiry, strike, option_type
    """
    
    return final_query

def create_professional_option_chain(df, spot_price):
    """Create a professional-looking option chain similar to NSE/Sensibull"""
    if len(df) == 0:
        return df
    
    df['moneyness'] = ((df['strike'] - spot_price) / spot_price * 100).round(2)
    
    call_df = df[df['option_type'] == 'call'].copy()
    put_df = df[df['option_type'] == 'put'].copy()
    
    option_chain = pd.DataFrame()
    strikes = sorted(df['strike'].unique())
    
    for strike in strikes:
        call_data = call_df[call_df['strike'] == strike]
        put_data = put_df[put_df['strike'] == strike]
        
        row = {
            'Strike': strike,
            'Moneyness': round((strike - spot_price) / spot_price * 100, 2)
        }
        
        if len(call_data) > 0:
            call_row = call_data.iloc[0]
            row.update({
                'Call_OI': call_row['open_interest'],
                'Call_Volume': call_row['volume'],
                'Call_LTP': call_row['close'],
                'Call_Bid': round(call_row['close'] * 0.98, 2),
                'Call_Ask': round(call_row['close'] * 1.02, 2),
            })
        else:
            row.update({k: 0 for k in ['Call_OI', 'Call_Volume', 'Call_IV', 'Call_LTP', 'Call_Bid', 'Call_Ask', 'Call_Delta']})
        
        if len(put_data) > 0:
            put_row = put_data.iloc[0]
            row.update({
                'Put_Bid': round(put_row['close'] * 0.98, 2),
                'Put_Ask': round(put_row['close'] * 1.02, 2),
                'Put_LTP': put_row['close'],
                'Put_Volume': put_row['volume'],
                'Put_OI': put_row['open_interest']
            })
        else:
            row.update({k: 0 for k in ['Put_Delta', 'Put_Bid', 'Put_Ask',
                                      'Put_LTP', 'Put_IV', 'Put_Volume', 'Put_OI']})
        
        option_chain = pd.concat([option_chain, pd.DataFrame([row])], ignore_index=True)
    
    return option_chain

def display_professional_chain(df, spot_price):
    """Display option chain in professional format with color coding"""
    
    def highlight_moneyness(val):
        if abs(val) <= 2:
            return 'background-color: #000000; font-weight: bold'
        elif val > 0:
            return 'background-color: #f8d7da'
        else:
            return 'background-color: #d1ecf1'
    
    styled_df = df.style.applymap(highlight_moneyness, subset=['Moneyness']) \
                        .format({
                            'Strike': '₹{:.0f}',
                            'Moneyness': '{:.1f}%',
                            'Call_LTP': '₹{:.2f}',
                            'Put_LTP': '₹{:.2f}',
                            'Call_Bid': '₹{:.2f}',
                            'Call_Ask': '₹{:.2f}',
                            'Put_Bid': '₹{:.2f}',
                            'Put_Ask': '₹{:.2f}',
                            'Call_IV': '{:.1f}%',
                            'Put_IV': '{:.1f}%',
                            'Call_OI': '{:,.0f}',
                            'Put_OI': '{:,.0f}',
                            'Call_Volume': '{:,.0f}',
                            'Put_Volume': '{:,.0f}'
                        })
    
    st.dataframe(styled_df, use_container_width=True, height=600)

def create_option_charts(df, spot_price):
    """Create interactive charts for option analysis"""
    
    fig_oi = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Open Interest Distribution', 'Volume Distribution', 'Put-Call Ratio'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    call_data = df[df['option_type'] == 'call']
    put_data = df[df['option_type'] == 'put']
    
    fig_oi.add_trace(
        go.Bar(x=call_data['strike'], y=call_data['open_interest'], 
               name='Call OI', marker_color='green', opacity=0.7),
        row=1, col=1
    )
    fig_oi.add_trace(
        go.Bar(x=put_data['strike'], y=put_data['open_interest'], 
               name='Put OI', marker_color='red', opacity=0.7),
        row=1, col=1
    )
    
    fig_oi.add_trace(
        go.Bar(x=call_data['strike'], y=call_data['volume'], 
               name='Call Volume', marker_color='lightgreen', opacity=0.7),
        row=1, col=2
    )
    fig_oi.add_trace(
        go.Bar(x=put_data['strike'], y=put_data['volume'], 
               name='Put Volume', marker_color='lightcoral', opacity=0.7),
        row=1, col=2
    )
    
    all_strikes = sorted(df['strike'].unique())
    
    pcr_data = []
    for strike in all_strikes:
        call_oi = call_data[call_data['strike'] == strike]['open_interest'].sum()
        put_oi = put_data[put_data['strike'] == strike]['open_interest'].sum()
        pcr = put_oi / call_oi if call_oi > 0 else 0
        pcr_data.append(pcr)
    
    fig_oi.add_trace(
        go.Scatter(x=all_strikes, y=pcr_data, mode='lines+markers',
                  name='PCR', line=dict(color='orange', width=3)),
        row=2, col=2
    )
    
    for row in [1, 2]:
        for col in [1, 2]:
            fig_oi.add_vline(x=spot_price, line_dash="dash", line_color="blue",
                           annotation_text=f"Spot: ₹{spot_price:.0f}", row=row, col=col)
    
    fig_oi.update_layout(height=800, showlegend=True, title_text="Option Chain Analytics")
    
    return fig_oi
    
def fetch_enhanced_option_chain(query_engine, exchange, underlying, target_datetime, 
                               expiry_filter, option_type_filter, strike_range, show_charts):
    
    with st.spinner("🔍 Fetching spot price..."):
        spot_price = get_spot_price(query_engine, exchange, underlying, target_datetime)
    
    if spot_price is None:
        st.error("❌ Could not fetch spot price for the selected underlying and datetime")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Spot Price", f"₹{spot_price:,.2f}")
    
    with st.spinner("Fetching option chain data..."):
        query = build_option_chain_query(
            exchange, underlying, target_datetime, 
            expiry_filter, option_type_filter, spot_price, strike_range
        )
        
        result, exec_time, error = query_engine.execute_query(query)
        
        if error:
            st.error(f"❌ Query Error: {error}")
            return
        
        if len(result) == 0:
            st.info("ℹ️ No option data found for the selected criteria")
            return
        
        st.success(f"✅ Query executed successfully in {exec_time:.2f} seconds")
        
        st.markdown("### Option Chain")
        if option_type_filter == "All":
            formatted_chain = create_professional_option_chain(result, spot_price)
            display_professional_chain(formatted_chain, spot_price)
        else:
            st.dataframe(result, use_container_width=True, height=600)
            
        st.markdown("#### Quick Stats")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Contracts", len(result))
        with col2:
            st.metric("Total Volume", f"{result['volume'].sum():,.0f}")
        with col3:
            st.metric("Total OI", f"{result['open_interest'].sum():,.0f}") 
        
        if show_charts and len(result) > 0:
            st.markdown("---")
            st.markdown("### Interactive Analytics")
            
            chart_fig = create_option_charts(result, spot_price)
            st.plotly_chart(chart_fig, use_container_width=True)

        st.markdown("---")
        st.markdown("### Download Data")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            csv = result.to_csv(index=False)
            st.download_button(
                "Raw Data (CSV)",
                csv,
                f"option_chain_{underlying}_{target_datetime.strftime('%Y%m%d_%H%M')}.csv",
                key="download_raw"
            )
        
        with col2:
            if option_type_filter == "All":
                formatted_csv = formatted_chain.to_csv(index=False)
                st.download_button(
                    "Formatted Chain (CSV)",
                    formatted_csv,
                    f"formatted_chain_{underlying}_{target_datetime.strftime('%Y%m%d_%H%M')}.csv",
                    key="download_formatted"
                )
        
        st.markdown("---")
        st.markdown("### Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            expiry_summary = result.groupby('expiry').agg({
                'volume': 'sum',
                'open_interest': 'sum',
                'strike': 'count'
            }).rename(columns={'strike': 'contracts'})
            
            st.markdown("#### Expiry-wise Summary")
            st.dataframe(expiry_summary, use_container_width=True)
        
        with col2:
            strike_summary = result.groupby('strike').agg({
                'volume': 'sum',
                'open_interest': 'sum'
            }).sort_values('open_interest', ascending=False).head(10)
            
            st.markdown("#### Top Strikes by OI")
            st.dataframe(strike_summary, use_container_width=True)