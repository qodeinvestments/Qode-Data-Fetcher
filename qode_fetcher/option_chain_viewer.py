import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
from data_utils import get_underlyings

def option_chain_viewer(query_engine):
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 2rem;
        text-align: center;
        border-bottom: 3px solid #3b82f6;
        padding-bottom: 1rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: 600;
        color: #374151;
        margin: 1.5rem 0 1rem 0;
        border-left: 4px solid #3b82f6;
        padding-left: 1rem;
    }
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    .sensibull-option-chain {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        font-size: 12px;
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    
    .sensibull-option-chain thead th {
        background-color: #f8f9fa;
        color: #495057;
        font-weight: 600;
        padding: 12px 8px;
        text-align: center;
        border: 1px solid #dee2e6;
        font-size: 11px;
    }
    
    .sensibull-option-chain tbody td {
        padding: 8px 6px;
        text-align: center;
        border: 1px solid #dee2e6;
        font-size: 11px;
    }
    
    .call-oi-change { color: #dc3545; font-weight: 500; }
    .call-oi-lakh { color: #6c757d; }
    .call-ltp { color: #000; font-weight: 600; }
    .call-volume { color: #6c757d; }
    
    .strike-price { 
        font-weight: bold; 
        color: #000;
        background-color: #fff3cd !important;
    }
    
    .put-ltp { color: #000; font-weight: 600; }
    .put-oi-change { color: #28a745; font-weight: 500; }
    .put-oi-lakh { color: #6c757d; }
    .put-volume { color: #6c757d; }
    
    .atm-strike {
        background-color: #fff3cd !important;
        font-weight: bold;
    }
    
    .itm-call { background-color: #d4edda; }
    .otm-call { background-color: #f8d7da; }
    .itm-put { background-color: #f8d7da; }
    .otm-put { background-color: #d4edda; }
    
    .controls-section {
        background: #f8fafc;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        border: 1px solid #e2e8f0;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Option Chain Analytics Platform</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns([2, 3, 2, 2])
    
    exchange_options = ["NSE", "BSE"]
    
    with col1:
        st.markdown("**Exchange**")
        selected_exchange = st.selectbox("", exchange_options, key="oc_exchange", label_visibility="collapsed")
    
    with col2:
        st.markdown("**Underlying**")
        underlying_options = get_underlyings(query_engine, selected_exchange, "Index")
        if underlying_options:
            selected_underlying = st.selectbox("", underlying_options, key="oc_underlying", label_visibility="collapsed")
        else:
            st.error(f"No index data available for {selected_exchange}")
            st.markdown('</div>', unsafe_allow_html=True)
            return
    
    with col3:
        st.markdown("**Date**")
        selected_date = st.date_input(
            "",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            key="oc_date",
            label_visibility="collapsed"
        )
    
    with col4:
        st.markdown("**Time**")
        selected_time = st.time_input(
            "",
            value=datetime.strptime("15:30:00", "%H:%M:%S").time(),
            key="oc_time",
            label_visibility="collapsed"
        )
    
    selected_datetime = datetime.combine(selected_date, selected_time)
    
    st.markdown("---")
    
    col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 2])
    
    with col1:
        st.markdown("**Expiry Filter**")
        expiry_filter = st.selectbox(
            "",
            ["All", "Current Week", "Current Month", "Next Month"],
            key="oc_expiry",
            label_visibility="collapsed"
        )
    
    with col2:
        st.markdown("**Option Type**")
        option_type_filter = st.selectbox(
            "",
            ["All", "Call", "Put"],
            key="oc_type",
            label_visibility="collapsed"
        )
    
    with col3:
        st.markdown("**Strike Range (%)**")
        strike_range = st.slider(
            "",
            min_value=1,
            max_value=50,
            value=10,
            step=1,
            key="oc_range",
            label_visibility="collapsed"
        )
    
    with col4:
        st.markdown("**Show Charts**")
        show_charts = st.checkbox("Enable", value=False, key="show_charts")
    
    with col5:
        st.markdown("**Action**")
        fetch_button = st.button("Fetch Data", type="primary", key="oc_fetch", use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
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

def get_previous_oi_data(query_engine, exchange, underlying, target_datetime):
    previous_datetime = target_datetime - timedelta(minutes=15)
    
    query = f"""
    SELECT 
        expiry,
        strike,
        option_type,
        open_interest as prev_oi
    FROM market_data.{exchange}_Options_{underlying}_Master 
    WHERE timestamp = '{previous_datetime}'
    """
    
    result, _, error = query_engine.execute_query(query)
    
    if error or len(result) == 0:
        return pd.DataFrame()
    
    return result

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

def create_sensibull_option_chain(df, spot_price, prev_oi_df):
    if len(df) == 0:
        return ""
    
    call_df = df[df['option_type'] == 'call'].copy()
    put_df = df[df['option_type'] == 'put'].copy()
    
    strikes = sorted(df['strike'].unique())
    
    html_content = """
    <table class="sensibull-option-chain">
        <thead>
            <tr>
                <th colspan="5" style="background-color: #e3f2fd; color: #1976d2;">CALLS</th>
                <th rowspan="2" style="background-color: #fff3e0; color: #f57c00; font-weight: bold;">Strike</th>
                <th colspan="5" style="background-color: #e8f5e8; color: #2e7d32;">PUTS</th>
            </tr>
            <tr>
                <th>OI Chg</th>
                <th>OI</th>
                <th>Volume</th>
                <th>LTP</th>
                <th>Chg%</th>
                <th>LTP</th>
                <th>Chg%</th>
                <th>Volume</th>
                <th>OI</th>
                <th>OI Chg</th>
            </tr>
        </thead>
        <tbody>
    """
    
    for strike in strikes:
        call_data = call_df[call_df['strike'] == strike]
        put_data = put_df[put_df['strike'] == strike]
        
        is_atm = abs(strike - spot_price) <= spot_price * 0.01
        
        call_itm = strike < spot_price
        put_itm = strike > spot_price
        
        strike_class = "atm-strike" if is_atm else ""
        call_class = "itm-call" if call_itm else "otm-call"
        put_class = "itm-put" if put_itm else "otm-put"
        
        html_content += f'<tr class="{strike_class}">'
        
        if len(call_data) > 0:
            call_row = call_data.iloc[0]
            
            prev_call_oi = 0
            if len(prev_oi_df) > 0:
                prev_call = prev_oi_df[(prev_oi_df['strike'] == strike) & (prev_oi_df['option_type'] == 'call')]
                if len(prev_call) > 0:
                    prev_call_oi = prev_call.iloc[0]['prev_oi']
            
            oi_change = call_row['open_interest'] - prev_call_oi
            price_change = round((call_row['close'] - call_row['open']) / call_row['open'] * 100, 1) if call_row['open'] > 0 else 0
            
            oi_change_class = "call-oi-change" if oi_change < 0 else ""
            
            html_content += f'''
                <td class="{call_class} {oi_change_class}">{oi_change:+,.0f}</td>
                <td class="{call_class} call-oi-lakh">{call_row['open_interest']:,.0f}</td>
                <td class="{call_class} call-volume">{call_row['volume']:,.0f}</td>
                <td class="{call_class} call-ltp">{call_row['close']:.2f}</td>
                <td class="{call_class}">{price_change:+.1f}%</td>
            '''
        else:
            html_content += f'''
                <td class="{call_class}">-</td>
                <td class="{call_class}">-</td>
                <td class="{call_class}">-</td>
                <td class="{call_class}">-</td>
                <td class="{call_class}">-</td>
            '''
        
        html_content += f'<td class="strike-price">{int(strike)}</td>'
        
        if len(put_data) > 0:
            put_row = put_data.iloc[0]
            
            prev_put_oi = 0
            if len(prev_oi_df) > 0:
                prev_put = prev_oi_df[(prev_oi_df['strike'] == strike) & (prev_oi_df['option_type'] == 'put')]
                if len(prev_put) > 0:
                    prev_put_oi = prev_put.iloc[0]['prev_oi']
            
            oi_change = put_row['open_interest'] - prev_put_oi
            price_change = round((put_row['close'] - put_row['open']) / put_row['open'] * 100, 1) if put_row['open'] > 0 else 0
            
            oi_change_class = "put-oi-change" if oi_change > 0 else ""
            
            html_content += f'''
                <td class="{put_class} put-ltp">{put_row['close']:.2f}</td>
                <td class="{put_class}">{price_change:+.1f}%</td>
                <td class="{put_class} put-volume">{put_row['volume']:,.0f}</td>
                <td class="{put_class} put-oi-lakh">{put_row['open_interest']:,.0f}</td>
                <td class="{put_class} {oi_change_class}">{oi_change:+,.0f}</td>
            '''
        else:
            html_content += f'''
                <td class="{put_class}">-</td>
                <td class="{put_class}">-</td>
                <td class="{put_class}">-</td>
                <td class="{put_class}">-</td>
                <td class="{put_class}">-</td>
            '''
        
        html_content += '</tr>'
    
    html_content += """
        </tbody>
    </table>
    """
    
    return html_content

def create_advanced_analytics(df, spot_price):
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Open Interest Profile', 'Volume Profile',
            'Put-Call Ratio (OI)', 'Put-Call Ratio (Volume)'
        ),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    call_data = df[df['option_type'] == 'call'].sort_values('strike')
    put_data = df[df['option_type'] == 'put'].sort_values('strike')
    
    fig.add_trace(
        go.Bar(x=call_data['strike'], y=call_data['open_interest'], 
               name='Call OI', marker_color='#10b981', opacity=0.8),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=put_data['strike'], y=-put_data['open_interest'], 
               name='Put OI', marker_color='#ef4444', opacity=0.8),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=call_data['strike'], y=call_data['volume'], 
               name='Call Vol', marker_color='#34d399', opacity=0.8),
        row=1, col=2
    )
    fig.add_trace(
        go.Bar(x=put_data['strike'], y=-put_data['volume'], 
               name='Put Vol', marker_color='#f87171', opacity=0.8),
        row=1, col=2
    )
    
    all_strikes = sorted(df['strike'].unique())
    pcr_oi = []
    pcr_vol = []
    
    for strike in all_strikes:
        call_oi = call_data[call_data['strike'] == strike]['open_interest'].sum()
        put_oi = put_data[put_data['strike'] == strike]['open_interest'].sum()
        call_vol = call_data[call_data['strike'] == strike]['volume'].sum()
        put_vol = put_data[put_data['strike'] == strike]['volume'].sum()
        
        pcr_oi.append(put_oi / call_oi if call_oi > 0 else 0)
        pcr_vol.append(put_vol / call_vol if call_vol > 0 else 0)
    
    fig.add_trace(
        go.Scatter(x=all_strikes, y=pcr_oi, mode='lines+markers',
                  name='PCR (OI)', line=dict(color='#8b5cf6', width=3)),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=all_strikes, y=pcr_vol, mode='lines+markers',
                  name='PCR (Vol)', line=dict(color='#f59e0b', width=3)),
        row=2, col=2
    )
    
    for row in range(1, 3):
        for col in range(1, 3):
            fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                         line_width=2, row=row, col=col)
    
    fig.update_layout(
        height=1200, 
        showlegend=True,
        title_text="Option Chain Analytics",
        title_font_size=20,
        template="plotly_white"
    )
    
    return fig

def create_market_microstructure_analysis(df, spot_price):
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    plt.style.use('seaborn-v0_8-darkgrid')
    
    call_data = df[df['option_type'] == 'call']
    put_data = df[df['option_type'] == 'put']
    
    axes[0, 0].bar(call_data['strike'], call_data['open_interest'], 
                   alpha=0.7, color='green', label='Call OI')
    axes[0, 0].bar(put_data['strike'], put_data['open_interest'], 
                   alpha=0.7, color='red', label='Put OI')
    axes[0, 0].axvline(spot_price, color='blue', linestyle='--', linewidth=2, label='Spot')
    axes[0, 0].set_title('Open Interest Distribution', fontsize=14, fontweight='bold')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    total_oi = df.groupby('strike').agg({'open_interest': 'sum', 'volume': 'sum'})
    axes[0, 1].scatter(total_oi.index, total_oi['open_interest'], 
                       s=total_oi['volume']/1000, alpha=0.6, c='purple')
    axes[0, 1].axvline(spot_price, color='blue', linestyle='--', linewidth=2)
    axes[0, 1].set_title('OI vs Volume (Bubble Size = Volume)', fontsize=14, fontweight='bold')
    axes[0, 1].grid(True, alpha=0.3)
    
    moneyness = ((df['strike'] - spot_price) / spot_price * 100).round(0)
    axes[1, 0].hist([call_data['close'], put_data['close']], 
                    bins=20, alpha=0.7, label=['Call Prices', 'Put Prices'], 
                    color=['green', 'red'])
    axes[1, 0].set_title('Option Premium Distribution', fontsize=14, fontweight='bold')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    strikes_sorted = sorted(df['strike'].unique())
    pcr_data = []
    for strike in strikes_sorted:
        call_oi = call_data[call_data['strike'] == strike]['open_interest'].sum()
        put_oi = put_data[put_data['strike'] == strike]['open_interest'].sum()
        pcr = put_oi / call_oi if call_oi > 0 else 0
        pcr_data.append(pcr)
    
    axes[1, 1].plot(strikes_sorted, pcr_data, 'o-', color='orange', linewidth=2, markersize=6)
    axes[1, 1].axvline(spot_price, color='blue', linestyle='--', linewidth=2)
    axes[1, 1].axhline(1, color='gray', linestyle=':', alpha=0.7)
    axes[1, 1].set_title('Put-Call Ratio by Strike', fontsize=14, fontweight='bold')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def fetch_enhanced_option_chain(query_engine, exchange, underlying, target_datetime, 
                               expiry_filter, option_type_filter, strike_range, show_charts):
    
    with st.spinner("Fetching spot price..."):
        spot_price = get_spot_price(query_engine, exchange, underlying, target_datetime)
    
    if spot_price is None:
        st.error("Could not fetch spot price for the selected underlying and datetime")
        return
    
    st.markdown('<div class="section-header">Market Overview</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric('Spot Price', f"{spot_price:,.2f}")
    
    with st.spinner("Fetching option chain data..."):
        query = build_option_chain_query(
            exchange, underlying, target_datetime, 
            expiry_filter, option_type_filter, spot_price, strike_range
        )
        
        result, exec_time, error = query_engine.execute_query(query)
        
        if error:
            st.error(f"Query Error: {error}")
            return
        
        if len(result) == 0:
            st.info("No option data found for the selected criteria")
            return
        
        prev_oi_df = get_previous_oi_data(query_engine, exchange, underlying, target_datetime)
        
        with col2:
            st.metric("Total Contracts", f"{len(result):,}")
        with col3:
            st.metric("Total Volume", f"{int(result['volume'].sum()):,}")
        with col4:
            st.metric("Total OI", f"{int(result['open_interest'].sum()):,}")
        st.success(f"Data loaded successfully in {exec_time:.2f} seconds")
        
        st.markdown('<div class="section-header">Option Chain</div>', unsafe_allow_html=True)
        
        if option_type_filter == "All":
            sensibull_html = create_sensibull_option_chain(result, spot_price, prev_oi_df)
            st.markdown(sensibull_html, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                csv = result.to_csv(index=False)
                st.download_button(
                    "Download Option Chain (CSV)",
                    csv,
                    f"option_chain_{underlying}_{target_datetime.strftime('%Y%m%d_%H%M')}.csv",
                    key="download_sensibull"
                )
        else:
            st.dataframe(result, use_container_width=True, height=600)
            csv = result.to_csv(index=False)
            st.download_button(
                "Download Raw Data (CSV)",
                csv,
                f"raw_data_{underlying}_{target_datetime.strftime('%Y%m%d_%H%M')}.csv",
                key="download_raw"
            )
        
        if show_charts and len(result) > 0:
            st.markdown('<div class="section-header">Analytics</div>', unsafe_allow_html=True)
            
            chart_fig = create_advanced_analytics(result, spot_price)
            st.plotly_chart(chart_fig, use_container_width=True)
            
            st.markdown('<div class="section-header">Market Microstructure</div>', unsafe_allow_html=True)
            
            micro_fig = create_market_microstructure_analysis(result, spot_price)
            st.pyplot(micro_fig)
        
        st.markdown('<div class="section-header">Summary Analytics</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Expiry-wise Breakdown**")
            expiry_summary = result.groupby('expiry').agg({
                'volume': 'sum',
                'open_interest': 'sum',
                'strike': 'count'
            }).rename(columns={'strike': 'contracts'})
            
            expiry_summary = expiry_summary.style.format({
                'volume': '{:,.0f}',
                'open_interest': '{:,.0f}',
                'contracts': '{:,.0f}'
            })
            
            st.dataframe(expiry_summary, use_container_width=True)
        
        with col2:
            st.markdown("**Top Strikes by Open Interest**")
            strike_summary = result.groupby('strike').agg({
                'volume': 'sum',
                'open_interest': 'sum'
            }).sort_values('open_interest', ascending=False).head(10)
            
            strike_summary = strike_summary.style.format({
                'volume': '{:,.0f}',
                'open_interest': '{:,.0f}'
            })
            
            st.dataframe(strike_summary, use_container_width=True)