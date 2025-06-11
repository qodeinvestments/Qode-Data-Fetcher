import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from data_utils import get_underlyings, build_option_chain_query, get_available_expiries, get_spot_price
from app_styles import get_option_chain_styles
from plots import create_advanced_analytics, create_greeks_analysis, create_oi_volume_charts, create_pcr_charts, create_price_movement_chart

def check_iv_greeks_columns(query_engine, exchange, underlying):
    check_query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = '{exchange}_Options_{underlying}_Master' 
    AND table_schema = 'market_data'
    AND column_name IN ('iv', 'delta', 'gamma', 'theta', 'vega', 'rho')
    """
    
    result, _, error = query_engine.execute_query(check_query)
    
    if error or len(result) == 0:
        return []
    
    return result['column_name'].tolist()

def option_chain_viewer(query_engine):
    st.markdown(get_option_chain_styles(), unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Option Chain Analytics Platform</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns([2, 3, 2, 2])
    
    exchange_options = ["NSE", "BSE", "MCX"]
    
    with col1:
        st.markdown("**Exchange**")
        selected_exchange = st.selectbox("", exchange_options, key="oc_exchange", label_visibility="collapsed")
    
    with col2:
        st.markdown("**Underlying**")
        underlying_options = get_underlyings(query_engine, selected_exchange, "Options")
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
            value=datetime.strptime("09:15:00", "%H:%M:%S").time(),
            key="oc_time",
            label_visibility="collapsed"
        )
    
    selected_datetime = datetime.combine(selected_date, selected_time)
    
    st.markdown("---")
    
    available_expiries = get_available_expiries(query_engine, selected_exchange, selected_underlying, selected_datetime)
    
    if available_expiries is None or len(available_expiries) == 0:
        st.error(f"No expiry data available for {selected_exchange} - {selected_underlying} on {selected_datetime.strftime('%Y-%m-%d %H:%M')}")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 2, 2, 2, 2])
    
    with col1:
        st.markdown("**Expiry Mode**")
        expiry_mode = st.selectbox(
            "",
            ["Specific Date"],
            key="oc_expiry_mode",
            label_visibility="collapsed"
        )
    
    selected_expiry = None
    
    if expiry_mode == "Specific Date":
        with col2:
            st.markdown("**Select Expiry**")
            expiry_options = [
                pd.to_datetime(exp).strftime("%d %b %y") if not pd.isnull(exp) else str(exp)
                for exp in available_expiries
            ]
            
            expiry_map = {}
            for exp in available_expiries:
                if not pd.isnull(exp):
                    expiry_map[pd.to_datetime(exp).strftime("%d %b %y")] = exp
                else:
                    expiry_map[str(exp)] = exp

            selected_expiry_label = st.selectbox(
                "",
                expiry_options,
                key="oc_specific_expiry",
                label_visibility="collapsed"
            )
            selected_expiry = expiry_map[selected_expiry_label]
    
    with col3 if expiry_mode == "Specific Date" else col4:
        st.markdown("**Option Type**")
        option_type_filter = st.selectbox(
            "",
            ["All", "Call", "Put"],
            key="oc_type",
            label_visibility="collapsed"
        )
    
    with col4 if expiry_mode == "Specific Date" else col5:
        st.markdown("**Strike Range (%)**")
        strike_range = st.slider(
            "",
            min_value=1,
            max_value=50,
            value=5,
            step=1,
            key="oc_range",
            label_visibility="collapsed"
        )
    
    with col5 if expiry_mode == "Specific Date" else col6:
        st.markdown("**Show Charts**")
        show_charts = st.checkbox("Enable", value=False, key="show_charts")
    
    with col6:
        st.markdown("**Fetch Data**")
        fetch_button = st.button("Fetch Data", type="primary", key="oc_fetch", use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    if fetch_button:
        fetch_enhanced_option_chain(
            query_engine,
            selected_exchange,
            selected_underlying,
            selected_datetime,
            selected_expiry,
            option_type_filter,
            strike_range,
            show_charts
        )

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

def create_sensibull_option_chain(df, spot_price, prev_oi_df, has_iv, has_greeks):
    if len(df) == 0:
        return ""
    
    call_df = df[df['option_type'] == 'call'].copy()
    put_df = df[df['option_type'] == 'put'].copy()
    
    strikes = sorted(df['strike'].unique())
    
    iv_header = "<th>IV</th>" if has_iv else ""
    delta_header = "<th>Delta</th>" if has_greeks else ""
    gamma_header = "<th>Gamma</th>" if has_greeks else ""
    theta_header = "<th>Theta</th>" if has_greeks else ""
    
    call_headers = f"<th>OI Chg</th><th>OI</th><th>Volume</th><th>LTP</th><th>Chg%</th>{iv_header}{delta_header}{gamma_header}{theta_header}"
    put_headers = f"{theta_header}{gamma_header}{delta_header}{iv_header}<th>LTP</th><th>Chg%</th><th>Volume</th><th>OI</th><th>OI Chg</th>"
    
    html_content = f"""
    <table class="sensibull-option-chain">
        <thead>
            <tr>
                <th colspan="{5 + (1 if has_iv else 0) + (3 if has_greeks else 0)}" style="background-color: #e3f2fd; color: #1976d2;">CALLS</th>
                <th rowspan="2" style="background-color: #fff3e0; color: #f57c00; font-weight: bold;">Strike</th>
                <th colspan="{5 + (1 if has_iv else 0) + (3 if has_greeks else 0)}" style="background-color: #e8f5e8; color: #2e7d32;">PUTS</th>
            </tr>
            <tr>
                {call_headers}
                {put_headers}
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
            
            call_cells = f'''
                <td class="{call_class} {oi_change_class}">{oi_change:+,.0f}</td>
                <td class="{call_class} call-oi-lakh">{call_row['open_interest']:,.0f}</td>
                <td class="{call_class} call-volume">{call_row['volume']:,.0f}</td>
                <td class="{call_class} call-ltp">{call_row['close']:.2f}</td>
                <td class="{call_class}">{price_change:+.1f}%</td>
            '''
            
            if has_iv and 'iv' in call_row:
                iv_val = f"{call_row['iv']*100:.2f}" if pd.notna(call_row['iv']) else "-"
                call_cells += f'<td class="{call_class}">{iv_val}</td>'
            
            if has_greeks:
                if 'delta' in call_row:
                    delta_val = f"{call_row['delta']:.3f}" if pd.notna(call_row['delta']) else "-"
                    call_cells += f'<td class="{call_class}">{delta_val}</td>'
                if 'gamma' in call_row:
                    gamma_val = f"{call_row['gamma']:.4f}" if pd.notna(call_row['gamma']) else "-"
                    call_cells += f'<td class="{call_class}">{gamma_val}</td>'
                if 'theta' in call_row:
                    theta_val = f"{call_row['theta']:.3f}" if pd.notna(call_row['theta']) else "-"
                    call_cells += f'<td class="{call_class}">{theta_val}</td>'
            
            html_content += call_cells
        else:
            empty_cols = 5 + (1 if has_iv else 0) + (3 if has_greeks else 0)
            html_content += f'<td class="{call_class}">-</td>' * empty_cols
        
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
            
            put_cells = ""
            
            if has_greeks:
                if 'theta' in put_row:
                    theta_val = f"{put_row['theta']:.3f}" if pd.notna(put_row['theta']) else "-"
                    put_cells += f'<td class="{put_class}">{theta_val}</td>'
                if 'gamma' in put_row:
                    gamma_val = f"{put_row['gamma']:.4f}" if pd.notna(put_row['gamma']) else "-"
                    put_cells += f'<td class="{put_class}">{gamma_val}</td>'
                if 'delta' in put_row:
                    delta_val = f"{put_row['delta']:.3f}" if pd.notna(put_row['delta']) else "-"
                    put_cells += f'<td class="{put_class}">{delta_val}</td>'
            
            if has_iv and 'iv' in put_row:
                iv_val = f"{put_row['iv']*100:.2f}" if pd.notna(put_row['iv']) else "-"
                put_cells += f'<td class="{put_class}">{iv_val}</td>'
            
            put_cells += f'''
                <td class="{put_class} put-ltp">{put_row['close']:.2f}</td>
                <td class="{put_class}">{price_change:+.1f}%</td>
                <td class="{put_class} put-volume">{put_row['volume']:,.0f}</td>
                <td class="{put_class} put-oi-lakh">{put_row['open_interest']:,.0f}</td>
                <td class="{put_class} {oi_change_class}">{oi_change:+,.0f}</td>
            '''
            
            html_content += put_cells
        else:
            empty_cols = 5 + (1 if has_iv else 0) + (3 if has_greeks else 0)
            html_content += f'<td class="{put_class}">-</td>' * empty_cols
        
        html_content += '</tr>'
    
    html_content += """
        </tbody>
    </table>
    """
    
    return html_content

def fetch_enhanced_option_chain(query_engine, exchange, underlying, target_datetime, 
                               selected_expiry, option_type_filter, strike_range, show_charts):

    with st.spinner("Checking available columns..."):
        available_columns = check_iv_greeks_columns(query_engine, exchange, underlying)
        has_iv = 'iv' in available_columns
        has_greeks = any(col in available_columns for col in ['delta', 'gamma', 'theta', 'vega'])

    with st.spinner("Fetching spot price..."):
        spot_price = get_spot_price(query_engine, exchange, underlying, target_datetime)

    if spot_price is None:
        st.error("Could not fetch spot price for the selected underlying and datetime")
        return

    tabs = st.tabs(["Market Overview", "Option Chain", "Analytics", "Summary Analytics"])

    with tabs[0]:
        st.markdown('<div class="section-header">Market Overview</div>', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric('Spot Price', f"{spot_price:,.2f}")

    with st.spinner("Fetching option chain data..."):
        query = build_option_chain_query(
            exchange, underlying, target_datetime, 
            selected_expiry, option_type_filter, spot_price, strike_range, available_columns
        )

        result, exec_time, error = query_engine.execute_query(query)

        if error:
            st.error(f"Query Error: {error}")
            return

        if len(result) == 0:
            st.info("No option data found for the selected criteria")
            return

        prev_oi_df = get_previous_oi_data(query_engine, exchange, underlying, target_datetime)

        with tabs[0]:
            with col2:
                st.metric("Total Contracts", f"{len(result):,}")
            with col3:
                st.metric("Total Volume", f"{int(result['volume'].sum()):,}")
            with col4:
                st.metric("Total OI", f"{int(result['open_interest'].sum()):,}")
            st.success(f"Data loaded successfully in {exec_time:.2f} seconds")
            if has_iv or has_greeks:
                st.info(f"Additional data available: {'IV' if has_iv else ''}{', Greeks' if has_greeks else ''}")

        with tabs[1]:
            st.markdown('<div class="section-header">Option Chain</div>', unsafe_allow_html=True)
            if option_type_filter == "All":
                sensibull_html = create_sensibull_option_chain(result, spot_price, prev_oi_df, has_iv, has_greeks)
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

        with tabs[2]:
            if show_charts and len(result) > 0:
                st.markdown('<div class="section-header">Analytics</div>', unsafe_allow_html=True)
                chart_fig = create_advanced_analytics(result, spot_price)
                st.plotly_chart(chart_fig, use_container_width=True)
                if has_greeks:
                    st.markdown('<div class="section-header">Greeks Analysis</div>', unsafe_allow_html=True)
                    greeks_fig = create_greeks_analysis(result, spot_price)
                    if greeks_fig:
                        st.plotly_chart(greeks_fig, use_container_width=True)
            else:
                st.info("Enable 'Show Charts' to view analytics.")

        with tabs[3]:
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
