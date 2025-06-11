import streamlit as st
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import plotly.graph_objects as go
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

CURRENCIES = {
    'INR': '₹', 'USD': '$', 'EUR': '€', 'GBP': '£', 'JPY': '¥', 'AED': 'د.إ', 'CAD': 'C$'
}

class BlackScholesCalculator:
    def __init__(self, S, K, T, r, sigma, option_type='call'):
        self.S = S
        self.K = K
        self.T = T
        self.r = r
        self.sigma = sigma
        self.option_type = option_type.lower()
        self._d1_cache = None
        self._d2_cache = None
    
    def d1(self):
        if self._d1_cache is None:
            self._d1_cache = (np.log(self.S / self.K) + (self.r + 0.5 * self.sigma**2) * self.T) / (self.sigma * np.sqrt(self.T))
        return self._d1_cache
    
    def d2(self):
        if self._d2_cache is None:
            self._d2_cache = self.d1() - self.sigma * np.sqrt(self.T)
        return self._d2_cache
    
    def option_price(self):
        d1_val = self.d1()
        d2_val = self.d2()
        
        if self.option_type == 'call':
            return self.S * norm.cdf(d1_val) - self.K * np.exp(-self.r * self.T) * norm.cdf(d2_val)
        else:
            return self.K * np.exp(-self.r * self.T) * norm.cdf(-d2_val) - self.S * norm.cdf(-d1_val)
    
    def delta(self):
        d1_val = self.d1()
        return norm.cdf(d1_val) if self.option_type == 'call' else norm.cdf(d1_val) - 1
    
    def gamma(self):
        return norm.pdf(self.d1()) / (self.S * self.sigma * np.sqrt(self.T))
    
    def theta(self):
        d1_val = self.d1()
        d2_val = self.d2()
        
        if self.option_type == 'call':
            theta = (-self.S * norm.pdf(d1_val) * self.sigma / (2 * np.sqrt(self.T)) 
                    - self.r * self.K * np.exp(-self.r * self.T) * norm.cdf(d2_val))
        else:
            theta = (-self.S * norm.pdf(d1_val) * self.sigma / (2 * np.sqrt(self.T)) 
                    + self.r * self.K * np.exp(-self.r * self.T) * norm.cdf(-d2_val))
        
        return theta / 365
    
    def vega(self):
        return self.S * norm.pdf(self.d1()) * np.sqrt(self.T) / 100
    
    def rho(self):
        d2_val = self.d2()
        multiplier = 1 if self.option_type == 'call' else -1
        return multiplier * self.K * self.T * np.exp(-self.r * self.T) * norm.cdf(multiplier * d2_val) / 100

@st.cache_data
def calculate_implied_volatility(market_price, S, K, T, r, option_type='call'):
    def objective(sigma):
        try:
            bs = BlackScholesCalculator(S, K, T, r, sigma, option_type)
            return bs.option_price() - market_price
        except:
            return float('inf')
    
    try:
        return brentq(objective, 0.001, 5.0, maxiter=100)
    except:
        return None

@st.cache_data
def generate_sensitivity_data(S, K, T, r, sigma, price_multiplier=0.2):
    price_range = np.linspace(S * (1 - price_multiplier), S * (1 + price_multiplier), 50)
    
    call_data = np.array([BlackScholesCalculator(p, K, T, r, sigma, 'call').option_price() for p in price_range])
    put_data = np.array([BlackScholesCalculator(p, K, T, r, sigma, 'put').option_price() for p in price_range])
    delta_call_data = np.array([BlackScholesCalculator(p, K, T, r, sigma, 'call').delta() for p in price_range])
    delta_put_data = np.array([BlackScholesCalculator(p, K, T, r, sigma, 'put').delta() for p in price_range])
    
    return price_range, call_data, put_data, delta_call_data, delta_put_data

def greeks_calculator():    
    st.markdown('<div class="main-header">Global Asset Options Pricing Calculator</div>', unsafe_allow_html=True)
    
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            currency = st.selectbox("🌍 Select Currency", list(CURRENCIES.keys()), index=0)
            currency_symbol = CURRENCIES[currency]
    
    st.markdown("---")
    
    tab1, tab2, tab3 = st.tabs(["Pricing & Greeks", "Sensitivity Analysis", "Tools & Utilities"])
    
    with tab1:
        st.markdown('<div class="input-section">', unsafe_allow_html=True)
        st.subheader("Input Parameters")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Asset & Strike Prices**")
            S = st.number_input(f"Current Asset Price ({currency_symbol})", value=25108.0, min_value=0.01, format="%.4f")
            K = st.number_input(f"Strike Price ({currency_symbol})", value=25100.0, min_value=0.01, format="%.4f")
            
            st.markdown("**Expiration Settings**")
            expiry_method = st.selectbox("Expiry Input Method", ["Calendar Date", "Days Count", "Years Fraction"])
            
            if expiry_method == "Calendar Date":
                expiry_date = st.date_input("Expiration Date", value=datetime.now().date() + timedelta(days=2))
                T = max((expiry_date - datetime.now().date()).days / 365.0, 1/365)
            elif expiry_method == "Days Count":
                days = st.number_input("Days to Expiry", value=30, min_value=1, max_value=3650)
                T = days / 365.0
            else:
                T = st.number_input("Years to Expiry", value=0.08, min_value=0.001, max_value=10.0, format="%.4f")
        
        with col2:
            st.markdown("**Market Parameters**")
            r = st.number_input("Risk-free Rate (%)", value=6.0, min_value=0.0, max_value=100.0, format="%.2f") / 100
            sigma = st.number_input("Volatility (%)", value=18.0, min_value=0.1, max_value=500.0, format="%.2f") / 100
            
            st.markdown("**Option Configuration**")
            option_type = st.selectbox("Option Type", ["Call", "Put"])
            
            moneyness = (S / K - 1) * 100
            moneyness_label = "ITM" if (moneyness > 0 and option_type == "Call") or (moneyness < 0 and option_type == "Put") else "OTM"
            
            st.info(f"Time to expiration: **{T:.4f} years** ({T*365:.0f} days)")
            st.info(f"Moneyness: **{moneyness:+.2f}%** ({moneyness_label})")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        if T <= 0:
            st.error("Time to expiration must be positive!")
            return
        
        bs_call = BlackScholesCalculator(S, K, T, r, sigma, 'call')
        bs_put = BlackScholesCalculator(S, K, T, r, sigma, 'put')
        
        st.markdown('<div class="results-section">', unsafe_allow_html=True)
        st.subheader(f"Option Prices ({currency_symbol})")
        
        call_price = bs_call.option_price()
        put_price = bs_put.option_price()
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Call Price", f"{currency_symbol}{call_price:.4f}")
        with col2:
            st.metric("Put Price", f"{currency_symbol}{put_price:.4f}")
        with col3:
            intrinsic = max(S - K, 0) if option_type == "Call" else max(K - S, 0)
            st.metric("Intrinsic Value", f"{currency_symbol}{intrinsic:.4f}")
        with col4:
            time_value = (call_price if option_type == "Call" else put_price) - intrinsic
            st.metric("Time Value", f"{currency_symbol}{time_value:.4f}")
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        st.subheader("The Greeks")
        
        bs = bs_call if option_type.lower() == 'call' else bs_put
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            delta_val = bs.delta()
            st.metric("Δ Delta", f"{delta_val:.4f}", help="Price sensitivity to underlying movement")
            gamma_val = bs.gamma()
            st.metric("Γ Gamma", f"{gamma_val:.4f}", help="Rate of change of Delta")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            theta_val = bs.theta()
            st.metric("Θ Theta (daily)", f"{currency_symbol}{theta_val:.4f}", 
                     delta=f"{theta_val*7:.4f} weekly", help="Time decay per day")
            vega_val = bs.vega()
            st.metric("ν Vega", f"{vega_val:.4f}", help="Volatility sensitivity")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            rho_val = bs.rho()
            st.metric("ρ Rho", f"{rho_val:.4f}", help="Interest rate sensitivity")
            prob_itm = norm.cdf(bs.d2()) if option_type == "Call" else norm.cdf(-bs.d2())
            st.metric("Prob ITM", f"{prob_itm:.1%}", help="Probability of finishing ITM")
            st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        st.subheader("Market Sensitivity Analysis")
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            analysis_type = st.selectbox("Analysis Type", 
                                       ["Price Sensitivity", "Time Decay", "Volatility Impact"])
            
            if st.button("Generate Analysis", use_container_width=True, type="primary"):
                st.session_state.generate_charts = True
        
        with col2:
            if st.session_state.get('generate_charts', False):
                with st.spinner("Generating interactive charts..."):
                    if analysis_type == "Price Sensitivity":
                        price_range, call_data, put_data, delta_call_data, delta_put_data = generate_sensitivity_data(S, K, T, r, sigma)
                        
                        fig1 = go.Figure()
                        fig1.add_trace(go.Scatter(x=price_range, y=call_data, name='Call Price', 
                                                line=dict(color='#10b981', width=3)))
                        fig1.add_trace(go.Scatter(x=price_range, y=put_data, name='Put Price', 
                                                line=dict(color='#ef4444', width=3)))
                        fig1.add_vline(x=S, line_dash="dash", annotation_text="Current Price", line_color="#3b82f6")
                        fig1.add_vline(x=K, line_dash="dot", annotation_text="Strike Price", line_color="#f59e0b")
                        fig1.update_layout(
                            title="Option Price vs Underlying Price", 
                            xaxis_title=f"Asset Price ({currency_symbol})", 
                            yaxis_title=f"Option Price ({currency_symbol})",
                            height=500,
                            template="plotly_white"
                        )
                        st.plotly_chart(fig1, use_container_width=True)
                        
                        fig2 = go.Figure()
                        fig2.add_trace(go.Scatter(x=price_range, y=delta_call_data, name='Call Delta', 
                                                line=dict(color='#10b981', width=3)))
                        fig2.add_trace(go.Scatter(x=price_range, y=delta_put_data, name='Put Delta', 
                                                line=dict(color='#ef4444', width=3)))
                        fig2.add_vline(x=S, line_dash="dash", annotation_text="Current Price", line_color="#3b82f6")
                        fig2.add_vline(x=K, line_dash="dot", annotation_text="Strike Price", line_color="#f59e0b")
                        fig2.update_layout(
                            title="Delta vs Underlying Price", 
                            xaxis_title=f"Asset Price ({currency_symbol})", 
                            yaxis_title="Delta",
                            height=500,
                            template="plotly_white"
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                    
                    elif analysis_type == "Time Decay":
                        time_range = np.linspace(T, 0.001, 50)
                        call_theta_data = []
                        put_theta_data = []
                        
                        for t in time_range:
                            bs_temp_call = BlackScholesCalculator(S, K, t, r, sigma, 'call')
                            bs_temp_put = BlackScholesCalculator(S, K, t, r, sigma, 'put')
                            call_theta_data.append(bs_temp_call.option_price())
                            put_theta_data.append(bs_temp_put.option_price())
                        
                        fig3 = go.Figure()
                        fig3.add_trace(go.Scatter(x=time_range*365, y=call_theta_data, name='Call Price', 
                                                line=dict(color='#10b981', width=3)))
                        fig3.add_trace(go.Scatter(x=time_range*365, y=put_theta_data, name='Put Price', 
                                                line=dict(color='#ef4444', width=3)))
                        fig3.add_vline(x=T*365, line_dash="dash", annotation_text="Current Time", line_color="#3b82f6")
                        fig3.update_layout(
                            title="Time Decay Analysis", 
                            xaxis_title="Days to Expiry", 
                            yaxis_title=f"Option Price ({currency_symbol})",
                            height=500,
                            template="plotly_white"
                        )
                        st.plotly_chart(fig3, use_container_width=True)
                    
                    else:
                        vol_range = np.linspace(0.1, 1.0, 50)
                        call_vega_data = []
                        put_vega_data = []
                        
                        for vol in vol_range:
                            bs_temp_call = BlackScholesCalculator(S, K, T, r, vol, 'call')
                            bs_temp_put = BlackScholesCalculator(S, K, T, r, vol, 'put')
                            call_vega_data.append(bs_temp_call.option_price())
                            put_vega_data.append(bs_temp_put.option_price())
                        
                        fig4 = go.Figure()
                        fig4.add_trace(go.Scatter(x=vol_range*100, y=call_vega_data, name='Call Price', 
                                                line=dict(color='#10b981', width=3)))
                        fig4.add_trace(go.Scatter(x=vol_range*100, y=put_vega_data, name='Put Price', 
                                                line=dict(color='#ef4444', width=3)))
                        fig4.add_vline(x=sigma*100, line_dash="dash", annotation_text="Current Vol", line_color="#3b82f6")
                        fig4.update_layout(
                            title="Volatility Impact Analysis", 
                            xaxis_title="Volatility (%)",
                            yaxis_title=f"Option Price ({currency_symbol})",
                            height=500,
                            template="plotly_white"
                        )
                        st.plotly_chart(fig4, use_container_width=True)
    
    with tab3:
        st.subheader("🛠️ Professional Tools")
        
        current_price = call_price if option_type == "Call" else put_price
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown('<div class="input-section">', unsafe_allow_html=True)
            st.markdown("**Implied Volatility Calculator**")
            
            market_price = st.number_input(f"Market Price ({currency_symbol})", 
                                         value=call_price if option_type == "Call" else put_price, 
                                         min_value=0.01, format="%.4f")
            
            if st.button("🔍 Calculate Implied Volatility", use_container_width=True, type="primary"):
                with st.spinner("Computing implied volatility..."):
                    iv = calculate_implied_volatility(market_price, S, K, T, r, option_type.lower())
                    if iv:
                        vol_diff = (iv - sigma) * 100
                        st.success(f"Implied Volatility: **{iv*100:.2f}%**")
                        if vol_diff > 0:
                            st.info(f"{vol_diff:+.2f}% higher than input volatility")
                        else:
                            st.info(f"{vol_diff:+.2f}% lower than input volatility")
                    else:
                        st.error("❌ Could not calculate implied volatility")
            
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="input-section">', unsafe_allow_html=True)
            st.markdown("**Parameter Export & Analysis**")
            
            if st.button("📋 Copy Parameters", use_container_width=True):
                params = f"S={S}, K={K}, T={T:.4f}, r={r*100:.2f}%, σ={sigma*100:.2f}%"
                st.code(params, language="text")
            
            st.markdown("**Target Price Analysis**")
            target_price = st.number_input("Target Option Price", value=current_price*1.1, format="%.4f")
            
            if st.button("Reverse Engineer", use_container_width=True):
                if target_price > 0:
                    target_iv = calculate_implied_volatility(target_price, S, K, T, r, option_type.lower())
                    if target_iv:
                        st.success(f"Required Volatility: **{target_iv*100:.2f}%**")
                        vol_change = ((target_iv / sigma) - 1) * 100
                        st.info(f"Volatility change needed: **{vol_change:+.1f}%**")
                    else:
                        st.error("❌ Target price not achievable")
            
            st.markdown('</div>', unsafe_allow_html=True)