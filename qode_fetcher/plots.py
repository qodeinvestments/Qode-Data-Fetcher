import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

def create_greeks_analysis(df, spot_price):
    if 'delta' not in df.columns and 'gamma' not in df.columns and 'theta' not in df.columns and 'vega' not in df.columns:
        return None
        
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Delta Profile', 'Gamma Profile', 'Theta Profile', 'Vega Profile')
    )
    
    call_data = df[df['option_type'] == 'call'].sort_values('strike')
    put_data = df[df['option_type'] == 'put'].sort_values('strike')
    
    if 'delta' in df.columns:
        fig.add_trace(
            go.Scatter(x=call_data['strike'], y=call_data['delta'], 
                      mode='lines+markers', name='Call Delta', line=dict(color='green')),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=put_data['strike'], y=put_data['delta'], 
                      mode='lines+markers', name='Put Delta', line=dict(color='red')),
            row=1, col=1
        )
    
    if 'gamma' in df.columns:
        fig.add_trace(
            go.Scatter(x=call_data['strike'], y=call_data['gamma'], 
                      mode='lines+markers', name='Call Gamma', line=dict(color='blue')),
            row=1, col=2
        )
        fig.add_trace(
            go.Scatter(x=put_data['strike'], y=put_data['gamma'], 
                      mode='lines+markers', name='Put Gamma', line=dict(color='orange')),
            row=1, col=2
        )
    
    if 'theta' in df.columns:
        fig.add_trace(
            go.Scatter(x=call_data['strike'], y=call_data['theta'], 
                      mode='lines+markers', name='Call Theta', line=dict(color='purple')),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(x=put_data['strike'], y=put_data['theta'], 
                      mode='lines+markers', name='Put Theta', line=dict(color='brown')),
            row=2, col=1
        )
    
    if 'vega' in df.columns:
        fig.add_trace(
            go.Scatter(x=call_data['strike'], y=call_data['vega'], 
                      mode='lines+markers', name='Call Vega', line=dict(color='pink')),
            row=2, col=2
        )
        fig.add_trace(
            go.Scatter(x=put_data['strike'], y=put_data['vega'], 
                      mode='lines+markers', name='Put Vega', line=dict(color='cyan')),
            row=2, col=2
        )
    
    for row in range(1, 3):
        for col in range(1, 3):
            fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                         line_width=2, row=row, col=col)
    
    fig.update_layout(
        height=800,
        showlegend=True,
        title_text="Greeks Analysis",
        title_font_size=20,
        template="plotly_white"
    )
    
    return fig

def create_oi_volume_charts(df, spot_price):
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Open Interest Distribution', 'Volume Distribution')
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
    
    fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                 line_width=2, row=1, col=1)
    fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                 line_width=2, row=1, col=2)
    
    fig.update_layout(
        height=600,
        showlegend=True,
        title_text="Open Interest & Volume Analysis",
        title_font_size=18,
        template="plotly_white"
    )
    
    return fig

def create_pcr_charts(df, spot_price):
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Put-Call Ratio (OI)', 'Put-Call Ratio (Volume)')
    )
    
    call_data = df[df['option_type'] == 'call'].sort_values('strike')
    put_data = df[df['option_type'] == 'put'].sort_values('strike')
    
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
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=all_strikes, y=pcr_vol, mode='lines+markers',
                  name='PCR (Vol)', line=dict(color='#f59e0b', width=3)),
        row=1, col=2
    )
    
    fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                 line_width=2, row=1, col=1)
    fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937",
                 line_width=2, row=1, col=2)
    
    fig.update_layout(
        height=500,
        showlegend=True,
        title_text="Put-Call Ratio Analysis",
        title_font_size=18,
        template="plotly_white"
    )
    
    return fig

def create_price_movement_chart(df, spot_price):
    fig = go.Figure()
    
    call_data = df[df['option_type'] == 'call'].sort_values('strike')
    put_data = df[df['option_type'] == 'put'].sort_values('strike')
    
    if 'open' in df.columns and 'close' in df.columns:
        call_change = ((call_data['close'] - call_data['open']) / call_data['open'] * 100).fillna(0)
        put_change = ((put_data['close'] - put_data['open']) / put_data['open'] * 100).fillna(0)
        
        fig.add_trace(
            go.Scatter(x=call_data['strike'], y=call_change,
                      mode='lines+markers', name='Call % Change',
                      line=dict(color='green', width=2))
        )
        
        fig.add_trace(
            go.Scatter(x=put_data['strike'], y=put_change,
                      mode='lines+markers', name='Put % Change',
                      line=dict(color='red', width=2))
        )
    
    fig.add_vline(x=spot_price, line_dash="dash", line_color="#1f2937", line_width=2)
    fig.add_hline(y=0, line_dash="dot", line_color="gray", line_width=1)
    
    fig.update_layout(
        height=500,
        title="Price Movement Analysis",
        xaxis_title="Strike Price",
        yaxis_title="Percentage Change (%)",
        template="plotly_white"
    )
    
    return fig