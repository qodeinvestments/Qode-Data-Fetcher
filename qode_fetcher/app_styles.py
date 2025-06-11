def get_option_chain_styles():
    return """
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
        font-size: 10px;
        border-collapse: collapse;
        width: 100%;
        margin: 20px 0;
    }
    
    .sensibull-option-chain thead th {
        background-color: #f8f9fa;
        color: #495057;
        font-weight: 600;
        padding: 8px 4px;
        text-align: center;
        border: 1px solid #dee2e6;
        font-size: 9px;
    }
    
    .sensibull-option-chain tbody td {
        padding: 6px 3px;
        text-align: center;
        border: 1px solid #dee2e6;
        font-size: 9px;
    }
    
    .call-oi-change { color: #dc3545; font-weight: 500; }
    .call-oi-lakh { color: #6c757d; }
    .call-ltp { color: #000; font-weight: 600; }
    .call-volume { color: #6c757d; }
    .call-iv { color: #1976d2; font-weight: 500; }
    .call-delta { color: #2e7d32; font-weight: 500; }
    
    .strike-price { 
        font-weight: bold; 
        color: #000;
        background-color: #fff3cd !important;
    }
    
    .put-ltp { color: #000; font-weight: 600; }
    .put-oi-change { color: #28a745; font-weight: 500; }
    .put-oi-lakh { color: #6c757d; }
    .put-volume { color: #6c757d; }
    .put-iv { color: #1976d2; font-weight: 500; }
    .put-delta { color: #d32f2f; font-weight: 500; }
    
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
    """