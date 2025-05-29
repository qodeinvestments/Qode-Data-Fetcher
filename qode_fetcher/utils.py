from typing import Dict

def parse_table_name(table_name: str) -> Dict[str, str]:
    parts = table_name.replace('market_data.', '').split('_')
    
    info = {
        'exchange': parts[0] if len(parts) > 0 else '',
        'instrument_type': parts[1] if len(parts) > 1 else '',
        'underlying': parts[2] if len(parts) > 2 else ''
    }
    
    if info['instrument_type'].lower() == 'options':
        if len(parts) >= 6:
            info['expiry'] = parts[3]
            info['strike'] = parts[4]
            info['option_type'] = parts[5]
    elif info['instrument_type'].lower() == 'futures':
        if len(parts) >= 4:
            info['expiry'] = parts[3] if len(parts) > 3 else 'continuous'
    
    return info

def describe_table_type(table_name):
    parts = table_name.split('_')
    
    if len(parts) < 3:
        return "Unknown format"
    
    exchange = parts[0]
    instrument = parts[1]
    underlying = parts[2]
    
    if instrument.lower() == 'index':
        return f"{exchange} {underlying} Index data"
    
    elif instrument.lower() == 'options':
        if len(parts) >= 6:
            expiry = parts[3]
            strike = parts[4]
            option_type = parts[5]
            return f"{exchange} {underlying} {option_type} option (Strike: {strike}, Expiry: {expiry})"
        else:
            return f"{exchange} {underlying} Options data"
    
    elif instrument.lower() == 'futures':
        if len(parts) >= 4:
            cycle = parts[3]
            return f"{exchange} {underlying} Futures (Cycle {cycle})"
        else:
            return f"{exchange} {underlying} Futures data"
    
    elif instrument.lower() == 'stocks':
        return f"{exchange} {underlying} Stock data"
    
    else:
        return f"{exchange} {instrument} {underlying} data"