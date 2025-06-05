def get_sample_queries():
    return {
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
            SUM(v) AS volume,
            (array_agg(symbol))[1] AS symbol,
            (array_agg(oi ORDER BY timestamp DESC))[1] AS oi
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
            AVG(c) AS fut_close,
            (array_agg(symbol))[1] AS symbol
        FROM market_data.NSE_Futures_NIFTY
        GROUP BY day
        )
        SELECT
        i.day,
        i.index_close,
        f.fut_close,
        f.symbol,
        f.fut_close - i.index_close AS premium,
        CASE WHEN f.fut_close - i.index_close > 0 THEN 'Contango' ELSE 'Backwardation' END AS market_state
        FROM index_data i
        JOIN futures_data f ON i.day = f.day
        ORDER BY i.day;""",
        
        "Stock aggregation": """SELECT
        date_trunc('day', timestamp) AS day,
        (array_agg(o ORDER BY timestamp))[1] AS open,
        MAX(h) AS high,
        MIN(l) AS low,
        (array_agg(c ORDER BY timestamp DESC))[1] AS close,
        SUM(v) AS volume
        FROM market_data.NSE_Stocks_RELIANCE
        WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
        GROUP BY day
        ORDER BY day;""",
        
        "Futures open interest analysis": """SELECT
        date_trunc('day', timestamp) AS day,
        (array_agg(symbol))[1] AS symbol,
        AVG(c) AS avg_close,
        (array_agg(oi ORDER BY timestamp DESC))[1] AS final_oi,
        SUM(v) AS total_volume,
        (final_oi - LAG((array_agg(oi ORDER BY timestamp DESC))[1]) OVER (ORDER BY day)) AS oi_change
        FROM market_data.NSE_Futures_NIFTY
        WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'
        GROUP BY day
        ORDER BY day;"""
    }