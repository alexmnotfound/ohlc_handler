import pandas as pd
import argparse
import sys
import os
import logging
from datetime import datetime

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_handler import DBHandler
from config import market_config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_to_csv(ticker: str = None, timeframe: str = None, output_file: str = None):
    """
    Export OHLCV data and indicators to CSV file.
    
    Args:
        ticker (str, optional): Specific ticker to export. If None, exports all tickers.
        timeframe (str, optional): Specific timeframe to export. If None, exports all timeframes.
        output_file (str, optional): Output CSV file path. If None, generates a filename.
    """
    db = DBHandler()
    try:
        # Build the base query
        query = """
        SELECT
            ohlc.*,

            -- CE Data
            ce.atr_period,
            ce.atr_multiplier,
            ce.atr_value,
            ce.long_stop,
            ce.short_stop,
            ce.direction,
            ce.buy_signal,
            ce.sell_signal,

            -- EMA values by period
            ema_11.value AS ema_11,
            ema_22.value AS ema_22,
            ema_50.value AS ema_50,
            ema_200.value AS ema_200,

            -- OBV
            obv.obv,
            obv.ma_period,
            obv.ma_value,
            obv.bb_std,
            obv.upper_band,
            obv.lower_band,

            -- RSI 14
            rsi.value AS rsi_14,

            -- Pivot Points
            pivot.pp,
            pivot.r1,
            pivot.r2,
            pivot.r3,
            pivot.s1,
            pivot.s2,
            pivot.s3

        FROM
            ohlc_data ohlc

        LEFT JOIN ce_data ce
            ON ohlc.ticker = ce.ticker
           AND ohlc.timeframe = ce.timeframe
           AND ohlc.timestamp = ce.timestamp

        LEFT JOIN ema_data ema_11
            ON ohlc.ticker = ema_11.ticker
           AND ohlc.timeframe = ema_11.timeframe
           AND ohlc.timestamp = ema_11.timestamp
           AND ema_11.period = 11

        LEFT JOIN ema_data ema_22
            ON ohlc.ticker = ema_22.ticker
           AND ohlc.timeframe = ema_22.timeframe
           AND ohlc.timestamp = ema_22.timestamp
           AND ema_22.period = 22

        LEFT JOIN ema_data ema_50
            ON ohlc.ticker = ema_50.ticker
           AND ohlc.timeframe = ema_50.timeframe
           AND ohlc.timestamp = ema_50.timestamp
           AND ema_50.period = 50

        LEFT JOIN ema_data ema_200
            ON ohlc.ticker = ema_200.ticker
           AND ohlc.timeframe = ema_200.timeframe
           AND ohlc.timestamp = ema_200.timestamp
           AND ema_200.period = 200

        LEFT JOIN obv_data obv
            ON ohlc.ticker = obv.ticker
           AND ohlc.timeframe = obv.timeframe
           AND ohlc.timestamp = obv.timestamp

        LEFT JOIN rsi_data rsi
            ON ohlc.ticker = rsi.ticker
           AND ohlc.timeframe = rsi.timeframe
           AND ohlc.timestamp = rsi.timestamp
           AND rsi.period = 14

        LEFT JOIN LATERAL (
            SELECT *
            FROM pivot_data p
            WHERE p.ticker = ohlc.ticker
              AND p.timestamp <= ohlc.timestamp
            ORDER BY p.timestamp DESC
            LIMIT 1
        ) AS pivot ON TRUE
        WHERE 1=1
        """
        
        params = []
        
        # Add filters if specified
        if ticker:
            query += " AND ohlc.ticker = %s"
            params.append(ticker)
        if timeframe:
            query += " AND ohlc.timeframe = %s"
            params.append(timeframe)
            
        # Add order by
        query += " ORDER BY ohlc.timestamp DESC"
        
        # Execute query
        logger.info(f"Fetching data for ticker={ticker}, timeframe={timeframe}")
        db.cur.execute(query, params)
        data = db.cur.fetchall()
        
        if not data:
            logger.warning("No data found matching the criteria")
            return
            
        # Convert to DataFrame
        columns = [
            'ticker', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'candle_pattern',
            'atr_period', 'atr_multiplier', 'atr_value', 'long_stop', 'short_stop', 'direction', 'buy_signal', 'sell_signal',
            'ema_11', 'ema_22', 'ema_50', 'ema_200',
            'obv', 'ma_period', 'ma_value', 'bb_std', 'upper_band', 'lower_band',
            'rsi_14',
            'pp', 'r1', 'r2', 'r3', 's1', 's2', 's3'
        ]
        df = pd.DataFrame(data, columns=columns)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Generate output filename if not provided
        if not output_file:
            ticker_str = ticker if ticker else 'all'
            timeframe_str = timeframe if timeframe else 'all'
            output_file = f"ohlc_indicators_{ticker_str}_{timeframe_str}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Data exported to {output_file}")
        
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}")
        raise
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser(description='Export OHLCV data and indicators to CSV')
    parser.add_argument('--ticker', help='Specific ticker to export (e.g., BTCUSDT)')
    parser.add_argument('--timeframe', help='Specific timeframe to export (e.g., 1h)')
    parser.add_argument('--output', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # If no ticker specified, export all tickers
    if not args.ticker:
        for ticker in market_config.TICKERS:
            # If no timeframe specified, export all timeframes
            if not args.timeframe:
                for timeframe in market_config.TIMEFRAMES.keys():
                    export_to_csv(ticker, timeframe, args.output)
            else:
                export_to_csv(ticker, args.timeframe, args.output)
    else:
        # If no timeframe specified, export all timeframes for the specified ticker
        if not args.timeframe:
            for timeframe in market_config.TIMEFRAMES.keys():
                export_to_csv(args.ticker, timeframe, args.output)
        else:
            export_to_csv(args.ticker, args.timeframe, args.output)

if __name__ == "__main__":
    main() 