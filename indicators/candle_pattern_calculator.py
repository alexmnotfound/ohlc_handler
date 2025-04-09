import pandas as pd
import numpy as np
import logging
from db_handler import DBHandler
from datetime import datetime
from config import market_config

logger = logging.getLogger(__name__)

class CandlePatternCalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_patterns(self, ticker: str, timeframe: str) -> None:
        """Calculate candlestick patterns for a given ticker and timeframe"""
        try:
            # Fetch OHLC data from database
            data = self.db.get_klines(ticker, timeframe)
            if not data:
                logger.warning(f"No data found for {ticker} {timeframe}")
                return

            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # Convert numeric columns
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['close'] = pd.to_numeric(df['close'])
            df['timestamp'] = pd.to_numeric(df['timestamp'])

            # Calculate patterns
            df_with_patterns = self._calculate_patterns(df.copy())
            self._save_patterns(ticker, timeframe, df_with_patterns)
            logger.info(f"Calculated and saved candlestick patterns for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating patterns for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()

    def _calculate_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate various candlestick patterns"""
        if df.empty:
            return df

        # Calculate basic candle properties
        df['body_size'] = abs(df['close'] - df['open'])
        df['upper_wick'] = df['high'] - df[['open', 'close']].max(axis=1)
        df['lower_wick'] = df[['open', 'close']].min(axis=1) - df['low']
        df['is_bullish'] = df['close'] > df['open']
        df['is_doji'] = df['body_size'] < (df['high'] - df['low']) * 0.1  # Body less than 10% of range
        df['total_range'] = df['high'] - df['low']
        df['body_ratio'] = df['body_size'] / df['total_range']
        df['upper_wick_ratio'] = df['upper_wick'] / df['total_range']
        df['lower_wick_ratio'] = df['lower_wick'] / df['total_range']

        # Initialize pattern column
        df['pattern'] = ''

        # Calculate patterns
        for i in range(1, len(df)):
            # Single candle patterns
            if df.loc[i, 'is_doji']:
                df.loc[i, 'pattern'] = 'Doji'
            elif df.loc[i, 'is_bullish']:
                if df.loc[i, 'lower_wick_ratio'] > 0.6 and df.loc[i, 'body_ratio'] < 0.3:
                    df.loc[i, 'pattern'] = 'Hammer'
                elif df.loc[i, 'body_ratio'] > 0.8 and df.loc[i, 'upper_wick_ratio'] < 0.1 and df.loc[i, 'lower_wick_ratio'] < 0.1:
                    df.loc[i, 'pattern'] = 'Bullish Marubozu'
                elif df.loc[i, 'body_ratio'] > 0.5 and df.loc[i, 'lower_wick_ratio'] > 0.3:
                    df.loc[i, 'pattern'] = 'Inverted Hammer'
            else:  # Bearish candle
                if df.loc[i, 'upper_wick_ratio'] > 0.6 and df.loc[i, 'body_ratio'] < 0.3:
                    df.loc[i, 'pattern'] = 'Shooting Star'
                elif df.loc[i, 'body_ratio'] > 0.8 and df.loc[i, 'upper_wick_ratio'] < 0.1 and df.loc[i, 'lower_wick_ratio'] < 0.1:
                    df.loc[i, 'pattern'] = 'Bearish Marubozu'
                elif df.loc[i, 'body_ratio'] > 0.5 and df.loc[i, 'upper_wick_ratio'] > 0.3:
                    df.loc[i, 'pattern'] = 'Hanging Man'

            # Two candle patterns
            if i > 0:
                prev = i - 1
                curr_body = df.loc[i, 'body_size']
                prev_body = df.loc[prev, 'body_size']
                
                # Engulfing patterns
                if (df.loc[prev, 'is_bullish'] and not df.loc[i, 'is_bullish'] and 
                    df.loc[i, 'close'] < df.loc[prev, 'open'] and 
                    df.loc[i, 'open'] > df.loc[prev, 'close'] and
                    curr_body > prev_body * 1.5):
                    df.loc[i, 'pattern'] = 'Bearish Engulfing'
                elif (not df.loc[prev, 'is_bullish'] and df.loc[i, 'is_bullish'] and 
                      df.loc[i, 'close'] > df.loc[prev, 'open'] and 
                      df.loc[i, 'open'] < df.loc[prev, 'close'] and
                      curr_body > prev_body * 1.5):
                    df.loc[i, 'pattern'] = 'Bullish Engulfing'
                
                # Tweezer patterns
                elif (df.loc[prev, 'is_bullish'] and not df.loc[i, 'is_bullish'] and
                      abs(df.loc[prev, 'high'] - df.loc[i, 'high']) < df.loc[i, 'total_range'] * 0.1):
                    df.loc[i, 'pattern'] = 'Tweezer Top'
                elif (not df.loc[prev, 'is_bullish'] and df.loc[i, 'is_bullish'] and
                      abs(df.loc[prev, 'low'] - df.loc[i, 'low']) < df.loc[i, 'total_range'] * 0.1):
                    df.loc[i, 'pattern'] = 'Tweezer Bottom'

            # Three candle patterns
            if i > 1:
                prev2 = i - 2
                prev1 = i - 1
                curr = i
                
                # Morning/Evening Star
                if (not df.loc[prev2, 'is_bullish'] and not df.loc[prev1, 'is_bullish'] and df.loc[curr, 'is_bullish'] and
                    df.loc[curr, 'close'] > df.loc[prev1, 'open'] and
                    df.loc[prev1, 'body_size'] < df.loc[prev2, 'body_size'] * 0.3):
                    df.loc[curr, 'pattern'] = 'Morning Star'
                elif (df.loc[prev2, 'is_bullish'] and df.loc[prev1, 'is_bullish'] and not df.loc[curr, 'is_bullish'] and
                      df.loc[curr, 'close'] < df.loc[prev1, 'open'] and
                      df.loc[prev1, 'body_size'] < df.loc[prev2, 'body_size'] * 0.3):
                    df.loc[curr, 'pattern'] = 'Evening Star'
                
        return df

    def _save_patterns(self, ticker: str, timeframe: str, df: pd.DataFrame) -> None:
        """Save candlestick patterns to database"""
        try:
            # Prepare data for batch update
            values = []
            for idx, row in df.iterrows():
                if row['pattern']:  # Only update if pattern was detected
                    dt = datetime.utcfromtimestamp(row['timestamp'] / 1000)
                    values.append((
                        ticker,
                        timeframe,
                        dt,
                        row['pattern']
                    ))

            if values:
                # Update the candle_pattern column in ohlc_data table
                self.db.cur.executemany(
                    """
                    UPDATE ohlc_data 
                    SET candle_pattern = %s
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    [(pattern, ticker, timeframe, timestamp) for ticker, timeframe, timestamp, pattern in values]
                )
                self.db.conn.commit()
                logger.info(f"Updated {len(values)} candlestick patterns")
            else:
                logger.warning("No candlestick patterns to update")

        except Exception as e:
            logger.error(f"Error saving candlestick patterns: {str(e)}")
            self.db.conn.rollback()
            raise 