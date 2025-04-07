import numpy as np
import pandas as pd
from typing import List, Dict
import logging
from db_handler import DBHandler
from datetime import datetime, timezone
from config import market_config

logger = logging.getLogger(__name__)

class IndicatorCalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_indicators(self, ticker: str, timeframe: str) -> None:
        """Calculate EMA indicator for a given ticker and timeframe"""
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
            df['close'] = pd.to_numeric(df['close'])
            df['timestamp'] = pd.to_numeric(df['timestamp'])

            # Calculate EMA
            self._calculate_ema(df, ticker, timeframe)
            logger.info(f"Calculated EMA indicators for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating indicators for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()

    def _calculate_ema(self, df: pd.DataFrame, ticker: str, timeframe: str) -> None:
        """Calculate Exponential Moving Average and save to database"""
        try:
            # Calculate and save EMAs one period at a time
            for period in market_config.EMA_PERIODS:
                # Calculate EMA for this period
                ema_values = df['close'].ewm(span=period, adjust=False).mean()
                
                # Prepare records for this period
                ema_records = []
                for timestamp, value in zip(df['timestamp'], ema_values):
                    # Convert timestamp from milliseconds to naive UTC datetime
                    # Must use naive datetime just like in save_klines
                    dt = datetime.utcfromtimestamp(timestamp / 1000)  # No timezone info
                    ema_records.append({
                        'ticker': ticker,
                        'timeframe': timeframe,
                        'timestamp': dt,
                        'period': period,
                        'value': float(value)
                    })
                
                # Save this period's records to database
                if ema_records:
                    self.db.save_ema_data(ema_records)
                    logger.info(f"Saved {len(ema_records)} EMA records for period {period}")
            
        except Exception as e:
            logger.error(f"Error calculating EMA: {str(e)}")
            raise 