import pandas as pd
import numpy as np
import logging
from db_handler import DBHandler
from datetime import datetime
from config import market_config

logger = logging.getLogger(__name__)

class RSICalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_rsi(self, ticker: str, timeframe: str) -> None:
        """Calculate RSI for a given ticker and timeframe"""
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

            # Calculate RSI for each configured period
            for period in market_config.RSI_PERIODS:
                df_with_rsi = self._calculate_rsi_values(df.copy(), period)
                self._save_rsi_data(ticker, timeframe, df_with_rsi, period)
                logger.info(f"Calculated and saved RSI with period {period} for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating RSI for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()

    def _calculate_rsi_values(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        Calculate RSI values using the provided algorithm
        """
        # Calculate price differences
        df['dif'] = df['close'].diff()
        
        # Separate gains and losses
        df['win'] = np.where(df['dif'] > 0, df['dif'], 0)
        df['loss'] = np.where(df['dif'] < 0, abs(df['dif']), 0)
        
        # Calculate exponential moving averages
        df['ema_win'] = df.win.ewm(alpha=1/period).mean()
        df['ema_loss'] = df.loss.ewm(alpha=1/period).mean()
        
        # Calculate RS and RSI
        df['rs'] = df.ema_win / df.ema_loss
        # Handle case where ema_loss is 0 (all gains, no losses)
        df['rsi'] = np.where(df['ema_loss'] == 0, 100, 100 - (100 / (1 + df.rs)))
        
        # Debug log
        if not df.empty:
            last_row = df.iloc[-1]
            logger.debug(f"Last calculated RSI value: timestamp={last_row['timestamp']}, RSI={last_row['rsi']}")
            
        return df

    def _save_rsi_data(self, ticker: str, timeframe: str, df: pd.DataFrame, period: int) -> None:
        """Save RSI values to database"""
        try:
            # Prepare records for database
            rsi_records = []
            for timestamp, rsi_value in zip(df['timestamp'], df['rsi']):
                # Convert timestamp from milliseconds to UTC datetime
                dt = datetime.utcfromtimestamp(timestamp / 1000)
                
                # Skip NaN values (usually at the beginning of the series)
                if pd.isna(rsi_value):
                    continue
                    
                rsi_records.append({
                    'ticker': ticker,
                    'timeframe': timeframe,
                    'timestamp': dt,
                    'period': period,
                    'value': float(rsi_value)
                })
            
            # Save to database if we have records
            if rsi_records:
                self.db.save_rsi_data(rsi_records)
                logger.info(f"Saved {len(rsi_records)} RSI records for period {period}")
            else:
                logger.warning(f"No valid RSI records to save for period {period}")
                
        except Exception as e:
            logger.error(f"Error saving RSI data: {str(e)}")
            raise 