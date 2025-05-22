import pandas as pd
import numpy as np
import logging
from core import DBHandler
from datetime import datetime
from config import market_config

logger = logging.getLogger(__name__)

class PivotCalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_pivots(self, ticker: str, timeframe: str = "1M") -> None:
        """Calculate monthly pivot points for a given ticker"""
        try:
            # For pivots, we only work with monthly timeframe
            if timeframe != "1M":
                logger.info(f"Pivots are only calculated for monthly timeframe, not for {timeframe}")
                return

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

            # Calculate pivots
            df_with_pivots = self._calculate_pivot_values(df.copy())
            self._save_pivot_data(ticker, timeframe, df_with_pivots)
            logger.info(f"Calculated and saved pivot points for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating pivots for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()

    def _calculate_pivot_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate pivot points based on previous period's High, Low, and Close.
        """
        if df.empty:
            return df
            
        # Need High, Low, Close columns for pivot calculation
        df['High'] = df['high']  # Align column names with the algorithm
        df['Low'] = df['low']
        df['Close'] = df['close']
        
        # Calculate Pivot Point
        df['PP'] = (df.High.shift(1) + df.Low.shift(1) + df.Close.shift(1)) / 3
        
        # Calculate Resistance Levels
        df['R1'] = (2 * df.PP) - df.Low.shift(1)
        df['R2'] = df.PP + (df.High.shift(1) - df.Low.shift(1))
        df['R3'] = df.High.shift(1) + (2 * (df.PP - df.Low.shift(1)))
        df['R4'] = df.PP * 3 + (df.High.shift(1) - 3 * df.Low.shift(1))
        df['R5'] = df.PP * 4 + (df.High.shift(1) - 4 * df.Low.shift(1))
        
        # Calculate Support Levels
        df['S1'] = (2 * df.PP) - df.High.shift(1)
        df['S2'] = df.PP - (df.High.shift(1) - df.Low.shift(1))
        df['S3'] = df.Low.shift(1) - (2 * (df.High.shift(1) - df.PP))
        df['S4'] = df.PP * 3 - (3 * df.High.shift(1) - df.Low.shift(1))
        df['S5'] = df.PP * 4 - (4 * df.High.shift(1) - df.Low.shift(1))
        
        return df

    def _save_pivot_data(self, ticker: str, timeframe: str, df: pd.DataFrame) -> None:
        """Save pivot values to database"""
        try:
            # Prepare records for database
            pivot_records = []
            for idx, row in df.iterrows():
                # Skip rows with NaN values for PP (first row will always be NaN)
                if pd.isna(row['PP']):
                    continue
                    
                # Convert timestamp from milliseconds to UTC datetime
                dt = datetime.utcfromtimestamp(row['timestamp'] / 1000)
                
                pivot_records.append({
                    'ticker': ticker,
                    'timeframe': timeframe,
                    'timestamp': dt,
                    'pp': float(row['PP']),
                    'r1': float(row['R1']),
                    'r2': float(row['R2']),
                    'r3': float(row['R3']),
                    'r4': float(row['R4']),
                    'r5': float(row['R5']),
                    's1': float(row['S1']),
                    's2': float(row['S2']),
                    's3': float(row['S3']),
                    's4': float(row['S4']),
                    's5': float(row['S5']),
                })
            
            # Save to database if we have records
            if pivot_records:
                self.db.save_pivot_data(pivot_records)
                logger.info(f"Saved {len(pivot_records)} pivot records")
            else:
                logger.warning(f"No valid pivot records to save")
                
        except Exception as e:
            logger.error(f"Error saving pivot data: {str(e)}")
            raise 