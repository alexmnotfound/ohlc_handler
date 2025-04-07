import pandas as pd
import numpy as np
import logging
from db_handler import DBHandler
from datetime import datetime
from config import market_config

logger = logging.getLogger(__name__)

class CECalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_ce(self, ticker: str, timeframe: str) -> None:
        """Calculate Chandelier Exit for a given ticker and timeframe"""
        try:
            # Fetch OHLC data from database
            data = self.db.get_klines(ticker, timeframe)
            if not data:
                logger.warning(f"No data found for {ticker} {timeframe}")
                return

            # Create DataFrame
            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])

            # Parse types
            df['open'] = pd.to_numeric(df['open'])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['close'] = pd.to_numeric(df['close'])
            df['timestamp'] = pd.to_numeric(df['timestamp'])

            # Calculate CE indicator
            df_with_ce = self._calculate_ce_values(df)
            
            # Save to database
            self._save_ce_data(ticker, timeframe, df_with_ce)
            logger.info(f"Calculated and saved Chandelier Exit for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating CE for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()
            
    def _calculate_ce_values(self, df):
        """
        Calculate the Chandelier Exit indicator exactly matching TradingView's PineScript implementation:
        
        atr = mult * ta.atr(length)
        longStop = (useClose ? ta.highest(close, length) : ta.highest(length)) - atr
        longStopPrev = nz(longStop[1], longStop)
        longStop := close[1] > longStopPrev ? math.max(longStop, longStopPrev) : longStop
        shortStop = (useClose ? ta.lowest(close, length) : ta.lowest(length)) + atr
        shortStopPrev = nz(shortStop[1], shortStop)
        shortStop := close[1] < shortStopPrev ? math.min(shortStop, shortStopPrev) : shortStop
        dir = close > shortStopPrev ? 1 : close < longStopPrev ? -1 : dir
        """
        # Get parameters
        length = market_config.CE_PERIOD
        mult = market_config.CE_MULTIPLIER
        use_close = True  # Default TradingView setting
        
        # Create a working copy of the dataframe
        result = df.copy()
        
        # Calculate ATR exactly like TradingView - traditional implementation
        # First calculate True Range
        result['tr1'] = result['high'] - result['low']
        result['tr2'] = abs(result['high'] - result['close'].shift(1))
        result['tr3'] = abs(result['low'] - result['close'].shift(1))
        result['tr'] = result[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # TradingView's ta.atr() uses Wilder's smoothing (equivalent to RMA in TradingView)
        # This is different from a simple moving average
        alpha = 1/length
        result['atr'] = pd.Series(dtype=float)
        
        # First ATR value is simple average of first 'length' TRs
        first_atr = result['tr'].iloc[:length].mean()
        result.loc[result.index[length-1], 'atr'] = first_atr
        
        # Rest use the RMA formula: atr = prev_atr * (length-1)/length + tr * 1/length
        for i in range(length, len(result)):
            prev_atr = result.loc[result.index[i-1], 'atr']
            curr_tr = result.loc[result.index[i], 'tr']
            result.loc[result.index[i], 'atr'] = prev_atr * (1 - alpha) + curr_tr * alpha
        
        # Multiply by the multiplier
        result['atr'] = result['atr'] * mult
        
        # Initialize the result columns
        result['long_stop'] = pd.Series(dtype=float)
        result['short_stop'] = pd.Series(dtype=float)
        result['dir'] = pd.Series(dtype=int)
        result['dir'].iloc[0] = 1  # Initial direction is long (default in PineScript)
        
        # Process each row to match PineScript's calculation exactly
        for i in range(length, len(result)):
            # Get lookback start index
            start_idx = i - length + 1
            
            # Get current values
            curr_idx = result.index[i]
            curr_atr = result.loc[curr_idx, 'atr']
            
            # Calculate highest/lowest over lookback period
            if use_close:
                highest = result.loc[result.index[start_idx:i+1], 'close'].max()
                lowest = result.loc[result.index[start_idx:i+1], 'close'].min()
            else:
                highest = result.loc[result.index[start_idx:i+1], 'high'].max()
                lowest = result.loc[result.index[start_idx:i+1], 'low'].min()
            
            # Calculate raw stops
            raw_long_stop = highest - curr_atr
            raw_short_stop = lowest + curr_atr
            
            # Get previous values
            if i > 0:
                prev_idx = result.index[i-1]
                prev_close = result.loc[prev_idx, 'close']
                curr_close = result.loc[curr_idx, 'close']
                
                # Get previous stops
                if i > length and not pd.isna(result.loc[prev_idx, 'long_stop']):
                    prev_long_stop = result.loc[prev_idx, 'long_stop']
                    prev_short_stop = result.loc[prev_idx, 'short_stop']
                    
                    # Apply the exact PineScript logic
                    # longStop := close[1] > longStopPrev ? math.max(longStop, longStopPrev) : longStop
                    if prev_close > prev_long_stop:
                        long_stop = max(raw_long_stop, prev_long_stop)
                    else:
                        long_stop = raw_long_stop
                    
                    # shortStop := close[1] < shortStopPrev ? math.min(shortStop, shortStopPrev) : shortStop
                    if prev_close < prev_short_stop:
                        short_stop = min(raw_short_stop, prev_short_stop)
                    else:
                        short_stop = raw_short_stop
                    
                    # Store calculated stops
                    result.loc[curr_idx, 'long_stop'] = long_stop
                    result.loc[curr_idx, 'short_stop'] = short_stop
                    
                    # Update direction
                    # dir := close > shortStopPrev ? 1 : close < longStopPrev ? -1 : dir
                    prev_dir = result.loc[prev_idx, 'dir']
                    if curr_close > prev_short_stop:
                        result.loc[curr_idx, 'dir'] = 1
                    elif curr_close < prev_long_stop:
                        result.loc[curr_idx, 'dir'] = -1
                    else:
                        result.loc[curr_idx, 'dir'] = prev_dir
                else:
                    # First calculation after enough data
                    result.loc[curr_idx, 'long_stop'] = raw_long_stop
                    result.loc[curr_idx, 'short_stop'] = raw_short_stop
                    result.loc[curr_idx, 'dir'] = 1  # Default to long
            else:
                # Initialize first value
                result.loc[curr_idx, 'long_stop'] = raw_long_stop
                result.loc[curr_idx, 'short_stop'] = raw_short_stop
                result.loc[curr_idx, 'dir'] = 1  # Default to long
        
        # Generate buy/sell signals
        result['buy_signal'] = (result['dir'] == 1) & (result['dir'].shift(1) == -1)
        result['sell_signal'] = (result['dir'] == -1) & (result['dir'].shift(1) == 1)
        
        # Clean up temporary columns
        result = result.drop(['tr1', 'tr2', 'tr3', 'tr'], axis=1, errors='ignore')
                
        return result

    def _save_ce_data(self, ticker: str, timeframe: str, df: pd.DataFrame) -> None:
        """Save Chandelier Exit values to database"""
        try:
            ce_records = []
            for idx, row in df.iterrows():
                # Skip rows with NaN values in key fields
                if pd.isna(row['long_stop']) or pd.isna(row['short_stop']) or pd.isna(row['dir']):
                    continue

                dt = datetime.utcfromtimestamp(row['timestamp'] / 1000)

                ce_records.append({
                    'ticker': ticker,
                    'timeframe': timeframe,
                    'timestamp': dt,
                    'atr_period': market_config.CE_PERIOD,
                    'atr_multiplier': market_config.CE_MULTIPLIER,
                    'atr_value': float(row['atr']) if not pd.isna(row['atr']) else None,
                    'long_stop': float(row['long_stop']),
                    'short_stop': float(row['short_stop']),
                    'direction': int(row['dir']),
                    'buy_signal': bool(row['buy_signal']),
                    'sell_signal': bool(row['sell_signal']),
                })

            if ce_records:
                self.db.save_ce_data(ce_records)
                logger.info(f"Saved {len(ce_records)} CE records")
            else:
                logger.warning("No valid CE records to save")

        except Exception as e:
            logger.error(f"Error saving CE data: {str(e)}")
            raise
