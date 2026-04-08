import pandas as pd
import logging
from typing import Optional
from core import DBHandler
from datetime import datetime
from config import market_config

logger = logging.getLogger(__name__)


class ATRCalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate_atr(self, ticker: str, timeframe: str, only_save_last_n: Optional[int] = None) -> None:
        """Calculate ATR (Wilder's RMA) for all configured periods."""
        try:
            data = self.db.get_klines(ticker, timeframe)
            if not data:
                logger.warning(f"No data found for {ticker} {timeframe}")
                return

            max_period = max(market_config.ATR_PERIODS)
            if len(data) < max_period:
                logger.warning(
                    f"Not enough candles for ATR {ticker} {timeframe}: "
                    f"required {max_period}, got {len(data)}"
                )
                return

            df = pd.DataFrame(data, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            df['high'] = pd.to_numeric(df['high'])
            df['low'] = pd.to_numeric(df['low'])
            df['close'] = pd.to_numeric(df['close'])
            df['timestamp'] = pd.to_numeric(df['timestamp'])

            # True Range
            df['tr'] = pd.concat([
                df['high'] - df['low'],
                (df['high'] - df['close'].shift(1)).abs(),
                (df['low'] - df['close'].shift(1)).abs(),
            ], axis=1).max(axis=1)

            atr_records = []
            for period in market_config.ATR_PERIODS:
                atr = self._wilder_rma(df['tr'], period)
                for i, (idx, row) in enumerate(df.iterrows()):
                    val = atr.iloc[i]
                    if pd.isna(val):
                        continue
                    dt = datetime.utcfromtimestamp(row['timestamp'] / 1000)
                    atr_records.append({
                        'ticker': ticker,
                        'timeframe': timeframe,
                        'timestamp': dt,
                        'period': period,
                        'value': float(val),
                    })

            if only_save_last_n is not None and only_save_last_n > 0:
                atr_records = atr_records[-only_save_last_n * len(market_config.ATR_PERIODS):]

            if atr_records:
                self.db.save_atr_data(atr_records)
                logger.info(f"Saved {len(atr_records)} ATR records for {ticker} {timeframe}")
            else:
                logger.warning(f"No valid ATR records for {ticker} {timeframe}")

        except Exception as e:
            logger.error(f"Error calculating ATR for {ticker} {timeframe}: {str(e)}")
            raise
        finally:
            self.db.close()

    @staticmethod
    def _wilder_rma(series: pd.Series, period: int) -> pd.Series:
        """Wilder's smoothed moving average (RMA), same as TradingView ta.atr()."""
        result = pd.Series(index=series.index, dtype=float)
        alpha = 1 / period
        # Seed with SMA of first `period` values
        first_valid = series.iloc[:period].mean()
        result.iloc[period - 1] = first_valid
        for i in range(period, len(series)):
            result.iloc[i] = result.iloc[i - 1] * (1 - alpha) + series.iloc[i] * alpha
        return result
