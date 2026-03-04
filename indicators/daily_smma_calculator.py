"""
Daily SMMA 99: RMA (Wilder's smoothing) of daily close with period 99.
Matches Pine Script: ta.rma(close, 99) on timeframe "D".
"""
import logging
from typing import List, Dict, Optional
from datetime import datetime
from core import DBHandler

logger = logging.getLogger(__name__)

SMMA_PERIOD = 99


class DailySMMACalculator:
    def __init__(self):
        self.db = DBHandler()

    def calculate(self, ticker: str, only_save_last_n: Optional[int] = None) -> None:
        """Compute Daily SMMA 99 for ticker from 1d close. Requires at least 99 daily candles."""
        try:
            data = self.db.get_klines(ticker, "1d")
            if not data or len(data) < SMMA_PERIOD:
                logger.warning(
                    f"Not enough daily data for {ticker} (need {SMMA_PERIOD}, got {len(data) if data else 0})"
                )
                return
            closes = [float(row[4]) for row in data]
            timestamps_ms = [int(row[0]) for row in data]
            rma_values = self._rma(closes, SMMA_PERIOD)
            records = []
            for i in range(SMMA_PERIOD - 1, len(closes)):
                ts = datetime.utcfromtimestamp(timestamps_ms[i] / 1000)
                records.append({
                    "ticker": ticker,
                    "timestamp": ts,
                    "value": rma_values[i],
                })
            if only_save_last_n and only_save_last_n > 0:
                records = records[-only_save_last_n:]
            if records:
                self.db.save_daily_smma_99(records)
                logger.info(f"Calculated and saved Daily SMMA 99 for {ticker} ({len(records)} rows)")
        except Exception as e:
            logger.error(f"Error calculating Daily SMMA 99 for {ticker}: {str(e)}")
            raise
        finally:
            self.db.close()

    @staticmethod
    def _rma(source: List[float], length: int) -> List[float]:
        """RMA (Wilder's): first value = SMA of first `length` values, then RMA[i] = (RMA[i-1]*(length-1) + source[i])/length."""
        out = [0.0] * len(source)
        if len(source) < length:
            return out
        sma_start = sum(source[:length]) / length
        out[length - 1] = sma_start
        for i in range(length, len(source)):
            out[i] = (out[i - 1] * (length - 1) + source[i]) / length
        return out
