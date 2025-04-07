import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Optional
import logging
from config import db_config, market_config
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DBHandler:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(db_config.connection_string)
            self.cur = self.conn.cursor()
            logger.info(f"Successfully connected to PostgreSQL at {db_config.host}:{db_config.port}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def get_last_candle_date(self, symbol: str, interval: str) -> Optional[datetime]:
        """Get the last available candle date for a given symbol and timeframe"""
        try:
            self.cur.execute(
                """
                SELECT timestamp 
                FROM ohlc_data 
                WHERE ticker = %s AND timeframe = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
                """,
                (symbol, interval)
            )
            result = self.cur.fetchone()
            if result:
                # Subtract one interval to ensure we get the last candle again
                last_date = result[0]
                if interval.endswith('h'):
                    hours = int(interval[:-1])
                    return last_date - timedelta(hours=hours)
                elif interval.endswith('d'):
                    days = int(interval[:-1])
                    return last_date - timedelta(days=days)
                elif interval.endswith('w'):
                    weeks = int(interval[:-1])
                    return last_date - timedelta(weeks=weeks)
                elif interval.endswith('m'):
                    # For monthly intervals, subtract 30 days
                    months = int(interval[:-1])
                    return last_date - timedelta(days=30*months)
            return None
        except Exception as e:
            logger.error(f"Error getting last candle date: {str(e)}")
            return None

    def save_klines(self, symbol: str, interval: str, klines_data: List[Dict]):
        """Save klines data to PostgreSQL ohlc_data table"""
        try:
            # Prepare data for batch insert
            values = []
            for kline in klines_data:
                # Convert millisecond timestamp to UTC datetime
                timestamp = datetime.utcfromtimestamp(kline[0] / 1000)
                
                values.append((
                    symbol,              # ticker
                    interval,            # timeframe
                    timestamp,           # timestamp 
                    float(kline[1]),     # open
                    float(kline[2]),     # high
                    float(kline[3]),     # low
                    float(kline[4]),     # close
                    float(kline[5]),     # volume
                    ''                   # candle_pattern 
                ))

            if values:
                execute_values(
                    self.cur,
                    """
                    INSERT INTO ohlc_data (
                        ticker, timeframe, timestamp, open, high, low, close, volume, candle_pattern
                    )
                    VALUES %s
                    ON CONFLICT (ticker, timeframe, timestamp) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        candle_pattern = EXCLUDED.candle_pattern
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} records for {symbol} {interval}")
            else:
                logger.warning(f"No data to save for {symbol} {interval}")
                
        except Exception as e:
            logger.error(f"Error saving klines data: {str(e)}")
            self.conn.rollback()
            raise

    def close(self):
        """Close the database connection"""
        try:
            self.cur.close()
            self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise 