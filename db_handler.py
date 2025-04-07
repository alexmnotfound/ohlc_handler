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

    def save_ema_data(self, ema_records: List[Dict]):
        """Save EMA data to PostgreSQL ema_data table"""
        try:
            # Prepare data for batch insert
            values = []
            for record in ema_records:
                # Try to use the raw timestamp string to avoid timezone issues
                timestamp_str = record['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                values.append((
                    record['ticker'],
                    record['timeframe'],
                    timestamp_str,  # Use string format instead of datetime object
                    record['period'],
                    record['value']
                ))
            
            if values:
                # Use explicit timestamp casting in the SQL
                # This should override any timezone settings
                self.cur.executemany(
                    """
                    INSERT INTO ema_data (
                        ticker, timeframe, timestamp, period, value
                    )
                    VALUES (%s, %s, %s::timestamp, %s, %s)
                    ON CONFLICT (ticker, timeframe, timestamp, period) DO NOTHING
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} EMA records")
            else:
                logger.warning("No EMA data to save")
                
        except Exception as e:
            logger.error(f"Error saving EMA data: {str(e)}")
            self.conn.rollback()
            raise

    def get_klines(self, symbol: str, interval: str) -> List[Dict]:
        """Get OHLC data from database for a given symbol and timeframe"""
        try:
            self.cur.execute(
                """
                SELECT 
                    timestamp,  -- First get the actual timestamp for debugging
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    EXTRACT(EPOCH FROM timestamp) * 1000 as close_time,
                    0 as quote_asset_volume,
                    0 as number_of_trades,
                    0 as taker_buy_base_asset_volume,
                    0 as taker_buy_quote_asset_volume,
                    '' as ignore
                FROM ohlc_data 
                WHERE ticker = %s AND timeframe = %s 
                ORDER BY timestamp ASC
                """,
                (symbol, interval)
            )
            
            # Convert to list of lists to match Binance API format
            results = self.cur.fetchall()
            if not results:
                logger.warning(f"No OHLC data found for {symbol} {interval}")
                return []
                
            # Convert results to the expected format
            formatted_results = []
            for row in results:
                # Skip the first column (original timestamp) and use the ms timestamp
                formatted_results.append([int(row[1])] + list(row[2:]))
                
            return formatted_results
        except Exception as e:
            logger.error(f"Error fetching OHLC data: {str(e)}")
            return []

    def save_rsi_data(self, rsi_records: List[Dict]):
        """Save RSI data to PostgreSQL rsi_data table"""
        try:
            # Prepare data for batch insert
            values = [
                (
                    record['ticker'],
                    record['timeframe'],
                    record['timestamp'],
                    record['period'],
                    record['value']
                )
                for record in rsi_records
            ]

            if values:
                execute_values(
                    self.cur,
                    """
                    INSERT INTO rsi_data (
                        ticker, timeframe, timestamp, period, value
                    )
                    VALUES %s
                    ON CONFLICT (ticker, timeframe, timestamp, period) DO NOTHING
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} RSI records")
            else:
                logger.warning("No RSI data to save")
                
        except Exception as e:
            logger.error(f"Error saving RSI data: {str(e)}")
            self.conn.rollback()
            raise

    def save_obv_data(self, obv_records: List[Dict]):
        """Save OBV data to PostgreSQL obv_data table"""
        try:
            # Prepare data for batch insert
            values = []
            for record in obv_records:
                values.append((
                    record['ticker'],
                    record['timeframe'],
                    record['timestamp'],
                    record['obv'],
                    record['ma_period'],
                    record['ma_value'],
                    record['bb_std'],
                    record['upper_band'],
                    record['lower_band']
                ))

            if values:
                execute_values(
                    self.cur,
                    """
                    INSERT INTO obv_data (
                        ticker, timeframe, timestamp, obv, ma_period, ma_value, 
                        bb_std, upper_band, lower_band
                    )
                    VALUES %s
                    ON CONFLICT (ticker, timeframe, timestamp) DO NOTHING
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} OBV records")
            else:
                logger.warning("No OBV data to save")
                
        except Exception as e:
            logger.error(f"Error saving OBV data: {str(e)}")
            self.conn.rollback()
            raise

    def save_pivot_data(self, pivot_records: List[Dict]):
        """Save pivot data to PostgreSQL pivot_data table"""
        try:
            # Prepare data for batch insert
            values = []
            for record in pivot_records:
                values.append((
                    record['ticker'],
                    record['timeframe'],
                    record['timestamp'],
                    record['pp'],
                    record['r1'],
                    record['r2'],
                    record['r3'],
                    record['r4'],
                    record['r5'],
                    record['s1'],
                    record['s2'],
                    record['s3'],
                    record['s4'],
                    record['s5']
                ))

            if values:
                execute_values(
                    self.cur,
                    """
                    INSERT INTO pivot_data (
                        ticker, timeframe, timestamp, pp, r1, r2, r3, r4, r5,
                        s1, s2, s3, s4, s5
                    )
                    VALUES %s
                    ON CONFLICT (ticker, timeframe, timestamp) DO NOTHING
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} pivot records")
            else:
                logger.warning("No pivot data to save")
                
        except Exception as e:
            logger.error(f"Error saving pivot data: {str(e)}")
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