import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Optional
import logging
from config import db_config, market_config
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DBHandler:
    def __init__(self):
        """Initialize database connection"""
        try:
            self.conn = psycopg2.connect(
                host=db_config.host,
                port=db_config.port,
                database=db_config.database,
                user=db_config.user,
                password=db_config.password
            )
            self.cur = self.conn.cursor()
            logger.info(f"Successfully connected to PostgreSQL at {db_config.host}:{db_config.port}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def get_last_candle(self, ticker: str, timeframe: str) -> Optional[tuple]:
        """Get the last candle for a given ticker and timeframe"""
        try:
            self.cur.execute(
                """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM ohlc_data 
                WHERE ticker = %s AND timeframe = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
                """,
                (ticker, timeframe)
            )
            return self.cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting last candle: {str(e)}")
            return None

    def get_last_candle_date(self, ticker: str, timeframe: str) -> Optional[datetime]:
        """Get the timestamp of the last candle for a given ticker and timeframe"""
        try:
            self.cur.execute(
                """
                SELECT timestamp 
                FROM ohlc_data 
                WHERE ticker = %s AND timeframe = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
                """,
                (ticker, timeframe)
            )
            result = self.cur.fetchone()
            return result[0] if result else None
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

    def get_klines(self, symbol: str, interval: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get OHLC data from database for a given symbol and timeframe"""
        try:
            query = """
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
            """
            params = [symbol, interval]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            
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

    def save_ce_data(self, ce_records: List[Dict]):
        """Save Chandelier Exit data to PostgreSQL ce_data table"""
        try:
            # Prepare data for batch insert
            values = []
            for record in ce_records:
                values.append((
                    record['ticker'],
                    record['timeframe'],
                    record['timestamp'],
                    record['atr_period'],
                    record['atr_multiplier'],
                    record['atr_value'],
                    record['long_stop'],
                    record['short_stop'],
                    record['direction'],
                    record['buy_signal'],
                    record['sell_signal']
                ))

            if values:
                execute_values(
                    self.cur,
                    """
                    INSERT INTO ce_data (
                        ticker, timeframe, timestamp, atr_period, atr_multiplier, atr_value,
                        long_stop, short_stop, direction, buy_signal, sell_signal
                    )
                    VALUES %s
                    ON CONFLICT (ticker, timeframe, timestamp) DO NOTHING
                    """,
                    values
                )
                self.conn.commit()
                logger.info(f"Successfully saved {len(values)} Chandelier Exit records")
            else:
                logger.warning("No Chandelier Exit data to save")
                
        except Exception as e:
            logger.error(f"Error saving Chandelier Exit data: {str(e)}")
            self.conn.rollback()
            raise

    def get_rsi_data(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get RSI data from database for a given symbol and timeframe"""
        try:
            query = """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    period,
                    value
                FROM rsi_data 
                WHERE ticker = %s AND timeframe = %s 
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            return [{
                'timestamp': int(row[0]),
                'period': row[1],
                'value': float(row[2]) if row[2] is not None else None
            } for row in results]
        except Exception as e:
            logger.error(f"Error fetching RSI data: {str(e)}")
            return []

    def get_ema_data(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get EMA data from database for a given symbol and timeframe"""
        try:
            query = """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    period,
                    value
                FROM ema_data 
                WHERE ticker = %s AND timeframe = %s 
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            return [{
                'timestamp': int(row[0]),
                'period': row[1],
                'value': float(row[2]) if row[2] is not None else None
            } for row in results]
        except Exception as e:
            logger.error(f"Error fetching EMA data: {str(e)}")
            return []

    def get_obv_data(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get OBV data from database for a given symbol and timeframe"""
        try:
            query = """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    obv,
                    ma_value,
                    upper_band,
                    lower_band
                FROM obv_data 
                WHERE ticker = %s AND timeframe = %s 
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            return [{
                'timestamp': int(row[0]),
                'obv': float(row[1]) if row[1] is not None else None,
                'ma_value': float(row[2]) if row[2] is not None else None,
                'upper_band': float(row[3]) if row[3] is not None else None,
                'lower_band': float(row[4]) if row[4] is not None else None
            } for row in results]
        except Exception as e:
            logger.error(f"Error fetching OBV data: {str(e)}")
            return []

    def get_ce_data(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get Chandelier Exit data from database for a given symbol and timeframe"""
        try:
            query = """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    atr_value,
                    long_stop,
                    short_stop,
                    direction,
                    buy_signal,
                    sell_signal
                FROM ce_data 
                WHERE ticker = %s AND timeframe = %s 
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            return [{
                'timestamp': int(row[0]),
                'atr_value': float(row[1]) if row[1] is not None else None,
                'long_stop': float(row[2]) if row[2] is not None else None,
                'short_stop': float(row[3]) if row[3] is not None else None,
                'direction': row[4],
                'buy_signal': row[5],
                'sell_signal': row[6]
            } for row in results]
        except Exception as e:
            logger.error(f"Error fetching Chandelier Exit data: {str(e)}")
            return []

    def get_pivot_data(self, symbol: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[Dict]:
        """Get Pivot Points data from database for a given symbol and timeframe"""
        try:
            query = """
                SELECT 
                    EXTRACT(EPOCH FROM timestamp) * 1000 as timestamp_ms,
                    pp,
                    r1, r2, r3, r4, r5,
                    s1, s2, s3, s4, s5
                FROM pivot_data 
                WHERE ticker = %s AND timeframe = %s 
            """
            params = [symbol, timeframe]
            
            if start_date:
                query += " AND timestamp >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND timestamp <= %s"
                params.append(end_date)
            
            query += " ORDER BY timestamp ASC"
            
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            return [{
                'timestamp': int(row[0]),
                'pp': float(row[1]) if row[1] is not None else None,
                'r1': float(row[2]) if row[2] is not None else None,
                'r2': float(row[3]) if row[3] is not None else None,
                'r3': float(row[4]) if row[4] is not None else None,
                'r4': float(row[5]) if row[5] is not None else None,
                'r5': float(row[6]) if row[6] is not None else None,
                's1': float(row[7]) if row[7] is not None else None,
                's2': float(row[8]) if row[8] is not None else None,
                's3': float(row[9]) if row[9] is not None else None,
                's4': float(row[10]) if row[10] is not None else None,
                's5': float(row[11]) if row[11] is not None else None
            } for row in results]
        except Exception as e:
            logger.error(f"Error fetching Pivot Points data: {str(e)}")
            return []

    def close(self):
        """Close the database connection"""
        try:
            self.cur.close()
            self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
            raise 