import aiohttp
import logging
from typing import List, Dict, Optional
import time
from datetime import datetime, timezone
from config import api_config, market_config

logger = logging.getLogger(__name__)

class BinanceClient:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.session = None
        self.rate_limit = api_config.RATE_LIMIT
        self.request_timeout = api_config.REQUEST_TIMEOUT
        self.max_retries = api_config.MAX_RETRIES
        self.retry_delay = api_config.RETRY_DELAY

    async def _ensure_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()

    def _get_interval_ms(self, interval: str) -> int:
        """Convert interval string to milliseconds"""
        if interval.endswith('h'):
            return int(interval[:-1]) * 60 * 60 * 1000
        elif interval.endswith('d'):
            return int(interval[:-1]) * 24 * 60 * 60 * 1000
        elif interval.endswith('w'):
            return int(interval[:-1]) * 7 * 24 * 60 * 60 * 1000
        elif interval == '1M':  # Monthly interval
            return 30 * 24 * 60 * 60 * 1000  # Approximate month as 30 days
        else:
            raise ValueError(f"Invalid interval: {interval}")

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[List]:
        """Get klines/candlestick data"""
        try:
            await self._ensure_session()
            
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            if start_time:
                # Ensure start_time is in UTC
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                start_ms = int(start_time.timestamp() * 1000)
                params['startTime'] = start_ms
            
            if end_time:
                # Ensure end_time is in UTC
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                end_ms = int(end_time.timestamp() * 1000)
                params['endTime'] = end_ms
            
            logger.info(f"Making request to Binance API with params: {params}")
            async with self.session.get(f"{self.base_url}/klines", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        # Log first and last candle timestamps from response
                        first_candle_time = datetime.fromtimestamp(data[0][0] / 1000, tz=timezone.utc)
                        last_candle_time = datetime.fromtimestamp(data[-1][0] / 1000, tz=timezone.utc)
                        logger.info(f"Response first candle: {first_candle_time}")
                        logger.info(f"Response last candle: {last_candle_time}")
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"Error fetching klines: {error_text}")
                    return []
                
        except Exception as e:
            logger.error(f"Error in get_klines: {str(e)}")
            return []

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    def get_klines_sync(self, symbol: str, interval: str, start_time: datetime = None, end_time: datetime = None, limit: int = 1000) -> List[Dict]:
        """Fetch klines data from Binance API with optional date range"""
        endpoint = f"{self.base_url}/api/v3/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = int(start_time.timestamp() * 1000)
        if end_time:
            params['endTime'] = int(end_time.timestamp() * 1000)
        
        retry_count = 0
        
        while True:
            try:
                response = requests.get(endpoint, params=params, timeout=self.request_timeout)
                response.raise_for_status()
                
                candles = response.json()
                
                if not isinstance(candles, list):
                    raise ValueError(f"Unexpected API response format: {response.text[:200]}")
                
                # Validate candle data format
                for candle in candles:
                    if not isinstance(candle, list) or len(candle) < 6:
                        raise ValueError(f"Invalid candle data format: {candle}")
                    
                    # Validate numeric values
                    try:
                        timestamp = int(candle[0])
                        open_price = float(candle[1])
                        high = float(candle[2])
                        low = float(candle[3])
                        close = float(candle[4])
                        volume = float(candle[5])
                        
                        # Basic sanity checks
                        if high < low or high < 0 or low < 0 or volume < 0:
                            raise ValueError(f"Invalid price/volume values in candle: {candle}")
                            
                    except (ValueError, TypeError) as e:
                        raise ValueError(f"Invalid numeric values in candle: {candle}") from e
                
                return candles
                
            except requests.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    logger.error(f"Response body: {e.response.text}")
                else:
                    logger.error("No response received from server")
                retry_count += 1
                if retry_count > self.max_retries:
                    logger.error(f"Failed to fetch data after {self.max_retries} retries")
                    raise
                
                wait_time = self.retry_delay * (2 ** retry_count)  # Exponential backoff
                logger.warning(f"Request failed, retrying in {wait_time}s... ({retry_count}/{self.max_retries})")
                time.sleep(wait_time)
                continue
                
            except ValueError as e:
                logger.error(f"Invalid data received for {symbol} {interval}")
                raise
            
            except Exception as e:
                logger.error(f"Unexpected error fetching data for {symbol} {interval}")
                raise 