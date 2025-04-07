import requests
from typing import List, Dict
import time
from datetime import datetime, timezone
import logging
from config import api_config

logger = logging.getLogger(__name__)

class BinanceClient:
    def __init__(self):
        self.base_url = api_config.BASE_URL
        self.rate_limit = api_config.RATE_LIMIT
        self.request_timeout = api_config.REQUEST_TIMEOUT
        self.max_retries = api_config.MAX_RETRIES
        self.retry_delay = api_config.RETRY_DELAY

    def get_klines(self, symbol: str, interval: str, start_time: datetime = None, end_time: datetime = None, limit: int = 1000) -> List[Dict]:
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

    def _get_interval_ms(self, interval: str) -> int:
        """Convert interval string to milliseconds."""
        multipliers = {
            'm': 60 * 1000,
            'h': 60 * 60 * 1000,
            'd': 24 * 60 * 60 * 1000,
            'w': 7 * 24 * 60 * 60 * 1000
        }
        
        unit = interval[-1]
        number = int(interval[:-1])
        
        return number * multipliers[unit.lower()]

    def close(self):
        """Close the client connection"""
        pass  # No persistent connection to close 