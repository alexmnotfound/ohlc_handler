import aiohttp
import logging
from typing import List, Optional
from datetime import datetime, timezone
from config import api_config

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