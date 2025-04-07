from dataclasses import dataclass, field
from typing import List, Dict
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

@dataclass
class MarketConfig:
    DEFAULT_START_DATE: datetime = datetime(2020, 1, 1)
    # Trading pairs to monitor
    TICKERS: List[str] = field(default_factory=lambda: [
        'BTCUSDT',
        'ETHUSDT'
    ])

    # Timeframes to collect data for
    TIMEFRAMES: Dict[str, str] = field(default_factory=lambda: {
        # '1m': '1m',
        # '5m': '5m',
        # '15m': '15m',
        '1h': '1h',
        '4h': '4h',
        '1d': '1d',
        '1w': '1w',
        '1M': '1M'
    })

    # EMA periods to calculate
    EMA_PERIODS: List[int] = field(default_factory=lambda: [9, 20, 50, 100, 200])

    # RSI configuration
    RSI_PERIOD: int = 14
    RSI_OVERBOUGHT: int = 70
    RSI_OVERSOLD: int = 30

    # Pivot Points configuration
    PIVOT_PERIOD: str = '1M'  # Period for pivot point calculation

    # Chandelier Exit configuration
    CE_PERIOD: int = 22
    CE_MULTIPLIER: float = 3.0

    # OBV configuration
    OBV_MA_PERIOD: int = 20
    OBV_BB_PERIOD: int = 20
    OBV_BB_STD: float = 2.0

@dataclass
class APIConfig:
    BASE_URL: str = os.getenv("BINANCE_API_URL", "https://api.binance.com")
    RATE_LIMIT: int = 1200  # requests per minute
    REQUEST_TIMEOUT: int = 10  # seconds
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1  # seconds

@dataclass
class DatabaseConfig:
    def __init__(self):
        self.host = os.getenv("DB_HOST", "localhost")
        self.port = os.getenv("DB_PORT", "5432")
        self.database = os.getenv("DB_NAME", "YOUR_DB_NAME")
        self.user = os.getenv("DB_USER", "YOUR_DB_USER")
        self.password = os.getenv("DB_PASSWORD", "YOUR_DB_PASSWORD")

    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

# Create instances of config classes
market_config = MarketConfig()
api_config = APIConfig()
db_config = DatabaseConfig() 