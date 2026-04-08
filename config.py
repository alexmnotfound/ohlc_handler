from dataclasses import dataclass, field
from typing import List, Dict
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables from .env file
load_dotenv()

@dataclass
class MarketConfig:
    # Market Configuration
    DEFAULT_START_DATE: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)
    TICKERS: List[str] = field(default_factory=lambda: [
    "ARBUSDT", "ATOMUSDT", "AVAXUSDT", "BCHUSDT", "BNBUSDT", "BTCUSDT",
    "DOGEUSDT", "DOTUSDT", "ENAUSDT", "ETCUSDT", "ETHUSDT", "FILUSDT", "HBARUSDT",
    "INJUSDT", "IOTAUSDT", "LINKUSDT", "LTCUSDT", "MANAUSDT", "NEARUSDT", "NEOUSDT",
    "OPUSDT", "POLUSDT", "RENDERUSDT", "RUNEUSDT", "SANDUSDT", "SNXUSDT", "SOLUSDT",
    "SUIUSDT", "SUSDT", "TAOUSDT", "THETAUSDT", "TONUSDT", "UNIUSDT", "XMRUSDT", "XRPUSDT",
    ])
    TIMEFRAMES: Dict[str, str] = field(default_factory=lambda: {
        '1h': '1h',
        '4h': '4h',
        '1d': '1d',
        '1w': '1w',
        '1M': '1M'
    })

    # Technical Indicator Settings
    # EMA periods
    EMA_PERIODS: List[int] = field(default_factory=lambda: [11, 22, 50, 200])

    # RSI settings
    RSI_PERIOD: int = 14
    RSI_OVERBOUGHT: int = 70
    RSI_OVERSOLD: int = 30

    # OBV settings
    OBV_MA_TYPE: str = "EMA" 
    OBV_MA_PERIOD: int = 20
    OBV_BB_STD: float = 2.0

    # ATR settings
    ATR_PERIODS: List[int] = field(default_factory=lambda: [14, 22])

    # Chandelier Exit settings
    CE_PERIOD: int = 22
    CE_MULTIPLIER: float = 3.0

    # Pivot Points settings
    PIVOT_PERIOD: str = '1M'

    # Batch sizes for data fetching (in hours)
    BATCH_SIZES: Dict[str, int] = field(default_factory=lambda: {
        '1h': 24,      # 24 hours = 24 candles
        '4h': 24,      # 24 hours = 6 candles
        '1d': 168,     # 7 days = 7 candles
        '1w': 87000,   # ~10 years = ~522 candles
        '1M': 87000    # ~10 years = ~120 candles
    })

    # Update intervals (in minutes)
    UPDATE_INTERVALS: Dict[str, int] = field(default_factory=lambda: {
        '1h': 5,
        '4h': 15,
        '1d': 60,
        '1w': 360,
        '1M': 1440
    })

    # Lookback days so we have enough candles for indicators (EMA 200, CE 22).
    # Used when DB has no data (initial backfill) and when extending from last candle.
    LOOKBACK_DAYS: Dict[str, int] = field(default_factory=lambda: {
        '1h': 30,       # 30 days = 720 candles
        '4h': 90,       # ~540 candles (need 200+ for EMA)
        '1d': 250,      # 250 candles
        '1w': 1500,     # ~214 weeks
        '1M': 6100      # ~200 months for EMA 200
    })

@dataclass
class APIConfig:
    # Only the base URL comes from environment
    BASE_URL: str = os.getenv("BINANCE_API_URL", "https://api.binance.com")
    
    # Default values for other API settings
    RATE_LIMIT: int = 1200  # requests per minute
    REQUEST_TIMEOUT: int = 10  # seconds
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 1  # seconds

@dataclass
class DatabaseConfig:
    def __init__(self):
        # Database settings from environment - no default values
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.database = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        
        # Validate that all required environment variables are set
        if not all([self.host, self.port, self.database, self.user, self.password]):
            raise ValueError("Missing required database environment variables. Please check your .env file.")

    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class LoggingConfig:
    # Default logging configuration
    LEVEL: str = "INFO"
    FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Create instances of config classes
market_config = MarketConfig()
api_config = APIConfig()
db_config = DatabaseConfig()
logging_config = LoggingConfig() 