# Binance OHLCV Data Handler

A Python microservice that fetches and stores OHLCV (Open, High, Low, Close, Volume) data from Binance's KLINES v3 API into a PostgreSQL database. The service supports multiple trading pairs and timeframes, with automatic updates of current candles and calculation of technical indicators.

## Features

- Fetches historical and current OHLCV data from Binance
- Supports multiple trading pairs and timeframes
- Automatically updates current (unclosed) candles
- Stores data in PostgreSQL with UTC timestamps
- Calculates and stores technical indicators:
  - Exponential Moving Average (EMA)
  - Relative Strength Index (RSI)
  - On Balance Volume (OBV)
  - Chandelier Exit (CE)
  - Pivot Points (for monthly timeframe)
  - Candlestick Patterns
- Configurable through environment variables and config files
- Supports command-line arguments for flexible data retrieval

## Prerequisites

- Python 3.7+
- PostgreSQL database
- Access to Binance API (no API key required for public data)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your database configuration:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

4. Set up a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

## Database Setup

Ensure your PostgreSQL database has the required tables. The database schema includes:

### Main OHLC Data Table
```sql
CREATE TABLE ohlc_data (
    ticker text,
    timeframe text,
    timestamp timestamp with time zone,
    open numeric(18, 8),
    high numeric(18, 8),
    low numeric(18, 8),
    close numeric(18, 8),
    volume numeric(18, 8),
    candle_pattern text,
    PRIMARY KEY (ticker, timeframe, timestamp)
);
```

Additional tables for technical indicators (check create_tables.sql for schema):
- `ema_data` - For Exponential Moving Average values
- `rsi_data` - For Relative Strength Index values
- `obv_data` - For On Balance Volume values
- `ce_data` - For Chandelier Exit values

See the schema scripts in the `db` directory for complete database setup.

## Configuration

The service uses two types of configuration:

1. Environment Variables (`.env`):
Used for sensitive information and environment-specific settings:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

2. Application Configuration (`config.py`):
Used for application settings and defaults:

### Market Configuration
- `TICKERS`: List of trading pairs to monitor (default: ['BTCUSDT', 'ETHUSDT'])
- `TIMEFRAMES`: Dictionary of supported timeframes (default: 1h, 4h, 1d, 1w, 1M)
- `DEFAULT_START_DATE`: Default date to start fetching data if no data exists

### Technical Indicator Settings
- EMA:
  - `EMA_PERIODS`: List of periods to calculate (default: [11, 22, 50, 200])
- RSI:
  - `RSI_PERIOD`: Period for calculation (default: 14)
  - `RSI_OVERBOUGHT`: Overbought threshold (default: 70)
  - `RSI_OVERSOLD`: Oversold threshold (default: 30)
- OBV:
  - `OBV_MA_TYPE`: Moving average type (default: "SMA")
  - `OBV_MA_PERIOD`: Period for moving average (default: 14)
  - `OBV_BB_STD`: Standard deviation for Bollinger Bands (default: 2.0)
- Chandelier Exit:
  - `CE_PERIOD`: Period for calculation (default: 22)
  - `CE_MULTIPLIER`: Multiplier for stop calculation (default: 3.0)
- Pivot Points:
  - `PIVOT_PERIOD`: Period for calculation (default: '1M')

### API Configuration
- `BASE_URL`: Binance API base URL (default: "https://api.binance.com")
- `RATE_LIMIT`: Maximum requests per minute (default: 1200)
- `REQUEST_TIMEOUT`: Request timeout in seconds (default: 10)
- `MAX_RETRIES`: Maximum number of retry attempts (default: 3)
- `RETRY_DELAY`: Delay between retries in seconds (default: 1)

## Usage

### Basic Usage

Run the script without arguments to fetch data for all configured pairs and timeframes:
```bash
python main.py
```

The script will:
1. Check the last available data in the database for each pair/timeframe
2. Fetch new data from that point onwards
3. Update any existing candles (including the current one)
4. Calculate technical indicators based on the configuration

### Command-line Arguments

- `--symbol` or `--ticker`: Specific trading pair (e.g., BTCUSDT)
- `--start`: Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
- `--end`: End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
- `--interval` or `--timeframe`: Specific timeframe (e.g., 1h, 4h, 1d)
- `--skip-ohlc`: Skip fetching OHLC data and only calculate indicators
- `--skip-indicators`: Skip calculating indicators and only fetch OHLC data
- `--indicators`: Specify which indicators to calculate (comma-separated list, e.g., rsi,ema,obv,ce)

Examples:

1. Fetch specific symbol and timeframe:
```bash
python main.py --symbol BTCUSDT --interval 1h
```

2. Calculate all indicators for existing data (no new OHLC fetch):
```bash
python main.py --skip-ohlc
```

3. Calculate specific indicators only:
```bash
python main.py --skip-ohlc --indicators rsi,ema
```

4. Calculate Chandelier Exit for specific ticker and timeframe:
```bash
python main.py --skip-ohlc --indicators ce --ticker BTCUSDT --timeframe 1h
```

5. Fetch OHLC data without calculating indicators:
```bash
python main.py --skip-indicators
```

## Technical Indicators

### Exponential Moving Average (EMA)
- Configurable periods in `config.py` (EMA_PERIODS)
- Calculates EMA for close prices
- Stored in `ema_data` table

### Relative Strength Index (RSI)
- Configurable period in `config.py` (RSI_PERIOD)
- Stored in `rsi_data` table
- Optional upper/lower bands for overbought/oversold conditions

### On Balance Volume (OBV)
- Measures buying and selling pressure
- Can calculate moving averages on OBV values
- Supports different MA types (None, SMA, EMA, SMA + Bollinger Bands)
- Configurable period in `config.py` (OBV_MA_PERIOD)
- Stored in `obv_data` table

### Chandelier Exit (CE)
- Volatility-based stop-loss indicator
- Configurable period and multiplier in `config.py` (CE_PERIOD, CE_MULTIPLIER)
- Provides long/short stops and direction signals
- Stored in `ce_data` table
- Implementation matches TradingView's PineScript formula

### Pivot Points
- Calculated for monthly timeframe only
- Provides support and resistance levels
- Stored in `pivot_data` table
- Includes Pivot, R1-R3, and S1-S3 levels

### Candlestick Patterns
- Detects common Japanese candlestick patterns
- Stored in the `candle_pattern` column of `ohlc_data` table
- Detects the following patterns:
  - Single Candle Patterns:
    - Doji (small body, long wicks)
    - Hammer (bullish reversal)
    - Inverted Hammer (bullish reversal)
    - Shooting Star (bearish reversal)
    - Hanging Man (bearish reversal)
    - Marubozu (strong trend)
      - Bullish Marubozu
      - Bearish Marubozu
  - Two Candle Patterns:
    - Bullish/Bearish Engulfing
    - Bullish/Bearish Harami
    - Tweezer Top/Bottom
  - Three Candle Patterns:
    - Morning Star (bullish reversal)
    - Evening Star (bearish reversal)
- Patterns are automatically calculated and stored with OHLC data

## Scheduled Execution

To run the script periodically, create a wrapper script and set up a crontab entry:

1. Create a wrapper script `run_script.sh`:
```bash
#!/bin/bash
cd /path/to/your/ohlc_handler
source venv/bin/activate
python main.py >> logs/cron.log 2>&1
```

2. Make it executable:
```bash
chmod +x run_script.sh
```

3. Add to crontab to run every hour:
```
0 * * * * /path/to/your/ohlc_handler/run_script.sh
```

## Data Management

- The service maintains UTC timestamps for all data
- Current (unclosed) candles are saved and updated automatically
- When fetching new data, the service:
  - Starts from the last available candle in the database
  - Updates any existing candles with new data
  - Adds new candles as they become available
- Technical indicators are calculated based on the latest available data

## Error Handling

The service includes comprehensive error handling for:
- Database connection issues
- API request failures
- Data validation
- Invalid date ranges
- Unsupported timeframes
- Insufficient data for technical indicators

Errors are logged with appropriate context for debugging.

## Logging

The service logs important information and errors to help track its operation:
- Connection status
- Data retrieval progress
- Number of candles processed
- Technical indicator calculation details
- Any errors or warnings

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

[Your License Here] 