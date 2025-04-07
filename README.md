# Binance OHLCV Data Handler

A Python microservice that fetches and stores OHLCV (Open, High, Low, Close, Volume) data from Binance's KLINES v3 API into a PostgreSQL database. The service supports multiple trading pairs and timeframes, with automatic updates of current candles.

## Features

- Fetches historical and current OHLCV data from Binance
- Supports multiple trading pairs and timeframes
- Automatically updates current (unclosed) candles
- Stores data in PostgreSQL with UTC timestamps
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

## Database Setup

Ensure your PostgreSQL database has the following table:

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
- `TICKERS`: List of trading pairs to monitor (default: ['BTCUSDT', 'ETHUSDT'])
- `TIMEFRAMES`: Dictionary of supported timeframes (default: '1h', '4h', '1d')
- `DEFAULT_START_DATE`: Default date to start fetching data if no data exists
- Various technical indicator settings (EMA, RSI, etc.)

The `.env` file is used for sensitive data that shouldn't be in version control, while `config.py` contains the application's configuration structure and default values.

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

### Command-line Arguments

- `--symbol`: Specific trading pair (e.g., BTCUSDT)
- `--start`: Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
- `--end`: End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
- `--interval`: Specific timeframe (e.g., 1h, 4h, 1d)

Examples:

1. Fetch specific symbol and timeframe:
```bash
python main.py --symbol BTCUSDT --interval 1h
```

2. Fetch data for a specific date range:
```bash
python main.py --symbol BTCUSDT --start "2024-01-01" --end "2024-01-07"
```

3. Fetch all pairs for a specific timeframe:
```bash
python main.py --interval 4h
```

## Data Management

- The service maintains UTC timestamps for all data
- Current (unclosed) candles are saved and updated automatically
- When fetching new data, the service:
  - Starts from the last available candle in the database
  - Updates any existing candles with new data
  - Adds new candles as they become available

## Error Handling

The service includes comprehensive error handling for:
- Database connection issues
- API request failures
- Data validation
- Invalid date ranges
- Unsupported timeframes

Errors are logged with appropriate context for debugging.

## Logging

The service logs important information and errors to help track its operation:
- Connection status
- Data retrieval progress
- Number of candles processed
- First and last candle details
- Any errors or warnings

## Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

## License

[Your License Here] 