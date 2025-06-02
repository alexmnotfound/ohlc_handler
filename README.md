# OHLC Data Handler

A service for handling OHLC (Open, High, Low, Close) data from Binance, with support for various technical indicators and automated data updates.

## Features

- Fetches OHLC data from Binance for multiple trading pairs
- Supports multiple timeframes (1h, 4h, 1d, 1w, 1M)
- Calculates technical indicators:
  - EMA (Exponential Moving Average)
  - RSI (Relative Strength Index)
  - OBV (On-Balance Volume)
  - Pivot Points
  - Chandelier Exit
  - Candle Patterns
- Automated data updates via scheduler
- RESTful API endpoints for data access and updates
- PostgreSQL database for data storage

## Prerequisites

- Python 3.11 or higher
- PostgreSQL
- Docker and Docker Compose (for containerized deployment)

## Installation

### Local Development Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd ohlc_handler
```

2. Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env_example .env
# Edit .env with your configuration
```

### Local testing

0. Set up postgres:
```bash
  docker run --name local-postgres -e POSTGRES_DB=your_db_name -e POSTGRES_USER=your_db_user -e POSTGRES_PASSWORD=your_db_password -p 5432:5432 -d postgres
```

1. Spin it up
```bash
uvicorn api:app --reload --log-level debug
```


### Docker Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd ohlc_handler
```

2. Set up environment variables:
```bash
cp .env_example .env
# Edit .env with your configuration
```

3. Update the `.env` file with your database credentials:
```env
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_secure_password
BINANCE_API_URL=https://api.binance.com
```

4. Build and start the containers:
```bash
docker-compose up -d
```

The service will be available at `http://localhost:8000`.

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ohlc_data
DB_USER=your_db_user
DB_PASSWORD=your_db_password

# API Configuration
BINANCE_API_URL=https://api.binance.com
```

### Technical Indicators

The following indicators are calculated by default:

- EMA: 9, 20, 50, 100, 200 periods
- RSI: 14 periods
- OBV: 20-period EMA
- Chandelier Exit: 22 periods, 3.0 multiplier
- Pivot Points: Monthly

## Data Fetching

The service fetches data in batches to optimize API usage and memory management:

- 1h timeframe: 24 candles per batch
- 4h timeframe: 6 candles per batch
- 1d timeframe: 7 candles per batch
- 1w timeframe: 4-5 candles per batch
- 1M timeframe: 3 candles per batch

## API Endpoints

### Data Management

- `GET /ohlc/{symbol}/{timeframe}`: Get OHLC data
  ```bash
  # Get last 10 candles for BTCUSDT on 1h timeframe
  curl "http://localhost:8000/ohlc/BTCUSDT/1h"
  
  # Get candles with date range and limit
  curl "http://localhost:8000/ohlc/BTCUSDT/1h?start_date=2024-03-01&end_date=2024-03-15&limit=100"
  ```

- `POST /update/{symbol}/{timeframe}`: Update OHLC data
  ```bash
  # Update BTCUSDT data for 1h timeframe
  curl -X POST "http://localhost:8000/update/BTCUSDT/1h"
  
  # Update without calculating indicators
  curl -X POST "http://localhost:8000/update/BTCUSDT/1h?calculate_indicators=false"
  ```

- `GET /status`: Get service status
  ```bash
  # Check service and database status
  curl "http://localhost:8000/status"
  ```

### Example Responses

#### OHLC Data Response
```json
[
  {
    "symbol": "BTCUSDT",
    "timeframe": "1h",
    "timestamp": 1710000000000,
    "datetime": "2024-03-10T12:00:00+00:00",
    "open": 68500.50,
    "high": 68700.25,
    "low": 68400.75,
    "close": 68650.00,
    "volume": 125.45,
    "indicators": {
      "rsi": {
        "14": 65.23
      },
      "ema": {
        "9": 68500.25,
        "20": 68450.75,
        "50": 68300.50,
        "100": 68100.25,
        "200": 67800.00
      },
      "obv": {
        "value": 1250000.45,
        "ma": 1245000.75,
        "upper_band": 1260000.25,
        "lower_band": 1230000.50
      },
      "ce": {
        "atr": 250.75,
        "long_stop": 68300.25,
        "short_stop": 68800.50,
        "direction": "long",
        "buy_signal": true,
        "sell_signal": false
      },
      "pivot": {
        "pp": 68500.00,
        "r1": 68800.00,
        "r2": 69100.00,
        "r3": 69400.00,
        "r4": 69700.00,
        "r5": 70000.00,
        "s1": 68200.00,
        "s2": 67900.00,
        "s3": 67600.00,
        "s4": 67300.00,
        "s5": 67000.00
      }
    }
  }
]
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
