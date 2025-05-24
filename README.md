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
- `POST /update/{symbol}/{timeframe}`: Update OHLC data
- `GET /status`: Get service status

### Technical Indicators

- `GET /indicators/{symbol}/{timeframe}`: Get all indicators
- `GET /indicators/{symbol}/{timeframe}/{indicator}`: Get specific indicator

## Automatic Updates

The service automatically updates data at regular intervals:

- 1h timeframe: Every 5 minutes
- 4h timeframe: Every 15 minutes
- 1d timeframe: Every hour
- 1w timeframe: Every 6 hours
- 1M timeframe: Every 24 hours


## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
