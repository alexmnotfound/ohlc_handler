# OHLC Data Handler

A service for collecting OHLC (Open, High, Low, Close) data from Binance, storing it in PostgreSQL, and calculating technical indicators with scheduled updates and a FastAPI interface.

## Features

- Pulls OHLC data for multiple symbols and timeframes
- Calculates EMA, RSI, OBV, Pivot Points, Chandelier Exit, and candle patterns
- Stores data in PostgreSQL tables per indicator
- Exposes HTTP endpoints for reads and update triggers
- Updates driven by cron (fixed times per timeframe)

## Requirements

- Python 3.11+
- PostgreSQL
- Docker + Docker Compose for containerized workflows

## Quick start (Docker + Make)

- Copy `.env_example` to `.env` and set `DB_*`
- Run `make setup` for first-time setup (build, Postgres, init-db, backfill)
- Run `make up` to start the full stack
- Use `make help` for all targets

## Local development

- Create and activate a virtual environment
- Install dependencies with `pip install -r requirements.txt`
- Start Postgres (Docker or local)
- Initialize tables with `python db/init_db.py`
- Backfill OHLC and indicators with `python processor.py`
- Run the API with `uvicorn api:app --reload --host 0.0.0.0 --port 8000`

## Configuration

Environment variables loaded from `.env`:

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `BINANCE_API_URL` is read in `config.py`; the Binance client uses a fixed `https://api.binance.com/api/v3` base URL

Defaults and market settings live in `config.py`: tickers `BTCUSDT`, `ETHUSDT`; timeframes `1h`, `4h`, `1d`, `1w`, `1M`; indicator periods (EMA 11/22/50/200, RSI 14, OBV MA 20, CE 22/3.0, pivots monthly).

## Database

Schema and setup helpers are in `db/`. Timestamp columns are stored as `timestamp without time zone` and all data is treated as UTC.

## Cron (scheduled updates)

There is no in-app scheduler. Use cron on the host (e.g. the VM) so each timeframe runs at a fixed time (e.g. shortly after candle close). All times below are **UTC**.

| Timeframe | When to run (UTC) | Crontab line |
|-----------|-------------------|---------------|
| 1h        | 5 min past every hour | `5 * * * * curl -sSf -X POST http://localhost:8000/update/timeframe/1h` |
| 4h        | 10 min past at 00,04,08,12,16,20 | `10 0,4,8,12,16,20 * * * curl -sSf -X POST http://localhost:8000/update/timeframe/4h` |
| 1d        | 00:15 daily | `15 0 * * * curl -sSf -X POST http://localhost:8000/update/timeframe/1d` |
| 1w        | Mon 00:20 | `20 0 * * 1 curl -sSf -X POST http://localhost:8000/update/timeframe/1w` |
| 1M        | 1st of month 00:30 | `30 0 1 * * curl -sSf -X POST http://localhost:8000/update/timeframe/1M` |

Replace `http://localhost:8000` with your API base URL if cron runs on another host. To skip indicator calculation: `.../update/timeframe/1h?calculate_indicators=false`.

## API

Endpoints:

- `GET /` health message
- `GET /status` database connectivity status
- `GET /ohlc/{symbol}/{timeframe}` with optional `start_date`, `end_date`, `limit`
- `POST /update/timeframe/{timeframe}` update all symbols for one timeframe (for cron)
- `POST /update/{symbol}/{timeframe}` with optional `calculate_indicators`
- `POST /update/{symbol}` update all timeframes for one symbol
- `POST /update` update all symbols and timeframes

Responses return OHLC data plus indicator values when available. Indicators are calculated on the server and stored in separate tables.

## Alert system integration

The alert system runs as a separate service. It should **read** from the same Postgres DB and **trigger updates** via this API when values are missing or stale.

**Base URL:** `http://localhost:8000` (local) or your OHLC Handler host in production.

### Endpoints summary

| Method | Path | Purpose |
|--------|------|--------|
| GET | `/status` | Check API and DB connectivity |
| GET | `/ohlc/{symbol}/{timeframe}` | Get candles + indicators (optional `start_date`, `end_date`, `limit`) |
| POST | `/update/timeframe/{timeframe}` | Update all symbols for one timeframe (for cron) |
| POST | `/update/{symbol}/{timeframe}` | Update one symbol/timeframe (optional `?calculate_indicators=false`) |
| POST | `/update/{symbol}` | Update all timeframes for one symbol |
| POST | `/update` | Update all symbols and timeframes |

**Allowed values:** `symbol` ∈ `BTCUSDT`, `ETHUSDT`; `timeframe` ∈ `1h`, `4h`, `1d`, `1w`, `1M`. Dates: `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` (UTC).

### Examples (curl)

```bash
# Health and DB status
curl -s http://localhost:8000/status

# Last 10 candles + indicators for BTC 1h (default)
curl -s "http://localhost:8000/ohlc/BTCUSDT/1h"

# Last 50 candles, optional date range
curl -s "http://localhost:8000/ohlc/BTCUSDT/1h?limit=50"
curl -s "http://localhost:8000/ohlc/ETHUSDT/4h?start_date=2025-01-01&end_date=2025-02-01&limit=100"

# Trigger update when data is missing/stale (alert system or cron)
curl -s -X POST "http://localhost:8000/update/timeframe/1h"
curl -s -X POST "http://localhost:8000/update/BTCUSDT/1h"
curl -s -X POST "http://localhost:8000/update/BTCUSDT/1h?calculate_indicators=false"
curl -s -X POST "http://localhost:8000/update/BTCUSDT"
curl -s -X POST "http://localhost:8000/update"
```

### Reading from the database

Connect to the same Postgres instance with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. Tables (all timestamps UTC, `timestamp without time zone`): `ohlc_data`, `ema_data`, `rsi_data`, `obv_data`, `ce_data`, `pivot_data`.
