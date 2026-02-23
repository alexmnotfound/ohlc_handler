# OHLC Data Handler

A service for collecting OHLC (Open, High, Low, Close) data from Binance, storing it in PostgreSQL, and calculating technical indicators with cron-driven updates and a FastAPI interface.

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

Defaults and market settings live in `config.py`: tickers `BTCUSDT`, `ETHUSDT`; timeframes `1h`, `4h`, `1d`, `1w`, `1M`; indicator periods (EMA 11/22/50/200, RSI 14, OBV MA 20, CE 22/3.0, pivots monthly). `LOOKBACK_DAYS` controls how much history is fetched when backfilling or extending from the last candle to ensure enough data for indicators.

**Adding a new ticker:** Append the symbol to `TICKERS` in `config.py`, then commit and deploy so the VPS (and any clone) gets the updated list; restart the OHLC API after deploy. For a new ticker there is no last candle in the DB, so the fetcher automatically uses `LOOKBACK_DAYS` per timeframe and backfills from (now − lookback) to now. Run the usual update (CLI `python processor.py` or API `POST /update/{symbol}/{timeframe}`, or update all symbols/timeframes); no extra step is required.

## Database

Schema and setup helpers are in `db/`. Timestamp columns are stored as `timestamp without time zone` and all data is treated as UTC.

## Cron (scheduled updates)

There is no in-app scheduler. Use cron on the host (e.g. the VM) to call `POST /timeframe/{timeframe}/update` shortly after each candle close, at fixed UTC times. Use the `calculate_indicators=false` query parameter when you want a data-only update.

## API

Endpoints:

- `GET /` health message
- `GET /status` database connectivity status
- `GET /ohlc/{symbol}/{timeframe}` with optional `start_date`, `end_date`, `limit`
- `POST /timeframe/{timeframe}/update` update all symbols for one timeframe (for cron)
- `POST /update/timeframe/{timeframe}` alias for the same cron update
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
| POST | `/timeframe/{timeframe}/update` | Update all symbols for one timeframe (for cron) |
| POST | `/update/{symbol}/{timeframe}` | Update one symbol/timeframe (optional `?calculate_indicators=false`) |
| POST | `/update/{symbol}` | Update all timeframes for one symbol |
| POST | `/update` | Update all symbols and timeframes |

**Allowed values:** `symbol` ∈ list in `config.py` (`TICKERS`); `timeframe` ∈ `1h`, `4h`, `1d`, `1w`, `1M`. Dates: `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` (UTC).

### Reading from the database

Connect to the same Postgres instance with `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. Tables (all timestamps UTC, `timestamp without time zone`): `ohlc_data`, `ema_data`, `rsi_data`, `obv_data`, `ce_data`, `pivot_data`.
