# Database Setup

This folder contains scripts for setting up and managing the PostgreSQL database for the OHLC handler.

## Files

- `create_tables.sql`: SQL script for creating all tables with proper schema
- `init_db.py`: Python script to initialize the database
- `fix_timestamp_columns.py`: Utility script to fix any timestamp column issues

## Database Schema

The database includes the following tables:

1. `ohlc_data`: Stores candle data for each ticker and timeframe
2. `ema_data`: Stores EMA indicator values
3. `rsi_data`: Reserved for RSI indicator values (future use)
4. `macd_data`: Reserved for MACD indicator values (future use)

## Important Note About Timestamps

All timestamp columns are defined as `TIMESTAMP WITHOUT TIME ZONE` to ensure consistent behavior across tables. When manipulating timestamps in code, always use naive datetime objects (without timezone info) when storing them in the database.

## Setup Instructions

### Option 1: Using the Python Script

Run the initialization script to create all tables:

```bash
python db/init_db.py
```

### Option 2: Manual Setup in DBeaver or psql

1. Connect to your PostgreSQL database
2. Open and execute the SQL script `create_tables.sql`

## Verifying Setup

To verify that tables were created correctly:

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
```

To check the timestamp column types:

```sql
SELECT table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'public' AND column_name = 'timestamp';
```

All timestamp columns should show `timestamp without time zone`. 