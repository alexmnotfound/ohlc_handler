-- Database schema for OHLC Handler
-- This script creates all necessary tables with proper column types

-- OHLC data table for storing candle data
CREATE TABLE IF NOT EXISTS ohlc_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC(18, 8) NOT NULL,
    high NUMERIC(18, 8) NOT NULL,
    low NUMERIC(18, 8) NOT NULL,
    close NUMERIC(18, 8) NOT NULL,
    volume NUMERIC(24, 8) NOT NULL,
    candle_pattern TEXT,
    PRIMARY KEY (ticker, timeframe, timestamp)
);

-- Create index for faster querying by ticker and timeframe
CREATE INDEX IF NOT EXISTS idx_ohlc_ticker_timeframe 
ON ohlc_data(ticker, timeframe);

-- EMA indicator data table
CREATE TABLE IF NOT EXISTS ema_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    period INTEGER NOT NULL,
    value NUMERIC(18, 8) NOT NULL,
    PRIMARY KEY (ticker, timeframe, timestamp, period)
);

-- Create index for faster querying by ticker, timeframe and period
CREATE INDEX IF NOT EXISTS idx_ema_ticker_timeframe_period 
ON ema_data(ticker, timeframe, period);

-- RSI indicator data table (for future use)
CREATE TABLE IF NOT EXISTS rsi_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    period INTEGER NOT NULL,
    value NUMERIC(18, 8) NOT NULL,
    PRIMARY KEY (ticker, timeframe, timestamp, period)
);

-- MACD indicator data table (for future use)
CREATE TABLE IF NOT EXISTS macd_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    fast_period INTEGER NOT NULL,
    slow_period INTEGER NOT NULL,
    signal_period INTEGER NOT NULL,
    macd_value NUMERIC(18, 8) NOT NULL,
    signal_value NUMERIC(18, 8) NOT NULL,
    histogram_value NUMERIC(18, 8) NOT NULL,
    PRIMARY KEY (ticker, timeframe, timestamp, fast_period, slow_period, signal_period)
);

-- OBV (On Balance Volume) data table
CREATE TABLE IF NOT EXISTS obv_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    obv NUMERIC(24, 8) NOT NULL,
    ma_period INTEGER NOT NULL,
    ma_value NUMERIC(24, 8),
    bb_std NUMERIC(8, 4) NOT NULL,
    upper_band NUMERIC(24, 8),
    lower_band NUMERIC(24, 8),
    PRIMARY KEY (ticker, timeframe, timestamp)
);

-- Create index for faster querying by ticker and timeframe
CREATE INDEX IF NOT EXISTS idx_obv_ticker_timeframe 
ON obv_data(ticker, timeframe);

-- Add the pivot_data table if it doesn't exist yet
CREATE TABLE IF NOT EXISTS pivot_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    pp NUMERIC(20, 8) NOT NULL,
    r1 NUMERIC(20, 8) NOT NULL,
    r2 NUMERIC(20, 8) NOT NULL,
    r3 NUMERIC(20, 8) NOT NULL,
    r4 NUMERIC(20, 8) NOT NULL,
    r5 NUMERIC(20, 8) NOT NULL,
    s1 NUMERIC(20, 8) NOT NULL,
    s2 NUMERIC(20, 8) NOT NULL,
    s3 NUMERIC(20, 8) NOT NULL,
    s4 NUMERIC(20, 8) NOT NULL,
    s5 NUMERIC(20, 8) NOT NULL,
    PRIMARY KEY (ticker, timeframe, timestamp)
);

-- Add index for better performance on pivot lookups
CREATE INDEX IF NOT EXISTS idx_pivot_ticker_timeframe 
ON pivot_data(ticker, timeframe);

-- Add the ce_data table if it doesn't exist yet
CREATE TABLE IF NOT EXISTS ce_data (
    ticker TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    atr_period INTEGER NOT NULL,
    atr_multiplier NUMERIC(5, 2) NOT NULL,
    atr_value NUMERIC(20, 8),
    long_stop NUMERIC(20, 8),
    short_stop NUMERIC(20, 8),
    direction INTEGER NOT NULL,
    buy_signal BOOLEAN NOT NULL DEFAULT FALSE,
    sell_signal BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (ticker, timeframe, timestamp)
);

-- Add index for better performance on chandelier exit lookups
CREATE INDEX IF NOT EXISTS idx_ce_ticker_timeframe 
ON ce_data(ticker, timeframe);

-- Comment: All timestamp columns use TIMESTAMP WITHOUT TIME ZONE
-- This ensures consistent behavior between all tables 