from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel
import logging
import os
import psycopg2
from dotenv import load_dotenv
from processor import fetch_historical_data
from config import market_config, logging_config
from core import DBHandler
from indicators.calculator import IndicatorCalculator
from indicators.rsi_calculator import RSICalculator
from indicators.obv_calculator import OBVCalculator
from indicators.pivot_calculator import PivotCalculator
from indicators.ce_calculator import CECalculator
from indicators.candle_pattern_calculator import CandlePatternCalculator

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, logging_config.LEVEL),
    format=logging_config.FORMAT
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="OHLC Handler API",
    description="API for handling OHLC data and indicators",
    version="1.0.0"
)

class OHLCResponse(BaseModel):
    symbol: str
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    indicators: dict

@app.on_event("startup")
def check_db_connection():
    try:
        # Debug log the environment variables (without password)
        logger.info(f"Attempting database connection with: host={os.getenv('DB_HOST')}, port={os.getenv('DB_PORT')}, dbname={os.getenv('DB_NAME')}, user={os.getenv('DB_USER')}")
        
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            connect_timeout=3
        )
        conn.close()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

@app.get("/")
async def root():
    return {"message": "OHLC Handler API is running"}

@app.get("/status")
def status():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            connect_timeout=3
        )
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": "not connected", "detail": str(e)}

@app.get("/ohlc/{symbol}/{timeframe}")
async def get_ohlc_data(
    symbol: str,
    timeframe: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"),
    limit: Optional[int] = Query(10, description="Number of candles to return (default: 10)")
):
    try:
        # Validate symbol
        if symbol not in market_config.TICKERS:
            raise HTTPException(status_code=400, detail=f"Invalid symbol. Must be one of {market_config.TICKERS}")
        
        # Validate timeframe
        if timeframe not in market_config.TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"Invalid timeframe. Must be one of {list(market_config.TIMEFRAMES.keys())}")
        
        # Parse dates if provided
        start = None
        end = None
        
        if start_date:
            try:
                start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
        
        if end_date:
            try:
                end = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")
        
        # Get database handler
        db = DBHandler()
        try:
            # Get OHLC data from database
            klines = db.get_klines(symbol, timeframe, start, end)
            if not klines:
                return []
            
            # Get indicators
            indicators = {}
            
            # Get RSI data
            rsi_data = db.get_rsi_data(symbol, timeframe, start, end)
            if rsi_data:
                indicators['rsi'] = rsi_data
            
            # Get EMA data
            ema_data = db.get_ema_data(symbol, timeframe, start, end)
            if ema_data:
                indicators['ema'] = ema_data
            
            # Get OBV data
            obv_data = db.get_obv_data(symbol, timeframe, start, end)
            if obv_data:
                indicators['obv'] = obv_data
            
            # Get Chandelier Exit data
            ce_data = db.get_ce_data(symbol, timeframe, start, end)
            if ce_data:
                indicators['ce'] = ce_data
            
            # Always get monthly pivot points
            # Calculate the start and end of the current month for pivot points
            first_candle = datetime.fromtimestamp(klines[0][0] / 1000, tz=timezone.utc)
            last_candle = datetime.fromtimestamp(klines[-1][0] / 1000, tz=timezone.utc)
            
            # Get the start of the month for the first candle
            pivot_start = first_candle.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Get the end of the month for the last candle
            if last_candle.month == 12:
                pivot_end = last_candle.replace(year=last_candle.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                pivot_end = last_candle.replace(month=last_candle.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            
            logger.info(f"Fetching pivot points from {pivot_start} to {pivot_end}")
            pivot_data = db.get_pivot_data(symbol, '1M', pivot_start, pivot_end)
            logger.info(f"Fetched pivot data: {pivot_data}")
            
            if pivot_data:
                # Create a dictionary of pivot points by month
                monthly_pivots = {}
                for pivot in pivot_data:
                    # Convert timestamp to datetime
                    dt = datetime.fromtimestamp(pivot['timestamp'] / 1000, tz=timezone.utc)
                    # Use year and month as key
                    month_key = (dt.year, dt.month)
                    monthly_pivots[month_key] = pivot
                    logger.info(f"Added pivot points for {dt.year}-{dt.month}: {pivot}")
            
            # Format response
            response = []
            # Take only the last 'limit' candles and reverse them to get newest first
            for kline in reversed(klines[-limit:]):
                timestamp = kline[0]
                # Convert timestamp to datetime
                dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                
                candle_data = {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': timestamp,
                    'datetime': dt.isoformat(),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5]),
                    'indicators': {}
                }
                
                # Always add monthly pivot for the candle's month if available
                if 'monthly_pivots' in locals():
                    month_key = (dt.year, dt.month)
                    if month_key in monthly_pivots:
                        pivot = monthly_pivots[month_key]
                        candle_data['indicators']['pivot'] = {
                            'pp': pivot['pp'],
                            'r1': pivot['r1'],
                            'r2': pivot['r2'],
                            'r3': pivot['r3'],
                            'r4': pivot['r4'],
                            'r5': pivot['r5'],
                            's1': pivot['s1'],
                            's2': pivot['s2'],
                            's3': pivot['s3'],
                            's4': pivot['s4'],
                            's5': pivot['s5']
                        }
                
                # Add other indicators for this timestamp
                for indicator_name, indicator_data in indicators.items():
                    if indicator_name in ['rsi', 'ema', 'obv', 'ce']:
                        for data_point in indicator_data:
                            if data_point['timestamp'] == timestamp:
                                if indicator_name not in candle_data['indicators']:
                                    candle_data['indicators'][indicator_name] = {}
                                if indicator_name in ['rsi', 'ema']:
                                    candle_data['indicators'][indicator_name][str(data_point['period'])] = data_point['value']
                                elif indicator_name == 'obv':
                                    candle_data['indicators'][indicator_name] = {
                                        'value': data_point['obv'],
                                        'ma': data_point['ma_value'],
                                        'upper_band': data_point['upper_band'],
                                        'lower_band': data_point['lower_band']
                                    }
                                elif indicator_name == 'ce':
                                    candle_data['indicators'][indicator_name] = {
                                        'atr': data_point['atr_value'],
                                        'long_stop': data_point['long_stop'],
                                        'short_stop': data_point['short_stop'],
                                        'direction': data_point['direction'],
                                        'buy_signal': data_point['buy_signal'],
                                        'sell_signal': data_point['sell_signal']
                                    }
                response.append(candle_data)
            
            return response
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update/{symbol}/{timeframe}")
async def trigger_update(
    symbol: str,
    timeframe: str,
    calculate_indicators: bool = True
):
    try:
        # Validate symbol and timeframe
        if symbol not in market_config.TICKERS:
            raise HTTPException(status_code=400, detail=f"Invalid symbol. Must be one of {market_config.TICKERS}")
        
        if timeframe not in market_config.TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"Invalid timeframe. Must be one of {list(market_config.TIMEFRAMES.keys())}")
        
        # Fetch latest data
        klines = await fetch_historical_data(symbol, timeframe)
        
        if calculate_indicators:
            # Calculate all indicators
            calculator = IndicatorCalculator()
            rsi_calculator = RSICalculator()
            obv_calculator = OBVCalculator()
            pivot_calculator = PivotCalculator()
            ce_calculator = CECalculator()
            pattern_calculator = CandlePatternCalculator()
            
            try:
                # Calculate each indicator
                calculator.calculate_indicators(symbol, timeframe)
                rsi_calculator.calculate_rsi(symbol, timeframe)
                obv_calculator.calculate_obv(symbol, timeframe)
                pivot_calculator.calculate_pivots(symbol, timeframe)
                ce_calculator.calculate_ce(symbol, timeframe)
                pattern_calculator.calculate_patterns(symbol, timeframe)
            finally:
                # Close database connections
                if hasattr(calculator, 'db'):
                    calculator.db.close()
                if hasattr(rsi_calculator, 'db'):
                    rsi_calculator.db.close()
                if hasattr(obv_calculator, 'db'):
                    obv_calculator.db.close()
                if hasattr(pivot_calculator, 'db'):
                    pivot_calculator.db.close()
                if hasattr(ce_calculator, 'db'):
                    ce_calculator.db.close()
                if hasattr(pattern_calculator, 'db'):
                    pattern_calculator.db.close()
        
        return {
            "message": f"Successfully updated {symbol} {timeframe} data",
            "candles_updated": len(klines)
        }
    
    except Exception as e:
        logger.error(f"Error updating data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update/{symbol}")
async def trigger_update_symbol(
    symbol: str,
    calculate_indicators: bool = True
):
    """Update all timeframes for a specific symbol"""
    try:
        # Validate symbol
        if symbol not in market_config.TICKERS:
            raise HTTPException(status_code=400, detail=f"Invalid symbol. Must be one of {market_config.TICKERS}")
        
        results = []
        for timeframe in market_config.TIMEFRAMES:
            try:
                # Fetch latest data
                klines = await fetch_historical_data(symbol, timeframe)
                
                if calculate_indicators:
                    # Calculate all indicators
                    calculator = IndicatorCalculator()
                    rsi_calculator = RSICalculator()
                    obv_calculator = OBVCalculator()
                    pivot_calculator = PivotCalculator()
                    ce_calculator = CECalculator()
                    pattern_calculator = CandlePatternCalculator()
                    
                    try:
                        # Calculate each indicator
                        calculator.calculate_indicators(symbol, timeframe)
                        rsi_calculator.calculate_rsi(symbol, timeframe)
                        obv_calculator.calculate_obv(symbol, timeframe)
                        pivot_calculator.calculate_pivots(symbol, timeframe)
                        ce_calculator.calculate_ce(symbol, timeframe)
                        pattern_calculator.calculate_patterns(symbol, timeframe)
                    finally:
                        # Close database connections
                        if hasattr(calculator, 'db'):
                            calculator.db.close()
                        if hasattr(rsi_calculator, 'db'):
                            rsi_calculator.db.close()
                        if hasattr(obv_calculator, 'db'):
                            obv_calculator.db.close()
                        if hasattr(pivot_calculator, 'db'):
                            pivot_calculator.db.close()
                        if hasattr(ce_calculator, 'db'):
                            ce_calculator.db.close()
                        if hasattr(pattern_calculator, 'db'):
                            pattern_calculator.db.close()
                
                results.append({
                    "timeframe": timeframe,
                    "candles_updated": len(klines)
                })
            except Exception as e:
                logger.error(f"Error updating {symbol} {timeframe}: {str(e)}")
                results.append({
                    "timeframe": timeframe,
                    "error": str(e)
                })
        
        return {
            "message": f"Updated all timeframes for {symbol}",
            "results": results
        }
    
    except Exception as e:
        logger.error(f"Error updating data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update")
async def trigger_update_all(
    calculate_indicators: bool = True
):
    """Update all symbols and timeframes"""
    try:
        results = []
        for symbol in market_config.TICKERS:
            symbol_results = []
            for timeframe in market_config.TIMEFRAMES:
                try:
                    # Fetch latest data
                    klines = await fetch_historical_data(symbol, timeframe)
                    
                    if calculate_indicators:
                        # Calculate all indicators
                        calculator = IndicatorCalculator()
                        rsi_calculator = RSICalculator()
                        obv_calculator = OBVCalculator()
                        pivot_calculator = PivotCalculator()
                        ce_calculator = CECalculator()
                        pattern_calculator = CandlePatternCalculator()
                        
                        try:
                            # Calculate each indicator
                            calculator.calculate_indicators(symbol, timeframe)
                            rsi_calculator.calculate_rsi(symbol, timeframe)
                            obv_calculator.calculate_obv(symbol, timeframe)
                            pivot_calculator.calculate_pivots(symbol, timeframe)
                            ce_calculator.calculate_ce(symbol, timeframe)
                            pattern_calculator.calculate_patterns(symbol, timeframe)
                        finally:
                            # Close database connections
                            if hasattr(calculator, 'db'):
                                calculator.db.close()
                            if hasattr(rsi_calculator, 'db'):
                                rsi_calculator.db.close()
                            if hasattr(obv_calculator, 'db'):
                                obv_calculator.db.close()
                            if hasattr(pivot_calculator, 'db'):
                                pivot_calculator.db.close()
                            if hasattr(ce_calculator, 'db'):
                                ce_calculator.db.close()
                            if hasattr(pattern_calculator, 'db'):
                                pattern_calculator.db.close()
                    
                    symbol_results.append({
                        "timeframe": timeframe,
                        "candles_updated": len(klines)
                    })
                except Exception as e:
                    logger.error(f"Error updating {symbol} {timeframe}: {str(e)}")
                    symbol_results.append({
                        "timeframe": timeframe,
                        "error": str(e)
                    })
            
            results.append({
                "symbol": symbol,
                "results": symbol_results
            })
        
        return {
            "message": "Updated all symbols and timeframes",
            "results": results
        }
    
    except Exception as e:
        logger.error(f"Error updating data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 