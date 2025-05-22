from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from datetime import datetime, timezone
from typing import List, Optional
from pydantic import BaseModel
import logging
from processor import fetch_historical_data
from config import market_config
from core import DBHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from indicators.calculator import IndicatorCalculator
from indicators.rsi_calculator import RSICalculator
from indicators.obv_calculator import OBVCalculator
from indicators.pivot_calculator import PivotCalculator
from indicators.ce_calculator import CECalculator
from indicators.candle_pattern_calculator import CandlePatternCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="OHLC Handler API",
    description="API for handling OHLC data and indicators",
    version="1.0.0"
)

# Initialize scheduler
scheduler = AsyncIOScheduler()

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

async def update_all_data():
    """Background task to update all data periodically"""
    logger.info("Starting periodic data update")
    try:
        for ticker in market_config.TICKERS:
            for timeframe in market_config.TIMEFRAMES:
                try:
                    await fetch_historical_data(ticker, timeframe)
                    logger.info(f"Updated {ticker} {timeframe}")
                except Exception as e:
                    logger.error(f"Error updating {ticker} {timeframe}: {str(e)}")
    except Exception as e:
        logger.error(f"Error in periodic update: {str(e)}")


@app.on_event("startup")
async def startup_event():
    """Initialize scheduler and start background tasks on startup"""
    # Schedule updates based on timeframes
    for timeframe in market_config.TIMEFRAMES:
        if timeframe == '1h':
            # Update hourly data every 5 minutes
            scheduler.add_job(
                update_all_data,
                CronTrigger(minute='*/5'),
                id=f'update_{timeframe}'
            )
        elif timeframe == '4h':
            # Update 4h data every 15 minutes
            scheduler.add_job(
                update_all_data,
                CronTrigger(minute='*/15'),
                id=f'update_{timeframe}'
            )
        elif timeframe == '1d':
            # Update daily data every hour
            scheduler.add_job(
                update_all_data,
                CronTrigger(minute='0'),
                id=f'update_{timeframe}'
            )
        elif timeframe == '1w':
            # Update weekly data every 6 hours
            scheduler.add_job(
                update_all_data,
                CronTrigger(hour='*/6'),
                id=f'update_{timeframe}'
            )
        elif timeframe == '1M':
            # Update monthly data once a day
            scheduler.add_job(
                update_all_data,
                CronTrigger(hour='0', minute='0'),
                id=f'update_{timeframe}'
            )
    
    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started")

@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown scheduler on application shutdown"""
    scheduler.shutdown()
    logger.info("Scheduler shut down")

@app.get("/")
async def root():
    return {"message": "OHLC Handler API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/ohlc/{symbol}/{timeframe}")
async def get_ohlc_data(
    symbol: str,
    timeframe: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)")
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
            # Get OHLC data
            klines = await fetch_historical_data(symbol, timeframe, start, end)
            
            # Get all indicators for each candle
            response = []
            for kline in reversed(klines):  # Reverse the order to get newest first
                timestamp = datetime.utcfromtimestamp(kline[0] / 1000)
                
                # Get EMA values
                db.cur.execute(
                    """
                    SELECT period, value 
                    FROM ema_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                ema_values = {f"ema_{row[0]}": float(row[1]) if row[1] is not None else None for row in db.cur.fetchall()}
                
                # Get RSI values
                db.cur.execute(
                    """
                    SELECT period, value 
                    FROM rsi_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                rsi_values = {f"rsi_{row[0]}": float(row[1]) if row[1] is not None else None for row in db.cur.fetchall()}
                
                # Get OBV values
                db.cur.execute(
                    """
                    SELECT obv, ma_value, upper_band, lower_band
                    FROM obv_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                obv_row = db.cur.fetchone()
                obv_values = {
                    "obv": float(obv_row[0]) if obv_row and obv_row[0] is not None else None,
                    "obv_ma": float(obv_row[1]) if obv_row and obv_row[1] is not None else None,
                    "obv_upper": float(obv_row[2]) if obv_row and obv_row[2] is not None else None,
                    "obv_lower": float(obv_row[3]) if obv_row and obv_row[3] is not None else None
                } if obv_row else {}
                
                # Get Pivot values
                db.cur.execute(
                    """
                    SELECT pp, r1, r2, r3, r4, r5, s1, s2, s3, s4, s5
                    FROM pivot_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                pivot_row = db.cur.fetchone()
                pivot_values = {
                    "pp": float(pivot_row[0]) if pivot_row and pivot_row[0] is not None else None,
                    "r1": float(pivot_row[1]) if pivot_row and pivot_row[1] is not None else None,
                    "r2": float(pivot_row[2]) if pivot_row and pivot_row[2] is not None else None,
                    "r3": float(pivot_row[3]) if pivot_row and pivot_row[3] is not None else None,
                    "r4": float(pivot_row[4]) if pivot_row and pivot_row[4] is not None else None,
                    "r5": float(pivot_row[5]) if pivot_row and pivot_row[5] is not None else None,
                    "s1": float(pivot_row[6]) if pivot_row and pivot_row[6] is not None else None,
                    "s2": float(pivot_row[7]) if pivot_row and pivot_row[7] is not None else None,
                    "s3": float(pivot_row[8]) if pivot_row and pivot_row[8] is not None else None,
                    "s4": float(pivot_row[9]) if pivot_row and pivot_row[9] is not None else None,
                    "s5": float(pivot_row[10]) if pivot_row and pivot_row[10] is not None else None
                } if pivot_row else {}
                
                # Get CE values
                db.cur.execute(
                    """
                    SELECT atr_value, long_stop, short_stop, direction, buy_signal, sell_signal
                    FROM ce_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                ce_row = db.cur.fetchone()
                ce_values = {
                    "atr": float(ce_row[0]) if ce_row and ce_row[0] is not None else None,
                    "long_stop": float(ce_row[1]) if ce_row and ce_row[1] is not None else None,
                    "short_stop": float(ce_row[2]) if ce_row and ce_row[2] is not None else None,
                    "direction": ce_row[3] if ce_row and ce_row[3] is not None else None,
                    "buy_signal": ce_row[4] if ce_row and ce_row[4] is not None else None,
                    "sell_signal": ce_row[5] if ce_row and ce_row[5] is not None else None
                } if ce_row else {}
                
                # Get candle pattern
                db.cur.execute(
                    """
                    SELECT candle_pattern
                    FROM ohlc_data 
                    WHERE ticker = %s AND timeframe = %s AND timestamp = %s
                    """,
                    (symbol, timeframe, timestamp)
                )
                pattern_row = db.cur.fetchone()
                pattern = pattern_row[0] if pattern_row and pattern_row[0] is not None else None
                
                # Combine all data
                candle_data = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": kline[0],
                    "datetime": timestamp.isoformat(),
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                    "indicators": {
                        **ema_values,
                        **rsi_values,
                        **obv_values,
                        **pivot_values,
                        **ce_values,
                        "pattern": pattern
                    }
                }
                response.append(candle_data)
            
            return response
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Error fetching OHLC data with indicators: {str(e)}")
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