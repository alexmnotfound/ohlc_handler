from core import BinanceClient
import logging
from datetime import datetime, timezone, timedelta
from config import market_config, logging_config
import argparse
from core import DBHandler
from indicators.calculator import IndicatorCalculator
from indicators.rsi_calculator import RSICalculator
from indicators.obv_calculator import OBVCalculator
from indicators.pivot_calculator import PivotCalculator
from indicators.ce_calculator import CECalculator
from indicators.candle_pattern_calculator import CandlePatternCalculator
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=getattr(logging, logging_config.LEVEL),
    format=logging_config.FORMAT
)
logger = logging.getLogger(__name__)

def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("Date must be in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")

async def fetch_historical_data(ticker: str, timeframe: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[List]:
    """Fetch historical klines data for a given ticker and timeframe"""
    try:
        # Initialize database handler
        db = DBHandler()
        client = BinanceClient()
        
        try:
            # Get the last candle from database
            last_candle = db.get_last_candle(ticker, timeframe)
            
            if last_candle:
                # Debug logging for raw timestamp
                raw_timestamp = last_candle[0]
                logger.info(f"Raw timestamp from last candle: {raw_timestamp}")
                
                # Convert timestamp to UTC datetime
                last_timestamp = datetime.fromtimestamp(raw_timestamp / 1000, tz=timezone.utc)
                # Calculate minimum required candles for all indicators
                max_period = max(
                    market_config.CE_PERIOD,  # Chandelier Exit
                    max(market_config.EMA_PERIODS),  # EMA
                    market_config.RSI_PERIOD,  # RSI
                    market_config.OBV_MA_PERIOD,  # OBV
                )
                
                # Calculate minimum days needed based on timeframe
                if timeframe == '1h':
                    min_days = 7  
                elif timeframe == '4h':
                    min_days = 14 
                elif timeframe == '1d':
                    min_days = 30 
                elif timeframe == '1w':
                    min_days = 60 
                elif timeframe == '1M':
                    min_days = 90 
                else:
                    min_days = 7  # Default to 7 days
                
                # Start from min_days before the last candle
                start_time = last_timestamp - timedelta(days=min_days)
            else:
                # If no data exists, use start_date or default to 30 days ago
                if start_date:
                    start_time = start_date.replace(tzinfo=timezone.utc)
                else:
                    # Default to 30 days ago if no data exists
                    start_time = datetime.now(timezone.utc) - timedelta(days=30)
                    logger.info(f"No existing data found, using default start date: {start_time}")
            
            # Use provided end_date or default to now
            end_time = end_date.replace(tzinfo=timezone.utc) if end_date else datetime.now(timezone.utc)
            
            # Fetch data in batches
            all_candles = []
            current_start = start_time
            
            while current_start < end_time:
                # Calculate batch end time based on timeframe
                if timeframe == '1h':
                    batch_end = min(current_start + timedelta(hours=24), end_time)
                elif timeframe == '4h':
                    batch_end = min(current_start + timedelta(days=24), end_time)
                elif timeframe == '1d':
                    batch_end = min(current_start + timedelta(days=24), end_time)
                elif timeframe == '1w':
                    batch_end = min(current_start + timedelta(days=30), end_time)
                elif timeframe == '1M':
                    batch_end = min(current_start + timedelta(days=90), end_time)
                else:
                    batch_end = end_time
                
                logger.info(f"Fetching {ticker} {timeframe} data from {current_start} to {batch_end}")
                
                # Fetch data from Binance
                candles = await client.get_klines(
                    symbol=ticker,
                    interval=timeframe,
                    start_time=current_start,
                    end_time=batch_end
                )
                
                if candles:
                    logger.info(f"Retrieved {len(candles)} candles for {ticker} {timeframe}")
                    # Log first and last candle timestamps from the batch
                    if len(candles) > 0:
                        first_candle_time = datetime.fromtimestamp(candles[0][0] / 1000, tz=timezone.utc)
                        last_candle_time = datetime.fromtimestamp(candles[-1][0] / 1000, tz=timezone.utc)
                        logger.info(f"First candle in batch: {first_candle_time}")
                        logger.info(f"Last candle in batch: {last_candle_time}")
                    all_candles.extend(candles)
                    
                    # Save to database
                    db.save_klines(ticker, timeframe, candles)
                    logger.info(f"Saved {len(candles)} candles to database for {ticker} {timeframe}")
                else:
                    logger.warning(f"No data found for {ticker} {timeframe} in batch {current_start} to {batch_end}")
                
                # Move to next batch
                current_start = batch_end
            
            logger.info(f"Total candles fetched: {len(all_candles)}")
            return all_candles
            
        finally:
            # Close database connection
            db.close()
            # Close Binance client session
            await client.close()
            
    except Exception as e:
        logger.error(f"Error fetching data for {ticker} {timeframe}: {str(e)}")
        raise

def process_ohlc_data():
    """CLI interface for processing OHLC data and calculating indicators"""
    parser = argparse.ArgumentParser(description='Fetch historical klines data from Binance')
    parser.add_argument('--ticker', type=str, help='Trading pair ticker (e.g., BTCUSDT)')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--timeframe', type=str, help='Time timeframe (e.g., 1h, 4h, 1d)')
    parser.add_argument('--skip-indicators', action='store_true', help='Skip indicator calculation')
    parser.add_argument('--indicators', type=str, 
                        choices=['all', 'ema', 'rsi', 'obv', 'pivot', 'ce', 'patterns'], 
                        default='all', 
                        help='Specify which indicators to calculate (default: all)')
    parser.add_argument('--skip-ohlc', action='store_true', help='Skip OHLC data fetching')

    args = parser.parse_args()

    # Parse dates if provided
    start_date = parse_date(args.start) if args.start else None
    end_date = parse_date(args.end) if args.end else None

    if start_date and end_date and start_date >= end_date:
        raise ValueError("Start date must be before end date")

    # Determine which tickers to process
    tickers = [args.ticker] if args.ticker else market_config.TICKERS
    # Determine which timeframes to process
    timeframes = [args.timeframe] if args.timeframe else list(market_config.TIMEFRAMES.keys())

    logger.info(f"Processing {len(tickers)} tickers and {len(timeframes)} timeframes")
    if start_date and not args.skip_ohlc:
        logger.info(f"Date range: {start_date} to {end_date or 'now'}")

    for ticker in tickers:
        for timeframe in timeframes:
            try:
                # Fetch OHLC data if not skipped
                if not args.skip_ohlc:
                    logger.info(f"Fetching OHLC data for {ticker} {timeframe}")
                    fetch_historical_data(ticker, timeframe, start_date, end_date)
                
                # Calculate indicators if not skipped
                if not args.skip_indicators:
                    # Calculate EMA
                    if args.indicators in ['all', 'ema']:
                        logger.info(f"Calculating EMA for {ticker} {timeframe}")
                        calculator = IndicatorCalculator()
                        calculator.calculate_indicators(ticker, timeframe)
                    
                    # Calculate RSI
                    if args.indicators in ['all', 'rsi']:
                        logger.info(f"Calculating RSI for {ticker} {timeframe}")
                        rsi_calculator = RSICalculator()
                        rsi_calculator.calculate_rsi(ticker, timeframe)
                    
                    # Calculate OBV
                    if args.indicators in ['all', 'obv']:
                        logger.info(f"Calculating OBV for {ticker} {timeframe}")
                        obv_calculator = OBVCalculator()
                        obv_calculator.calculate_obv(ticker, timeframe)
                        
                    # Calculate Pivot Points (only for monthly timeframe)
                    if args.indicators in ['all', 'pivot'] and timeframe == "1M":
                        logger.info(f"Calculating Pivot Points for {ticker} {timeframe}")
                        pivot_calculator = PivotCalculator()
                        pivot_calculator.calculate_pivots(ticker, timeframe)
                    
                    # Calculate Chandelier Exit
                    if args.indicators in ['all', 'ce']:
                        logger.info(f"Calculating Chandelier Exit for {ticker} {timeframe}")
                        ce_calculator = CECalculator()
                        ce_calculator.calculate_ce(ticker, timeframe)
                    
                    # Calculate Candle Patterns
                    if args.indicators in ['all', 'patterns']:
                        logger.info(f"Calculating Candle Patterns for {ticker} {timeframe}")
                        pattern_calculator = CandlePatternCalculator()
                        pattern_calculator.calculate_patterns(ticker, timeframe)
                        
            except Exception as e:
                logger.error(f"Failed to process {ticker} {timeframe}: {str(e)}")
                continue

if __name__ == "__main__":
    process_ohlc_data() 