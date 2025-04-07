from binance_client import BinanceClient
import logging
from datetime import datetime, timezone, timedelta
from config import market_config
import argparse
from db_handler import DBHandler
from indicators.calculator import IndicatorCalculator
from indicators.rsi_calculator import RSICalculator
from indicators.obv_calculator import OBVCalculator
from indicators.pivot_calculator import PivotCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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

def fetch_historical_data(ticker: str, timeframe: str, start_date: datetime = None, end_date: datetime = None) -> list:
    """Fetch historical klines data for a ticker and timeframe"""
    try:
        client = BinanceClient()
        db = DBHandler()
        
        # If no start_date provided, get last candle date from DB or use default
        if start_date is None:
            last_date = db.get_last_candle_date(ticker, timeframe)
            if last_date:
                # If the last candle is from today, start from beginning of day
                today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                if last_date.date() == today.date():
                    start_date = today
                    logger.info(f"Last candle is from today, starting from beginning of day: {start_date}")
                else:
                    # Start from the last candle (we'll replace it as it might have been incomplete)
                    start_date = last_date
                    logger.info(f"Using last available date from DB: {start_date}")
            else:
                # Use default start date from config
                start_date = market_config.DEFAULT_START_DATE
                logger.info(f"No data found in DB, using default start date: {start_date}")
        
        # If no end_date provided, use current time
        if end_date is None:
            end_date = datetime.now(timezone.utc)
            # Round up to the next hour to ensure we get all candles
            if end_date.minute > 0 or end_date.second > 0 or end_date.microsecond > 0:
                end_date = end_date.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            # Add one interval to ensure we get the current incomplete candle
            interval_ms = client._get_interval_ms(timeframe)
            end_date = end_date + timedelta(milliseconds=interval_ms)
        
        # Ensure both dates are timezone-aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        
        logger.info(f"Fetching {ticker} {timeframe} data from {start_date} to {end_date}")
        
        all_klines = []
        
        # Special handling for monthly timeframe
        if timeframe == "1M":
            # For monthly data, we need to fetch each month separately to ensure we get all data
            current_date = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            while current_date < end_date:
                # Calculate next month
                if current_date.month == 12:
                    next_month = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    next_month = current_date.replace(month=current_date.month + 1)
                
                # Fetch data for this month
                logger.info(f"Fetching monthly data for {ticker} for {current_date.year}-{current_date.month}")
                klines = client.get_klines(ticker, timeframe, current_date, next_month)
                
                if klines:
                    logger.info(f"Retrieved {len(klines)} candles for {ticker} {timeframe} for {current_date.year}-{current_date.month}")
                    all_klines.extend(klines)
                    
                    # Save to database
                    db.save_klines(ticker, timeframe, klines)
                    logger.info(f"Saved {len(klines)} candles to database for {ticker} {timeframe}")
                
                # Move to next month regardless of whether we got data
                current_date = next_month
                
                # Small delay to avoid rate limiting
                import time
                time.sleep(0.5)
        else:
            # Standard handling for non-monthly timeframes
            current_start = start_date
            
            while current_start < end_date:
                # Calculate the end time for this batch (1000 candles)
                batch_end = current_start + timedelta(milliseconds=client._get_interval_ms(timeframe) * 1000)
                if batch_end > end_date:
                    batch_end = end_date
                    
                klines = client.get_klines(ticker, timeframe, current_start, batch_end)
                
                if klines:
                    logger.info(f"Retrieved {len(klines)} candles for {ticker} {timeframe}")
                    all_klines.extend(klines)
                    
                    # Update current_start to the timestamp of the last candle + 1 interval
                    last_candle_time = datetime.fromtimestamp(klines[-1][0] / 1000, timezone.utc)
                    current_start = last_candle_time + timedelta(milliseconds=client._get_interval_ms(timeframe))
                    
                    # Save to database
                    db.save_klines(ticker, timeframe, klines)
                    logger.info(f"Saved {len(klines)} candles to database for {ticker} {timeframe}")
                else:
                    logger.warning(f"No data found for {ticker} {timeframe} in batch {current_start} to {batch_end}")
                    break
        
        logger.info(f"Total candles fetched: {len(all_klines)}")
        if not all_klines:
            logger.warning(f"No data found for {ticker} {timeframe}")
            
        return all_klines
    except Exception as e:
        logger.error(f"Error fetching data for {ticker} {timeframe}: {str(e)}")
        raise
    finally:
        client.close()
        db.close()

def main():
    parser = argparse.ArgumentParser(description='Fetch historical klines data from Binance')
    parser.add_argument('--ticker', type=str, help='Trading pair ticker (e.g., BTCUSDT)')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--timeframe', type=str, help='Time timeframe (e.g., 1h, 4h, 1d)')
    parser.add_argument('--skip-indicators', action='store_true', help='Skip indicator calculation')
    parser.add_argument('--indicators', type=str, 
                        choices=['all', 'ema', 'rsi', 'obv', 'pivot'], 
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
                        
            except Exception as e:
                logger.error(f"Failed to process {ticker} {timeframe}: {str(e)}")
                continue

if __name__ == "__main__":
    main() 