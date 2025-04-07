from binance_client import BinanceClient
import logging
from datetime import datetime, timezone, timedelta
from config import market_config
import argparse
from db_handler import DBHandler
from indicators.calculator import IndicatorCalculator

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
            logger.info(f"start date: {start_date}")
            logger.info(f"end date: {end_date}")
        
        # Ensure both dates are timezone-aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        
        logger.info(f"Fetching {ticker} {timeframe} data from {start_date} to {end_date}")
        
        all_klines = []
        current_start = start_date
        
        while current_start < end_date:
            # Calculate the end time for this batch (1000 candles)
            batch_end = current_start + timedelta(milliseconds=client._get_interval_ms(timeframe) * 1000)
            if batch_end > end_date:
                batch_end = end_date
                
            logger.info(f"Fetching batch from {current_start} to {batch_end}")
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
        
        if all_klines:
            # Print first and last candle for verification
            first_candle = all_klines[0]
            last_candle = all_klines[-1]
            logger.info(f"First candle: {datetime.utcfromtimestamp(first_candle[0]/1000).replace(tzinfo=timezone.utc)} - Open: {first_candle[1]}, Close: {first_candle[4]}")
            logger.info(f"Last candle: {datetime.utcfromtimestamp(last_candle[0]/1000).replace(tzinfo=timezone.utc)} - Open: {last_candle[1]}, Close: {last_candle[4]}")
            logger.info(f"Total candles fetched: {len(all_klines)}")
        else:
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
    if start_date:
        logger.info(f"Date range: {start_date} to {end_date or 'now'}")
    else:
        logger.info("Using last available date from DB or default start date")

    for ticker in tickers:
        for timeframe in timeframes:
            try:
                # Fetch OHLC data
                fetch_historical_data(ticker, timeframe, start_date, end_date)
                
                # Calculate indicators if not skipped
                if not args.skip_indicators:
                    logger.info(f"Calculating indicators for {ticker} {timeframe}")
                    calculator = IndicatorCalculator()
                    calculator.calculate_indicators(ticker, timeframe)
            except Exception as e:
                logger.error(f"Failed to process {ticker} {timeframe}: {str(e)}")
                continue

if __name__ == "__main__":
    main() 