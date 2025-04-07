#!/usr/bin/env python
"""
Utility script to fix timestamp columns in the database.
This script ensures all timestamp columns are 'timestamp without time zone'
to maintain consistent behavior across tables.
"""

import os
import psycopg2
import logging
import sys

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import db_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fix_timestamp_columns():
    """Find and fix any timestamp columns with timezone info"""
    try:
        # Connect to the database
        logger.info(f"Connecting to PostgreSQL at {db_config.host}:{db_config.port}")
        conn = psycopg2.connect(db_config.connection_string)
        cur = conn.cursor()
        
        # Get all tables with timestamp columns
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND column_name = 'timestamp'
            ORDER BY table_name;
        """)
        columns = cur.fetchall()
        
        logger.info("Checking timestamp columns...")
        for table_name, column_name, data_type in columns:
            logger.info(f"Table: {table_name}, Column: {column_name}, Type: {data_type}")
            
            # Check if column has timezone info
            if 'with time zone' in data_type.lower():
                logger.warning(f"Found timestamp with timezone in {table_name}.{column_name}")
                
                # Fix the column type
                logger.info(f"Fixing {table_name}.{column_name}...")
                
                # First, find and drop the primary key constraint
                cur.execute(f"""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY';
                """)
                constraint = cur.fetchone()
                
                if constraint:
                    constraint_name = constraint[0]
                    logger.info(f"Dropping constraint {constraint_name}...")
                    cur.execute(f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name};")
                
                # Change the column type
                logger.info(f"Changing column type...")
                cur.execute(f"""
                    ALTER TABLE {table_name} 
                    ALTER COLUMN {column_name} TYPE timestamp without time zone;
                """)
                
                # Re-add the primary key (assuming it's the standard format)
                if table_name == 'ohlc_data':
                    logger.info("Re-adding primary key for ohlc_data...")
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD PRIMARY KEY (ticker, timeframe, timestamp);
                    """)
                elif table_name in ('ema_data', 'rsi_data'):
                    logger.info(f"Re-adding primary key for {table_name}...")
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD PRIMARY KEY (ticker, timeframe, timestamp, period);
                    """)
                elif table_name == 'macd_data':
                    logger.info("Re-adding primary key for macd_data...")
                    cur.execute(f"""
                        ALTER TABLE {table_name}
                        ADD PRIMARY KEY (ticker, timeframe, timestamp, fast_period, slow_period, signal_period);
                    """)
                
                logger.info(f"Fixed {table_name}.{column_name}")
        
        # Commit the changes
        conn.commit()
        
        # Verify all columns are fixed
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND column_name = 'timestamp'
            ORDER BY table_name;
        """)
        columns = cur.fetchall()
        
        logger.info("Verification after fixes:")
        for table_name, column_name, data_type in columns:
            logger.info(f"Table: {table_name}, Column: {column_name}, Type: {data_type}")
        
        # Close database connection
        cur.close()
        conn.close()
        logger.info("Timestamp column fix complete")
        
    except Exception as e:
        logger.error(f"Error fixing timestamp columns: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
        raise

if __name__ == "__main__":
    fix_timestamp_columns() 