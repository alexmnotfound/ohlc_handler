#!/usr/bin/env python
"""
Database initialization script.
Run this script to create all necessary tables for the OHLC handler.
"""

import os
import psycopg2
import logging
import sys

# Add parent directory to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import db_config, logging_config

# Configure logging
logging.basicConfig(
    level=getattr(logging, logging_config.LEVEL),
    format=logging_config.FORMAT
)
logger = logging.getLogger(__name__)

def init_database():
    """Initialize the database with the required schema"""
    try:
        # Connect to the database
        logger.info(f"Connecting to PostgreSQL at {db_config.host}:{db_config.port}")
        conn = psycopg2.connect(db_config.connection_string)
        cur = conn.cursor()
        
        # Read the SQL script
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'create_tables.sql')
        with open(script_path, 'r') as f:
            sql_script = f.read()
        
        # Execute the SQL script
        logger.info("Creating database tables...")
        cur.execute(sql_script)
        conn.commit()
        
        # Verify tables were created
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        
        logger.info("Tables created successfully:")
        for table in tables:
            logger.info(f"  - {table[0]}")
        
        # Close database connection
        cur.close()
        conn.close()
        logger.info("Database initialization complete")
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

if __name__ == "__main__":
    init_database() 