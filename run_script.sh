#!/bin/bash
cd /Users/mrcap/projects/ohlc_handler
source venv/bin/activate
python main.py >> logs/cron.log 2>&1