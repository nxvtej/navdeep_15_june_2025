import os
from datetime import time

# Configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), '../data')
MENU_HOURS_CSV = os.path.join(DATA_DIR, 'menu_hours.csv')
STORE_STATUS_CSV = os.path.join(DATA_DIR, 'store_status.csv')
TIMEZONES_CSV = os.path.join(DATA_DIR, 'timezones.csv')

# Default Values
DEFAULT_TIMEZONE = 'America/Chicago'
DEFAULT_MENU_HOURS = {
    'start_time_local': time(0, 0, 0),
    'end_time_local': time(23, 59, 59)
}

# Batch Sizes
STORE_STATUS_BATCH_SIZE = 100000
SMALL_TABLE_BATCH_SIZE = 50000

# Report Dir
REPORTS_DIR = os.path.join(DATA_DIR, 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)