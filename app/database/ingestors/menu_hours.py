"""
using engine (no session) to resolve confict with batch commit.
"""
import os
import pytz
import threading
import pandas as pd

from sqlalchemy import text
from datetime import datetime, time
from sqlalchemy.exc import IntegrityError
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.dialects.postgresql import insert

from app.database.db import engine
from app.database.models import Store, Store_Status, Menu_Hours, Timezone
from app.services.conflict import _get_insert_statement_on_conflict

from business.config import (
    DATA_DIR,
    MENU_HOURS_CSV,
    STORE_STATUS_CSV,
    TIMEZONES_CSV,
    SMALL_TABLE_BATCH_SIZE,
    STORE_STATUS_BATCH_SIZE,
    DEFAULT_MENU_HOURS,
    DEFAULT_TIMEZONE
)

def ingest_menu_hours(df: pd.DataFrame):
    """
    Ingests data from menu_hours.csv, applying explicit and default hours in batches.
    Filters out existing records to prevent IntegrityError during bulk inserts.
    """
    start_time = datetime.now()
    
    print(f"Ingesting data from {MENU_HOURS_CSV}...")
    try:
        with engine.begin() as conn:

            df.rename(columns={'dayOfWeek': 'day_of_week'}, inplace=True)
            print("stripping time from start_time_local and end_time_local")
            df['start_time_local'] = df['start_time_local'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time())
            df['end_time_local'] = df['end_time_local'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time())


            explicit_menu_hours_records = []
            for _, record in df.iterrows():
                explicit_menu_hours_records.append({
                    'store_id': str(record['store_id']),
                    'day_of_week': int(record['day_of_week']),
                    'start_time_local': record['start_time_local'],
                    'end_time_local': record['end_time_local']
                })
                    
            if explicit_menu_hours_records:
                total_count = 0
                print(f"Starting ingestion of {len(explicit_menu_hours_records)} new explicit menu hours records in batches of {SMALL_TABLE_BATCH_SIZE}...")
                for i in range(0, len(explicit_menu_hours_records), SMALL_TABLE_BATCH_SIZE):
                    batch = explicit_menu_hours_records[i:i + SMALL_TABLE_BATCH_SIZE]
                    try:
                        stmt = _get_insert_statement_on_conflict(Menu_Hours.__table__, batch, ['store_id', 'day_of_week', 'start_time_local', 'end_time_local'])
                        conn.execute(stmt)
                        total_count += len(batch)
                        print(f"Ingested {total_count} explicit menu hours records so far...")
                    except Exception as e:
                        print(f"Error ingesting explicit menu hours batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): , ......................")
                        """
                        ERROR: 
                        with open("menu_hours_errors.log", "a") as f:
                            f.write(f"\n{'='*80}\n")
                            f.write(f"Batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): {e}\n")
                            f.write(traceback.format_exc())
                        """
                        
                print(f"Total ingested {total_count} explicit menu hours records.")
            else:
                print("No new explicit menu hours to ingest.")

            existing_store_day_combinations = {
                (store_id, day_of_week)
                for store_id, day_of_week in conn.execute(text("SELECT store_id, day_of_week FROM menu_hours"))
            }

            all_known_stores_in_db = {row[0] for row in conn.execute(text("SELECT store_id FROM stores"))}

            default_menu_hours_records = []

            for store_id in all_known_stores_in_db:
                for day in range(7):
                    if (store_id, day) not in existing_store_day_combinations:
                        default_menu_hours_records.append({
                            'store_id': store_id,
                            'day_of_week': day,
                            'start_time_local': DEFAULT_MENU_HOURS['start_time_local'],
                            'end_time_local': DEFAULT_MENU_HOURS['end_time_local']
                        })

            if default_menu_hours_records:
                total_count = 0
                print(f"Starting ingestion of {len(default_menu_hours_records)} new default menu hours records in batches of {SMALL_TABLE_BATCH_SIZE}...")
                for i in range(0, len(default_menu_hours_records), SMALL_TABLE_BATCH_SIZE):
                    batch = default_menu_hours_records[i:i + SMALL_TABLE_BATCH_SIZE]
                    try:
                        stmt = _get_insert_statement_on_conflict(Menu_Hours.__table__, batch, ['store_id', 'day_of_week', 'start_time_local', 'end_time_local'])
                        conn.execute(stmt)
                        total_count += len(batch)
                        print(f"Ingested {total_count} default menu hours records so far...")
                    except Exception as e:
                        print(f"  Error adding default menu hours batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): {e}")
                print(f"Total added {total_count} default menu hours records.")
            else:
                print("No new default menu hours to add.")
    except FileNotFoundError as e:
        print(f"Error: The menu hours CSV file was not found. Please ensure it is in the 'data' directory. {e}")
    except Exception as e:
        print(f"An unexpected error occurred during menu hours ingestion: {e}")
    finally:
        print(f"Menu hours function ended in {datetime.now() - start_time} seconds.")
