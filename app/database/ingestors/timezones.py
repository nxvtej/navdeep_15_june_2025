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

from app.database.ingestors.stores import ingest_stores
from app.services.conflict import _get_insert_statement_on_conflict

from business.config import (
    DATA_DIR,
    MENU_HOURS_CSV,
    STORE_STATUS_CSV,
    TIMEZONES_CSV,
    SMALL_TABLE_BATCH_SIZE,
    DEFAULT_MENU_HOURS,
    DEFAULT_TIMEZONE
)

def ingest_timezones(df: pd.DataFrame):
    """
    Ingests data from timezones.csv, applying explicit and default timezones in batches.
    Filters out existing records to prevent IntegrityError during bulk inserts.
    """
    start_time = datetime.now()

    print(f"Ingesting data from {TIMEZONES_CSV}...")

    try:
        with engine.begin() as conn:

            df.dropna(subset=['store_id', 'timezone_str'], inplace=True)

            explicit_timezone_records = []
            for _, record in df.iterrows():
                store_id_str = str(record['store_id'])
                timezone_str_val = record['timezone_str']

                try:
                    pytz.timezone(timezone_str_val)
                    explicit_timezone_records.append({
                            'store_id': store_id_str,
                            'timezone_str': timezone_str_val
                        })
                except pytz.UnknownTimeZoneError:
                    print(f"Warning: Unknown timezone '{timezone_str_val}' for store {store_id_str}. Skipping this entry.")
                except Exception as e:
                    print(f"Error creating timezone record for {record}: {e}")

            if explicit_timezone_records:
                total_count = 0
                print(f"Starting ingestion of {len(explicit_timezone_records)} new explicit timezone records in batches of {SMALL_TABLE_BATCH_SIZE}...")
                for i in range(0, len(explicit_timezone_records), SMALL_TABLE_BATCH_SIZE):
                    batch = explicit_timezone_records[i:i + SMALL_TABLE_BATCH_SIZE]
                    try:
                        stmt = _get_insert_statement_on_conflict(Timezone.__table__, batch, ['store_id'])
                        
                        conn.execute(stmt)
                        total_count += len(batch)
                        print(f"  Ingested {total_count} explicit timezone records so far...")
                    except Exception as e:
                        print(f"  Error ingesting explicit timezone batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): {e}")
                print(f"Total ingested {total_count} explicit timezone records.")
            else:
                print("No new explicit timezones to ingest.")

            existing_tz_stores_distinct = {s[0] for s in conn.execute(
                text("SELECT DISTINCT store_id FROM timezones")
            )}
            all_known_stores_in_db = {s[0] for s in conn.execute(
                text("SELECT store_id FROM stores")
            )}
            
            default_timezone_records = []

            for store_id in all_known_stores_in_db:
                if store_id not in existing_tz_stores_distinct:
                    default_timezone_records.append({
                        'store_id': store_id,
                        'timezone_str': DEFAULT_TIMEZONE
                        })

            if default_timezone_records:
                total_count = 0
                print(f"Starting ingestion of {len(default_timezone_records)} new default timezone records in batches of {SMALL_TABLE_BATCH_SIZE}...")
                for i in range(0, len(default_timezone_records), SMALL_TABLE_BATCH_SIZE):
                    batch = default_timezone_records[i:i + SMALL_TABLE_BATCH_SIZE]
                    try:
                        stmt = _get_insert_statement_on_conflict(Timezone.__table__, batch, ['store_id'])
                        conn.execute(stmt)
                        total_count += len(batch)
                        print(f"  Ingested {total_count} default timezone records so far...")
                    except Exception as e:
                        print(f"  Error adding default timezone batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): {e}")
                print(f"Total added {total_count} default timezone records.")
            else:
                print("No new default timezones to add.")
    except FileNotFoundError as e:
        print(f"Error: The timezones CSV file was not found. Please ensure it is in the 'data' directory. {e}")
    except Exception as e:
        print(f"An unexpected error occurred during timezone ingestion: {e}")
    finally:
        print(f"Timezone ingestion function ended in {datetime.now() - start_time} seconds.")
