"""
using engine (no session) to resolve confict with batch commit.
"""
import os
import pytz
import numpy as np
import pandas as pd

from sqlalchemy import text
from datetime import datetime, time
from sqlalchemy.exc import IntegrityError
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.dialects.postgresql import insert

from app.database.db import engine
from app.database.models import Store, Store_Status, Menu_Hours, Timezone

from app.database.ingestors.stores import ingest_stores
from app.database.ingestors.menu_hours import ingest_menu_hours
from app.database.ingestors.timezones import ingest_timezones

from app.services.conflict import _get_insert_statement_on_conflict

from business.config import (
    DATA_DIR,
    MENU_HOURS_CSV,
    STORE_STATUS_CSV,
    TIMEZONES_CSV,
    STORE_STATUS_BATCH_SIZE,
    SMALL_TABLE_BATCH_SIZE,
    DEFAULT_MENU_HOURS,
    DEFAULT_TIMEZONE
)

def ingest_store_status(df: pd.DataFrame, threads=6):
    start_time = datetime.now()

    print("changing timestamp_utc column to datetime...")
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
    print("dropping the NA values from table...")
    df.dropna(subset=['store_id', 'status', 'timestamp_utc'], inplace=True)
    print("converting status column as boolean...")
    df['status'] = df['status'].apply(lambda x: True if str(x).lower() == 'active' else False)

    split_df = np.array_split(df, threads)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(ingest_batch, batch) for batch in split_df]
    for future in futures:
        try:
            future.result()  # Wait
        except Exception as e:
            print(f"An error occurred during store status ingestion: {e}")
            
    print(f"Store status function ended in {datetime.now() - start_time} seconds.")
def ingest_batch(df: pd.DataFrame):
    """
    Ingests data from store_status.csv in batches.
    Uses Python-side pre-filtering to prevent duplicates and avoid batch rollbacks.
    Handles timestamp conversion to timezone-aware UTC datetime.
    """

    try:
        
        records_to_insert = [
            {
                'store_id': str(row['store_id']),
                'timestamp_utc': row['timestamp_utc'],
                'status': row['status']
            }
            for _, row in df.iterrows()
        ]

        if not records_to_insert:
            print("No new store status records to ingest (all found in DB).")
            return

        total_count = 0
        print(f"Starting ingestion of {len(records_to_insert)} new store status records in batches of {STORE_STATUS_BATCH_SIZE}...")
        for i in range(0, len(records_to_insert), STORE_STATUS_BATCH_SIZE):
            batch = records_to_insert[i:i + STORE_STATUS_BATCH_SIZE]
            try:
                stmt = _get_insert_statement_on_conflict(Store_Status.__table__, batch, ['store_id', 'timestamp_utc'])
                with engine.begin() as conn:
                    conn.execute(stmt)
                total_count += len(batch)
                print(f"Ingested {total_count} store status records so far...")
            except Exception as e:
                print(f"Error ingesting batch {i//STORE_STATUS_BATCH_SIZE + 1} ({len(batch)} records) of store status records: {e}")
        print(f"Total ingested {total_count} store status records.")
    except FileNotFoundError as e:
        print(f"Error: The store status CSV file was not found. Please ensure it is in the 'data' directory. {e}")
    except Exception as e:
        print(f"An unexpected error occurred during store status ingestion: {e}")
