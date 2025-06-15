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
from app.database.models import Store
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

# Ingestion Functions
def ingest_stores(df_status: pd.DataFrame, df_hours: pd.DataFrame):
    """
    Ingests unique store IDs from all CSVs into the 'stores' table using bulk_insert_mappings.
    Filters out existing stores to prevent duplicates and applies batching.
    """

    try:
        all_store_ids = set(df_status['store_id'].unique())
        all_store_ids.update(df_hours['store_id'].unique())

        print(f"Ingesting {len(all_store_ids)} potential unique store IDs into the database...")

        records_to_insert = [{'store_id': str(store_id)} for store_id in all_store_ids]

        if records_to_insert:
            total_count = 0
            print(f"Starting ingestion of {len(records_to_insert)} new store IDs in batches of {SMALL_TABLE_BATCH_SIZE}...")
            for i in range(0, len(records_to_insert), SMALL_TABLE_BATCH_SIZE):
                batch = records_to_insert[i:i + SMALL_TABLE_BATCH_SIZE]
                try:
                    stmt = _get_insert_statement_on_conflict(Store.__table__, batch, ['store_id'])
                    with engine.begin() as conn:
                        conn.execute(stmt)
                    total_count += len(batch)
                    print(f"  Ingested {total_count} store IDs so far...")
                except Exception as e:
                    print(f"Error ingesting store IDs batch {i//SMALL_TABLE_BATCH_SIZE + 1} ({len(batch)} records): {e}")
            print(f"Total ingested {total_count} new store IDs. Duplicates were skipped if they already existed in the database.")
        else:
            print("No new store IDs to ingest (all found in DB).")
        print("Ingestion of stores completed.")
    except IntegrityError as e:
        print(f"IntegrityError during store ingestion: {e}. This likely means some store IDs already exist in the database.")
    except Exception as e:
        print(f"An unexpected error occurred during store ingestion: {e}")
  