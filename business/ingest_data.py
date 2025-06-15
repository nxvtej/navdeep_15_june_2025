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

from app.database.db import engine, Base
from app.database.models import Store, Store_Status, Menu_Hours, Timezone, Report

from app.database.ingestors.store_status import ingest_store_status
from app.database.ingestors.menu_hours import ingest_menu_hours
from app.database.ingestors.timezones import ingest_timezones
from app.database.ingestors.stores import ingest_stores

from business.config import (
    DATA_DIR,
    MENU_HOURS_CSV,
    STORE_STATUS_CSV,
    TIMEZONES_CSV,
    SMALL_TABLE_BATCH_SIZE,
    DEFAULT_MENU_HOURS,
    DEFAULT_TIMEZONE
)


def main():
    print("pid:", os.getpid())
    # create tables
    try:
        with engine.begin() as conn:
            Base.metadata.create_all(bind=engine)
        print("Database tables created successfully.")
    except Exception as e:
        print(f"Error creating database tables: {e}")
        return

    # Data directory
    if not os.path.exists(DATA_DIR):
        raise FileNotFoundError(f"Data directory does not exist: {DATA_DIR}")

    if not os.path.exists(MENU_HOURS_CSV) or not os.path.exists(STORE_STATUS_CSV) or not os.path.exists(TIMEZONES_CSV):
        raise FileNotFoundError(f"One or more CSV files not found: {[MENU_HOURS_CSV, STORE_STATUS_CSV, TIMEZONES_CSV]}.")


    try:
        print("Starting data ingestion process...")
        start_time = datetime.now()
        df_status = pd.read_csv(STORE_STATUS_CSV)
        df_hours = pd.read_csv(MENU_HOURS_CSV)
        df_timezone = pd.read_csv(TIMEZONES_CSV)
        print(f"CSV files loaded successfully in {datetime.now() - start_time} seconds.")
    except FileNotFoundError as e:
        print(f"Error: One or more CSV files not found. Please ensure they are in the 'data' directory. {e}")
        return
    except pd.errors.EmptyDataError as e:
        print(f"Error: One or more CSV files are empty. Please check the data files. {e}")
        return
    except pd.errors.ParserError as e:
        print(f"Error: There was a problem parsing one of the CSV files. Please check the data files. {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while loading CSV files: {e}")
        return  
    
    # pre-flight to conflict check 
    try:
        with engine.begin() as conn:
            conn.execute(text("SELECT 1 FROM stores LIMIT 1"))
        print("Database connection successful. Proceeding with data ingestion...")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return

    # exit(1)
    try:
        start_time = datetime.now()
        ingest_stores(df_status, df_hours)
        print(f"Ingested unique store IDs into the database in {datetime.now() - start_time} seconds.")
        """            
        ingest_store = threading.Thread(target=ingest_store_status, args=(db,)).start()
        ingest_menu = threading.Thread(target=ingest_menu_hours, args=(db,)).start()
        ingest_timezone = threading.Thread(target=ingest_timezones, args=(db,)).start()

        # wait for all threads
        ingest_store.join()
        ingest_menu.join()
        ingest_timezone.join()

        """

        # removing parent lvel threads
        """
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(ingest_store_status, df_status.copy()),
                executor.submit(ingest_menu_hours, df_hours.copy()),
                executor.submit(ingest_timezones, df_timezone.copy())
            ]

        for future in futures:
            try:
                future.result() 
            except Exception as e:
                print(f"An error occurred in a thread: {e}")
        """
        
        # direct function call
        ingest_store_status(df_status)
        ingest_menu_hours(df_hours)
        ingest_timezones(df_timezone)

        print("\nData ingestion process completed successfully.")
    except Exception as e:
        print(f"An error occurred during the data ingestion process: {e}")
    finally:
        print(f"Ingestion process finished in {datetime.now() - start_time} seconds.")

if __name__ == "__main__":
    main()