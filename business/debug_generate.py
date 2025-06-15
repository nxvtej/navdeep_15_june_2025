from datetime import datetime, timedelta, timezone, time
import pandas as pd
import pytz
import time as timer_module
from sqlalchemy.orm import Session as DBSession
from sqlalchemy import func
import os
from collections import defaultdict
import bisect
import uuid

# Import your database session and models
from app.database.db import Session, engine
from app.database.models import Store, Store_Status, Menu_Hours, Timezone, Report

# --- Configuration ---
DEFAULT_TIMEZONE = 'America/Chicago'
DEFAULT_BUSINESS_HOURS = {
    'start_time_local': time(0,0,0),
    'end_time_local': time(23,59,59)
}
REPORTS_DIR = "reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

# --- Helper Functions (Phase 2 & 3 from our breakdown) ---

def _get_store_details(db: DBSession, store_id: str):
    """
    Fetches a store's timezone and organized menu hours.
    Returns: Tuple (pytz_timezone_obj, menu_hours_dict)
    """
    store_timezone_entry = db.query(Timezone).filter(Timezone.store_id == store_id).first()
    timezone_str = store_timezone_entry.timezone_str if store_timezone_entry else DEFAULT_TIMEZONE
    try:
        pytz_timezone_obj = pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        print(f"Warning: Unknown timezone '{timezone_str}' for store {store_id}. Using default '{DEFAULT_TIMEZONE}'.")
        pytz_timezone_obj = pytz.timezone(DEFAULT_TIMEZONE)

    menu_hours_dict = defaultdict(lambda: [
        {'start_time_local': DEFAULT_BUSINESS_HOURS['start_time_local'],
         'end_time_local': DEFAULT_BUSINESS_HOURS['end_time_local']}
    ])

    explicit_hours = db.query(Menu_Hours).filter(Menu_Hours.store_id == store_id).all()
    
    for mh in explicit_hours:
        if mh.day_of_week not in menu_hours_dict or \
           menu_hours_dict[mh.day_of_week] == [{'start_time_local': DEFAULT_BUSINESS_HOURS['start_time_local'], 'end_time_local': DEFAULT_BUSINESS_HOURS['end_time_local']}]:
            menu_hours_dict[mh.day_of_week] = []

        menu_hours_dict[mh.day_of_week].append({
            'start_time_local': mh.start_time_local,
            'end_time_local': mh.end_time_local
        })

    return pytz_timezone_obj, menu_hours_dict

def _get_relevant_status_data(db: DBSession, store_id: str, period_start_utc: datetime, period_end_utc: datetime):
    """
    Fetches Store_Status records for a store within a given UTC period,
    plus the last known status *before* the period starts for accurate interpolation.
    Returns: List of sorted Store_Status objects.
    """
    status_within_period = db.query(Store_Status).filter(
        Store_Status.store_id == store_id,
        Store_Status.timestamp_utc >= period_start_utc,
        Store_Status.timestamp_utc < period_end_utc
    ).order_by(Store_Status.timestamp_utc).all()

    last_status_before_period = db.query(Store_Status).filter(
        Store_Status.store_id == store_id,
        Store_Status.timestamp_utc < period_start_utc
    ).order_by(Store_Status.timestamp_utc.desc()).first()

    all_relevant_statuses = []
    if last_status_before_period:
        all_relevant_statuses.append(last_status_before_period)
    all_relevant_statuses.extend(status_within_period)
    
    all_relevant_statuses.sort(key=lambda x: x.timestamp_utc)

    return all_relevant_statuses

def _get_status_at_time(sorted_status_data: list, current_time_utc: datetime) -> bool: # Changed return type hint to bool
    """
    Determines the interpolated 'active'/'inactive' status at a specific UTC timestamp
    based on sorted status data using a binary search (bisect).
    Returns True for 'active', False for 'inactive'.
    """
    # Assuming 'active' is True and 'inactive' is False based on fetched data
    current_status = False # Default if no status data available or before first entry (inactive)

    timestamps = [entry.timestamp_utc for entry in sorted_status_data]
    
    idx = bisect.bisect_right(timestamps, current_time_utc)

    if idx > 0:
        # Assuming status_entry.status returns a boolean now
        current_status = sorted_status_data[idx - 1].status 
    
    return current_status

def _is_within_business_hours(local_datetime: datetime, menu_hours_for_day: list) -> bool:
    """
    Checks if a local datetime falls within any of the store's defined business hours
    for that specific day.
    menu_hours_for_day: List of dicts, e.g., [{'start_time_local': datetime.time, 'end_time_local': datetime.time}]
    """
    if not menu_hours_for_day:
        return False

    current_time_obj = local_datetime.time()

    for hours_interval in menu_hours_for_day:
        start_time_bh = hours_interval['start_time_local']
        end_time_bh = hours_interval['end_time_local']

        if start_time_bh <= end_time_bh: # Normal day operation
            if start_time_bh <= current_time_obj <= end_time_bh:
                return True
        else: # Overnight operation
            if current_time_obj >= start_time_bh or current_time_obj <= end_time_bh:
                return True
    return False

def _get_all_utc_business_intervals_for_period(
    timezone_obj: pytz.BaseTzInfo,
    menu_hours_data: dict, # Dict with day_of_week as key, list of hour intervals as value
    period_start_utc: datetime,
    period_end_utc: datetime,
    debug_mode: bool = False # Added debug_mode
) -> list[tuple[datetime, datetime]]:
    """
    Generates a list of all UTC intervals where the store is open
    within the specified UTC reporting period.
    """
    utc_business_intervals = []

    if period_start_utc.tzinfo is None:
        period_start_utc = period_start_utc.replace(tzinfo=timezone.utc)
    
    # Iterate through days that could possibly overlap with the UTC reporting period.
    # We need to cover the local dates that period_start_utc and period_end_utc fall into,
    # plus buffer days to account for timezone shifts and overnight hours.
    start_local_date_for_loop = period_start_utc.astimezone(timezone_obj).date() - timedelta(days=2)
    end_local_date_for_loop = period_end_utc.astimezone(timezone_obj).date() + timedelta(days=2)

    current_local_date = start_local_date_for_loop
    while current_local_date <= end_local_date_for_loop:
        day_of_week = current_local_date.weekday() # 0=Monday, 6=Sunday
        
        daily_menu_hours = menu_hours_data[day_of_week]
        if debug_mode:
            print(f"  Local Date: {current_local_date}, Day of Week: {day_of_week}, Menu Hours: {daily_menu_hours}")

        for hours_interval in daily_menu_hours:
            start_time = hours_interval['start_time_local']
            end_time = hours_interval['end_time_local']

            local_bh_start_dt = datetime.combine(current_local_date, start_time).replace(tzinfo=timezone_obj)
            local_bh_end_dt = datetime.combine(current_local_date, end_time).replace(tzinfo=timezone_obj)

            if start_time > end_time: # Overnight shift
                local_bh_end_dt += timedelta(days=1)

            utc_bh_start_dt = local_bh_start_dt.astimezone(timezone.utc)
            utc_bh_end_dt = local_bh_end_dt.astimezone(timezone.utc)

            # Clip intervals to the reporting period
            overlap_start_utc = max(utc_bh_start_dt, period_start_utc)
            overlap_end_utc = min(utc_bh_end_dt, period_end_utc)

            if debug_mode:
                print(f"    Raw Local BH: {local_bh_start_dt} to {local_bh_end_dt}")
                print(f"    Raw UTC BH: {utc_bh_start_dt} to {utc_bh_end_dt}")
                print(f"    Clipped UTC BH: {overlap_start_utc} to {overlap_end_utc}")


            if overlap_start_utc < overlap_end_utc: # If there's a valid overlap
                utc_business_intervals.append((overlap_start_utc, overlap_end_utc))
        
        current_local_date += timedelta(days=1)

    # Sort and merge overlapping intervals
    if not utc_business_intervals:
        if debug_mode:
            print("  No UTC business intervals found.")
        return []

    utc_business_intervals.sort(key=lambda x: x[0])

    merged_intervals = []
    current_merged_start = None
    current_merged_end = None

    for start, end in utc_business_intervals:
        if current_merged_start is None:
            current_merged_start = start
            current_merged_end = end
        elif start <= current_merged_end: # Overlap or touch
            current_merged_end = max(current_merged_end, end)
        else: # No overlap, add current merged and start new
            merged_intervals.append((current_merged_start, current_merged_end))
            current_merged_start = start
            current_merged_end = end
    
    if current_merged_start is not None:
        merged_intervals.append((current_merged_start, current_merged_end))
    
    if debug_mode:
        print("\n  Merged UTC Business Intervals:")
        for start, end in merged_intervals:
            print(f"    {start} to {end} (Duration: {(end-start).total_seconds()/60.0:.2f} mins)")
        print("  --- End Merged BH ---")

    return merged_intervals


def _calculate_uptime_downtime_for_period(
    db: DBSession,
    store_id: str,
    timezone_obj: pytz.BaseTzInfo,
    menu_hours_data: dict, # Dict with day_of_week as key, list of hour intervals as value
    period_start_utc: datetime,
    period_end_utc: datetime, # End exclusive
    debug_mode: bool = False # Added debug_mode
) -> tuple[float, float]:
    """
    Calculates uptime and downtime for a single store over a specific UTC period,
    considering business hours and interpolating status, using an interval-based approach.
    Returns: (uptime_minutes, downtime_minutes)
    """
    uptime_minutes = 0.0
    downtime_minutes = 0.0

    if debug_mode:
        print(f"\n--- Calculating Uptime/Downtime for Store {store_id} ---")
        print(f"Period: {period_start_utc} to {period_end_utc}")

    relevant_status_data = _get_relevant_status_data(db, store_id, period_start_utc, period_end_utc)
    
    if debug_mode:
        print("Relevant Status Data (fetched):")
        for entry in relevant_status_data:
            print(f"  {entry.timestamp_utc} -> {entry.status}")
        print("--- End Relevant Status Data ---")

    # Extract just the timestamps from relevant_status_data for bisect
    status_timestamps = [entry.timestamp_utc for entry in relevant_status_data]
    
    # Pre-calculate all UTC business hour intervals for this period
    utc_business_hours_intervals = _get_all_utc_business_intervals_for_period(
        timezone_obj, menu_hours_data, period_start_utc, period_end_utc, debug_mode=debug_mode
    )

    # Collect all significant timestamps that define intervals for calculation
    all_event_timestamps_utc = set()
    all_event_timestamps_utc.add(period_start_utc)
    all_event_timestamps_utc.add(period_end_utc)

    for status_entry in relevant_status_data:
        # Only add status timestamps that are within the calculation period or define the start
        # of a segment inside the period.
        # It's crucial to only add timestamps that fall within the *actual* period boundaries
        # to avoid extraneous intervals. The last_status_before_period handles the state just before.
        if period_start_utc <= status_entry.timestamp_utc <= period_end_utc:
            all_event_timestamps_utc.add(status_entry.timestamp_utc)
    
    for bh_start, bh_end in utc_business_hours_intervals:
        all_event_timestamps_utc.add(bh_start)
        all_event_timestamps_utc.add(bh_end)

    sorted_event_timestamps_utc = sorted(list(all_event_timestamps_utc))

    if debug_mode:
        print("\nAll Sorted Event Timestamps (UTC):")
        for ts in sorted_event_timestamps_utc:
            print(f"  {ts}")
        print("--- End Event Timestamps ---")

    # Iterate through the defined intervals
    for i in range(len(sorted_event_timestamps_utc) - 1):
        interval_start_utc = sorted_event_timestamps_utc[i]
        interval_end_utc = sorted_event_timestamps_utc[i+1]

        # Clip interval to actual reporting period boundaries (important for edge cases)
        interval_start_utc_clipped = max(interval_start_utc, period_start_utc)
        interval_end_utc_clipped = min(interval_end_utc, period_end_utc)

        # If interval becomes invalid after clipping, skip
        if interval_start_utc_clipped >= interval_end_utc_clipped:
            continue

        duration_seconds = (interval_end_utc_clipped - interval_start_utc_clipped).total_seconds()
        duration_minutes = duration_seconds / 60.0

        if duration_minutes <= 0: # Avoid processing zero or negative duration intervals
            continue

        # Determine the store's status for this interval
        current_status = _get_status_at_time(relevant_status_data, interval_start_utc_clipped)

        # Check if this interval is within business hours (overlaps with any business hour interval)
        is_within_bh = False
        for bh_start, bh_end in utc_business_hours_intervals:
            overlap_start = max(interval_start_utc_clipped, bh_start)
            overlap_end = min(interval_end_utc_clipped, bh_end)
            
            if overlap_start < overlap_end: # Valid overlap found
                is_within_bh = True
                break

        if debug_mode:
            print(f"\nProcessing Interval: {interval_start_utc_clipped} to {interval_end_utc_clipped} ({duration_minutes:.2f} mins)")
            print(f"  Determined Status: {current_status}")
            print(f"  Is within Business Hours: {is_within_bh}")

        if is_within_bh:
            # FIX: Compare with True/False instead of 'active'/'inactive' strings
            if current_status is True: # Status is True (active)
                uptime_minutes += duration_minutes
                if debug_mode:
                    print(f"    Adding {duration_minutes:.2f} mins to Uptime. Current Uptime: {uptime_minutes:.2f}")
            elif current_status is False: # Status is False (inactive)
                downtime_minutes += duration_minutes
                if debug_mode:
                    print(f"    Adding {duration_minutes:.2f} mins to Downtime. Current Downtime: {downtime_minutes:.2f}")
            # Other statuses would be ignored for uptime/downtime.

    if debug_mode:
        print(f"\n--- Final Results for Store {store_id} ({period_start_utc} to {period_end_utc}) ---")
        print(f"Total Uptime: {uptime_minutes:.2f} minutes")
        print(f"Total Downtime: {downtime_minutes:.2f} minutes")
        print("--- End Store Debugging ---")

    return uptime_minutes, downtime_minutes


# --- Main Report Generation and Saving Function ---

def generate_report_data_and_save_csv(report_id: str, debug_target_store_id: str = None):
    """
    Main function to generate the report, save it to CSV, and update DB status.
    This function will be called as a background task.
    
    Args:
        report_id (str): Unique ID for the report.
        debug_target_store_id (str, optional): If provided, only this store_id will be processed
                                                and debug prints will be enabled. Defaults to None.
    """
    db: DBSession = None
    report_entry: Report = None
    try:
        db = Session()
        report_entry = db.query(Report).filter(Report.report_id == report_id).first()
        if not report_entry:
            print(f"Report ID {report_id} not found in DB for generation.")
            # If in debug mode and report_entry doesn't exist, create a dummy one for local testing
            if debug_target_store_id:
                print(f"Creating dummy report entry for debug ID: {report_id}")
                # FIX: Removed 'generated_at' from constructor as it might be auto-populated
                report_entry = Report(report_id=report_id, status="Running") 
                db.add(report_entry)
                db.commit()
            else:
                return # In non-debug mode, just exit if report_id isn't in DB
        
        # These fields are set AFTER object creation, which is usually safer if defaults exist
        report_entry.status = "Running"
        report_entry.generated_at = datetime.now(timezone.utc)
        db.commit()
        print(f"Report {report_id}: Status set to 'Running'.")

        latest_status_timestamp_utc = db.query(func.max(Store_Status.timestamp_utc)).scalar()

        if not latest_status_timestamp_utc:
            print(f"Report {report_id}: No store status data found. Cannot generate report.")
            report_entry.status = "Failed"
            report_entry.error_message = "No store status data available for report generation."
            report_entry.completed_at = datetime.now(timezone.utc)
            db.commit()
            return

        if latest_status_timestamp_utc.tzinfo is None:
            latest_status_timestamp_utc = latest_status_timestamp_utc.replace(tzinfo=timezone.utc)
        
        report_end_time_utc = latest_status_timestamp_utc.replace(second=0, microsecond=0) + timedelta(minutes=1)
        print(f"Report {report_id}: Calculations relative to: {report_end_time_utc} UTC")

        reporting_periods = [
            {
                "name": "last_hour",
                "start_utc": report_end_time_utc - timedelta(hours=1),
                "end_utc": report_end_time_utc
            },
            {
                "name": "last_day",
                "start_utc": report_end_time_utc - timedelta(hours=24),
                "end_utc": report_end_time_utc
            },
            {
                "name": "last_week",
                "start_utc": report_end_time_utc - timedelta(days=7),
                "end_utc": report_end_time_utc
            }
        ]

        # Determine which store IDs to process
        if debug_target_store_id:
            all_store_ids = [debug_target_store_id]
            print(f"DEBUG MODE: Processing only store_id: {debug_target_store_id}")
        else:
            all_store_ids_query = db.query(Store.store_id).distinct().all()
            all_store_ids = [s[0] for s in all_store_ids_query]

        report_data_list = []
        total_stores = len(all_store_ids)
        print(f"Report {report_id}: Found {total_stores} unique stores to process.")
        process_start_time = timer_module.monotonic()

        for i,store_id in enumerate(all_store_ids):
            elapsed_time_seconds = timer_module.monotonic() - process_start_time
            elapsed_minutes = int(elapsed_time_seconds // 60)
            elapsed_seconds = int(elapsed_time_seconds % 60)
            
            percentage_done = ((i + 1) / total_stores) * 100
            
            print(f"Processing store {store_id} ({i+1}/{total_stores} | {percentage_done:.2f}% done | Elapsed: {elapsed_minutes:02d}m {elapsed_seconds:02d}s)")

            store_report_row = {"store_id": store_id}
            
            is_debug_run_for_this_store = (debug_target_store_id is not None)

            try:
                timezone_obj, menu_hours_data = _get_store_details(db, store_id)
            except Exception as e:
                print(f"Report {report_id}: Error fetching details for store {store_id}: {e}. Skipping store.")
                for period in reporting_periods:
                    store_report_row[f"uptime_{period['name']}(minutes)"] = 0.0
                    store_report_row[f"downtime_{period['name']}(minutes)"] = 0.0
                report_data_list.append(store_report_row)
                continue


            for period in reporting_periods:
                if is_debug_run_for_this_store:
                    print(f"\n--- Period: {period['name']} ---")

                uptime_mins, downtime_mins = _calculate_uptime_downtime_for_period(
                    db, store_id, timezone_obj, menu_hours_data,
                    period['start_utc'], period['end_utc'],
                    debug_mode=is_debug_run_for_this_store # Pass the debug flag
                )
                
                if period['name'] == 'last_hour':
                    store_report_row["uptime_last_hour(minutes)"] = round(uptime_mins, 2)
                    store_report_row["downtime_last_hour(minutes)"] = round(downtime_mins, 2)
                elif period['name'] == 'last_day':
                    store_report_row["uptime_last_day(hours)"] = round(uptime_mins / 60.0, 2)
                    store_report_row["downtime_last_day(hours)"] = round(downtime_mins / 60.0, 2)
                elif period['name'] == 'last_week':
                    store_report_row["uptime_last_week(hours)"] = round(uptime_mins / 60.0, 2)
                    store_report_row["downtime_last_week(hours)"] = round(downtime_mins / 60.0, 2)
            
            report_data_list.append(store_report_row)
            db.commit()

        report_df = pd.DataFrame(report_data_list)
        
        output_columns = [
            "store_id",
            "uptime_last_hour(minutes)",
            "uptime_last_day(hours)",
            "uptime_last_week(hours)",
            "downtime_last_hour(minutes)",
            "downtime_last_day(hours)",
            "downtime_last_week(hours)"
        ]
        report_df = report_df[output_columns]

        report_filepath = os.path.join(REPORTS_DIR, f"{report_id}.csv")
        report_df.to_csv(report_filepath, index=False)
        print(f"Report {report_id}: Report saved to {report_filepath}")

        report_entry.status = "Completed"
        report_entry.completed_at = datetime.now(timezone.utc)
        report_entry.report_file_path = report_filepath
        db.commit()
        print(f"Report {report_id}: Status set to 'Completed'.")

    except Exception as e:
        print(f"Report {report_id}: An error occurred during report generation: {e}")
        if db and report_entry:
            db.rollback()
            report_entry.status = "Failed"
            report_entry.error_message = str(e)
            report_entry.completed_at = datetime.now(timezone.utc)
            db.commit()
        raise

    finally:
        if db:
            db.close()
            print(f"Report {report_id}: Database session closed.")


# --- Test execution for local debugging ---
if __name__ == "__main__":
    # To test, ensure your database is running and has data.
    
    # Prompt for store_id to debug
    store_id_to_debug = input("Enter store_id to debug (leave empty to run full report): ").strip()
    
    if store_id_to_debug:
        debug_report_id = "debug_report_" + str(uuid.uuid4())
        print(f"\nRunning debug report generation for Store ID: {store_id_to_debug} (Report ID: {debug_report_id})")
        try:
            generate_report_data_and_save_csv(debug_report_id, debug_target_store_id=store_id_to_debug)
            print(f"\nDebug report {debug_report_id} for store {store_id_to_debug} finished successfully. Check {REPORTS_DIR}/{debug_report_id}.csv")
        except Exception as e:
            print(f"\nDebug report {debug_report_id} for store {store_id_to_debug} failed: {e}")
    else:
        test_report_id = "full_report_" + datetime.now().strftime("%Y%m%d%H%M%S")
        print(f"\nRunning full report generation for ID: {test_report_id}")
        try:
            generate_report_data_and_save_csv(test_report_id)
            print(f"\nFull report {test_report_id} finished successfully. Check {REPORTS_DIR}/{test_report_id}.csv")
        except Exception as e:
            print(f"\nFull report {test_report_id} failed: {e}")
