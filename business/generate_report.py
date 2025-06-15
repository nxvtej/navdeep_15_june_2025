import os
import pytz
import bisect
import pandas as pd
import time as timer_module


from sqlalchemy import func
from collections import defaultdict
from sqlalchemy.orm import Session as DBSession
from datetime import datetime, timedelta, timezone, time

from app.database.db import Session, engine
from app.database.models import Store, Store_Status, Menu_Hours, Timezone, Report

from business.config import (
    REPORTS_DIR,
    DEFAULT_TIMEZONE,
    DEFAULT_MENU_HOURS
)


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

def _get_status_at_time(sorted_status_data: list, current_time_utc: datetime) -> str:
    """
    Determines the interpolated 'active'/'inactive' status at a specific UTC timestamp
    based on sorted status data using a binary search (bisect).
    """
    current_status = False 
    timestamps = [entry.timestamp_utc for entry in sorted_status_data]
    
    idx = bisect.bisect_right(timestamps, current_time_utc)

    if idx > 0:
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
    menu_hours_data: dict, 
    period_start_utc: datetime,
    period_end_utc: datetime
) -> list[tuple[datetime, datetime]]:
    """
    Generates a list of all UTC intervals where the store is open
    within the specified UTC reporting period.

    Args:
        timezone_obj: The pytz timezone object for the store.
        menu_hours_data: A dictionary mapping day_of_week (0-6) to a list of
                         time intervals (e.g., {'start_time_local': time, 'end_time_local': time}).
        period_start_utc: The start of the reporting period in UTC.
        period_end_utc: The end of the reporting period in UTC (exclusive).

    Returns:
        A list of tuples, where each tuple is (utc_start_datetime, utc_end_datetime)
        representing a continuous period of business hours in UTC.
    """
    utc_business_intervals = []

    if period_start_utc.tzinfo is None:
        period_start_utc = period_start_utc.replace(tzinfo=timezone.utc)
    
    start_local_date_for_loop = period_start_utc.astimezone(timezone_obj).date() - timedelta(days=2) # Start a bit earlier
    end_local_date_for_loop = period_end_utc.astimezone(timezone_obj).date() + timedelta(days=2) # End a bit later

    current_local_date = start_local_date_for_loop
    while current_local_date <= end_local_date_for_loop:
        day_of_week = current_local_date.weekday() # 0=Monday, 6=Sunday
        
        daily_menu_hours = menu_hours_data[day_of_week]

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

            if overlap_start_utc < overlap_end_utc: # If there's a valid overlap
                utc_business_intervals.append((overlap_start_utc, overlap_end_utc))
        
        current_local_date += timedelta(days=1)

    # Sort and merge overlapping intervals
    if not utc_business_intervals:
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
        else:
            merged_intervals.append((current_merged_start, current_merged_end))
            current_merged_start = start
            current_merged_end = end
    
    if current_merged_start is not None:
        merged_intervals.append((current_merged_start, current_merged_end))

    return merged_intervals


def _calculate_uptime_downtime_for_period(
    db: DBSession,
    store_id: str,
    timezone_obj: pytz.BaseTzInfo,
    menu_hours_data: dict, 
    period_start_utc: datetime,
    period_end_utc: datetime
) -> tuple[float, float]:
    """
    Calculates uptime and downtime for a single store over a specific UTC period,
    considering business hours and interpolating status, using an interval-based approach.
    Returns: (uptime_minutes, downtime_minutes)
    """
    uptime_minutes = 0.0
    downtime_minutes = 0.0

    relevant_status_data = _get_relevant_status_data(db, store_id, period_start_utc, period_end_utc)
    
    status_timestamps = [entry.timestamp_utc for entry in relevant_status_data]    
    utc_business_hours_intervals = _get_all_utc_business_intervals_for_period(
        timezone_obj, menu_hours_data, period_start_utc, period_end_utc
    )

    all_event_timestamps_utc = set()
    all_event_timestamps_utc.add(period_start_utc)
    all_event_timestamps_utc.add(period_end_utc)

    for status_entry in relevant_status_data:
        if period_start_utc <= status_entry.timestamp_utc <= period_end_utc:
            all_event_timestamps_utc.add(status_entry.timestamp_utc)
    
    for bh_start, bh_end in utc_business_hours_intervals:
        all_event_timestamps_utc.add(bh_start)
        all_event_timestamps_utc.add(bh_end)

    sorted_event_timestamps_utc = sorted(list(all_event_timestamps_utc))

    for i in range(len(sorted_event_timestamps_utc) - 1):
        interval_start_utc = sorted_event_timestamps_utc[i]
        interval_end_utc = sorted_event_timestamps_utc[i+1]

        # Skipping interval wchich is invalid or outside the main reporting period
        if interval_start_utc >= interval_end_utc or \
           interval_start_utc >= period_end_utc or \
           interval_end_utc <= period_start_utc:
            continue
        
        # Clip interval to actual reporting period boundaries (redundant if previous check is robust, but safe)
        interval_start_utc = max(interval_start_utc, period_start_utc)
        interval_end_utc = min(interval_end_utc, period_end_utc)

        # Re-check
        if interval_start_utc >= interval_end_utc:
            continue

        duration_seconds = (interval_end_utc - interval_start_utc).total_seconds()
        duration_minutes = duration_seconds / 60.0

        current_status = _get_status_at_time(relevant_status_data, interval_start_utc)

        # Check if this interval is within business hours (overlaps with any business hour interval)
        is_within_bh = False
        for bh_start, bh_end in utc_business_hours_intervals:
            overlap_start = max(interval_start_utc, bh_start)
            overlap_end = min(interval_end_utc, bh_end)
            
            if overlap_start < overlap_end: # Valid overlap found
                is_within_bh = True
                break # Found one, no need to check others for this interval

        if is_within_bh:
            if current_status == True:
                uptime_minutes += duration_minutes
            elif current_status == False:
                downtime_minutes += duration_minutes
            # Note: If status is anything other than 'active' or 'inactive', it's ignored for uptime/downtime.

    return uptime_minutes, downtime_minutes


# Main Report Generator

def generate_report_data_and_save_csv(report_id: str):
    """
    Main function to generate the report, save it to CSV, and update DB status.
    This function will be called as a background task.
    """
    db: DBSession = None
    report_entry: Report = None
    try:
        db = Session()
        report_entry = db.query(Report).filter(Report.report_id == report_id).first()
        if not report_entry:
            print(f"Report ID {report_id} not found in DB for generation.")
            return
        
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
                uptime_mins, downtime_mins = _calculate_uptime_downtime_for_period(
                    db, store_id, timezone_obj, menu_hours_data,
                    period['start_utc'], period['end_utc']
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
