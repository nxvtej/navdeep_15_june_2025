from datetime import datetime, timedelta, timezone, time as dt_time
import pytz
from collections import defaultdict

def verify_store_report(
    store_id: str,
    store_timezone_str: str,
    menu_hours_data: dict,
    all_store_status_data: list, 
    report_end_time_utc_str: str,
    period_in_hours: int = 24 
):
    print(f"\n--- Starting Optimized Verification for Store: {store_id} ({period_in_hours} hours) ---")

    pytz_timezone_obj = pytz.timezone(store_timezone_str)

    report_end_utc = datetime.fromisoformat(report_end_time_utc_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
    report_start_utc = report_end_utc - timedelta(hours=period_in_hours)

    print(f"Report Period (UTC): {report_start_utc.isoformat()} to {report_end_utc.isoformat()}")

    all_store_status_data.sort(key=lambda x: x['timestamp_utc'])

    initial_status_for_period = "inactive"
    status_pointer_index = 0
    for i, status_entry in enumerate(all_store_status_data):
        if status_entry['timestamp_utc'] <= report_start_utc:
            initial_status_for_period = status_entry['status']
            status_pointer_index = i
        else:
            break 

    print(f"Initial status for this period at {report_start_utc.isoformat()}: {initial_status_for_period}")

    event_points_utc = set()
    event_points_utc.add(report_start_utc)
    event_points_utc.add(report_end_utc)

    for status_entry in all_store_status_data:
        if report_start_utc - timedelta(hours=1) <= status_entry['timestamp_utc'] <= report_end_utc + timedelta(hours=1):
            event_points_utc.add(status_entry['timestamp_utc'])

    current_day_utc = report_start_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    end_day_utc_loop = report_end_utc.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2)

    while current_day_utc <= end_day_utc_loop:
        current_day_local = current_day_utc.astimezone(pytz_timezone_obj)
        day_of_week = current_day_local.weekday()

        hours_for_day = menu_hours_data.get(day_of_week)

        if hours_for_day: 
            for interval in hours_for_day:
                start_time_local_str = interval['start_time_local']
                end_time_local_str = interval['end_time_local']

                start_dt_local_time = datetime.strptime(start_time_local_str, "%H:%M:%S").time()
                end_dt_local_time = datetime.strptime(end_time_local_str, "%H:%M:%S").time()

                start_dt_local = pytz_timezone_obj.localize(datetime.combine(current_day_local.date(), start_dt_local_time))
                end_dt_local = pytz_timezone_obj.localize(datetime.combine(current_day_local.date(), end_dt_local_time))
                
                if start_dt_local_time > end_dt_local_time:
                    end_dt_local += timedelta(days=1)
                
                event_points_utc.add(start_dt_local.astimezone(timezone.utc))
                event_points_utc.add(end_dt_local.astimezone(timezone.utc))
        
        current_day_utc += timedelta(days=1)

    event_points_utc = sorted(list(event_points_utc))
    final_event_points_utc = [p for p in event_points_utc if report_start_utc <= p <= report_end_utc]

    if report_start_utc not in final_event_points_utc:
        final_event_points_utc.insert(0, report_start_utc)
        
    if report_end_utc not in final_event_points_utc:
        final_event_points_utc.append(report_end_utc)

    unique_event_points_utc = []
    if final_event_points_utc:
        unique_event_points_utc.append(final_event_points_utc[0])
        for p in final_event_points_utc[1:]:
            if p != unique_event_points_utc[-1]:
                unique_event_points_utc.append(p)
    event_points_utc = unique_event_points_utc

    print(f"Number of unique event points: {len(event_points_utc)}")

    manual_uptime_minutes = 0.0
    manual_downtime_minutes = 0.0

    for i in range(len(event_points_utc) - 1):
        interval_start_utc = event_points_utc[i]
        interval_end_utc = event_points_utc[i+1]

        while (status_pointer_index < len(all_store_status_data) and
               all_store_status_data[status_pointer_index]['timestamp_utc'] <= interval_start_utc):
            initial_status_for_period = all_store_status_data[status_pointer_index]['status']
            status_pointer_index += 1
        
        current_status = initial_status_for_period

        check_time_utc = interval_start_utc
        check_time_local = check_time_utc.astimezone(pytz_timezone_obj)
        day_of_week = check_time_local.weekday()
        current_time_obj = check_time_local.time()

        is_within_bh = False
        hours_for_day = menu_hours_data.get(day_of_week)
        if hours_for_day:
            for bh_interval in hours_for_day:
                start_bh_time = datetime.strptime(bh_interval['start_time_local'], "%H:%M:%S").time()
                end_bh_time = datetime.strptime(bh_interval['end_time_local'], "%H:%M:%S").time()

                if start_bh_time <= end_bh_time:
                    if start_bh_time <= current_time_obj < end_bh_time:
                        is_within_bh = True
                        break
                else:
                    if current_time_obj >= start_bh_time or current_time_obj < end_bh_time:
                        is_within_bh = True
                        break

        duration_minutes = (interval_end_utc - interval_start_utc).total_seconds() / 60.0

        if is_within_bh:
            if current_status == 'active':
                manual_uptime_minutes += duration_minutes
            elif current_status == 'inactive':
                manual_downtime_minutes += duration_minutes

    print(f"\n--- Final Results for Store {store_id} ({period_in_hours} hours) ---")
    print(f"Manual Uptime: {manual_uptime_minutes:.2f} minutes ({manual_uptime_minutes / 60:.2f} hours)")
    print(f"Manual Downtime: {manual_downtime_minutes:.2f} minutes ({manual_downtime_minutes / 60:.2f} hours)")
    print("------------------------------------------")

test_store_id = '1e9bbb65-4c00-4f13-80d4-4f93b7ade7fd'
store_timezone_str = 'America/Boise'

test_menu_hours_data = defaultdict(list)
for day in range(7):
    test_menu_hours_data[day].append({'start_time_local': '10:30:00', 'end_time_local': '21:00:00'})

test_all_store_status_data = [
    {'timestamp_utc': datetime(2024, 10, 13, 2, 13, 49, 765346, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 13, 5, 15, 25, 610893, tzinfo=timezone.utc), 'status': 'inactive'},
    {'timestamp_utc': datetime(2024, 10, 13, 8, 14, 55, 40601, tzinfo=timezone.utc), 'status': 'inactive'},
    {'timestamp_utc': datetime(2024, 10, 13, 11, 14, 58, 164469, tzinfo=timezone.utc), 'status': 'inactive'},
    {'timestamp_utc': datetime(2024, 10, 13, 20, 14, 4, 357592, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 13, 23, 16, 0, 21755, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 14, 2, 13, 52, 117236, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 14, 5, 15, 49, 905135, tzinfo=timezone.utc), 'status': 'inactive'},
    {'timestamp_utc': datetime(2024, 10, 14, 14, 15, 1, 137276, tzinfo=timezone.utc), 'status': 'inactive'},
    {'timestamp_utc': datetime(2024, 10, 14, 17, 15, 4, 362399, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 14, 20, 16, 40, 34665, tzinfo=timezone.utc), 'status': 'active'},
    {'timestamp_utc': datetime(2024, 10, 14, 23, 14, 14, 500354, tzinfo=timezone.utc), 'status': 'active'},
]

latest_overall_status_timestamp = max(entry['timestamp_utc'] for entry in test_all_store_status_data)
report_end_time_utc = latest_overall_status_timestamp.replace(second=0, microsecond=0) + timedelta(minutes=1)
report_end_time_utc_str = report_end_time_utc.isoformat()

# VERIFICATION LAST DAY
print(f"Calculations will be relative to: {report_end_time_utc_str}")
verify_store_report(
    store_id=test_store_id,
    store_timezone_str=store_timezone_str,
    menu_hours_data=test_menu_hours_data,
    all_store_status_data=test_all_store_status_data,
    report_end_time_utc_str=report_end_time_utc_str,
    period_in_hours=24
)

# VERIFICATION LAST WEEK
verify_store_report(
    store_id=test_store_id,
    store_timezone_str=store_timezone_str,
    menu_hours_data=test_menu_hours_data,
    all_store_status_data=test_all_store_status_data,
    report_end_time_utc_str=report_end_time_utc_str,
    period_in_hours=7*24
)