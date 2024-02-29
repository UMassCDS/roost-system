from datetime import datetime, timedelta
import re
import os
import pytz
from azure.storage.blob import ContainerClient


####################################
# Helpers
####################################
def datetime_range(start=None, end=None, delta=timedelta(minutes=1), inclusive=True):
    """Construct a generator for a range of dates

    Args:
        start (datetime): start time
        end (datetime): end time
        delta (timedelta): time increment
        inclusive (bool): whether to include the end date

    Returns:
        Generator object
    """
    t = start or datetime.now()

    if inclusive:
        keep_going = lambda s, e: s <= e
    else:
        keep_going = lambda s, e: s < e

    while keep_going(t, end):
        yield t
        t = t + delta
    return


def blob_date_prefix(t):
    """Construct prefix of h5 file containing only its year, month and date

    Args:
        t (datetime): timestamp of file

    Returns:
        string: blob prefix contain date information

    Example format:
            blob name: 2022060100_06_ODIMH5_PVOL6S_VOL_CASET.h5
        return val: 20220601
    """
    prefix = "%04d%02d%02d" % (t.year, t.month, t.day)
    return prefix


def obtain_blob_timestamp(key):
    _, key = os.path.split(key)
    vals = re.match("(\d{4}\d{2}\d{2}\d{2}_\d{2})(\.?\w+)", key)
    (timestamp, suffix) = vals.groups()
    t = datetime.strptime(timestamp, "%Y%m%d%H_%M")
    return pytz.utc.localize(t)


####################################
# Azure setup
####################################


def get_station_day_scan_keys(
    start_time,
    end_time,
    station,  # not being used right now
    stride_in_minutes=6,
    thresh_in_minutes=6,
    sa_connection_str=None,
    sa_container_name="caset-2022",
):
    container_client = ContainerClient.from_connection_string(
        conn_str=sa_connection_str, container_name=sa_container_name
    )

    blob_names = []
    current_time = start_time
    # This loop usually runs only once, but we put it in the while loop
    # in case utc times for a day span across dates
    while current_time <= end_time:
        date_prefix = blob_date_prefix(current_time)
        blob_names.extend(
            list(container_client.list_blob_names(name_starts_with=date_prefix))
        )
        current_time = current_time + timedelta(days=1)

    if not blob_names:
        return []

    # # iterate by time and select the appropriate scans
    times = list(
        datetime_range(start_time, end_time, timedelta(minutes=stride_in_minutes))
    )
    time_thresh = timedelta(minutes=thresh_in_minutes)

    selected_blob_names = []
    current_idx = 0
    for t in times:
        t_current = obtain_blob_timestamp(blob_names[current_idx])

        while current_idx + 1 < len(blob_names):
            t_next = obtain_blob_timestamp(blob_names[current_idx + 1])
            if abs(t_current - t) < abs(t_next - t):
                break
            t_current = t_next
            current_idx += 1

        if abs(t_current - t) <= time_thresh:
            selected_blob_names.append(blob_names[current_idx])

    return selected_blob_names


def download_scan(
    key,
    data_dir,
    file_download_loc,
    sa_connection_str=None,
    sa_container_name="caset-2022",
):
    container_client = ContainerClient.from_connection_string(
        conn_str=sa_connection_str, container_name=sa_container_name
    )

    local_file = os.path.join(data_dir, file_download_loc)
    local_dir, filename = os.path.split(local_file)
    os.makedirs(local_dir, exist_ok=True)

    if not os.path.isfile(local_file):
        with open(file=local_file, mode="wb") as f:
            f.write(container_client.download_blob(key).readall())
