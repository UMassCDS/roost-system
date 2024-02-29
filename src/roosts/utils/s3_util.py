import boto3
from datetime import datetime, timedelta
import re
import os
import pytz


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


def s3_key(t, station):
    """Construct (prefix of) s3 key for NEXRAD file

    Args:
        t (datetime): timestamp of file
        station (string): station identifier

    Returns:
        string: s3 key, excluding version string suffix

    Example format:
            s3 key: 2015/05/02/KMPX/KMPX20150502_021525_V06.gz
        return val: 2015/05/02/KMPX/KMPX20150502_021525
    """

    key = "%04d/%02d/%02d/%04s/%04s%04d%02d%02d_%02d%02d%02d" % (
        t.year,
        t.month,
        t.day,
        station,
        station,
        t.year,
        t.month,
        t.day,
        t.hour,
        t.minute,
        t.second,
    )

    return key


def s3_prefix(t, station=None):
    prefix = "%04d/%02d/%02d" % (t.year, t.month, t.day)
    if station is not None:
        prefix = prefix + "/%04s/%04s" % (station, station)
    return prefix


def parse_key(key):
    path, key = os.path.split(key)
    vals = re.match("(\w{4})(\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2})(\.?\w+)", key)
    (station, timestamp, suffix) = vals.groups()
    t = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
    return pytz.utc.localize(t), station


####################################
# AWS setup
####################################


def get_station_day_scan_keys(
    start_time,
    end_time,
    station,
    stride_in_minutes=3,
    thresh_in_minutes=3,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    if aws_access_key_id is None and aws_secret_access_key is None:
        bucket = boto3.resource("s3", region_name="us-east-2").Bucket(
            "noaa-nexrad-level2"
        )
    else:
        bucket = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="us-east-2",
        ).Bucket("noaa-nexrad-level2")
    start_key = s3_key(start_time, station)
    end_key = s3_key(end_time, station)

    keys = []
    current_time = start_time
    while current_time < end_time + timedelta(days=1):
        prefix = s3_prefix(current_time, station)
        objects = bucket.objects.filter(Prefix=prefix)
        keys.extend([o.key for o in objects if o.key >= start_key and o.key <= end_key])
        current_time = current_time + timedelta(days=1)

    if not keys:
        return []

    # iterate by time and select the appropriate scans
    times = list(
        datetime_range(start_time, end_time, timedelta(minutes=stride_in_minutes))
    )
    time_thresh = timedelta(minutes=thresh_in_minutes)

    selected_keys = []
    current_idx = 0
    for t in times:
        t_current, _ = parse_key(keys[current_idx])

        while current_idx + 1 < len(keys):
            t_next, _ = parse_key(keys[current_idx + 1])
            if abs(t_current - t) < abs(t_next - t):
                break
            t_current = t_next
            current_idx += 1

        if abs(t_current - t) <= time_thresh:
            selected_keys.append(keys[current_idx])

    return selected_keys


def download_scan(
    key,
    data_dir,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    if aws_access_key_id is None and aws_secret_access_key is None:
        bucket = boto3.resource("s3", region_name="us-east-2").Bucket(
            "noaa-nexrad-level2"
        )
    else:
        bucket = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="us-east-2",
        ).Bucket("noaa-nexrad-level2")

    local_file = os.path.join(data_dir, key)
    local_dir, filename = os.path.split(local_file)
    os.makedirs(local_dir, exist_ok=True)

    if not os.path.isfile(local_file):
        bucket.download_file(key, local_file)
