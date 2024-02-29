from datetime import datetime, timedelta
import pytz
import ephem
from roosts.utils.nexrad_util import NEXRAD_LOCATIONS


def get_days_list(start_date_str, end_date_str):
    def format_date_string(date_string):  # Input yyyymmdd
        formatted_date = datetime(
            int(date_string[:4]), int(date_string[4:6]), int(date_string[6:8]), 0, 0
        )
        return formatted_date  # datetime object indicating the beginning of the date without time zone info

    start_date = format_date_string(start_date_str)
    end_date = format_date_string(end_date_str) + timedelta(days=1)  # right exclusive
    days = []
    current_date = start_date
    while current_date < end_date:
        days.append(current_date)
        current_date += timedelta(
            days=1
        )  # no tzinfo, same wall clock time, may not be 24h
    return days


def get_sun_activity_time(
    station,
    input_daytime,  # must not have tzinfo
    sun_activity,
    silent=True,
):
    # Here's an alternative method which I don't fully understand for getting sun activity times
    # https://github.com/darkecology/cajundata/blob/master/scripts/util.py#L483

    assert sun_activity in [
        "sunrise",
        "sunset",
    ], "Unknown sun activity, must be either sunrise or sunset"

    # Make an observer
    obs = ephem.Observer()

    # Provide lat, lon and elevation of radar
    sinfo = NEXRAD_LOCATIONS[station]
    obs.lat = str(sinfo["lat"])
    obs.lon = str(sinfo["lon"])
    obs.elev = sinfo["elev"]

    # add tzinfo and convert to utc time
    obs.date = pytz.timezone(sinfo["tz"]).localize(input_daytime).astimezone(pytz.utc)

    if not silent:
        print(obs.lat, obs.lon, obs.elev)

    # Taken from the refernce link
    # To get U.S. Naval Astronomical Almanac values, use these settings
    # Using the same definition of sunrise and sunset for Canadian data
    obs.pressure = 0
    obs.horizon = "-0:34"

    sun = ephem.Sun()
    if sun_activity == "sunrise":
        return pytz.utc.localize(obs.next_rising(sun).datetime())
    else:
        return pytz.utc.localize(obs.next_setting(sun).datetime())


def scan_key_to_utc_time(scan):
    return pytz.utc.localize(
        datetime(
            int(scan[4:8]),  # year
            int(scan[8:10]),  # month
            int(scan[10:12]),  # date
            int(scan[13:15]),  # hour
            int(scan[15:17]),  # min
            int(scan[17:19]),  # sec
        )
    )


def scan_key_to_local_time(scan):
    utc_time = scan_key_to_utc_time(scan)
    local_time = utc_time.astimezone(pytz.timezone(NEXRAD_LOCATIONS[scan[:4]]["tz"]))
    return local_time.strftime("%Y%m%d_%H%M%S")
