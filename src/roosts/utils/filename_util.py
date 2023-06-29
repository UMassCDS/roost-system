import os

#updating the file names to be of the format for a more generic extraction
#format: stationname_year_month_day_hour_min_sec
def format_us_file_name(current_file_name):
    station_name = current_file_name[0:4]
    year = current_file_name[4:8]
    month = current_file_name[8:10]
    date = current_file_name[10:12]
    hour = current_file_name[13:15]
    min = current_file_name[15:17]
    sec = current_file_name[17:19]

    fields = [station_name, year, month, date, hour, min, sec]

    new_file_name = '_'.join(fields)
    return new_file_name

def format_canadian_file_name(current_file_name):
    file_name_splits = current_file_name.split("_")
    utc_date_station = file_name_splits[0] + file_name_splits[1]
    station_name = os.path.splitext(file_name_splits[-1])[0]
    year = utc_date_station[0:4]
    month = utc_date_station[4:6]
    date = utc_date_station[6:8]
    hour = utc_date_station[9:11]
    min = utc_date_station[11:13]
    sec = "00"

    fields = [station_name, year, month, date, hour, min, sec]

    new_file_name = '_'.join(fields)
    return new_file_name

def format_canadian_sample_file_name(current_file_name):
    file_name_splits = current_file_name.split("_")
    utc_date_station = file_name_splits[0]
    station_name = os.path.splitext(file_name_splits[-1])[0]
    year = utc_date_station[0:4]
    month = utc_date_station[4:6]
    date = utc_date_station[6:8]
    hour = utc_date_station[9:11]
    min = utc_date_station[11:13]
    sec = "00"

    fields = [station_name, year, month, date, hour, min, sec]

    new_file_name = '_'.join(fields)
    return new_file_name

def format_file_name(file_name, data_type):
    if data_type:
        return format_canadian_file_name(file_name)
    return format_us_file_name(file_name)


def get_station_name(scan):
    return scan.split("_")[0]

def get_year(scan):
    return scan.split("_")[1]

def get_month(scan):
    return scan.split("_")[2]

def get_day(scan):
    return scan.split("_")[3]

def get_hour(scan):
    return scan.split("_")[4]

def get_min(scan):
    return scan.split("_")[5]

def get_sec(scan):
    return scan.split("_")[6]
    
