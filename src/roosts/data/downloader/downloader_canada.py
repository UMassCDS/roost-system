import os

from tqdm import tqdm

from roosts.utils.azure_sa_util import download_scan

from .downloader import Downloader


class DownloaderCanada(Downloader):

    """
    This class iteratively downloads the radar scans in a certain date range from a
    certain radar station in a daily basis from Azure. Station-day is the minimum unit of
    tracking roosts.
    """

    """
        Canada station name to a four character name mapping to keep it inline with the American naming convention.
        This is done to minimize the set of changes made to the roost-system repo which expects naming in a certain format.
    """
    canada_station_map = {
        "CASET": "CSET",
        "CASSF": "CSAF",
        "CASVD": "CSVD",
    }

    def __init__(self, download_dir, npz_dir, sa_connection_str, sa_container_name):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        self.npz_dir = npz_dir
        self.sa_connection_str = sa_connection_str
        self.sa_container_name = sa_container_name

    def get_file_download_loc(self, key):
        """For each key, obtain the location for download,
            to match American data download locations
        Eg: canadian file key = '2022060109_18_ODIMH5_PVOL6S_VOL_CASET.h5'
        output = 2022/06/01/CSET/CSET20220601_091800
        """
        utc_year = key[:4]
        utc_month = key[4:6]
        utc_date = key[6:8]
        utc_station = self.canada_station_map[key[32:37]]
        utc_hour = key[8:10]
        utc_min = key[11:13]
        utc_sec = "00"  # Hardcoded as seconds information not present for Canadian data
        filename = (
            f"{utc_station}{utc_year}{utc_month}{utc_date}_{utc_hour}{utc_min}{utc_sec}"
        )
        return f"{utc_year}/{utc_month}/{utc_date}/{utc_station}/{filename}"

    def download_scans(self, keys, logger):
        """Download radar scans from Azure"""

        valid_keys = []  # list of the file path of downloaded scans
        for key in tqdm(keys, desc="Downloading"):
            file_download_loc = self.get_file_download_loc(key)
            file_download_name = file_download_loc.split("/")[-1]
            # skip if an npz file is already rendered
            if os.path.exists(os.path.join(self.npz_dir, f"{file_download_loc}.npz")):
                valid_keys.append(file_download_loc)
                continue

            try:
                download_scan(
                    key,
                    self.download_dir,
                    file_download_loc,
                    sa_connection_str=self.sa_connection_str,
                    sa_container_name=self.sa_container_name,
                )
                valid_keys.append(file_download_loc)
                logger.info("[Download Success] scan %s" % file_download_name)
            except Exception as ex:
                logger.error(
                    "[Download Failure] scan %s - %s" % (file_download_name, str(ex))
                )

        return valid_keys
