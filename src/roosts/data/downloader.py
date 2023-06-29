import os
from roosts.utils.s3_util import download_scan
from roosts.utils.azure_blob_util import AzureBlobFileDownloader
from tqdm import tqdm
from roosts.utils.s3_util import get_station_day_scan_keys


class Downloader:

    """ 
        This class iteratively downloads the radar scans in a certain date range from a certain radar station
        in a daily basis. Station-day is the minimum unit of tracking roosts.
    """

    def __init__(self, download_dir, npz_dir, aws_access_key_id=None, aws_secret_access_key=None, canadian_data=False):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)
        self.npz_dir = npz_dir
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.canadian_data = canadian_data

        self.azure_download_util = AzureBlobFileDownloader()

    def download_scans(self, keys, logger):
        if self.canadian_data:
            return self.download_scans_canada(self, keys, logger)
        else:
            return self.download_scans_us(self, keys, logger)

    def download_scans_us(self, keys, logger):
        """ Download radar scans from AWS """

        valid_keys = [] # list of the file path of downloaded scans
        for key in tqdm(keys, desc="Downloading"):
            # skip if an npz file is already rendered
            if os.path.exists(os.path.join(self.npz_dir, f"{os.path.splitext(key)[0]}.npz")):
                valid_keys.append(key)
                continue

            try:
                download_scan(
                    key,
                    self.download_dir,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                )
                valid_keys.append(key)
                logger.info('[Download Success] scan %s' % key.split("/")[-1])
            except Exception as ex:
                logger.error('[Download Failure] scan %s - %s' % (key.split("/")[-1], str(ex)))

        return valid_keys

    def download_scans_canada(self, keys, logger):
        """ Download radar scans from Azure """
        print("Downloading scans from azure!")
        valid_keys = [] # list of the file path of downloaded scans
        for key in tqdm(keys, desc="Downloading"):
            # skip if an npz file is already rendered
            if os.path.exists(os.path.join(self.npz_dir, f"{os.path.splitext(key)[0]}.npz")):
                valid_keys.append(key)
                continue

            try:
                self.azure_download_util.download_scan(
                    key,
                    self.download_dir,
                )
                valid_keys.append(key)
                logger.info('[Download Success] scan %s' % key.split("/")[-1])
            except Exception as ex:
                logger.error('[Download Failure] scan %s - %s' % (key.split("/")[-1], str(ex)))

        return valid_keys