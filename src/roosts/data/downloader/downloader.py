from abc import ABC, abstractmethod


class Downloader(ABC):

    """
    Base class for download from Azure, AWS:
    iteratively downloads the radar scans in a certain date range from a
    certain radar station in a daily basis. Station-day is the minimum unit of
    tracking roosts.
    """

    @abstractmethod
    def download_scans(self, keys, logger):
        """Download radar scans from AWS/Azure"""
        pass
