from azure.storage.blob import BlobServiceClient
import os
from roosts.utils.filename_util import format_canadian_file_name
from datetime import datetime, timedelta

# IMPORTANT: Replace connection string with your storage account connection string
# Usually starts with DefaultEndpointsProtocol=https;...
MY_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=roostcanada;AccountKey=ghfT5MgxQZ6HdAWVbS2jjGu9UIVtn+/RGDRoMytla37wAInTKdx62oCx6HvBV5OwBRMz1QAvOoTS+ASt/OrSHg==;EndpointSuffix=core.windows.net"

# Replace with blob container
MY_BLOB_CONTAINER = "caset2022" #copied from Access Keys
 
# Replace with the local folder where you want files to be downloaded
LOCAL_BLOB_PATH = "./"

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
    
    key = '%04d/%02d/%02d/%04s/%04s%04d%02d%02d_%02d%02d%02d' % (
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
        t.second
    )
    
    return key

def s3_prefix(t, station=None):
    prefix = '%04d/%02d/%02d' % (t.year, t.month, t.day)
    if station is not None:
        prefix = prefix + '/%04s/%04s' % (station, station)
    return prefix

def get_station_date_scan_keys(self, start_time,
        end_time,
        station,
        stride_in_minutes=3,
        thresh_in_minutes=3,
    ):
    blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
    my_container = blob_service_client.get_container_client(MY_BLOB_CONTAINER)

    my_blobs = []
    current_time = start_time
    while current_time <= end_time:
        current_time = current_time + timedelta(days=1)
        my_blobs.extend(my_container.list_blobs(name_starts_with=current_time))

    res = [res.append(blob.name) for blob in my_blobs]
    print('keys are ', res)
    return res

class AzureBlobFileDownloader:
    def __init__(self):
        print("Intializing AzureBlobFileDownloader")
 
        # Initialize the connection to Azure storage account
        self.blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)
        self.my_container = self.blob_service_client.get_container_client(MY_BLOB_CONTAINER)
 
    def save_blob(self,file_name,file_content):
        # Get full path to the file
        download_file_path = os.path.join(LOCAL_BLOB_PATH, file_name)
 
        # for nested blobs, create local path as well!
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
 
        with open(download_file_path, "wb") as file:
            file.write(file_content)
 
    def download_all_blobs_in_container(self):
        my_blobs = self.my_container.list_blobs()
        for blob in my_blobs:
            print(blob.name)
        bytes = self.my_container.get_blob_client(blob).download_blob().readall()
        self.save_blob(format_canadian_file_name(blob.name), bytes)

    def download_scan(self,
        key,
        data_dir,
    ):
        # Download the blob to a local file
        # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory

        local_file = os.path.join(data_dir, key)
        download_file_path = os.path.join(local_file, key)
        container_client = self.blob_service_client.get_container_client(container= self.my_container) 
        print("\nDownloading blob to \n\t" + download_file_path)

        with open(file=download_file_path, mode="wb") as download_file:
            download_file.write(container_client.download_blob(key).readall())

    def get_station_date_scan_keys(self, start_time,
        end_time,
        station,
        stride_in_minutes=3,
        thresh_in_minutes=3,):
        my_blobs = self.my_container.list_blobs() #TODO: filter this.
        res = []
        return [res.append(blob.name) for blob in my_blobs]

# Initialize class and upload files
azure_blob_file_downloader = AzureBlobFileDownloader()
azure_blob_file_downloader.download_all_blobs_in_container()