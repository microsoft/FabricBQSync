import json
import base64 as b64

from typing import Iterator, Tuple
from google.cloud import storage
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Core import ContextAwareBase

from FabricSync.BQ.Threading import QueueProcessor

class BucketStorageClient(ContextAwareBase):
    """
    Client for interacting with Google Cloud Storage buckets.
    Provides methods to manage storage paths and delete folders.
    Attributes:
        UserConfig (ConfigDataset): Configuration for the dataset.
        credentials (str): Base64 encoded service account credentials.
        __client (storage.Client): Google Cloud Storage client instance.
    """

    def __init__(self, config:ConfigDataset, credentials:str) -> None:
        """
        Initializes a new instance of the BucketStorageClient class.
        Args:
            config (ConfigDataset): Configuration for the dataset.
            credentials (str): Base64 encoded service account credentials.
        """
        self.UserConfig = config
        self.credentials = credentials
        self.__client:storage.Client = None

    @property
    def _client(self) -> storage.Client:
        """
        Gets the Google Cloud Storage client.
        If the client is not initialized, it creates a new instance using the provided credentials.
        Returns:
            storage.Client: The Google Cloud Storage client.
        """
        if not self.__client:
            key = json.loads(b64.b64decode(self.credentials))
            gcp_credentials = gcpCredentials.from_service_account_info(key)        
            self.__client = storage.Client(project=self.UserConfig.GCP.Storage.ProjectID, credentials=gcp_credentials)

        return self.__client

    def get_storage_prefix(self, schedule_id:str, task_id:str) -> str:
        """
        Constructs the storage prefix for a given schedule and task ID.
        Args:
            schedule_id (str): The ID of the schedule.
            task_id (str): The ID of the task. 
        Returns:
            str: The storage prefix.
        """
        prefix = self.UserConfig.GCP.Storage.PrefixPath

        if prefix:
            storage_prefix = f"{prefix}/fabric_sync/{self.UserConfig.ID}/{schedule_id}/{task_id}"
        else:
            storage_prefix =  f"fabric_sync/{self.UserConfig.ID}/{schedule_id}/{task_id}"
        
        self.Logger.debug(f"BUCKET STORAGE CLIENT - STORAGE PREFIX - {storage_prefix}...")

        return storage_prefix

    def get_storage_path(self, schedule_id:str, task_id:str) -> str:
        """
        Constructs the full storage path for a given schedule and task ID.
        Args:
            schedule_id (str): The ID of the schedule.
            task_id (str): The ID of the task.
        Returns:
            str: The full storage path.
        """
        uri = self.UserConfig.GCP.Storage.BucketUri
        path = f"gs://{uri}/{self.get_storage_prefix(schedule_id, task_id)}"

        self.Logger.debug(f"BUCKET STORAGE CLIENT - STORAGE PATH - {path}...")

        return path
    
    def __chunks(self, lst:list, n:int) -> Iterator[list]:
        """
        Splits a list into chunks of a specified size.
        Args:
            lst (list): The list to split into chunks.
            n (int): The size of each chunk.
        Yields:
            Iterator[list]: An iterator over the chunks of the list.
        """
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def delete_folder(self, bucket_name:str, folder:str):
        """
        Deletes a folder from a specified bucket in Google Cloud Storage.
        Args:
            bucket (str): The name of the bucket.
            folder (str): The folder path to delete.
        """
        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket_name}) - FOLDER - {folder}...")
        bucket = self._client.bucket(bucket_name)
        bucket_blobs = list(bucket.list_blobs(prefix=folder))
        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket_name}) - FOLDER - {folder} - {len(bucket_blobs)} blobs to delete...")

        chunk_size = 100
        if len(bucket_blobs) > chunk_size:
            self.Logger.warning(f"""It is not recommended to delete large numbers of files through the GCP Storage API. 
                                Consider using Object Lifecycle Management policies instead. (Num of blobs: {len(bucket_blobs)})""")

        if bucket_blobs:
            processor = QueueProcessor(self.UserConfig.Async.Parallelism)

            chunk_num = 1

            for chunk in self.__chunks(bucket_blobs, chunk_size):
                processor.put((bucket_name, folder, chunk_num, chunk))
                chunk_num += 1
        
        processor.process(self.__delete_blobs)

        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket_name}) - FOLDER - {folder} - {len(bucket_blobs)} DELETED...")
    
    def __delete_blobs(self, work_item:Tuple[str, str, int, list]) -> None:
        bucket_name, folder, chunk_num, blobs = work_item
        bucket = self._client.bucket(bucket_name)

        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket_name}) - FOLDER - {folder} - Deleting chunk {chunk_num}...")
        with self._client.batch():
            bucket.delete_blobs(blobs=blobs)