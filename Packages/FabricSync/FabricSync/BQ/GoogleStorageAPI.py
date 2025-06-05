import json
import base64 as b64

from google.cloud import storage
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Core import ContextAwareBase

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
    
    def delete_folder(self, bucket:str, folder:str):
        """
        Deletes a folder from a specified bucket in Google Cloud Storage.
        Args:
            bucket (str): The name of the bucket.
            folder (str): The folder path to delete.
        """
        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket}) - FOLDER - {folder}...")
        bucket = self._client.bucket(bucket)
        bucket_blobs = list(bucket.list_blobs(prefix=folder))
        bucket.delete_blobs(blobs=bucket_blobs)
        self.Logger.debug(f"BUCKET STORAGE CLIENT - DELETE - BUCKET ({bucket}) - FOLDER - {folder} - {len(bucket_blobs)} DELETED...")