import json
import base64 as b64

from google.cloud import storage
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Model.Config import ConfigDataset

class BucketStorageClient():
    def __init__(self, config:ConfigDataset, credentials:str) -> None:
        self.UserConfig = config
        self.credentials = credentials
        self.__client:storage.Client = None

    @property
    def _client(self) -> storage.Client:
        if not self.__client:
            key = json.loads(b64.b64decode(self.credentials))
            gcp_credentials = gcpCredentials.from_service_account_info(key)        
            self.__client = storage.Client(project=self.UserConfig.GCP.Storage.ProjectID, credentials=gcp_credentials)

        return self.__client

    def get_storage_prefix(self, schedule_id:str, task_id:str) -> str:
        prefix = self.UserConfig.GCP.Storage.PrefixPath

        if prefix:
            return f"{prefix}/fabric_sync/{self.UserConfig.ID}/{schedule_id}/{task_id}"
        else:
            return f"fabric_sync/{self.UserConfig.ID}/{schedule_id}/{task_id}"

    def get_storage_path(self, schedule_id:str, task_id:str) -> str:
        uri = self.UserConfig.GCP.Storage.BucketUri
        return f"gs://{uri}/{self.get_storage_prefix(schedule_id, task_id)}"
    
    def delete_folder(self, bucket:str, folder:str):
        bucket = self._client.bucket(bucket)
        bucket.delete_blobs(blobs=list(bucket.list_blobs(prefix=folder)))