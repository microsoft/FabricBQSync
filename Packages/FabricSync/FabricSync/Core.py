from abc import ABC, abstractmethod
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import DataFrame
import json
import base64 as b64
from pathlib import Path
import os
import hashlib
import time
import functools
import warnings
import sqlvalidator
import traceback

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from contextlib import ContextDecorator
from dataclasses import dataclass, field
from typing import Any, Callable, ClassVar, Dict, Optional

from .Enum import *
from .Metastore import FabricMetastore
from .Config import *

def ignore_warnings(category: Warning):
    def ignore_warnings_decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=category)
                return func(*args, **kwargs)
        return wrapper
    return ignore_warnings_decorator

class SyncTimerError(Exception):
    """A custom exception for Sync Timer class"""

@dataclass
class SyncTimer(ContextDecorator):
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._start_time is not None:
            raise SyncTimerError(f"Timer is already running.")

        self._start_time = time.perf_counter()

    def stop(self) -> float:
        if self._start_time is None:
            raise SyncTimerError(f"Timer is not running.")

        self.elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

        return self.elapsed_time

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()
    
    def __str__(self):
        if self.elapsed_time:
            return f"{(self.elapsed_time/60):.4f} mins"
        else:
            return None

warnings.filterwarnings('ignore', category=UserWarning)

class BigQueryClient():
    def __init__(self, context:SparkSession, project_id:str, credentials:str):
        key = json.loads(b64.b64decode(credentials))
        bq_credentials = Credentials.from_service_account_info(key)

        self.Context = context
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id, credentials=bq_credentials)
    
    @ignore_warnings(category=UserWarning)
    def read_to_dataframe(self, query:str):
        query = self.client.query(query)
        bq = query.to_dataframe()
        
        df = None

        if not bq.empty:
            df = self.Context.createDataFrame(bq)

        return df

class ConfigBase():
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Initializes the Config class with the given parameters.
        Args:
            context (SparkSession): The Spark session context.
            user_config: The user configuration settings.
            gcp_credential (str): The Google Cloud Platform credential string.
        """

        self.Context = context
        self.UserConfig = user_config
        self.GCPCredential = gcp_credential
        self.Metastore = FabricMetastore(context)
    
    def get_bq_reader_config(self, partition_filter:str = None):
        """
        Returns the configuration dictionary for the BigQuery Spark Connector.
        Parameters:
            partition_filter (str, optional): Filter for tables that have mandatory partition filters or when reading table partitions.
        Returns:
            dict: Configuration dictionary containing Spark Reader options for the BigQuery Spark Connector, including:
                - credentials: GCP service account credentials.
                - viewsEnabled: Set to "true" to enable reading queries, views, or information schema.
                - materializationProject (optional): Billing project ID where views will be materialized to temporary tables for storage API.
                - materializationDataset (optional): Dataset where views will be materialized to temporary tables for storage API.
                - parentProject (optional): Billing project ID for API transaction costs, defaults to service account project ID if not specified.
                - filter (optional): Filter for tables that have mandatory partition filters or when reading table partitions.
        """
        cfg = {
            "credentials" : self.GCPCredential,
            "viewsEnabled" : "true"
        }
    
        if self.UserConfig.GCPCredential.MaterializationProjectID:
            cfg["materializationProject"] = self.UserConfig.GCPCredential.MaterializationProjectID
        
        if self.UserConfig.GCPCredential.MaterializationDataset:
            cfg["materializationDataset"] = self.UserConfig.GCPCredential.MaterializationDataset

        if self.UserConfig.GCPCredential.BillingProjectID:
            cfg["parentProject"] = self.UserConfig.GCPCredential.BillingProjectID

        if partition_filter:
            cfg["filter"] = partition_filter
        
        return cfg
        
    def read_bq_partition_to_dataframe(self, project_id: str, table:str, partition_filter:str, cache_results:bool=True, api:BigQueryAPI=BigQueryAPI.STORAGE) -> DataFrame:
        return self.read_bq_to_dataframe(project_id=project_id, query=table, partition_filter=partition_filter, cache_results=cache_results, api=api)

    def read_bq_to_dataframe(self, project_id: str, query:str, partition_filter:str=None, cache_results:bool=True, api:BigQueryAPI=BigQueryAPI.STORAGE) -> DataFrame:
        if api == BigQueryAPI.STORAGE:
            df = self.read_bq_storage_to_dataframe(query, partition_filter)
        else:
            df = self.read_bq_standard_to_dataframe(project_id, query, partition_filter)
        
        if cache_results:
            df.cache()
        
        return df

    def read_bq_storage_to_dataframe(self, query:str, partition_filter:str=None) -> DataFrame:
        cfg = self.get_bq_reader_config(partition_filter=partition_filter)
        df = self.Context.read.format("bigquery").options(**cfg).load(query)

        return df

    def read_bq_standard_to_dataframe(self, project_id: str, query:str, partition_filter:str=None) -> DataFrame:
        sql_query = self.build_bq_query(query, partition_filter)

        bq_client = BigQueryClient(self.Context, project_id, self.GCPCredential)
        df = bq_client.read_to_dataframe(query)

        return df

    def build_bq_query(self, query:str, partition_filter:str=None) -> str:
        if self.is_sql_query(query):
            sql = query
        else:
            sql = f"SELECT * FROM {query}"

        if partition_filter:
            q = sqlvalidator.parse(sql)

            if q.sql_query and q.sql_query.where_clause:
                sql = " ".join([sql, f"AND {partition_filter}"])
            else:
                sql = " ".join([sql, f"WHERE {partition_filter}"])
        
        return sql

    def is_sql_query(self, query:str):
        sql_query = sqlvalidator.parse(query)
        return sql_query.is_valid()


class SyncBase():
    '''
    Base class for sync objects that require access to user-supplied configuration
    '''
    def __init__(self, context:SparkSession, config_path:str, clean_session:bool = False):
        """
        Init method loads the user JSON config from the supplied path.
        """
        if config_path is None:
            raise Exception("Missing Path to JSON User Config")

        self.Context = context
        self.ConfigPath = config_path
        self.UserConfig = None
        self.GCPCredential = None
        self.ConfigMD5Hash = None

        self.Metastore = FabricMetastore(context)

        if clean_session:
            self.Metastore.cleanup_session()
            
        self.UserConfig = self.ensure_user_config()
        self.GCPCredential = self.load_gcp_credential()
        self.Context.sql(f"USE {self.UserConfig.Fabric.MetadataLakehouse}")
    
    def ensure_user_config(self) -> ConfigDataset:
        """
        Load the user JSON config if it hasn't been loaded or 
        returns the local user config as an ConfigDataset object
        """
        if self.UserConfig is None and self.ConfigPath is not None:
            config = self.load_user_config(self.ConfigPath)
            cfg = ConfigDataset(config)

            self.validate_user_config(cfg)
            
            return cfg
        else:
            return self.UserConfig
    
    def generate_md5_file_hash(self, file_path:str):
        """
        Generates a md5 hash for a file. Used to detect user-config changes to trigger configuration reloads
        """
        if not os.path.exists(file_path):
            raise Exception("MD5 File Hash Failed: File path provided does not exists.")

        md5 = hashlib.md5()
        BUF_SIZE = 65536  # 64KB

        with open(file_path, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)

        return md5.hexdigest()
    

    def validate_json_file(self, config_path:str) -> bool:
        with open(config_path, 'r') as file:
            try:
                data = json.load(file, strict=False)
            except ValueError as err:
                print(traceback.format_exc())
                return False
            
        return True

    def get_current_config_hash(self, config_df:DataFrame) -> str:
        if not self.ConfigMD5Hash:
            cfg = [c for c in config_df.collect()]

            if cfg:
                self.ConfigMD5Hash = cfg[0]["md5_file_hash"]
        
        return self.ConfigMD5Hash

    def load_user_config(self, config_path:str)->str:
        """
        If the spark dataframe is not cached, loads the user config JSON to a dataframe,
        caches it, creates a temporary session view and then returns a JSON object
        """
        config_df = None

        if not self.Context.catalog.tableExists("user_config_json"):
            config_df = self.load_user_config_from_json(config_path)
        else:
            config_df = self.Context.table("user_config_json")

            file_hash = self.generate_md5_file_hash(config_path)
            current_hash = self.get_current_config_hash(config_df)

            if file_hash != current_hash:
                config_df = self.load_user_config_from_json(config_path)
            
        return json.loads(config_df.toJSON().first())

    def load_user_config_from_json(self, config_path:str) -> DataFrame:
        """
        Loads and caches the user config json file
        """
        if not os.path.exists(config_path):
            raise Exception("JSON User Config does not exists at the path supplied")

        if not self.validate_json_file(config_path):
            raise Exception("Invalid JSON User Config: JSON is not well-formed")

        self.ConfigMD5Hash = self.generate_md5_file_hash(config_path)
        df_schema = self.Context.read.json(self.Context.sparkContext.parallelize([SyncConstants.CONFIG_JSON_TEMPLATE]))

        cfg_json = Path(config_path).read_text()

        config_df = self.Context.read.schema(df_schema.schema).json(self.Context.sparkContext.parallelize([cfg_json]))
        config_df = config_df.withColumn("md5_file_hash", lit(self.ConfigMD5Hash))
        config_df.createOrReplaceTempView("user_config_json")
        config_df.cache()

        return config_df

    def validate_user_config(self, cfg:ConfigDataset) -> bool:
        """
        Validates the user config JSON to make sure all required config is supplied
        """
        if cfg is None:
            raise RuntimeError("Invalid User Config")    
        
        validation_errors = []

        if not validation_errors and len(validation_errors) > 0:
            config_errors = "\r\n".join(validation_errors)
            raise ValueError(f"Errors in User Config JSON File:\r\n{config_errors}")
        
        return True

    def load_gcp_credential(self) -> str:
        """
        GCP credentials can be supplied as a base64 encoded string or as a path to 
        the GCP service account JSON credentials. If a path is supplied, the JSON file 
        is loaded and the contents serialized to a base64 string
        """
        cred = None

        if self.is_base64(self.UserConfig.GCPCredential.Credential):
            cred = self.UserConfig.GCPCredential.Credential
        else:
            file_contents = self.read_credential_file()
            cred = self.convert_to_base64string(file_contents)
            
        return cred

    def read_credential_file(self) -> str:
        """
        Reads credential file from the Notebook Resource file path
        """
        credential = f"{self.UserConfig.GCPCredential.CredentialPath}"

        if not os.path.exists(credential):
           raise Exception(f"GCP Credential file does not exists at the path supplied:{credential}")
        
        txt = Path(credential).read_text()
        txt = txt.replace("\n", "").replace("\r", "")

        return txt

    def convert_to_base64string(self, credential_val:str) -> str:
        """
        Converts string to base64 encoding, returns ascii value of bytes
        """
        credential_val_bytes = credential_val.encode("ascii") 
        
        base64_bytes = b64.b64encode(credential_val_bytes) 
        base64_string = base64_bytes.decode("ascii") 

        return base64_string

    def is_base64(self, val:str) -> str:
        """
        Evaluates a string to determine if its base64 encoded
        """
        try:
            if isinstance(val, str):
                sb_bytes = bytes(val, 'ascii')
            elif isinstance(val, bytes):
                sb_bytes = val
            else:
                raise ValueError("Argument must be string or bytes")

            return b64.b64encode(b64.b64decode(sb_bytes)) == sb_bytes
        except Exception as e:
            return False

class SourceMetadata(ConfigBase):
    @abstractmethod
    def sync_metadata(self):
        pass

    @abstractmethod
    def auto_detect_config(self):
        pass

    @abstractmethod
    def create_proxy_views(self):
        pass

class ScheduleBuilder(ConfigBase):
    @abstractmethod
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = str(ScheduleType.AUTO)) -> str:
        pass

class SourceSync(ConfigBase):
    @abstractmethod
    def run_schedule(self, group_schedule_id:str, optimize_metadata:bool=True):
        pass