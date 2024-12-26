from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import SparkSession, DataFrame
from delta.tables import *
import json
import base64 as b64
from pathlib import Path
import os
import time
import warnings
from queue import PriorityQueue
from threading import Thread, Lock

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from contextlib import ContextDecorator
from dataclasses import dataclass, field
from typing import Any, Optional

from .Enum import *
from .Metastore import FabricMetastore
from .Model.Config import *
from .Model.Query import *
from .Logging import *
from .Exceptions import *
from .Constants import SyncConstants
from .Validation import SqlValidator

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
        
class ThreadSafeList():
    def __init__(self):
        self._list = list()
        self._lock = Lock()

    def append(self, value):
        with self._lock:
            self._list.append(value)

    def pop(self):
        with self._lock:
            return self._list.pop()

    def get(self, index):
        with self._lock:
            return self._list[index]

    def length(self):
        with self._lock:
            return len(self._list)

class ThreadSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = Lock()

    def get_or_set(self, key, value):
        with self._lock:
            if key in self._dict:
                return self._dict.get(key)
            else:
                self._dict[key] = value
                return value

    def set(self, key, value):
        with self._lock:
            self._dict[key] = value

    def get(self, key):
        with self._lock:
            return self._dict.get(key)

    def remove(self, key):
        with self._lock:
            return self._dict.pop(key, None)

    def contains(self, key):
        with self._lock:
            return key in self._dict

    def items(self):
        with self._lock:
            return list(self._dict.items())

    def keys(self):
        with self._lock:
            return list(self._dict.keys())

    def values(self):
        with self._lock:
            return list(self._dict.values())


class QueueProcessor:
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.workQueue = PriorityQueue()
        self.exceptions = ThreadSafeList()
        self.exception_hook = None

    def process(self, sync_function, exception_hook=None):
        self.exception_hook = exception_hook

        for i in range(self.num_threads):
            t=Thread(target=self._task_runner, args=(sync_function, self.workQueue))
            t.name = f"{SyncConstants.THREAD_PREFIX}_{i}"
            t.daemon = True
            t.start() 
            
        self.workQueue.join()

    def _task_runner(self, sync_function, workQueue:PriorityQueue):
        while not workQueue.empty():
            value = workQueue.get()

            try:
                sync_function(value)
            except Exception as e:
                self.exceptions.append(e)  
                logging.error(msg=f"QUEUE PROCESS THREAD ERROR: {e}")

                if self.exception_hook:
                    self.exception_hook(value)
            finally:
                workQueue.task_done()

    def put(self, task):
        self.workQueue.put(item=task)
    
    def empty(self):
        return self.workQueue.empty()
    
    @property
    def has_exceptions(self):
        return self.exceptions.length() > 0
    
class BigQueryClient():
    def __init__(self, context:SparkSession, project_id:str, credentials:str):
        key = json.loads(b64.b64decode(credentials))
        bq_credentials = Credentials.from_service_account_info(key)

        self.Context = context
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id, credentials=bq_credentials)

    def read_to_dataframe(self, sql:str, schema:StructType = None):
        query = self.client.query(sql)
        bq = query.to_dataframe()
        
        if not bq.empty:
            return self.Context.createDataFrame(bq, schema=schema)
        else:
            return None

class ConfigBase():
    def __init__(self, context:SparkSession, user_config:ConfigDataset, gcp_credential:str):
        self.Context = context
        self.UserConfig = user_config
        self.GCPCredential = gcp_credential
        self.Metastore = FabricMetastore(context)
        self.Logger = SyncLogger(context).get_logger()

    def get_bq_reader_config(self, query:BQQueryModel):
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
            "project": query.ProjectId,
            "dataset": query.Dataset,
            "credentials" : self.GCPCredential,
            "viewsEnabled" : "true",
            "bigQueryJobLabel" : self.UserConfig.ID
        }
    
        if self.UserConfig.GCP.API.MaterializationProjectID:
            cfg["materializationProject"] = self.UserConfig.GCP.API.MaterializationProjectID
        else:
            cfg["materializationProject"] = query.ProjectId
        
        if self.UserConfig.GCP.API.MaterializationDataset:
            cfg["materializationDataset"] = self.UserConfig.GCP.API.MaterializationDataset
        else:
            cfg["materializationDataset"] = query.Dataset

        if self.UserConfig.GCP.API.BillingProjectID:
            cfg["parentProject"] = self.UserConfig.GCP.API.BillingProjectID
        else:
            cfg["parentProject"] = query.ProjectId

        if query.PartitionFilter:
            cfg["filter"] = query.PartitionFilter
        
        return cfg
        
    def read_bq_to_dataframe(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        try:
            if query.API == str(BigQueryAPI.STORAGE):
                df = self.read_bq_storage_to_dataframe(query)
            else:
                df = self.read_bq_standard_to_dataframe(query, schema)

            if query.Cached:
                df.cache()
        except Exception as e:
            raise BQConnectorError(msg="Read to dataframe failed.", query=query) from e
        
        return df

    def read_bq_storage_to_dataframe(self, query:BQQueryModel) -> DataFrame:
        cfg = self.get_bq_reader_config(query)

        q = query.TableName if not query.Query else query.Query

        if query.Predicate:
            q = self.build_bq_query(query)

        df = self.Context.read.format("bigquery").options(**cfg).load(q)

        return df

    def read_bq_standard_to_dataframe(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        sql_query = self.build_bq_query(query)

        bq_client = BigQueryClient(self.Context, query.ProjectId, self.GCPCredential)
        df = bq_client.read_to_dataframe(sql_query, schema)

        return df

    def build_bq_query(self, query:BQQueryModel) -> str:
        sql = query.Query if query.Query else query.TableName

        if not SqlValidator.is_valid(sql):
            sql = f"SELECT * FROM {query.TableName}"

        if query.PartitionFilter:
            query.add_predicate(query.PartitionFilter)

        if query.Predicate:
            p = [f"{p.Type} {p.Predicate}" for p in query.Predicate]
            predicates = " ".join(p)

            if not SqlValidator.has_predicate(sql):  
                idx = predicates.find(" ")          
                sql = f"{sql} WHERE {predicates[idx+1:]}"
            else:
                sql = f"{sql} {predicates}"

        return sql

class SyncBase():
    '''
    Base class for sync objects that require access to user-supplied configuration
    '''
    def __init__(self, context:SparkSession, config_path:str, clean_session:bool = False):
        """
        Init method loads the user JSON config from the supplied path.
        """
        self._logger = None

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
            
        self.UserConfig = self.load_user_config_from_json(self.ConfigPath)
        self.GCPCredential = self.load_gcp_credential()
        self.configure_session_context()
        self.Logger = SyncLogger(context).get_logger()
        self.Context.sql(f"USE {self.UserConfig.Fabric.MetadataLakehouse}")

    def configure_session_context(self):
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.application_id", str(self.UserConfig.ApplicationID))
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.name", self.UserConfig.ID)
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_path", self.UserConfig.Logging.LogPath)
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_level", str(self.UserConfig.Logging.LogLevel))
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_telemetry", str(self.UserConfig.Logging.Telemetry))
        self.Context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.telemetry_endpoint", 
            f"{self.UserConfig.Logging.TelemetryEndPoint}.azurewebsites.net")

    def load_user_config_from_json(self, config_path:str) -> Tuple[DataFrame, str]:
        """
        Loads and caches the user config json file
        """
        try:
            if not os.path.exists(config_path):
                raise ValueError("JSON User Config does not exists at the path supplied")

            config = Path(config_path).read_text()

            cfg = ConfigDataset.model_validate_json(config)
            cfg.apply_table_defaults(config)

            config_df = self.Context.read.json(self.Context.sparkContext.parallelize([cfg.model_dump_json()]))
            config_df.createOrReplaceTempView("user_config_json")
            config_df.cache()
        except Exception as e:
            raise SyncConfigurationError("Failed to load user configuration") from e

        return cfg

    def load_gcp_credential(self) -> str:
        if self.is_base64(self.UserConfig.GCP.GCPCredential.Credential):
            return self.UserConfig.GCP.GCPCredential.Credential
        else:
            try:
                file_contents = self.read_credential_file()
                return self.convert_to_base64string(file_contents)
            except Exception as e:
                raise SyncConfigurationError("Failed to GCP credential file") from e

    def read_credential_file(self) -> str:
        """
        Reads credential file from the Notebook Resource file path
        """
        credential = f"{self.UserConfig.GCP.GCPCredential.CredentialPath}"

        if not os.path.exists(credential):
           raise ValueError(f"GCP Credential file does not exists at the path supplied:{credential}")
        
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