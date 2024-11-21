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

from .Enum import *
from .Metastore import FabricMetastore
from .Config import *

class ConfigBase():
    """
    ConfigBase is a base configuration class for handling BigQuery operations using Spark.
    Methods:
        __init__(context: SparkSession, user_config, gcp_credential: str):
            Initializes the ConfigBase class with Spark context, user configuration, and GCP credentials.
        get_bq_reader_config(partition_filter: str = None):
            Generates Spark reader options required for the BigQuery Spark Connector.
        read_bq_partition_to_dataframe(table: str, partition_filter: str, cache_results: bool = False) -> DataFrame:
            Reads a specific partition from a BigQuery table using the BigQuery Spark connector.
        read_bq_to_dataframe(query: str, partition_filter: str = None, cache_results: bool = False) -> DataFrame:
            Reads a BigQuery table using the BigQuery Spark connector.
        write_lakehouse_table(df: DataFrame, lakehouse: str, tbl_nm: str, mode: LoadType = LoadType.OVERWRITE):
            Writes a DataFrame to the lakehouse using the Lakehouse.TableName notation.
    """

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
        
    def read_bq_partition_to_dataframe(self, table:str, partition_filter:str, cache_results:bool=False) -> DataFrame:
        """
        Reads a BigQuery partition into a DataFrame.
        Args:
            table (str): The name of the BigQuery table.
            partition_filter (str): The filter to apply to the partition.
            cache_results (bool, optional): Whether to cache the results. Defaults to False.
        Returns:
            DataFrame: The resulting DataFrame from the BigQuery partition.
        """
        return self.read_bq_to_dataframe(query=table, partition_filter=partition_filter, cache_results=cache_results)

    def read_bq_to_dataframe(self, query:str, partition_filter:str=None, cache_results:bool=False) -> DataFrame:
        """
        Reads data from BigQuery into a DataFrame based on the provided SQL query.
        Args:
            query (str): The SQL query to execute against BigQuery.
            partition_filter (str, optional): An optional partition filter to apply to the query. Defaults to None.
            cache_results (bool, optional): If True, caches the resulting DataFrame in memory. Defaults to False.
        Returns:
            DataFrame: The resulting DataFrame containing the data from BigQuery.
        """

        cfg = self.get_bq_reader_config(partition_filter=partition_filter)

        df = self.Context.read.format("bigquery").options(**cfg).load(query)
        
        if cache_results:
            df.cache()
        
        return df

    def write_lakehouse_table(self, df:DataFrame, lakehouse:str, tbl_nm:str, mode:LoadType=LoadType.OVERWRITE):
        """
        Writes a DataFrame to a specified lakehouse table.
        Parameters:
        df (DataFrame): The DataFrame to be written.
        lakehouse (str): The name of the lakehouse.
        tbl_nm (str): The name of the table within the lakehouse.
        mode (LoadType, optional): The mode for writing the DataFrame. Defaults to LoadType.OVERWRITE.
        Returns:
        None
        """

        dest_table = f"{lakehouse}.{tbl_nm}"

        df.write \
            .mode(str(mode)) \
            .saveAsTable(dest_table)


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
        """
        Validates if the given JSON file is properly formatted.
        Args:
            config_path (str): The path to the JSON file to be validated.
        Returns:
            bool: True if the JSON file is valid, False otherwise.
        """

        with open(config_path, 'r') as file:
            try:
                json.load(file)
            except ValueError:
                return False
            
        return True

    def get_current_config_hash(self, config_df:DataFrame) -> str:
        """
        Retrieves the current configuration hash.
        This method checks if the `ConfigMD5Hash` attribute is already set. If not, it collects the configuration
        data from the provided DataFrame and sets the `ConfigMD5Hash` attribute to the MD5 file hash from the first
        row of the DataFrame.
        Args:
            config_df (DataFrame): A DataFrame containing configuration data with an 'md5_file_hash' column.
        Returns:
            str: The MD5 hash of the current configuration.
        """
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