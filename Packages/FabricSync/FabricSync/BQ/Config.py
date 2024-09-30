from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import DataFrame
from datetime import datetime, timezone
import json
import base64 as b64
from pathlib import Path
import os
import hashlib
from enum import Enum

from FabricSync.BQ.Metastore import *

class BigQueryObjectType(Enum):
    BASE_TABLE = "BASE_TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"

class LoadStrategy(Enum):
    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"

class PartitionType(Enum):
    TIME = "TIME"
    RANGE = "RANGE"
    TIME_INGESTION = "TIME_INGESTION"

class BQDataType(Enum):
    TIMESTAMP = "TIMESTAMP"

class LoadType(Enum):
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

class SyncStatus(Enum):
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"

class SyncConstants:
    '''
    Class representing various string constants used through-out
    '''
    DEFAULT_ID = "BQ_SYNC_LOADER"
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"
    TIMESTAMP = "TIMESTAMP"
    RANGE = "RANGE"
    
    AUTO = "AUTO"
    TIME = "TIME"    
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"

    COMPLETE = "COMPLETE"

    INITIAL_FULL_OVERWRITE = "INITIAL_FULL_OVERWRITE"

    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_TABLE_OPTIONS = "INFORMATION_SCHEMA.TABLE_OPTIONS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    INFORMATION_SCHEMA_VIEWS = "INFORMATION_SCHEMA.VIEWS"
    INFORMATION_SCHEMA_MATERIALIZED_VIEWS = "INFORMATION_SCHEMA.MATERIALIZED_VIEWS"

    CONFIG_JSON_TEMPLATE = """
    {
        "id":"",
        "load_all_tables":true,
        "load_views":false,
        "load_materialized_views":false,
        "autodetect":true,
        "fabric":{
            "workspace_id":"",
            "metadata_lakehouse":"",
            "target_lakehouse":"",
            "target_schema":"",
            "enable_schemas":false
        },
        "gcp_credentials":{
            "projects": [
                {
                    "project_id":"",
                    "datasets": [
                        {
                            "dataset":""
                        }
                    ]
                }
            ],
            "credential_path":"",
            "credential":"",
            "materialization_project_id":"",
            "materialization_dataset":"",
            "billing_project_id":""
        },        
        "async":{
            "enabled":true,
            "parallelism":5,
            "cell_timeout":0,
            "notebook_timeout":0
        },
        "tables":[
        {
            "priority":100,
            "project_id":"",
            "dataset":"",
            "table_name":"",
            "object_type":"",
            "enabled":true,
            "source_query":"",
            "enforce_partition_expiration":true,
            "allow_schema_evolution":true,
            "load_strategy":"",
            "load_type":"",
            "interval":"",
            "flatten_table":false,
            "flatten_inplace":true,
            "explode_arrays": false,
            "table_maintenance":{
                "enabled":true,
                "interval":""
            },
            "table_options":[
            {
                "key":"",
                "value":""
            }
            ],
            "keys":[
            {
                "column":""
            }
            ],
            "partitioned":{
                "enabled":false,
                "type":"",
                "column":"",
                "partition_grain":"",
                "partition_data_type":"",
                "partition_range":""
            },
            "watermark":{
                "column":""
            },
            "lakehouse_target":{
                "lakehouse":"",
                "schema":"",
                "table_name":""
            }
        }
        ]	
    }
    """

    def get_load_strategies () -> List[str]:
        return [SyncConstants.FULL, SyncConstants.PARTITION, SyncConstants.WATERMARK, SyncConstants.TIME_INGESTION]

    def get_load_types() -> List[str]:
        return [SyncConstants.OVERWRITE, SyncConstants.APPEND, SyncConstants.MERGE]

    def get_partition_types() -> List[str]:
        return [SyncConstants.TIME, SyncConstants.TIME_INGESTION]

    def get_partition_grains() -> List[str]:
        return [SyncConstants.YEAR, SyncConstants.MONTH, SyncConstants.DAY, SyncConstants.HOUR]
    
    def get_information_schema_views() -> List[str]:
        return [SyncConstants.INFORMATION_SCHEMA_TABLES, \
            SyncConstants.INFORMATION_SCHEMA_PARTITIONS, \
            SyncConstants.INFORMATION_SCHEMA_COLUMNS, \
            SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS, \
            SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE, \
            SyncConstants.INFORMATION_SCHEMA_TABLE_OPTIONS, \
            SyncConstants.INFORMATION_SCHEMA_VIEWS, \
            SyncConstants.INFORMATION_SCHEMA_MATERIALIZED_VIEWS]

class SyncSchedule:
    """
    Scheduled configuration object that also is used to track and store telemetry from load process
    """
    EndTime:datetime = None
    SourceRows:int = 0
    InsertedRows:int = 0
    UpdatedRows:int = 0
    DeltaVersion:str = None
    SparkAppId:str = None
    MaxWatermark:str = None
    Status:str = None
    FabricPartitionColumns:list[str] = None

    def __init__(self, row:Row):
        """
        Scheduled load Configuration load from Data Row
        """
        self.Row = row
        self.StartTime = datetime.now(timezone.utc)
        self.SyncId = row["sync_id"]
        self.GroupScheduleId = row["group_schedule_id"]
        self.ScheduleId = row["schedule_id"]
        self.LoadStrategy = row["load_strategy"]
        self.LoadType = row["load_type"]
        self.InitialLoad = row["initial_load"]
        self.LastScheduleLoadDate = row["last_schedule_dt"]
        self.Priority = row["priority"]
        self.ProjectId = row["project_id"]
        self.Dataset = row["dataset"]
        self.TableName = row["table_name"]
        self.ObjectType = row["object_type"]
        self.SourceQuery = row["source_query"]
        self.MaxWatermark = row["max_watermark"]
        self.WatermarkColumn = row["watermark_column"]
        self.IsPartitioned = row["is_partitioned"]
        self.PartitionColumn = row["partition_column"]
        self.PartitionType = row["partition_type"]
        self.PartitionGrain = row["partition_grain"]
        self.PartitionId = row["partition_id"]     
        self.PartitionDataType = row["partition_data_type"]   
        self.PartitionRange = row["partition_range"]     
        self.Lakehouse = row["lakehouse"]
        self.LakehouseSchema = row["lakehouse_schema"]
        self.DestinationTableName = row["lakehouse_table_name"]
        self.UseLakehouseSchema = row["use_lakehouse_schema"]
        self.EnforcePartitionExpiration = row["enforce_partition_expiration"]
        self.AllowSchemaEvolution = row["allow_schema_evolution"]
        self.EnableTableMaintenance = row["table_maintenance_enabled"]
        self.TableMaintenanceInterval = row["table_maintenance_interval"]
        self.FlattenTable = row["flatten_table"]
        self.FlattenInPlace = row["flatten_inplace"]
        self.ExplodeArrays = row["explode_arrays"]
    
    @property
    def TableOptions(self) -> dict[str, str]:
        """
        Returns the configured table options
        """
        opts = {}

        if self.Row["table_options"]:
            for r in self.Row["table_options"]:
                opts[r["key"]] = r["value"]
                
        return opts

    @property
    def SummaryLoadType(self) -> str:
        """
        Summarized the load strategy based on context
        """
        if self.InitialLoad and not self.IsTimeIngestionPartitioned and not self.IsRangePartitioned:
            return SyncConstants.INITIAL_FULL_OVERWRITE
        else:
            return "{0}_{1}".format(self.LoadStrategy, self.LoadType)
    
    @property
    def Mode(self) -> str:
        """
        Returns the write mode based on context
        """
        if self.InitialLoad:
            return SyncConstants.OVERWRITE
        else:
            return self.LoadType
    
    @property
    def Keys(self) -> list[str]:
        """
        Returns list of keys
        """        
        if self.Row["primary_keys"]:
            return [k for k in self.Row["primary_keys"]]
        else:
            return None
        
    @property
    def PrimaryKey(self) -> str:
        """
        Returns the first instance of primary key. Only used for tables with a single primary key
        """        
        if self.Row["primary_keys"]:
            return self.Row["primary_keys"][0]
        else:
            return None

    @property
    def LakehouseTableName(self) -> str:
        """
        Returns the two-part Lakehouse table name
        """
        if self.UseLakehouseSchema:
            table_nm = f"{self.Lakehouse}.{self.LakehouseSchema}.{self.DestinationTableName}"
        else:
            table_nm = f"{self.Lakehouse}.{self.DestinationTableName}"

        return table_nm

    @property
    def BQObjectType(self) -> BigQueryObjectType:
        """
        Returns the type of BigQuery object (table, view, etc)
        """
        return BigQueryObjectType[self.BQObjectType.upper()]

    @property
    def BQTableName(self) -> str:
        """
        Returns the three-part BigQuery table name
        """
        return f"{self.ProjectId}.{self.Dataset}.{self.TableName}"


    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        """
        Bool indicator for time ingestion tables
        """
        return self.LoadStrategy == SyncConstants.TIME_INGESTION

    @property
    def IsRangePartitioned(self) -> bool:
        """
        Bol indicator for range partitioned tables
        """
        return self.LoadStrategy == SyncConstants.PARTITION and self.PartitionType == SyncConstants.RANGE
    
    @property
    def IsTimePartitionedStrategy(self) -> bool:
         """
         Bool indicator for the two time partitioned strategies
         """
         return ((self.LoadStrategy == SyncConstants.PARTITION and 
            self.PartitionType == SyncConstants.TIME) or self.LoadStrategy == SyncConstants.TIME_INGESTION)
    
    def UpdateRowCounts(self, src:int, insert:int = 0, update:int = 0):
        """
        Updates the telemetry row counts based on table configuration
        """
        self.SourceRows += src

        if not self.LoadType == SyncConstants.MERGE:
            self.InsertedRows += src            
            self.UpdatedRows = 0
        else:
            self.InsertedRows += insert
            self.UpdatedRows += update

class JSONConfigObj:
    """
    Base object with JSON helper methods
    """
    def get_json_conf_val(self, json:str, config_key:str, default_val = None):
        """
        Extracts a value from the user config JSON doc by key. If it doesn't
        exist the default value is returned
        """
        if json and config_key in json:
            return json[config_key]
        else:
            return default_val
        
class ConfigDataset(JSONConfigObj):
    """
    User Config class for Big Query project/dataset configuration
    """
    def __init__(self, json_config:str):
        """
        Loads from use config JSON
        """
        super().__init__()
        self.ID  = super().get_json_conf_val(json_config, "id", SyncConstants.DEFAULT_ID)
        self.LoadAllTables = super().get_json_conf_val(json_config, "load_all_tables", True)
        self.LoadViews = super().get_json_conf_val(json_config, "load_views", False)
        self.LoadMaterializedViews = super().get_json_conf_val(json_config, "load_materialized_views", False)
        self.Autodetect = super().get_json_conf_val(json_config, "autodetect", True)

        self.Tables = []

        if "fabric" in json_config:
            self.Fabric = ConfigFabric(json_config["fabric"])
        else:
            self.Fabric = ConfigFabric()
        
        if "gcp_credentials" in json_config:
            self.GCPCredential = ConfigGCPCredential(json_config["gcp_credentials"])
        else:
            self.GCPCredential= ConfigGCPCredential()

        if "async" in json_config:
            self.Async = ConfigAsync(json_config["async"])
        else:
            self.Async = ConfigAsync()

        if "tables" in json_config:
            for t in json_config["tables"]:
                self.Tables.append(ConfigBQTable(t))
    
    def get_table_name_list(self, project:str, dataset:str, obj_type:BigQueryObjectType, only_enabled:bool = False) -> list[str]:
        """
        Returns a list of table names from the user configuration
        """
        tables = [t for t in self.Tables \
            if t.ProjectID == project and t.Dataset == dataset and t.ObjectType == obj_type.name]

        if only_enabled:
            tables = [t for t in tables if t.Enabled == True]

        return [str(x.TableName) for x in tables]

class ConfigFabric(JSONConfigObj):
    """
    Fabric model
    """
    def __init__(self, json_config:str = None):
        """
        Loads from Fabric config JSON object
        """
        super().__init__()

        self.WorkspaceID = super().get_json_conf_val(json_config, "workspace_id", None)
        self.MetadataLakehouse = super().get_json_conf_val(json_config, "metadata_lakehouse", None)
        self.TargetLakehouse = super().get_json_conf_val(json_config, "target_lakehouse", None)
        self.TargetLakehouseSchema = super().get_json_conf_val(json_config, "target_schema", None)
        self.EnableSchemas = super().get_json_conf_val(json_config, "enable_schemas", None)
                
class ConfigGCPProject(JSONConfigObj):
    """
    GCP Billing Project Model
    """
    def __init__(self, json_config:str):
        """
        Loads from GCP project id config JSON object
        """
        super().__init__()

        self.ProjectID = super().get_json_conf_val(json_config, "project_id", None)
        self.Datasets = []

        if json_config and "datasets" in json_config:
            for d in json_config["datasets"]:
                self.Datasets.append(ConfigGCPDataset(d))

class ConfigGCPDataset(JSONConfigObj):
    """
    GCP Dataset model
    """
    def __init__(self, json_config:str = None):
        """
        Loads from GCP dataset config JSON object
        """
        super().__init__()
        self.Dataset = super().get_json_conf_val(json_config, "dataset", None)

class ConfigGCPCredential(JSONConfigObj):
    """
    GCP Credential model
    """
    def __init__(self, json_config:str = None):
        """
        Loads from GCP credential config JSON object
        """
        super().__init__()

        self.CredentialPath = super().get_json_conf_val(json_config, "credential_path", None)
        self.AccessToken = super().get_json_conf_val(json_config, "access_token", None)
        self.Credential = super().get_json_conf_val(json_config, "credential", None)
        self.MaterializationProjectID = super().get_json_conf_val(json_config, "materialization_project_id", None)
        self.MaterializationDataset = super().get_json_conf_val(json_config, "materialization_dataset", None)
        self.BillingProjectID = super().get_json_conf_val(json_config, "billing_project_id", None)
        self.Projects = []

        if json_config and "projects" in json_config:
            for p in json_config["projects"]:
                self.Projects.append(ConfigGCPProject(p))                

class ConfigTableMaintenance(JSONConfigObj):
    """
    User Config class for table maintenance
    """
    def __init__(self, json_config:str = None):
        """
        Loads from table maintenance config JSON object
        """
        self.Enabled = super().get_json_conf_val(json_config, "enabled", False)
        self.Interval = super().get_json_conf_val(json_config, "interval", "MONTH")
                

class ConfigAsync(JSONConfigObj):
    """
    User Config class for parallelized async loading configuration
    """
    def __init__(self, json_config:str = None):
        """
        Loads from async config JSON object
        """
        super().__init__()
        self.Enabled = super().get_json_conf_val(json_config, "enabled", False)
        self.Parallelism = super().get_json_conf_val(json_config, "parallelism", 5)
        self.NotebookTimeout = super().get_json_conf_val(json_config, "notebook_timeout", 1800)
        self.CellTimeout = super().get_json_conf_val(json_config, "cell_timeout", 300)

class ConfigTableColumn:
    """
    User Config class for Big Query Table table column mapping configuration
    """
    def __init__(self, col:str = ""):
        self.Column = col

class ConfigLakehouseTarget(JSONConfigObj):
    """
    User Config class for Big Query Table Lakehouse target mapping configuration
    """
    def __init__(self, json_config:str = None):
        """
        Loads from lakehouse target config JSON object
        """
        super().__init__()

        self.Lakehouse = super().get_json_conf_val(json_config, "lakehouse", "")
        self.Schema = super().get_json_conf_val(json_config, "schema", "")
        self.Table = super().get_json_conf_val(json_config, "table_name", "")
                

class ConfigPartition(JSONConfigObj):
    """
    User Config class for Big Query Table partition configuration
    """
    def __init__(self, json_config:str = None):
        """
        Loads from user config JSON object
        """
        super().__init__()

        self.Enabled = super().get_json_conf_val(json_config, "enabled", False)
        self.PartitionType = super().get_json_conf_val(json_config, "type", "")
        self.PartitionColumn = super().get_json_conf_val(json_config, "column", "")
        self.Granularity = super().get_json_conf_val(json_config, "partition_grain", "")
        self.PartitionDataType = super().get_json_conf_val(json_config, "partition_data_type", "")
        self.PartitionRange = super().get_json_conf_val(json_config, "partition_range", "")

class ConfigBQTable (JSONConfigObj):
    """
    User Config class for Big Query Table mapping configuration
    """
    def __str__(self):
        return str(self.TableName)

    def __init__(self, json_config:str = None):
        """
        Loads from user config JSON object
        """
        super().__init__()

        self.ProjectID = super().get_json_conf_val(json_config, "project_id", "")
        self.Dataset = super().get_json_conf_val(json_config, "dataset", "")
        self.TableName = super().get_json_conf_val(json_config, "table_name", "")
        self.ObjectType  = super().get_json_conf_val(json_config, "object_type", "BASE_TABLE")
        self.Priority = super().get_json_conf_val(json_config, "priority", 100)
        self.SourceQuery = super().get_json_conf_val(json_config, "source_query", "")
        self.LoadStrategy = super().get_json_conf_val(json_config, "load_strategy" , SyncConstants.FULL)
        self.LoadType = super().get_json_conf_val(json_config, "load_type", SyncConstants.OVERWRITE)
        self.Interval =  super().get_json_conf_val(json_config, "interval", SyncConstants.AUTO)
        self.Enabled =  super().get_json_conf_val(json_config, "enabled", True)
        self.EnforcePartitionExpiration = super().get_json_conf_val(json_config, "enforce_partition_expiration", False)
        self.EnableDeletionVectors = super().get_json_conf_val(json_config, "enable_deletion_vectors", False)
        self.AllowSchemaEvolution = super().get_json_conf_val(json_config, "allow_schema_evolution", False)
        self.FlattenTable = super().get_json_conf_val(json_config, "flatten_table", False)
        self.FlattenInPlace = super().get_json_conf_val(json_config, "flatten_inplace", True)
        self.ExplodeArrays = super().get_json_conf_val(json_config, "explode_arrays", True)
        
        self.TableOptions:dict[str, str] = {}

        if "lakehouse_target" in json_config:
            self.LakehouseTarget = ConfigLakehouseTarget(json_config["lakehouse_target"])
        else:
            self.LakehouseTarget = ConfigLakehouseTarget()
        
        if "watermark" in json_config:
            self.Watermark = ConfigTableColumn(
                super().get_json_conf_val(json_config["watermark"], "column", ""))
        else:
            self.Watermark = ConfigTableColumn()

        if "partitioned" in json_config:
            self.Partitioned = ConfigPartition(json_config["partitioned"])
        else:
            self.Partitioned = ConfigPartition()
        
        if "table_options" in json_config:
            for o in json_config["table_options"]:
                self.TableOptions[o["key"]] = o["value"]

        if "table_maintenance" in json_config:
            self.TableMaintenance = ConfigTableMaintenance(json_config["table_maintenance"])
        else:
            self.TableMaintenance = ConfigTableMaintenance()

        self.Keys = []

        if "keys" in json_config:
            for c in json_config["keys"]:
                self.Keys.append(ConfigTableColumn(
                    super().get_json_conf_val(c, "column", "")))

class ConfigBase():
    '''
    Base class for sync objects that require access to user-supplied configuration
    '''
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Init method loads the common config base class
        """
        self.Context = context
        self.UserConfig = user_config
        self.GCPCredential = gcp_credential
        self.Metastore = FabricMetastore(context)
    
    def get_bq_reader_config(self, partition_filter:str = None):
        """
        Spark Reader options required for the BigQuery Spark Connector
        --parentProject - billing project id for the API transaction costs, defaults to service account project id if not specified
        --credentials - gcp service account credentials
        --viewEnabled - required to be true when reading queries, views or information schema
        --materializationProject - billing project id where the views will be materialized out to temp tables for storage api
        --materializationDataset - dataset where views will be materialized out to temp tables for storage api
        --filter - required for tables that have mandatory partition filters or when reading table partitions
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
        Reads a specific partition using the BigQuery spark connector.
        BigQuery does not support table decorator so the table and partition info 
        is passed using options
        """        
        return self.read_bq_to_dataframe(query=table, partition_filter=partition_filter, cache_results=cache_results)

    def read_bq_to_dataframe(self, query:str, partition_filter:str=None, cache_results:bool=False) -> DataFrame:
        """
        Reads a BigQuery table using the BigQuery spark connector
        """
        cfg = self.get_bq_reader_config(partition_filter=partition_filter)

        df = self.Context.read.format("bigquery").options(**cfg).load(query)
        
        if cache_results:
            df.cache()
        
        return df

    def write_lakehouse_table(self, df:DataFrame, lakehouse:str, tbl_nm:str, mode:str="OVERWRITE"):
        """
        Write a DataFrame to the lakehouse using the Lakehouse.TableName notation
        """
        dest_table = f"{lakehouse}.{tbl_nm}"

        df.write \
            .mode(mode) \
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