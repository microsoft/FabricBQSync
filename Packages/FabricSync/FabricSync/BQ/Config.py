from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import DataFrame
from datetime import datetime, timezone
import json
from json import JSONEncoder
import base64 as b64
from pathlib import Path
import os

class SyncConstants:
    '''
    Class representing various string constants used through-out
    '''
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"
    MERGE = "MERGE"
    AUTO = "AUTO"
    TIME = "TIME"    
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"

    INITIAL_FULL_OVERWRITE = "INITIAL_FULL_OVERWRITE"
    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_TABLE_OPTIONS = "INFORMATION_SCHEMA.TABLE_OPTIONS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"

    SQL_TBL_SYNC_SCHEDULE = "bq_sync_schedule"
    SQL_TBL_SYNC_CONFIG = "bq_sync_configuration"
    SQL_TBL_DATA_TYPE_MAP = "bq_data_type_map"
    SQL_TBL_SYNC_SCHEDULE_TELEMETRY = "bq_sync_schedule_telemetry"

    CONFIG_JSON_TEMPLATE = """
    {
        "load_all_tables": true,
        "autodetect": true,
        "master_reset": false,
        "metadata_lakehouse": "",
        "target_lakehouse": "",
        
        "gcp_credentials": {
            "project_id": "",
            "dataset": "",
            "credential_path": "",
            "api_token": "",
            "credential": ""
        },
        
        "async": {
            "enabled": true,
            "parallelism": 5,
            "cell_timeout": 0,
            "notebook_timeout": 0
        },
        
        
        "tables": [
        {
            "load_priority": 100,
            "table_name": "",
            "enabled": true,
            "source_query": "",
            "enforce_partition_expiration": true,
            "allow_schema_evoluton": true,
            "load_strategy": "",
            "load_type": "",
            "interval": "",
            "table_maintenance":{
                "enabled": true,
                "interval": ""
            },
            "table_options": [
            {
                "key": "",
                "value": ""
            }
            ],
            "keys": [
            {
                "column": ""
            }
            ],
            "partitioned": {
                "enabled": false,
                "type" : "",
                "column": "",
                "partition_grain":""
            },
            "watermark": {
                "column": ""
            },
            "lakehouse_target": {
                "lakehouse": "",
                "table_name": ""
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

class ScheduleDAG:
    """
    Schedule DAG for Run Multiple Notebook implementation
    """
    def __init__(self, timeout:int=7200, concurrency:int=5):
        """
        Schedule DAG configuration. Maps DAG dependencies and sets paralellism concurrency for load
        """
        self.activities:list[DAGActivity] = []
        self.timeoutInSeconds:int = timeout
        self.concurrency:int = concurrency

class ScheduleDAGEncoder(JSONEncoder):
        """
        JSON Encoder for Schedule DAG
        """
        def default(self, o):
            return o.__dict__
            
class DAGActivity:
    """
    DAG Activity for Run Multiple Notebook implementation
    """
    def __init__(self, name:str, path:str, timeout:int = 3600, retry:int =  None, \
                 retryInterval:int = None, dependencies:list[str] = [], **keyword_args):
        """
        DAG activity configuration. Keyword args are used to pass notebook params
        """
        self.name = name
        self.path = path
        self.timeoutPerCellInSeconds = timeout
        self.retry = retry
        self.retryIntervalInSeconds = retryInterval
        self.dependencies = dependencies
        self.args = keyword_args

class SyncSchedule:
    """
    Scheduled configuration object that also is used to track and store telemetry from load process
    """
    EndTime:datetime = None
    SourceRows:int = 0
    DestRows:int = 0
    InsertedRows:int = 0
    UpdatedRows:int = 0
    DeltaVersion:str = None
    SparkAppId:str = None
    MaxWatermark:str = None
    Status:str = None
    FabricPartitionColumns: list[str] = None

    def __init__(self, row:Row):
        """
        Scheduled load Configuration load from Data Row
        """
        self.Row = row
        self.StartTime = datetime.now(timezone.utc)
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
        self.SourceQuery = row["source_query"]
        self.MaxWatermark = row["max_watermark"]
        self.WatermarkColumn = row["watermark_column"]
        self.IsPartitioned = row["is_partitioned"]
        self.PartitionColumn = row["partition_column"]
        self.PartitionType = row["partition_type"]
        self.PartitionGrain = row["partition_grain"]
        self.PartitionId = row["partition_id"]             
        self.Lakehouse = row["lakehouse"]
        self.DestinationTableName = row["lakehouse_table_name"]
        self.EnforcePartitionExpiration = row["enforce_partition_expiration"]
        self.AllowSchemaEvolution = row["allow_schema_evoluton"]
        self.EnableTableMaintenance = row["table_maintenance_enabled"]
        self.TableMaintenanceInterval = row["table_maintenance_interval"]
    
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
        if self.InitialLoad:
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
        return "{0}.{1}".format(self.Lakehouse, self.DestinationTableName)
        
    @property
    def BQTableName(self) -> str:
        """
        Returns the three-part BigQuery table name
        """
        return "{0}.{1}.{2}".format(self.ProjectId, self.Dataset, self.TableName)

    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        """
        Bool indicator for time ingestion tables
        """
        return self.LoadStrategy == SyncConstants.TIME_INGESTION

    @property
    def IsTimePartitionedStrategy(self) -> bool:
         """
         Bool indicator for the two time partitioned strategies
         """
         return (self.LoadStrategy == SyncConstants.PARTITION or \
                self.LoadStrategy == SyncConstants.TIME_INGESTION)
    
    def UpdateRowCounts(self, src:int, dest:int, insert:int = 0, update:int = 0):
        """
        Updates the telemetry row counts based on table configuration
        """
        self.SourceRows += src
        self.DestRows += dest

        if not self.LoadType == SyncConstants.MERGE:
            match self.LoadStrategy:
                case SyncConstants.WATERMARK:
                    self.InsertedRows += src     
                case SyncConstants.PARTITION:
                    self.InsertedRows += dest  
                case _:
                    self.InsertedRows += dest
            
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
        if config_key in json:
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
        self.LoadAllTables = super().get_json_conf_val(json_config, "load_all_tables", True)
        self.Autodetect = super().get_json_conf_val(json_config, "autodetect", True)
        self.MasterReset = super().get_json_conf_val(json_config, "master_reset", False)
        self.MetadataLakehouse = super().get_json_conf_val(json_config, "metadata_lakehouse", None)
        self.TargetLakehouse = super().get_json_conf_val(json_config, "target_lakehouse", None)
        self.Tables = []

        if "gcp_credentials" in json_config:
            self.GCPCredential = ConfigGCPCredential(
                super().get_json_conf_val(json_config["gcp_credentials"], "project_id", None),
                super().get_json_conf_val(json_config["gcp_credentials"], "dataset", None),
                super().get_json_conf_val(json_config["gcp_credentials"], "credential_path", None),
                super().get_json_conf_val(json_config["gcp_credentials"], "access_token", None),
                super().get_json_conf_val(json_config["gcp_credentials"], "credential", None)
            )
        else:
            self.GCPCredential = ConfigGCPCredential()

        if "async" in json_config:
            self.Async = ConfigAsync(
                super().get_json_conf_val(json_config["async"], "enabled", False),
                super().get_json_conf_val(json_config["async"], "parallelism", None),
                super().get_json_conf_val(json_config["async"], "notebook_timeout", None),
                super().get_json_conf_val(json_config["async"], "cell_timeout", None)
            )
        else:
            self.Async = ConfigAsync()

        if "tables" in json_config:
            for t in json_config["tables"]:
                self.Tables.append(ConfigBQTable(t))
    
    @property
    def ProjectID(self) -> str:
        """
        GCP Project ID
        """
        return self.GCPCredential.ProjectID
    
    @property
    def Dataset(self) -> str:
        """
        GCP Dataset
        """
        return self.GCPCredential.Dataset
    
    def get_table_name_list(self) -> list[str]:
        """
        Returns a list of table names from the user configuration
        """
        return [str(x.TableName) for x in self.Tables]

    def get_bq_table_fullname(self, tbl_name:str) -> str:
        """
        Returns three-part BigQuery table name
        """
        return f"{self.ProjectID}.{self.Dataset}.{tbl_name}"

    def get_lakehouse_tablename(self, lakehouse:str, tbl_name:str) -> str:
        """
        Reurns two-part Lakehouse table name
        """
        return f"{lakehouse}.{tbl_name}"

    def flatten_3part_tablename(self, tbl_name:str) -> str:
        """
        Replaces special characters in the GCP project name and returns three-part
        name with underscores
        """
        clean_project_id = self.ProjectID.replace("-", "_")
        return f"{clean_project_id}_{self.Dataset}_{tbl_name}"

class ConfigGCPCredential:
    """
    GCP Credential model
    """
    def __init__(self, project_id = None, dataset = None, path:str = None, token:str = None, credential:str = None):
        self.ProjectID = project_id
        self.Dataset = dataset
        self.CredentialPath = path
        self.AccessToken = token
        self.Credential = credential

class ConfigTableMaintenance:
    """
    User Config class for table maintenance
    """
    def __init__(self, enabled:bool = False, interval:str = None):
        self.Enabled = enabled
        self.Interval = interval

class ConfigAsync:
    """
    User Config class for parallelized async loading configuration
    """
    def __init__(self, enabled:bool = False, parallelism:int = 5, notebook_timeout:int = 1800, cell_timeout:int = 300):
        self.Enabled = enabled
        self.Parallelism = parallelism
        self.NotebookTimeout = notebook_timeout
        self.CellTimeout = cell_timeout

class ConfigTableColumn:
    """
    User Config class for Big Query Table table column mapping configuration
    """
    def __init__(self, col:str = ""):
        self.Column = col

class ConfigLakehouseTarget:
    """
    User Config class for Big Query Table Lakehouse target mapping configuration
    """
    def __init__(self, lakehouse:str = "", table:str = ""):
        self.Lakehouse = lakehouse
        self.Table = table

class ConfigPartition:
    """
    User Config class for Big Query Table partition configuration
    """
    def __init__(self, enabled:bool = False, partition_type:str = "", col:ConfigTableColumn = ConfigTableColumn(), grain:str = ""):
        self.Enabled = enabled
        self.PartitionType = partition_type
        self.PartitionColumn = col
        self.Granularity = grain

class ConfigBQTable (JSONConfigObj):
    """
    User Config class for Big Query Table mapping configuration
    """
    def __str__(self):
        return str(self.TableName)

    def __init__(self, json_config:str):
        """
        Loads from user config JSON object
        """
        super().__init__()

        self.TableName = super().get_json_conf_val(json_config, "table_name", "")
        self.Priority = super().get_json_conf_val(json_config, "priority", 100)
        self.SourceQuery = super().get_json_conf_val(json_config, "source_query", "")
        self.LoadStrategy = super().get_json_conf_val(json_config, "load_strategy" , SyncConstants.FULL)
        self.LoadType = super().get_json_conf_val(json_config, "load_type", SyncConstants.OVERWRITE)
        self.Interval =  super().get_json_conf_val(json_config, "interval", SyncConstants.AUTO)
        self.Enabled =  super().get_json_conf_val(json_config, "enabled", True)
        self.EnforcePartitionExpiration = super().get_json_conf_val(json_config, "enforce_partition_expiration", False)
        self.EnableDeletionVectors = super().get_json_conf_val(json_config, "enable_deletion_vectors", False)
        self.AllowSchemaEvolution = super().get_json_conf_val(json_config, "allow_schema_evoluton", False)
        self.TableOptions: dict[str, str] = {}

        if "lakehouse_target" in json_config:
            self.LakehouseTarget = ConfigLakehouseTarget( \
                super().get_json_conf_val(json_config["lakehouse_target"], "lakehouse", ""), \
                super().get_json_conf_val(json_config["lakehouse_target"], "table_name", ""))
        else:
            self.LakehouseTarget = ConfigLakehouseTarget()
        
        if "watermark" in json_config:
            self.Watermark = ConfigTableColumn( \
                super().get_json_conf_val(json_config["watermark"], "column", ""))
        else:
            self.Watermark = ConfigTableColumn()

        if "partitioned" in json_config:
            self.Partitioned = ConfigPartition( \
                super().get_json_conf_val(json_config["partitioned"], "enabled", False), \
                super().get_json_conf_val(json_config["partitioned"], "type", ""), \
                super().get_json_conf_val(json_config["partitioned"], "column", ""), \
                super().get_json_conf_val(json_config["partitioned"], "partition_grain", ""))
        else:
            self.Partitioned = ConfigPartition()
        
        if "table_options" in json_config:
            for o in json_config["table_options"]:
                self.TableOptions[o["key"]] = o["value"]

        if "table_maintenance" in json_config:
            self.TableMaintenance = ConfigTableMaintenance( \
                super().get_json_conf_val(json_config["table_maintenance"], "enabled", False), \
                super().get_json_conf_val(json_config["table_maintenance"], "interval", "MONTH"))
        else:
            self.TableMaintenance = ConfigTableMaintenance()

        self.Keys = []

        if "keys" in json_config:
            for c in json_config["keys"]:
                self.Keys.append(ConfigTableColumn( \
                    super().get_json_conf_val(c, "column", "")))
   
class ConfigBase():
    '''
    Base class for sync objects that require access to user-supplied configuration
    '''
    def __init__(self, context: SparkSession, spark_utils, config_path:str, force_reload_config:bool = False):
        """
        Init method loads the user JSON config from the supplied path.
        """
        if config_path is None:
            raise Exception("Missing Path to JSON User Config")

        self.__context__ = context
        self.__spark_utils__ = spark_utils
        self.ConfigPath = config_path
        self.UserConfig = None
        self.GCPCredential = None

        self.UserConfig = self.ensure_user_config(force_reload_config)

        self.GCPCredential = self.load_gcp_credential()
    
    @property
    def Context(self) -> SparkSession:
        return self.__context__
    
    @property
    def SparkUtils(self):
        return self.__spark_utils__
    
    def ensure_user_config(self, reload_config:bool) -> ConfigDataset:
        """
        Load the user JSON config if it hasn't been loaded or 
        returns the local user config as an ConfigDataset object
        """
        if (self.UserConfig is None or reload_config) and self.ConfigPath is not None:
            config = self.load_user_config(self.ConfigPath, reload_config)

            cfg = ConfigDataset(config)

            self.validate_user_config(cfg)
            
            return cfg
        else:
            return self.UserConfig
    
    def load_user_config(self, config_path:str, reload_config:bool)->str:
        """
        If the spark dataframe is not cached, loads the user config JSON to a dataframe,
        caches it, creates a temporary session view and then returns a JSON object
        """
        config_df = None

        if not self.Context.catalog.tableExists("user_config_json") or reload_config:
            if not os.path.exists(f"{self.SparkUtils.nbResPath}{config_path}"):
                raise Exception("JSON User Config does not exists at the path supplied")

            df_schema = spark.read.json(spark.sparkContext.parallelize([SyncConstants.CONFIG_JSON_TEMPLATE]))

            cfg_json = Path(f"{self.SparkUtils.nbResPath}{config_path}").read_text()

            config_df = self.Context.read.schema(df_schema.schema).json(self.Context.sparkContext.parallelize([cfg_json]))
            config_df.createOrReplaceTempView("user_config_json")
            config_df.cache()
        else:
            config_df = self.Context.table("user_config_json")
            
        return json.loads(config_df.toJSON().first())

    def validate_user_config(self, cfg:ConfigDataset) -> bool:
        """
        Validates the user config JSON to make sure all required config is supplied
        """
        if cfg is None:
            raise RuntimeError("Invalid User Config")    
        
        validation_errors = []

        if not cfg.ProjectID:
            validation_errors.append("GCP Project ID missing or empty")
        
        if not cfg.Dataset:
            validation_errors.append("GCP Dataset missing or empty")

        if not cfg.MetadataLakehouse:
            validation_errors.append("Metadata Lakehouse missing or empty")
        
        if not cfg.TargetLakehouse:
            validation_errors.append("Target Lakehouse missing or empty")

        if not cfg.GCPCredential.CredentialPath and not cfg.GCPCredential.Credential:
            validation_errors.append("GCP Credentials Path and GCP Credentials cannot both be empty")
        
        for t in cfg.Tables:
            if not t.TableName:
                validation_errors.append("Unknown table, table with missing or empty Table Name")
                continue

            if t.LoadStrategy and not t.LoadStrategy in SyncConstants.get_load_strategies():
                validation_errors.append(f"Table {t.TableName} has a missing or invalid load strategy")

            if t.LoadType and not t.LoadType in SyncConstants.get_load_types():
                validation_errors.append(f"Table {t.TableName} has a missing or invalid load type")
            
            if t.LoadStrategy == SyncConstants.WATERMARK:
                if t.Watermark is None or not t.Watermark.Column:
                    validation_errors.append(f"Table {t.TableName} is configured for Watermark but is missing the Watermark column")

    
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
        credential = f"{self.SparkUtils.nbResPath}{self.UserConfig.GCPCredential.CredentialPath}"

        if not os.path.exists(credential):
           raise Exception("GCP Credential file does not exists at the path supplied.")
        
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
        except Exception:
                return False

    def read_bq_partition_to_dataframe(self, table:str, partition_filter:str, cache_results:bool=False) -> DataFrame:
        """
        Reads a specific partition using the BigQuery spark connector.
        BigQuery does not support table decorator so the table and partition info 
        is passed using options
        """
        df = self.Context.read \
            .format("bigquery") \
            .option("parentProject", self.UserConfig.ProjectID) \
            .option("credentials", self.GCPCredential) \
            .option("viewsEnabled", "true") \
            .option("materializationDataset", self.UserConfig.Dataset) \
            .option("table", table) \
            .option("filter", partition_filter) \
            .load()
        
        if cache_results:
            df.cache()
        
        return df

    def read_bq_to_dataframe(self, query:str, cache_results:bool=False) -> DataFrame:
        """
        Reads a BigQuery table using the BigQuery spark connector
        """
        df = self.Context.read \
            .format("bigquery") \
            .option("parentProject", self.UserConfig.ProjectID) \
            .option("credentials", self.GCPCredential) \
            .option("viewsEnabled", "true") \
            .option("materializationDataset", self.UserConfig.Dataset) \
            .load(query)
        
        if cache_results:
            df.cache()
        
        return df

    def write_lakehouse_table(self, df:DataFrame, lakehouse:str, tbl_nm:str, mode:str=SyncConstants.OVERWRITE):
        """
        Write a DataFrame to the lakehouse using the Lakehouse.TableName notation
        """
        dest_table = self.UserConfig.get_lakehouse_tablename(lakehouse, tbl_nm)

        df.write \
            .mode(mode) \
            .saveAsTable(dest_table)
    
    def create_infosys_proxy_view(self, trgt:str, refresh:bool = False):
        """
        Creates a covering temporary view over top of the Big Query metadata tables
        """
        clean_nm = trgt.replace(".", "_")
        vw_nm = f"BQ_{clean_nm}"

        if not self.Context.catalog.tableExists(vw_nm) or refresh:
            tbl = self.UserConfig.flatten_3part_tablename(clean_nm)
            lakehouse_tbl = self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

            sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW {vw_nm}
            AS
            SELECT *
            FROM {lakehouse_tbl}
            """
            self.Context.sql(sql)

    def create_userconfig_tables_proxy_view(self):
        """
        Explodes the User Config table configuration into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_tables
            AS
            SELECT
                project_id, 
                dataset, 
                tbl.table_name,
                tbl.enabled,tbl.load_priority,tbl.source_query,
                tbl.load_strategy,tbl.load_type,tbl.interval,
                tbl.watermark.column as watermark_column,
                tbl.partitioned.enabled as partition_enabled,
                tbl.partitioned.type as partition_type,
                tbl.partitioned.column as partition_column,
                tbl.partitioned.partition_grain,
                tbl.lakehouse_target.lakehouse,
                tbl.lakehouse_target.table_name AS lakehouse_target_table,
                tbl.keys,
                tbl.enforce_partition_expiration AS enforce_partition_expiration,
                tbl.allow_schema_evoluton AS allow_schema_evoluton,
                tbl.table_maintenance.enabled AS table_maintenance_enabled,
                tbl.table_maintenance.interval AS table_maintenance_interval,
                tbl.table_options
            FROM (
                SELECT 
                    gcp_credentials.project_id as project_id,
                    gcp_credentials.dataset as dataset, 
                    EXPLODE(tables) AS tbl 
                FROM user_config_json)
        """
        self.Context.sql (sql)

    def create_userconfig_tables_cols_proxy_view(self):
        """
        Explodes the User Config table primary keys into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_table_keys
            AS
            SELECT
                project_id, dataset, table_name, pkeys.column
            FROM (
                SELECT
                    project_id, dataset, tbl.table_name, EXPLODE(tbl.keys) AS pkeys
                FROM (SELECT gcp_credentials.project_id as project_id,
                    gcp_credentials.dataset as dataset, 
                    EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        self.Context.sql(sql)

    def create_proxy_views(self, refresh:bool = False):
        """
        Create the user config and covering BQ information schema views
        """
        if not self.Context.catalog.tableExists("user_config_tables") or refresh:
            self.create_userconfig_tables_proxy_view()
        
        if not self.Context.catalog.tableExists("user_config_table_keys") or refresh:
            self.create_userconfig_tables_cols_proxy_view()

        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLES, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_PARTITIONS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_COLUMNS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLE_OPTIONS, refresh)