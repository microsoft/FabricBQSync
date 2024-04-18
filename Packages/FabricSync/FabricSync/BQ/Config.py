from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import DataFrame
from datetime import datetime, timezone
import json
from json import JSONEncoder
import base64
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
    AUTO = "AUTO"
    TIME = "TIME"
    INITIAL_FULL_OVERWRITE = "INITIAL_FULL_OVERWRITE"
    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"

    SQL_TBL_SYNC_SCHEDULE = "bq_sync_schedule"
    SQL_TBL_SYNC_CONFIG = "bq_sync_configuration"
    SQL_TBL_DATA_TYPE_MAP = "bq_data_type_map"
    SQL_TBL_SYNC_SCHEDULE_TELEMETRY = "bq_sync_schedule_telemetry"

class ScheduleDAG:
    """
    Schedule DAG for Run Multiple Notebook implementation
    """
    def __init__(
            self, 
            timeout:int=7200, 
            concurrency:int=5):
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
    def __init__(
            self, 
            name:str, 
            path:str, 
            timeout:int = 3600, 
            retry:int =  None, 
            retryInterval:int = None, 
            dependencies:list[str] = [], 
            **keyword_args):
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

    def __init__(
                self, 
                row: Row):
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
        self.ProjectId = row["project_id"]
        self.Dataset = row["dataset"]
        self.TableName = row["table_name"]
        self.SourceQuery = row["source_query"]
        self.MaxWatermark = row["max_watermark"]
        self.IsPartitioned = row["is_partitioned"]
        self.PartitionColumn = row["partition_column"]
        self.PartitionType = row["partition_type"]
        self.PartitionGrain = row["partition_grain"]
        self.WatermarkColumn = row["watermark_column"]
        self.LastScheduleLoadDate = row["last_schedule_dt"]
        self.Lakehouse = row["lakehouse"]
        self.DestinationTableName = row["lakehouse_table_name"]
        self.PartitionId = row["partition_id"]
    
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


    def UpdateRowCounts(
            self, 
            src:int, 
            dest:int, 
            insert:int, 
            update:int):
        """
        Updates the telemetry row counts based on table configuration
        """
        self.SourceRows += src
        self.DestRows += dest

        match self.LoadStrategy:
            case SyncConstants.WATERMARK:
                self.InsertedRows += src     
            case SyncConstants.PARTITION:
                self.InsertedRows += dest  
            case _:
                self.InsertedRows += dest

        self.UpdatedRows = 0

class ConfigDataset:
    """
    User Config class for Big Query project/dataset configuration
    """
    def __init__(
            self, 
            json_config:str):
        """
        Loads from use config JSON
        """
        self.ProjectID = self.get_json_conf_val(json_config, "project_id", None)
        self.Dataset = self.get_json_conf_val(json_config, "dataset", None)
        self.LoadAllTables = self.get_json_conf_val(json_config, "load_all_tables", True)
        self.Autodetect = self.get_json_conf_val(json_config, "autodetect", True)
        self.MasterReset = self.get_json_conf_val(json_config, "master_reset", False)
        self.MetadataLakehouse = self.get_json_conf_val(json_config, "metadata_lakehouse", None)
        self.TargetLakehouse = self.get_json_conf_val(json_config, "target_lakehouse", None)
        self.GCPCredentialPath = self.get_json_conf_val(json_config, "gcp_credential_path", None)
        self.Tables = []

        if "async" in json_config:
            self.Async = ConfigAsync(
                self.get_json_conf_val(json_config["async"], "enabled", False),
                self.get_json_conf_val(json_config["async"], "parallelism", None),
                self.get_json_conf_val(json_config["async"], "notebook_timeout", None),
                self.get_json_conf_val(json_config["async"], "cell_timeout", None)
            )
        else:
            self.Async = ConfigAsync()

        if "tables" in json_config:
            for t in json_config["tables"]:
                self.Tables.append(ConfigBQTable(t))

    def get_table_name_list(self) -> list[str]:
        """
        Returns a list of table names from the user configuration
        """
        return [str(x.TableName) for x in self.Tables]

    def get_bq_table_fullname(
            self, 
            tbl_name:str) -> str:
        """
        Returns three-part BigQuery table name
        """
        return f"{self.ProjectID}.{self.Dataset}.{tbl_name}"

    def get_lakehouse_tablename(
            self, 
            lakehouse:str, 
            tbl_name:str) -> str:
        """
        Reurns two-part Lakehouse table name
        """
        return f"{lakehouse}.{tbl_name}"

    def flatten_3part_tablename(
            self, 
            tbl_name:str) -> str:
        """
        Replaces special characters in the GCP project name and returns three-part
        name with underscores
        """
        clean_project_id = self.ProjectID.replace("-", "_")
        return f"{clean_project_id}_{self.Dataset}_{tbl_name}"
    
    def get_json_conf_val(
            self, 
            json:str, 
            config_key:str, 
            default_val = None):
        """
        Extracts a value from the user config JSON doc by key. If it doesn't
        exist the default value is returned
        """
        if config_key in json:
            return json[config_key]
        else:
            return default_val

class ConfigAsync:
    """
    User Config class for parallelized async loading configuration
    """
    def __init__(
            self, 
            enabled:bool = False, 
            parallelism:int = 5, 
            notebook_timeout:int = 1800, 
            cell_timeout:int = 300):
        self.Enabled = enabled
        self.Parallelism = parallelism
        self.NotebookTimeout = notebook_timeout
        self.CellTimeout = cell_timeout

class ConfigTableColumn:
    """
    User Config class for Big Query Table table column mapping configuration
    """
    def __init__(
            self, 
            col:str = ""):
        self.Column = col

class ConfigLakehouseTarget:
    """
    User Config class for Big Query Table Lakehouse target mapping configuration
    """
    def __init__(
            self, 
            lakehouse:str = "", 
            table:str = ""):
        self.Lakehouse = lakehouse
        self.Table = table

class ConfigPartition:
    """
    User Config class for Big Query Table partition configuration
    """
    def __init__(
            self, 
            enabled:bool = False, 
            partition_type:str = "", 
            col:ConfigTableColumn = ConfigTableColumn(), 
            grain:str = ""):
        self.Enabled = enabled
        self.PartitionType = partition_type
        self.PartitionColumn = col
        self.Granularity = grain

class ConfigBQTable:
    """
    User Config class for Big Query Table mapping configuration
    """
    def __str__(self):
        return str(self.TableName)

    def __init__(
            self, 
            json_config:str):
        """
        Loads from user config JSON object
        """
        self.TableName = self.get_json_conf_val(json_config, "table_name", "")
        self.Priority = self.get_json_conf_val(json_config, "priority", 100)
        self.SourceQuery = self.get_json_conf_val(json_config, "source_query", "")
        self.LoadStrategy = self.get_json_conf_val(json_config, "load_strategy" , SyncConstants.FULL)
        self.LoadType = self.get_json_conf_val(json_config, "load_type", SyncConstants.OVERWRITE)
        self.Interval =  self.get_json_conf_val(json_config, "interval", SyncConstants.AUTO)
        self.Enabled =  self.get_json_conf_val(json_config, "enabled", True)

        if "lakehouse_target" in json_config:
            self.LakehouseTarget = ConfigLakehouseTarget( \
                self.get_json_conf_val(json_config["lakehouse_target"], "lakehouse", ""), \
                self.get_json_conf_val(json_config["lakehouse_target"], "table_name", ""))
        else:
            self.LakehouseTarget = ConfigLakehouseTarget()
        
        if "watermark" in json_config:
            self.Watermark = ConfigTableColumn( \
                self.get_json_conf_val(json_config["watermark"], "column", ""))
        else:
            self.Watermark = ConfigTableColumn()

        if "partitioned" in json_config:
            self.Partitioned = ConfigPartition( \
                self.get_json_conf_val(json_config["partitioned"], "enabled", False), \
                self.get_json_conf_val(json_config["partitioned"], "type", ""), \
                self.get_json_conf_val(json_config["partitioned"], "column", ""), \
                self.get_json_conf_val(json_config["partitioned"], "partition_grain", ""))
        else:
            self.Partitioned = ConfigPartition()
        
        self.Keys = []

        if "keys" in json_config:
            for c in json_config["keys"]:
                self.Keys.append(ConfigTableColumn( \
                    self.get_json_conf_val(c, "column", "")))
        
    def get_json_conf_val(
            self, 
            json:str, 
            config_key:str, 
            default_val = None):
        """
        Extracts a value from the user config JSON doc by key. If it doesn't
        exist the default value is returned
        """
        if config_key in json:
            return json[config_key]
        else:
            return default_val
        
class ConfigBase():
    '''
    Base class for sync objects that require access to user-supplied configuration
    '''

    def __init__(
              self, 
              config_path:str, 
              force_reload_config:bool = False):
        """
        Init method loads the user JSON config from the supplied path.
        """
        if config_path is None:
            raise ValueError("Missing Path to JSON User Config")

        self.ConfigPath = config_path
        self.UserConfig = None
        self.GCPCredential = None

        self.UserConfig = self.ensure_user_config(force_reload_config)

        self.GCPCredential = self.load_gcp_credential()
    
    def ensure_user_config(
              self, 
              reload_config:bool) -> ConfigDataset:
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
    
    def load_user_config(
            self, 
            config_path:str, 
            reload_config:bool)->str:
        """
        If the spark dataframe is not cached, loads the user config JSON to a dataframe,
        caches it, creates a temporary session view and then returns a JSON object
        """
        config_df = None

        if not spark.catalog.tableExists("user_config_json") or reload_config:
            config_df = spark.read.option("multiline","true").json(config_path)
            config_df.createOrReplaceTempView("user_config_json")
            config_df.cache()
        else:
            config_df = spark.table("user_config_json")
            
        return json.loads(config_df.toJSON().first())

    def validate_user_config(
            self, 
            cfg:ConfigDataset) -> bool:
        """
        Validates the user config JSON to make sure all required config is supplied
        """
        if cfg is None:
            raise RuntimeError("Invalid User Config")    
        
        if cfg.GCPCredentialPath is None:
            raise ValueError("Missing GCP Credentials  path in JSON User Config")
        return True

    def load_gcp_credential(self) -> str:
        """
        GCP credentials can be supplied as a base64 encoded string or as a path to 
        the GCP service account JSON credentials. If a path is supplied, the JSON file 
        is loaded and the contents serialized to a base64 string
        """
        cred = None

        if self.is_base64(self.UserConfig.GCPCredentialPath):
            cred = self.UserConfig.GCPCredentialPath
        else:
            credential = f"{mssparkutils.nbResPath}{self.UserConfig.GCPCredentialPath}"

            if os.path.exists(credential):
                file_contents = self.read_credential_file(credential)
                cred = self.convert_to_base64string(file_contents)
            else:
                raise ValueError("Invalid GCP Credential path supplied.")

        return cred

    def read_credential_file(
            self, 
            credential_path:str) -> str:
        """
        Reads credential file from the Notebook Resource file path
        """
        txt = Path(credential_path).read_text()
        txt = txt.replace("\n", "").replace("\r", "")

        return txt

    def convert_to_base64string(
            self, 
            credential_val:str) -> str:
        """
        Converts string to base64 encoding, returns ascii value of bytes
        """
        credential_val_bytes = credential_val.encode("ascii") 
        
        base64_bytes = base64.b64encode(credential_val_bytes) 
        base64_string = base64_bytes.decode("ascii") 

        return base64_string

    def is_base64(
            self, 
            val:str) -> str:
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
                return base64.b64encode(base64.b64decode(sb_bytes)) == sb_bytes
        except Exception:
                return False

    def read_bq_partition_to_dataframe(
            self, 
            table:str, 
            partition_filter:str, 
            cache_results:bool=False) -> DataFrame:
        """
        Reads a specific partition using the BigQuery spark connector.
        BigQuery does not support table decorator so the table and partition info 
        is passed using options
        """
        df = spark.read \
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

    def read_bq_to_dataframe(
            self, 
            query:str, 
            cache_results:bool=False) -> DataFrame:
        """
        Reads a BigQuery table using the BigQuery spark connector
        """
        df = spark.read \
            .format("bigquery") \
            .option("parentProject", self.UserConfig.ProjectID) \
            .option("credentials", self.GCPCredential) \
            .option("viewsEnabled", "true") \
            .option("materializationDataset", self.UserConfig.Dataset) \
            .load(query)
        
        if cache_results:
            df.cache()
        
        return df

    def write_lakehouse_table(
            self, 
            df:DataFrame, 
            lakehouse:str, 
            tbl_nm:str, 
            mode:str=SyncConstants.OVERWRITE):
        """
        Write a DataFrame to the lakehouse using the Lakehouse.TableName notation
        """
        dest_table = self.UserConfig.get_lakehouse_tablename(lakehouse, tbl_nm)

        df.write \
            .mode(mode) \
            .saveAsTable(dest_table)
    
    def create_infosys_proxy_view(
            self, 
            trgt:str,
            refresh:bool = False):
        """
        Creates a covering temporary view over top of the Big Query metadata tables
        """
        clean_nm = trgt.replace(".", "_")
        vw_nm = f"BQ_{clean_nm}"

        if not spark.catalog.tableExists(vw_nm) or refresh:
            tbl = self.UserConfig.flatten_3part_tablename(clean_nm)
            lakehouse_tbl = self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

            sql = f"""
            CREATE OR REPLACE TEMPORARY VIEW {vw_nm}
            AS
            SELECT *
            FROM {lakehouse_tbl}
            """
            spark.sql(sql)

    def create_userconfig_tables_proxy_view(self):
        """
        Explodes the User Config table configuration into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_tables
            AS
            SELECT
                project_id, dataset, tbl.table_name,
                tbl.enabled,tbl.load_priority,tbl.source_query,
                tbl.load_strategy,tbl.load_type,tbl.interval,
                tbl.watermark.column as watermark_column,
                tbl.partitioned.enabled as partition_enabled,
                tbl.partitioned.type as partition_type,
                tbl.partitioned.column as partition_column,
                tbl.partitioned.partition_grain,
                tbl.lakehouse_target.lakehouse,
                tbl.lakehouse_target.table_name AS lakehouse_target_table,
                tbl.keys
            FROM (SELECT project_id, dataset, EXPLODE(tables) AS tbl FROM user_config_json)
        """
        spark.sql (sql)

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
                FROM (SELECT project_id, dataset, EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        spark.sql(sql)

    def create_proxy_views(
            self, 
            refresh:bool = False):
        """
        Create the user config and covering BQ information schema views
        """
        if not spark.catalog.tableExists("user_config_tables") or refresh:
            self.create_userconfig_tables_proxy_view()
        
        if not spark.catalog.tableExists("user_config_table_keys") or refresh:
            self.create_userconfig_tables_cols_proxy_view()

        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLES, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_PARTITIONS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_COLUMNS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS, refresh)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE, refresh)