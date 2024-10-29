from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json


from .Metastore import *
from .Enum import *
from .Core import *

class SyncConstants:
    '''
    Class representing various string constants used through-out
    '''
    CONFIG_JSON_TEMPLATE = """
    {
        "id":"",
        "enable_views":false,        
        "enable_materialized_views":false,        
        "enable_data_expiration":false,
        "load_all_tables":true,
        "load_all_views":false,
        "load_all_materialized_views":false,
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
        "table_defaults":{
            "priority": "",
            "project_id": "",
            "dataset":"",
            "object_type":"",
            "load_strategy": "",
            "load_type": "",
            "enabled": false,
            "enforce_expiration": false,
            "allow_schema_evolution": false,
            "interval": "",
            "flatten_table":false,
            "flatten_inplace":false,
            "explode_arrays":false,
            "table_maintenance":{
                "enabled":true,
                "interval":""
            },
            "table_options": [
            {
                "key": "",
                "value": ""
            }
            ],
            "watermark": {
                "column": ""
            }            
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
            "enforce_expiration":false,
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

    def enum_to_list(enum_obj)->list[str]:
        """
        Converts an enumeration object to a list of its member names.
            Args:
                enum_obj (Enum): The enumeration object to convert.
            Returns:
                list[str]: A list containing the names of the enumeration members.
        """
        return [x.name for x in list(enum_obj)]

    def get_load_strategies () -> List[str]:
        """
        Retrieves a list of load strategies.

        Returns:
            List[str]: A list of load strategy names as strings.
        """
        return SyncConstants.enum_to_list(LoadStrategy)

    def get_load_types() -> List[str]:
        """
        Retrieves a list of load types.
        This function converts the LoadType enumeration to a list of strings
        using the SyncConstants.enum_to_list method.
        Returns:
            List[str]: A list of load types as strings.
        """

        return SyncConstants.enum_to_list(LoadType)

    def get_partition_grains() -> List[str]:
        """
        Retrieve a list of partition grains.
        This function converts the CalendarInterval enumeration to a list of strings
        using the SyncConstants.enum_to_list method.
        Returns:
            List[str]: A list of partition grains as strings.
        """

        return SyncConstants.enum_to_list(CalendarInterval)
    
    def get_information_schema_views() -> List[str]:
        """
        Retrieves a list of information schema views.
        Returns:
            List[str]: A list of schema view names as strings.
        """

        return SyncConstants.enum_to_list(SchemaView)


class JSONConfigObj:
    """
    JSONConfigObj is a class designed to handle JSON configuration objects.
    Methods:
        get_json_conf_val(json: str, config_key: str, default_val=None):
            Extracts a value from the user config JSON document by key. If the key 
            doesn't exist, the default value is returned.
    """

    def get_json_conf_val(self, json:str, config_key:str, default_val = None):
        """
        Retrieves the value associated with a given key from a JSON string.
        Args:
            json (str): The JSON string to search within.
            config_key (str): The key whose value needs to be retrieved.
            default_val: The value to return if the key is not found in the JSON string. Defaults to None.
        Returns:
            The value associated with the given key if found, otherwise returns default_val.
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
        self.ID  = super().get_json_conf_val(json_config, "id", "BQ_SYNC_LOADER")
        self.LoadAllTables = super().get_json_conf_val(json_config, "load_all_tables", True)
        self.EnableViews = super().get_json_conf_val(json_config, "enable_views", False)
        self.LoadAllViews = super().get_json_conf_val(json_config, "load_all_views", False)
        self.EnableMaterializedViews = super().get_json_conf_val(json_config, "enable_materialized_views", False)
        self.LoadAllMaterializedViews = super().get_json_conf_val(json_config, "load_all_materialized_views", False)
        self.EnableDataExpiration = super().get_json_conf_val(json_config, "enable_data_expiration", False)
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

        if "table_defaults" in json_config:
            self.TableDefaults = ConfigBQTableDefault(json_config["table_defaults"])
        else:
            self.TableDefaults = ConfigBQTableDefault()

        if "async" in json_config:
            self.Async = ConfigAsync(json_config["async"])
        else:
            self.Async = ConfigAsync()

        if "tables" in json_config:
            for t in json_config["tables"]:
                self.Tables.append(ConfigBQTable(t, self.TableDefaults))
    
    def get_table_name_list(self, project:str, dataset:str, obj_type:BigQueryObjectType, only_enabled:bool = False) -> list[str]:
        """
        Returns a list of table names from the user configuration
        """
        tables = [t for t in self.Tables \
            if t.ProjectID == project and t.Dataset == dataset and t.ObjectType == str(obj_type)]

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

class ConfigBQTableDefault (JSONConfigObj):
    """
    User Config class for Big Query Table default configuration
    """
    def __str__(self):
        """
        Returns a string representation of the object.
        Returns:
            str: The table name as a string.
        """

        return str(self.TableName)

    def __init__(self, json_config:str = None, defaults = None):
        """
        Loads from user config JSON object
        """
        super().__init__()

        self.ProjectID = super().get_json_conf_val(json_config, "project_id", "" if not defaults else defaults.ProjectID)
        self.Dataset = super().get_json_conf_val(json_config, "dataset", "" if not defaults else defaults.Dataset)
        self.ObjectType  = super().get_json_conf_val(json_config, "object_type", "BASE_TABLE" if not defaults else defaults.ObjectType)
        self.Priority = super().get_json_conf_val(json_config, "priority", 100 if not defaults else defaults.Priority)
        self.LoadStrategy = super().get_json_conf_val(json_config, "load_strategy" , str(LoadStrategy.FULL)  if not defaults else defaults.LoadStrategy)
        self.LoadType = super().get_json_conf_val(json_config, "load_type", str(LoadType.OVERWRITE) if not defaults else defaults.LoadType)
        self.Interval =  super().get_json_conf_val(json_config, "interval", str(ScheduleType.AUTO) if not defaults else defaults.Interval)
        self.Enabled =  super().get_json_conf_val(json_config, "enabled", True if not defaults else defaults.Enabled)
        self.EnforceExpiration = super().get_json_conf_val(json_config, "enforce_expiration", False if not defaults else defaults.EnforceExpiration)
        self.AllowSchemaEvolution = super().get_json_conf_val(json_config, "allow_schema_evolution", False if not defaults else defaults.AllowSchemaEvolution)
        self.FlattenTable = super().get_json_conf_val(json_config, "flatten_table", False if not defaults else defaults.FlattenTable)
        self.FlattenInPlace = super().get_json_conf_val(json_config, "flatten_inplace", True if not defaults else defaults.FlattenInPlace)
        self.ExplodeArrays = super().get_json_conf_val(json_config, "explode_arrays", True if not defaults else defaults.ExplodeArrays)
        
        self.TableOptions:dict[str, str] = {}
        
        if "table_options" in json_config:
            for o in json_config["table_options"]:
                self.TableOptions[o["key"]] = o["value"]
        elif defaults:
            self.TableOptions = defaults.TableOptions

        if "watermark" in json_config:
            self.Watermark = ConfigTableColumn(
                super().get_json_conf_val(json_config["watermark"], "column", ""))
        elif defaults:
            self.Watermark = defaults.Watermark
        else:
            self.Watermark = ConfigTableColumn()
        
        if "table_maintenance" in json_config:
            self.TableMaintenance = ConfigTableMaintenance(json_config["table_maintenance"])
        elif defaults:
            self.TableMaintenance = defaults.TableMaintenance
        else:
            self.TableMaintenance = ConfigTableMaintenance()

class ConfigBQTable (ConfigBQTableDefault):
    """
    User Config class for Big Query Table mapping configuration
    """
    def __init__(self, json_config:str = None, defaults:ConfigBQTableDefault = None):
        """
        Loads from user config JSON object
        """
        super().__init__(json_config, defaults)

        self.TableName = super().get_json_conf_val(json_config, "table_name", "")
        self.SourceQuery = super().get_json_conf_val(json_config, "source_query", "")

        if "lakehouse_target" in json_config:
            self.LakehouseTarget = ConfigLakehouseTarget(json_config["lakehouse_target"])
        else:
            self.LakehouseTarget = ConfigLakehouseTarget()

        if "partitioned" in json_config:
            self.Partitioned = ConfigPartition(json_config["partitioned"])
        else:
            self.Partitioned = ConfigPartition()

        self.Keys = []

        if "keys" in json_config:
            for c in json_config["keys"]:
                self.Keys.append(ConfigTableColumn(
                    super().get_json_conf_val(c, "column", "")))
    
    def get_table_keys(self):
        """
        Retrieves the list of table keys.
        This method checks if the `Keys` attribute is present and not empty.
        If `Keys` is available, it extracts the `Column` attribute from each key
        and returns a list of these columns. If `Keys` is not available, it 
        returns an empty list.
        Returns:
            list: A list of columns from the table keys, or an empty list if no keys are present.
        """

        keys = []

        if self.Keys:
            keys = [k.Column for k in self.Keys]
        
        return keys

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

    def write_lakehouse_table(self, df:DataFrame, lakehouse:str, tbl_nm:str, mode:LoadType=LoadType.OVERWRITE):
        """
        Write a DataFrame to the lakehouse using the Lakehouse.TableName notation
        """
        dest_table = f"{lakehouse}.{tbl_nm}"

        df.write \
            .mode(str(mode)) \
            .saveAsTable(dest_table)