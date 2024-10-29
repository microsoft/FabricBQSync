from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json


from .Metastore import *
from .Enum import *
from .Core import *
from .Config import *

class SyncConfigurationHelper(SyncBase):
    """
    Helper class for synchronizing configuration settings.
    Methods
    -------
    __init__(context: SparkSession, config_path: str)
        Initializes the SyncConfigurationHelper with the given Spark session and configuration path.
    generate_user_config_json_from_metadata(path: str)
        Generates a user configuration JSON file from metadata and saves it to the specified path.
    build_user_config_json(tbls: list) -> str
        Builds a user configuration JSON string from a list of tables.
    get_gcp_dataset_config_json(ds: list)
        Returns a list of dictionaries containing GCP dataset configurations.
    get_gcp_project_config_json(proj) -> str
        Returns a dictionary containing GCP project configuration.
    get_table_keys_config_json(pk: list)
        Returns a list of dictionaries containing table key configurations.
    get_table_config_json(tbl: Row) -> str
        Returns a dictionary containing table configuration.
    """

    def __init__(self, context:SparkSession, config_path:str):
        """
        Initializes the Utils class.
        Args:
            context (SparkSession): The Spark session to be used.
            config_path (str): The path to the configuration file.
        """

        super().__init__(context, config_path, clean_session=True)
    
    def generate_user_config_json_from_metadata(self, path:str):
        def generate_user_config_json_from_metadata(self, path: str):
            """
            Generates a user configuration JSON file from metadata.
            This method retrieves synchronization configuration data from a BigQuery table,
            builds a user configuration JSON object, and writes it to a specified file path.
            Args:
                path (str): The file path where the JSON configuration will be saved.
            Raises:
                IOError: If there is an error writing the JSON file to the specified path.
            """

        bq_tables = self.Context.table(SyncConstants.SQL_TBL_SYNC_CONFIG)
        bq_tables = bq_tables.filter(col("sync_id") == self.UserConfig.ID)

        user_cfg = self.build_user_config_json(bq_tables.collect())

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(user_cfg, f, ensure_ascii=False, indent=4)

    def build_user_config_json(self, tbls:list) -> str:
        """
        Builds a user configuration JSON string based on the provided table list and user configuration settings.
        Args:
            tbls (list): A list of table configurations.
        Returns:
            str: A JSON string representing the user configuration.
        The JSON structure includes:
            - id: An empty string.
            - load_all_tables: A boolean indicating if all tables should be loaded.
            - load_views: A boolean indicating if views should be loaded.
            - load_materialized_views: A boolean indicating if materialized views should be loaded.
            - enable_data_expiration: A boolean indicating if data expiration should be enabled.
            - autodetect: A boolean indicating if autodetection should be enabled.
            - fabric: A dictionary containing Fabric-related configuration.
            - gcp_credentials: A dictionary containing GCP credentials and project information.
            - async: A dictionary containing asynchronous operation settings.
            - tables: A list of table configurations.
        """

        tables = list(map(lambda x: self.get_table_config_json(x), tbls))
        projects = list(map(lambda x: self.get_gcp_project_config_json(x), self.UserConfig.GCPCredential.Projects))

        return {
            "id":"",
            "load_all_tables":self.UserConfig.LoadAllTables,
            "load_views":self.UserConfig.LoadViews,
            "load_materialized_views":self.UserConfig.LoadMaterializedViews,
            "enable_data_expiration":self.UserConfig.EnableDataExpiration,
            "autodetect":self.UserConfig.Autodetect,
            "fabric":{
                "workspace_id":self.UserConfig.Fabric.WorkspaceID,
                "metadata_lakehouse":self.UserConfig.Fabric.MetadataLakehouse,
                "target_lakehouse":self.UserConfig.Fabric.TargetLakehouse,
                "target_schema":self.UserConfig.Fabric.TargetLakehouseSchema,
                "enable_schemas":self.UserConfig.Fabric.EnableSchemas,
            },            
            "gcp_credentials":{
                "projects": projects,
                "credential":self.UserConfig.GCPCredential.Credential,
                "materialization_project_id":self.UserConfig.GCPCredential.MaterializationProjectId,
                "materialization_dataset":self.UserConfig.GCPCredential.MaterializationDataset,
                "billing_project_id":self.UserConfig.GCPCredential.BillingProjectID
            },            
            "async":{
                "enabled":self.UserConfig.Async.Enabled,
                "parallelism":self.UserConfig.Async.Parallelism,
                "cell_timeout":self.UserConfig.Async.CellTimeout,
                "notebook_timeout":self.UserConfig.Async.NotebookTimeout
            }, 
            "tables": tables
        }

    def get_gcp_dataset_config_json(self, ds:list):
        """
        Generates a list of dictionaries containing dataset configurations.
        Args:
            ds (list): A list of dataset objects.
        Returns:
            list: A list of dictionaries, each containing a 'dataset' key with the corresponding dataset's configuration.
        """

        return [{"dataset": d.Dataset} for d in ds]

    def get_gcp_project_config_json(self, proj) -> str:
        """
        Generates a JSON configuration for a given GCP project.
        Args:
            proj: An object representing the GCP project. It should have the following attributes:
                - ProjectID: The ID of the GCP project.
                - Datasets: A list or collection of datasets associated with the project.
        Returns:
            str: A JSON string representing the configuration of the GCP project, including its project ID and datasets.
        """

        datasets = self.get_gcp_dataset_config_json(proj.Datasets)

        return {
            "project_id":proj.ProjectID,
            "datasets":datasets
        }

    def get_table_keys_config_json(self, pk:list):
        """
        Generates a list of dictionaries representing table keys from a given list of primary keys.
        Args:
            pk (list): A list of primary key column names.
        Returns:
            list: A list of dictionaries, each containing a single key-value pair where the key is "column" 
                  and the value is a primary key column name from the input list.
        """

        return [{"column": k} for k in pk]

    def get_table_config_json(self, tbl:Row) -> str:
        """
        Generates a JSON configuration for a table based on the provided Row object.
        Args:
            tbl (Row): A Row object containing table configuration details.
        Returns:
            str: A JSON string representing the table configuration.
        """

        keys = self.get_table_keys_config_json(tbl["primary_keys"])

        return {
            "priority":tbl["priority"],
            "project_id":tbl["project_id"],
            "dataset":tbl["dataset"],
            "table_name":tbl["table_name"],
            "object_type":tbl["object_type"],
            "enabled":tbl["enabled"],
            "source_query":tbl["source_query"],
            "enforce_expiration":tbl["enforce_partition_expiration"],
            "allow_schema_evolution":tbl["allow_schema_evolution"],
            "load_strategy":tbl["load_strategy"],
            "load_type":tbl["load_type"],
            "interval":tbl["interval"],
            "flatten_table":tbl["flatten_table"],
            "flatten_inplace":tbl["flatten_inplace"],
            "explode_arrays":tbl["explode_arrays"],
            "table_maintenance":{
                "enabled":tbl["table_maintenance_enabled"],
                "interval":tbl["table_maintenance_interval"]
            },
            "keys":keys,
            "partitioned":{
                "enabled":tbl["is_partitioned"],
                "type":tbl["partition_type"],
                "column":tbl["partition_column"],
                "partition_grain":tbl["partition_grain"],
                "partition_data_type":tbl["partition_data_type"],
                "partition_range":tbl["partition_range"]
            },
            "watermark":{
                "column":tbl["watermark_column"]
            },
            "lakehouse_target":{
                "lakehouse":tbl["lakehouse"],
                "schema":tbl["lakehouse_schema"],
                "table_name":tbl["lakehouse_table_name"]
            }
        }