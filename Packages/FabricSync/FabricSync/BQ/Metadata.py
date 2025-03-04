from typing import Tuple
from FabricSync.BQ.SyncCore import ConfigBase
from FabricSync.BQ.Threading import QueueProcessor
from FabricSync.BQ.Model.Core import (
    InformationSchemaModel, BQQueryModel, PredicateType
)
from FabricSync.BQ.Enum import (
    SyncLoadType, BigQueryObjectType, SchemaView,BigQueryAPI
)
from FabricSync.BQ.Exceptions import (
    MetadataSyncError
)
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Threading import SparkProcessor
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.SyncUtils import SyncUtil
from FabricSync.BQ.Utils import SyncTimer

class BQMetadataLoader(ConfigBase):
    """
    Class handles:
     
    1. Loads the table metadata from the BigQuery information schema tables to 
        the Lakehouse Delta tables
    2. Autodetect table sync configuration based on defined metadata & heuristics
    """
    def __init__(self) -> None:
        """
        Constructor for BQMetadataLoader class that initializes the schema models for the BigQuery information schema views.
        Args:
            user_config: The user configuration object
        Returns:
            None
        """
        super().__init__()
        self.__load_bq_information_schema_models()

    def __sync_bq_information_schema_core(self, project:str, dataset:str, type:BigQueryObjectType, mode:SyncLoadType)-> None:
        """
        Synchronize BigQuery information schema objects (tables, views, or materialized views) for a specified
        GCP project and dataset based on the provided configuration and sync load type.
        Args:
            project (str): The GCP project ID where the BigQuery dataset resides.
            dataset (str): The name of the BigQuery dataset to be synchronized.
            type (BigQueryObjectType): The type of BigQuery object to process (e.g., BASE_TABLE, VIEW, MATERIALIZED_VIEW).
            mode (SyncLoadType): The data load mode (e.g., overwrite or append).
        Returns:
            None: The function writes the synchronized metadata to a Spark-managed table as defined by the schema model.
        Notes:
            - The sync process filters objects based on user configuration (e.g., whether to load all tables or apply specific filters).
            - Leverages different BigQuery APIs (STANDARD or STORAGE) depending on user settings.
            - Automatically builds and applies predicates to the query model to limit synchronized objects.
            - Logs status messages if metadata synchronization is skipped.
        """

        is_enabled = False

        match type:
            case BigQueryObjectType.VIEW:
                view = SchemaView.INFORMATION_SCHEMA_VIEWS
                is_enabled = self.UserConfig.AutoDiscover.Views.Enabled
            case BigQueryObjectType.MATERIALIZED_VIEW:
                view = SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS
                is_enabled = self.UserConfig.AutoDiscover.MaterializedViews.Enabled
            case BigQueryObjectType.BASE_TABLE:
                view = SchemaView.INFORMATION_SCHEMA_TABLES
                is_enabled = self.UserConfig.AutoDiscover.Tables.Enabled

        schema_model = self.schema_models[view]

        if is_enabled:
            filter_list = None
            filter = None

            if self.UserConfig.GCP.API.UseStandardAPI:
                bq_api = BigQueryAPI.STANDARD
            else:
                bq_api = BigQueryAPI.STORAGE

            qm = {
                "ProjectId": project,
                "Dataset": dataset,
                "Query": schema_model.get_base_sql(self.ID, project, dataset),
                "API": bq_api,
                "Cached": False
            }
            query_model = BQQueryModel(**qm)

            predicate_type = PredicateType.OR
            load_all = True

            match type:
                case BigQueryObjectType.BASE_TABLE:
                    query_model.add_predicate("table_type='BASE TABLE'")
                    query_model.add_predicate("table_name NOT LIKE '_bqc_%'")

                    if not self.UserConfig.LoadAllTables:
                        load_all = False
                        predicate_type = PredicateType.AND
                        
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)
                    filter = self.UserConfig.autodiscover.tables.filter
                case BigQueryObjectType.VIEW:
                    if not self.UserConfig.LoadAllViews:
                        load_all = False
                        predicate_type = PredicateType.AND                   
                    
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.VIEW, True)
                    filter = self.UserConfig.autodiscover.views.filter
                case BigQueryObjectType.MATERIALIZED_VIEW:
                    if not self.UserConfig.LoadAllMaterializedViews:
                        load_all = False
                        predicate_type = PredicateType.AND                    
                    
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.MATERIALIZED_VIEW, True)
                    filter = self.UserConfig.autodiscover.materialized_views.filter
                case _:
                    pass

            filter_pattern = SyncUtil.build_filter_predicate(filter)

            if not load_all:
                if filter_list:
                    tbls = ", ".join(f"'{t}'" for t in filter_list)
                else:
                    tbls = "''"

                if filter_pattern:
                    query_model.add_predicate(f"((table_name IN ({tbls})) OR ({filter_pattern}))", PredicateType.AND)
                else:
                    query_model.add_predicate(f"table_name IN ({tbls})", predicate_type)
            else:
                if filter_pattern:
                    query_model.add_predicate(filter_pattern, PredicateType.AND)

            df = self.read_bq_to_dataframe(query_model, schema=schema_model.get_df_schema())

            if not df:
                df = schema_model.get_empty_df(self.Context)
        else:
            self.Logger.sync_status(f"Metadata Sync for {view} SKIPPED (Not Enabled in Config)...", verbose=True)
            df = schema_model.get_empty_df(self.Context)
        
        df.write.partitionBy("sync_id").mode(mode.value).saveAsTable(f"{schema_model.TableName}")

    def __sync_bq_information_schema_table_dependent(self, project:str, dataset:str, view:SchemaView, mode:SyncLoadType)-> None: 
        """
        Synchronize BigQuery information schema objects (columns, key column usage, partitions, table constraints, table options) for a specified
        GCP project and dataset based on the provided configuration and sync load type.
        Args:
            project (str): The GCP project ID where the BigQuery dataset resides.
            dataset (str): The name of the BigQuery dataset to be synchronized.
            view (SchemaView): The type of BigQuery object to process (e.g., COLUMNS, KEY_COLUMN_USAGE, PARTITIONS, TABLE_CONSTRAINTS, TABLE_OPTIONS).
            mode (SyncLoadType): The data load mode (e.g., overwrite or append).
        Returns:
            None: The function writes the synchronized metadata to a Spark-managed table as defined by the schema model.
        Notes:
            - The sync process filters objects based on user configuration (e.g., whether to load all tables or apply specific filters).
            - Leverages different BigQuery APIs (STANDARD or STORAGE) depending on user settings.
            - Automatically builds and applies predicates to the query model to limit synchronized objects.
            - Logs status messages if metadata synchronization is skipped.
        """
        schema_model = self.schema_models[view]

        bql = schema_model.get_base_sql(self.ID, project, dataset, alias="c")
        bql = bql + \
            f" JOIN {project}.{dataset}.{SchemaView.INFORMATION_SCHEMA_TABLES} t ON t.table_catalog=c.table_catalog AND t.table_schema=c.table_schema AND t.table_name=c.table_name"

        if self.UserConfig.GCP.API.UseStandardAPI:
            bq_api = BigQueryAPI.STANDARD
        else:
            bq_api = BigQueryAPI.STORAGE

        qm = {
            "ProjectId": project,
            "Dataset": dataset,
            "Query": bql,
            "API": bq_api,
            "Cached": False
        }

        query_model = BQQueryModel(**qm)

        query_model.add_predicate("t.table_type='BASE TABLE'")
        query_model.add_predicate("t.table_name NOT LIKE '_bqc_%'")

        filter = self.UserConfig.autodiscover.tables.filter
        filter_pattern = SyncUtil.build_filter_predicate(filter)

        filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)

        if not self.UserConfig.LoadAllTables:
            if filter_list:
                tbls = ", ".join(f"'{t}'" for t in filter_list)
            else:
                tbls = "''"

            if filter_pattern:
                query_model.add_predicate(f"((t.table_name IN ({tbls})) OR (t.{filter_pattern}))", PredicateType.AND)          
            elif not self.UserConfig.LoadAllTables:
                query_model.add_predicate(f"t.table_name IN ({tbls})", PredicateType.AND) 
        else:
            if filter_pattern:
                query_model.add_predicate(f"t.{filter_pattern}", PredicateType.AND)                

        df = self.read_bq_to_dataframe(query_model, schema=schema_model.get_df_schema())

        if not df:
            df = schema_model.get_empty_df(self.Context)

        df.write.mode(mode.value).partitionBy("sync_id").saveAsTable(f"{schema_model.TableName}")

    def __load_bq_information_schema_models(self) -> None:
        """
        Load the schema models for the BigQuery information schema views.
        Args:
            None
        Returns:
            None: The function initializes the schema models for the information schema views.
        Notes:
            - The schema models are used to define the schema of the Delta tables that store the BigQuery information schema metadata.
        """
        self.schema_models = {}

        self.schema_models[SchemaView.INFORMATION_SCHEMA_TABLES] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_TABLES, 
            "table_catalog,table_schema,table_name,table_type,creation_time,ddl,is_change_history_enabled", 
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"table_type","nullable":true,"type":"string"},{"metadata":{},"name":"creation_time","nullable":true,"type":"timestamp"},{"metadata":{},"name":"ddl","nullable":true,"type":"string"},{"metadata":{},"name":"is_change_history_enabled","nullable":true,"type":"string"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_VIEWS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_VIEWS, 
            "table_catalog,table_schema,table_name,view_definition",
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"view_definition","nullable":true,"type":"string"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_COLUMNS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_COLUMNS, 
            "table_catalog,table_schema,table_name,column_name,ordinal_position,is_nullable,data_type,is_partitioning_column",
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"column_name","nullable":true,"type":"string"},{"metadata":{},"name":"ordinal_position","nullable":true,"type":"long"},{"metadata":{},"name":"is_nullable","nullable":true,"type":"string"},{"metadata":{},"name":"data_type","nullable":true,"type":"string"},{"metadata":{},"name":"is_partitioning_column","nullable":true,"type":"string"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_KEY_COLUMN_USAGE] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_KEY_COLUMN_USAGE, 
            "constraint_catalog,constraint_schema,constraint_name,table_catalog,table_schema,table_name,column_name,ordinal_position,position_in_unique_constraint",
            '{"fields":[{"metadata":{},"name":"constraint_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"constraint_schema","nullable":true,"type":"string"},{"metadata":{},"name":"constraint_name","nullable":true,"type":"string"},{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"column_name","nullable":true,"type":"string"},{"metadata":{},"name":"ordinal_position","nullable":true,"type":"long"},{"metadata":{},"name":"position_in_unique_constraint","nullable":true,"type":"long"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS, 
            "table_catalog,table_schema,table_name,last_refresh_time,refresh_watermark",
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"last_refresh_time","nullable":true,"type":"timestamp"},{"metadata":{},"name":"refresh_watermark","nullable":true,"type":"timestamp"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_PARTITIONS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_PARTITIONS, 
            "table_catalog,table_schema,table_name,partition_id,total_rows,total_logical_bytes,total_billable_bytes,last_modified_time,storage_tier",
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"partition_id","nullable":true,"type":"string"},{"metadata":{},"name":"total_rows","nullable":true,"type":"long"},{"metadata":{},"name":"total_logical_bytes","nullable":true,"type":"long"},{"metadata":{},"name":"total_billable_bytes","nullable":true,"type":"long"},{"metadata":{},"name":"last_modified_time","nullable":true,"type":"timestamp"},{"metadata":{},"name":"storage_tier","nullable":true,"type":"string"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_TABLE_CONSTRAINTS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_TABLE_CONSTRAINTS, 
            "constraint_catalog,constraint_schema,constraint_name,table_catalog,table_schema,table_name,constraint_type,is_deferrable,initially_deferred,enforced",
            '{"fields":[{"metadata":{},"name":"constraint_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"constraint_schema","nullable":true,"type":"string"},{"metadata":{},"name":"constraint_name","nullable":true,"type":"string"},{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"constraint_type","nullable":true,"type":"string"},{"metadata":{},"name":"is_deferrable","nullable":true,"type":"string"},{"metadata":{},"name":"initially_deferred","nullable":true,"type":"string"},{"metadata":{},"name":"enforced","nullable":true,"type":"string"}],"type":"struct"}')

        self.schema_models[SchemaView.INFORMATION_SCHEMA_TABLE_OPTIONS] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_TABLE_OPTIONS, 
            "table_catalog,table_schema,table_name,option_name,option_type,option_value",
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"option_name","nullable":true,"type":"string"},{"metadata":{},"name":"option_type","nullable":true,"type":"string"},{"metadata":{},"name":"option_value","nullable":true,"type":"string"}],"type":"struct"}')

    def __thread_exception_handler(self, value:Tuple):
        """
        Thread exception handler for the async metadata update process.
        Args:
            value (tuple): The tuple containing the thread exception information.
        Returns:
            None: The function logs the exception information.
        """
        #self.Logger.error(msg=f"ERROR {value}: {traceback.format_exc()}")
        self.Logger.error(msg=f"ERROR - {value[1]} FAILED: {value[2]}")

    def __async_bq_metadata(self) -> None:
        """
        Asynchronously update the BigQuery metadata for the information schema views.
        Args:
            None
        Returns:
            None: The function processes the metadata update for the information schema views.
        Notes:
            - The function uses a queue processor to handle the asynchronous metadata update process.
            - The function logs status messages if the metadata update fails.
        """
        self.Logger.sync_status(f"Async metadata update with parallelism of {self.UserConfig.Async.Parallelism}...", verbose=True)

        processor = QueueProcessor(num_threads=self.UserConfig.Async.Parallelism)

        for view in SyncConstants.get_information_schema_views():
            vw = SchemaView[view]
            match vw:
                case SchemaView.INFORMATION_SCHEMA_VIEWS | \
                    SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS | SchemaView.INFORMATION_SCHEMA_TABLES:
                    processor.put((1,view))
                case _:
                    processor.put((2,view))

        processor.process(self.__metadata_sync, self.__thread_exception_handler)

        if processor.has_exceptions:
            self.has_exception = True            
            self.Logger.error("ERROR: Async metadata failed....please resolve errors and try again...")
        else:
            self.has_exception = False

    def __metadata_sync(self, value:Tuple) -> None:
        """
        Synchronize the BigQuery metadata for the specified information schema view.
        Args:
            value (tuple): The tuple containing the view information.
        Returns:
            None: The function processes the metadata synchronization for the specified view.
        Notes:
            - The function logs status messages if the metadata synchronization fails.
        """
        view = SchemaView[value[1]]
        self.Logger.sync_status(f"Syncing metadata for {view}...", verbose=True)

        mode = SyncLoadType.APPEND

        with SyncTimer() as t:
            schema_model = self.schema_models[view]

            for p in self.UserConfig.GCP.Projects:
                for d in p.Datasets:
                    match view:
                        case SchemaView.INFORMATION_SCHEMA_TABLES:
                            self.__sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.BASE_TABLE, mode=mode)
                        case SchemaView.INFORMATION_SCHEMA_VIEWS:
                            self.__sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.VIEW, mode=mode)
                        case SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                            self.__sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.MATERIALIZED_VIEW, mode=mode)
                        case _:
                            self.__sync_bq_information_schema_table_dependent(project=p.ProjectID, dataset=d.Dataset, view=view, mode=mode)
        
        self.Logger.sync_status(f"Syncing metadata for {view} completed in {str(t)}...")

    def cleanup_information_schema(self) -> None:
        table_schema = "dbo" if self.EnableSchemas else None
        cmds = [f"DELETE FROM {LakehouseCatalog.resolve_table_name(table_schema, t)} WHERE sync_id='{self.ID}';" for t in SyncConstants.get_information_schema_tables()]
        SparkProcessor.process_command_list(cmds)

    @Telemetry.Metadata_Sync
    def sync_metadata(self) -> None:   
        """
        Synchronize the BigQuery metadata for the information schema views.
        Args:
            None
        Returns:
            None: The function processes the metadata synchronization for the information schema views.
        Notes:
            - The function logs status messages if the metadata synchronization fails.
        """
        self.Logger.sync_status(f"BQ Metadata Update with BQ {'STANDARD' if self.UserConfig.GCP.API.UseStandardAPI == True else 'STORAGE'} API...")
        with SyncTimer() as t:
            self.cleanup_information_schema()

            self.Context.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            self.__async_bq_metadata()
            self.Context.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        if not self.has_exception:
            self.Logger.sync_status(f"BQ Metadata Update completed in {str(t)}...")
        else:
            raise MetadataSyncError(msg="BQ Metadata sync failed. Please check the logs.")  

    @Telemetry.Auto_Discover
    def auto_detect_config(self) -> None:
        """
        Auto-detect the schema and configuration for the BigQuery tables based on the defined metadata and heuristics.
        Args:
            None
        Returns:
            None: The function auto-detects the schema and configuration for the BigQuery tables.
        Notes:
            - The function logs status messages if the auto-detection process fails.
        """
        SyncUtil.ensure_sync_views()
        self.Logger.sync_status("Auto-detecting schema/configuration...", verbose=True)

        with SyncTimer() as t:
            FabricMetastore.auto_detect_profiles()
        
        self.Logger.sync_status(f"Auto-detect schema/configuration completed in {str(t)}...")