from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import traceback

from .Model.Config import *
from .Core import *
from .Admin.DeltaTableUtility import *
from .Enum import *
from .Exceptions import *
from .Constants import SyncConstants
from .SyncUtils import SyncUtil

class BQMetadataLoader(ConfigBase):
    """
    Class handles:
     
    1. Loads the table metadata from the BigQuery information schema tables to 
        the Lakehouse Delta tables
    2. Autodetect table sync configuration based on defined metadata & heuristics
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Initializes the Loader class.
        Args:
            context (SparkSession): The Spark session context.
            user_config: User configuration settings.
            gcp_credential (str): Google Cloud Platform credential string.
        """
        super().__init__(context, user_config, gcp_credential)
        self.load_bq_information_schema_models()

    def sync_bq_information_schema_core(self, project:str, dataset:str, type:BigQueryObjectType, mode:LoadType):
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
                "Query": schema_model.get_base_sql(project, dataset),
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
            self.Logger.sync_status(f"Metadata Sync for {view} SKIPPED (Not Enabled in Config)...")
            df = schema_model.get_empty_df(self.Context)
        
        df.write.mode(str(mode)).saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{schema_model.TableName}")

    def sync_bq_information_schema_table_dependent(self, project:str, dataset:str, view:SchemaView, mode:LoadType): 
        schema_model = self.schema_models[view]

        bql = schema_model.get_base_sql(project, dataset, alias="c")
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

        df.write.mode(str(mode)).saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{schema_model.TableName}")

    def load_bq_information_schema_models(self):
        self.schema_models = {}

        self.schema_models[SchemaView.INFORMATION_SCHEMA_TABLES] = InformationSchemaModel.define(SchemaView.INFORMATION_SCHEMA_TABLES, 
            "table_catalog,table_schema,table_name,table_type,creation_time,ddl", 
            '{"fields":[{"metadata":{},"name":"table_catalog","nullable":true,"type":"string"},{"metadata":{},"name":"table_schema","nullable":true,"type":"string"},{"metadata":{},"name":"table_name","nullable":true,"type":"string"},{"metadata":{},"name":"table_type","nullable":true,"type":"string"},{"metadata":{},"name":"creation_time","nullable":true,"type":"timestamp"},{"metadata":{},"name":"ddl","nullable":true,"type":"string"}],"type":"struct"}')

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
    
    def cleanup_metadata_cache(self):
        metadata_views = SyncConstants.get_information_schema_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS BQ_{x.replace('.', '_')}"), metadata_views))

    def thread_exception_handler(self, value):
        self.Logger.error(msg=f"ERROR {value}: {traceback.format_exc()}")

    def async_bq_metadata(self):
        self.Logger.sync_status(f"Async metadata update with parallelism of {self.UserConfig.Async.Parallelism}...")

        processor = QueueProcessor(num_threads=self.UserConfig.Async.Parallelism)

        for view in SyncConstants.get_information_schema_views():
            vw = SchemaView[view]
            match vw:
                case SchemaView.INFORMATION_SCHEMA_VIEWS | \
                    SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS | SchemaView.INFORMATION_SCHEMA_TABLES:
                    processor.put((1,view))
                case _:
                    processor.put((2,view))

        processor.process(self.metadata_sync, self.thread_exception_handler)

        if processor.has_exceptions:
            self.has_exception = True            
            self.Logger.error("ERROR: Async metadata failed....please resolve errors and try again...")
        else:
            self.has_exception = False

    def metadata_sync(self, value):
        view = SchemaView[value[1]]
        self.Logger.sync_status(f"Syncing metadata for {view}...")

        mode = LoadType.OVERWRITE

        with SyncTimer() as t:
            #Clean-up existing table
            self.Context.sql(f"DROP TABLE IF EXISTS BQ_{str(view).replace('.', '_')}")

            for p in self.UserConfig.GCP.Projects:
                for d in p.Datasets:
                    match view:
                        case SchemaView.INFORMATION_SCHEMA_TABLES:
                            self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.BASE_TABLE, mode=mode)
                        case SchemaView.INFORMATION_SCHEMA_VIEWS:
                            self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.VIEW, mode=mode)
                        case SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                            self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.MATERIALIZED_VIEW, mode=mode)
                        case _:
                            self.sync_bq_information_schema_table_dependent(project=p.ProjectID, dataset=d.Dataset, view=view, mode=mode)
                    
                    if mode == LoadType.OVERWRITE:
                        mode = LoadType.APPEND
        
        self.Logger.sync_status(f"Syncing metadata for {view} completed in {str(t)}...")

    @Telemetry.Metadata_Sync
    def sync_metadata(self):        
        self.Logger.sync_status(f"BQ Metadata Update with BQ {'STANDARD' if self.UserConfig.GCP.API.UseStandardAPI == True else 'STORAGE'} API...")
        with SyncTimer() as t:
            self.async_bq_metadata()

        if not self.has_exception:
            self.Metastore.create_proxy_views()
            self.Logger.sync_status(f"BQ Metadata Update completed in {str(t)}...")
        else:
            raise MetadataSyncError(msg="BQ Metadata sync failed. Please check the logs.")  

    @Telemetry.Auto_Discover
    def auto_detect_config(self):
        self.Logger.sync_status("Auto-detecting schema/configuration...")

        with SyncTimer() as t:
            self.Metastore.auto_detect_profiles(self.UserConfig.ID)
        
        self.Logger.sync_status(f"Auto-detect schema/configuration completed in {str(t)}...")