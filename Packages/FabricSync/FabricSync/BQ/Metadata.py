from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from queue import PriorityQueue
from threading import Thread
import traceback

from ..Config import *
from ..Core import *
from ..Admin.DeltaTableUtility import *
from ..Enum import *

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

    def sync_bq_information_schema_core(self, project:str, dataset:str, type:BigQueryObjectType):
        match type:
            case BigQueryObjectType.VIEW:
                view = SchemaView.INFORMATION_SCHEMA_VIEWS
            case BigQueryObjectType.MATERIALIZED_VIEW:
                view = SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS
            case BigQueryObjectType.BASE_TABLE:
                view = SchemaView.INFORMATION_SCHEMA_TABLES
        
        bq_table = f"{project}.{dataset}.{view}"
        tbl_nm = f"BQ_{view}".replace(".", "_")

        bql = f"SELECT * FROM {bq_table}"
        predicates = []

        if type == BigQueryObjectType.BASE_TABLE:
            predicates.append("table_type='BASE TABLE'")
            predicates.append("table_name NOT LIKE '_bqc_%'")

        filter_list = None
        
        match type:
            case BigQueryObjectType.BASE_TABLE:
                if not self.UserConfig.LoadAllTables:
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)
            case BigQueryObjectType.VIEW:
                if not self.UserConfig.LoadAllViews:
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.VIEW, True)
            case BigQueryObjectType.MATERIALIZED_VIEW:
                if not self.UserConfig.LoadAllMaterializedViews:
                    filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.MATERIALIZED_VIEW, True)
            case _:
                pass

        if filter_list:
            tbls = ", ".join(f"'{t}'" for t in filter_list)
            predicates.append(f"table_name IN ({tbls})")

        if len(predicates) > 0:
            bql = bql + " WHERE " + " AND ".join(predicates)
        
        if self.UserConfig.UseStandardAPI:
            bq_api = BigQueryAPI.STANDARD
        else:
            bq_api = BigQueryAPI.STORAGE

        df = self.read_bq_to_dataframe(project, bql, cache_results=False, api=bq_api)

        if not df:
            df = self.create_placeholder_dataframe(view)

        df.write.mode("APPEND").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{tbl_nm}")

    def sync_bq_information_schema_table_dependent(self, project:str, dataset:str, dependent_tbl:SchemaView): 
        bq_table = f"{project}.{dataset}.{SchemaView.INFORMATION_SCHEMA_TABLES}"
        bq_dependent_tbl = f"{project}.{dataset}.{dependent_tbl}"
        tbl_nm = f"BQ_{dependent_tbl}".replace(".", "_")

        bql = f"""
        SELECT c.*
        FROM {bq_dependent_tbl} c
        JOIN {bq_table} t ON t.table_catalog=c.table_catalog AND t.table_schema=c.table_schema AND t.table_name=c.table_name
        WHERE t.table_type='BASE TABLE'
        AND t.table_name NOT LIKE '_bqc_%'
        """

        if not self.UserConfig.LoadAllTables:
            filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)
            
            if filter_list:
                tbls = ", ".join(f"'{t}'" for t in filter_list)
                bql = bql + f" AND table_name IN ({tbls})"            

        if self.UserConfig.UseStandardAPI:
            bq_api = BigQueryAPI.STANDARD
        else:
            bq_api = BigQueryAPI.STORAGE

        df = self.read_bq_to_dataframe(project, bql, cache_results=False, api=bq_api)

        if not df:
            df = self.create_placeholder_dataframe(dependent_tbl)

        df.write.mode(str(LoadType.APPEND)).saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{tbl_nm}")

    def cleanup_metadata_cache(self):
        metadata_views = SyncConstants.get_information_schema_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS BQ_{x.replace('.', '_')}"), metadata_views))

    def task_runner(self, sync_function, workQueue:PriorityQueue):
        while not workQueue.empty():
            value = SchemaView[workQueue.get()]

            try:
                sync_function(value)
            except Exception as e:
                print(f"ERROR {value}: {traceback.format_exc()}")
                self.has_exception = True
            finally:
                workQueue.task_done()

    def process_queue(self, workQueue:PriorityQueue, task_function, sync_function):
        self.has_exception = False

        for i in range(self.UserConfig.Async.Parallelism):
            t=Thread(target=task_function, args=(sync_function, workQueue))
            t.daemon = True
            t.start() 
            
        workQueue.join()
        
    def async_bq_metadata(self):
        print(f"Async metadata update with parallelism of {self.UserConfig.Async.Parallelism}...")

        workQueue = PriorityQueue()

        for view in SyncConstants.get_information_schema_views():
            vw = SchemaView[view]
            match vw:
                case SchemaView.INFORMATION_SCHEMA_VIEWS:
                    if self.UserConfig.EnableViews:
                        workQueue.put(view)
                case SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                    if self.UserConfig.EnableMaterializedViews:
                        workQueue.put(view)
                case _:
                    workQueue.put(view)

        self.process_queue(workQueue, self.task_runner, self.metadata_sync)

        if self.has_exception:            
            print("ERROR: Async metadata failed....please resolve errors and try again...")

    def metadata_sync(self, view:SchemaView):
        print(f"Syncing metadata for {view}...")

        with SyncTimer() as t:
            for p in self.UserConfig.GCPCredential.Projects:
                for d in p.Datasets:
                    match view:
                        case SchemaView.INFORMATION_SCHEMA_TABLES:
                            self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.BASE_TABLE)
                        case SchemaView.INFORMATION_SCHEMA_VIEWS:
                            if self.UserConfig.EnableViews:
                                self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.VIEW)
                        case SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                            if self.UserConfig.EnableMaterializedViews:
                                self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.MATERIALIZED_VIEW)
                        case _:
                            self.sync_bq_information_schema_table_dependent(project=p.ProjectID, dataset=d.Dataset, dependent_tbl=view)
        
        print(f"Syncing metadata for {view} completed in {str(t)}...")

    def sync_metadata(self) -> bool:
        """
        Loads the required INFORMATION_SCHEMA tables from BigQuery:

        1. TABLES
        2. PARTITIONS
        3. COLUMNS
        4. TABLE_CONSTRAINTS
        5. KEY_COLUMN_USAGE
        6. TABLE_OPTIONS
        7. VIEWS
        8. MATERIALIZED VIEWS
        """
        print(f"BQ Metadata Update with BQ {'STANDARD' if self.UserConfig.UseStandardAPI == True else 'STORAGE'} API...")
        with SyncTimer() as t:
            self.cleanup_metadata_cache()

            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            self.async_bq_metadata()
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

            if not self.has_exception:
                self.create_proxy_views()
        
        if not self.has_exception:
            print(f"BQ Metadata Update completed in {str(t)}...")

        return (self.has_exception == False)

    def create_proxy_views(self):
        """
        Create the user config and covering BQ information schema views
        """
        self.Metastore.create_userconfig_tables_proxy_view()        
        self.Metastore.create_userconfig_tables_cols_proxy_view()

        self.Metastore.create_autodetect_view()      

    def auto_detect_config(self):
        print("Auto-detecting schema/configuration...")

        with SyncTimer() as t:
            self.Metastore.auto_detect_table_profiles(self.UserConfig.ID, self.UserConfig.LoadAllTables)

            if self.UserConfig.EnableViews:
                self.Metastore.auto_detect_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllViews)

            if self.UserConfig.EnableMaterializedViews:
                self.Metastore.auto_detect_materialized_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllMaterializedViews)
        
        print(f"Auto-detect schema/configuration completed in {str(t)}...")
    
    def create_placeholder_dataframe(self, tbl:SchemaView):
        match tbl:
            case SchemaView.INFORMATION_SCHEMA_TABLES:
                df = self.create_tables_placeholder()
            case SchemaView.INFORMATION_SCHEMA_KEY_COLUMN_USAGE:
                df = self.create_key_column_usage_placeholder()
            case SchemaView.INFORMATION_SCHEMA_COLUMNS:
                df = self.create_columns_placeholder()
            case SchemaView.INFORMATION_SCHEMA_PARTITIONS:
                df = self.create_partitions_placeholder()
            case SchemaView.INFORMATION_SCHEMA_TABLE_CONSTRAINTS:
                df = self.create_table_constraints_placeholder()
            case SchemaView.INFORMATION_SCHEMA_TABLE_OPTIONS:
                df = self.create_table_options_placeholder()
            case SchemaView.INFORMATION_SCHEMA_VIEWS:
                df = self.create_view_placeholder()
            case SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                df = self.create_materialized_view_placeholder()

        return df

    def create_view_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('view_definition', StringType(), True), 
            StructField('check_option', StringType(), True), StructField('use_standard_sql', StringType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df

    def create_materialized_view_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('last_refresh_time', TimestampType(), True), 
            StructField('refresh_watermark', TimestampType(), True), 
            StructField('last_refresh_status', StructType([StructField('reason', StringType(), True), 
            StructField('location', StringType(), True), StructField('debug_info', StringType(), True), 
            StructField('message', StringType(), True)]), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df

    def create_key_column_usage_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('constraint_catalog', StringType(), True), StructField('constraint_schema', StringType(), True), 
            StructField('constraint_name', StringType(), True), StructField('table_catalog', StringType(), True), 
            StructField('table_schema', StringType(), True), StructField('table_name', StringType(), True), 
            StructField('column_name', StringType(), True), StructField('ordinal_position', LongType(), True), 
            StructField('position_in_unique_constraint', LongType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df
    
    def create_table_constraints_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('constraint_catalog', StringType(), True), StructField('constraint_schema', StringType(), True), 
            StructField('constraint_name', StringType(), True), StructField('table_catalog', StringType(), True), 
            StructField('table_schema', StringType(), True), StructField('table_name', StringType(), True), 
            StructField('constraint_type', StringType(), True), StructField('is_deferrable', StringType(), True), 
            StructField('initially_deferred', StringType(), True), StructField('enforced', StringType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df
    
    def create_table_options_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('option_name', StringType(), True), 
            StructField('option_type', StringType(), True), StructField('option_value', StringType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df
    
    def create_tables_placeholder(self) -> DataFrame:
        df_schema = StructType([
            StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('table_type', StringType(), True), 
            StructField('is_insertable_into', StringType(), True), StructField('is_typed', StringType(), True), 
            StructField('creation_time', TimestampType(), True), StructField('base_table_catalog', StringType(), True), 
            StructField('base_table_schema', StringType(), True), StructField('base_table_name', StringType(), True), 
            StructField('snapshot_time_ms', TimestampType(), True), StructField('ddl', StringType(), True), 
            StructField('default_collation_name', StringType(), True), 
            StructField('upsert_stream_apply_watermark', TimestampType(), True), 
            StructField('replica_source_catalog', StringType(), True), StructField('replica_source_schema', StringType(), True), 
            StructField('replica_source_name', StringType(), True), StructField('replication_status', StringType(), True), 
            StructField('replication_error', StringType(), True), StructField('is_change_history_enabled', StringType(), True), 
            StructField('sync_status', StructType([StructField('last_completion_time', TimestampType(), True), 
            StructField('error_time', TimestampType(), True), 
            StructField('error', StructType([StructField('reason', StringType(), True), StructField('location', StringType(), True), 
            StructField('message', StringType(), True)]), True)]), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df
    
    def create_partitions_placeholder(self) -> DataFrame:
        df_schema = StructType(
            [StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('partition_id', StringType(), True), 
            StructField('total_rows', LongType(), True), StructField('total_logical_bytes', LongType(), True), 
            StructField('total_billable_bytes', LongType(), True), 
            StructField('last_modified_time', TimestampType(), True), StructField('storage_tier', StringType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df
    
    def create_columns_placeholder(self) -> DataFrame:
        df_schema = StructType(
            [StructField('table_catalog', StringType(), True), StructField('table_schema', StringType(), True), 
            StructField('table_name', StringType(), True), StructField('column_name', StringType(), True), 
            StructField('ordinal_position', LongType(), True), StructField('is_nullable', StringType(), True), 
            StructField('data_type', StringType(), True), StructField('is_generated', StringType(), True), 
            StructField('generation_expression', StringType(), True), StructField('is_stored', StringType(), True), 
            StructField('is_hidden', StringType(), True), StructField('is_updatable', StringType(), True), 
            StructField('is_system_defined', StringType(), True), StructField('is_partitioning_column', StringType(), True), 
            StructField('clustering_ordinal_position', LongType(), True), StructField('collation_name', StringType(), True), 
            StructField('column_default', StringType(), True), StructField('rounding_mode', StringType(), True)])

        df = self.Context.createDataFrame(data = self.Context.sparkContext.emptyRDD(),
                                    schema = df_schema)
        
        return df