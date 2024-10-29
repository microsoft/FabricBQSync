from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from queue import PriorityQueue
from threading import Thread

from ..Config import *
from ..Core import *
from ..Admin.DeltaTableUtility import *
from ..Enum import *

class ConfigMetadataLoader(ConfigBase):
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
        """
            Synchronizes BigQuery information schema based on the specified type.
            Args:
                project (str): The BigQuery project ID.
                dataset (str): The BigQuery dataset ID.
                type (BigQueryObjectType): The type of BigQuery object to synchronize. 
                                           It can be VIEW, MATERIALIZED_VIEW, or BASE_TABLE.
            Returns:
                None
            Raises:
                ValueError: If the provided type is not a valid BigQueryObjectType.
            Notes:
                - This method reads the BigQuery information schema and filters the data based on the user configuration.
                - The filtered data is then written to the lakehouse table.
        """
        match type:
            case BigQueryObjectType.VIEW:
                view = SchemaView.INFORMATION_SCHEMA_VIEWS
            case BigQueryObjectType.MATERIALIZED_VIEW:
                view = SchemaView.INFORMATION_SCHEMA_MATERIALIZED_VIEWS
            case BigQueryObjectType.BASE_TABLE:
                view = SchemaView.INFORMATION_SCHEMA_TABLES
        
        bq_table = f"{project}.{dataset}.{view}"
        tbl_nm = f"BQ_{view}".replace(".", "_")

        if type == BigQueryObjectType.BASE_TABLE:
            bql = f"""
            SELECT *
            FROM {bq_table}
            WHERE table_type='BASE TABLE'
            AND table_name NOT LIKE '_bqc_%'
            """
        else:
            bql = f"""
            SELECT *
            FROM {bq_table}
            """

        df = self.read_bq_to_dataframe(bql)

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
            df = df.filter(col("table_name").isin(filter_list)) 

        self.write_lakehouse_table(df, self.UserConfig.Fabric.MetadataLakehouse, tbl_nm, LoadType.APPEND)

    def sync_bq_information_schema_table_dependent(self, project:str, dataset:str, dependent_tbl:SchemaView): 
        """
        Reads a child INFORMATION_SCHEMA table from BigQuery for the specified project and dataset.
        The child table is joined to the TABLES table to filter for BASE TABLEs and the results are
        written to the configured Fabric Metadata Lakehouse.
        
        Args:
            project (str): The BigQuery project ID.
            dataset (str): The BigQuery dataset ID.
            dependent_tbl (SchemaView): The dependent INFORMATION_SCHEMA table to read.
        Returns:
            None
        """
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

        df = self.read_bq_to_dataframe(bql)

        if not self.UserConfig.LoadAllTables:
            filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)
            df = df.filter(col("table_name").isin(filter_list)) 

        self.write_lakehouse_table(df, self.UserConfig.Fabric.MetadataLakehouse, tbl_nm, LoadType.APPEND)

    def cleanup_metadata_cache(self):
        metadata_views = SyncConstants.get_information_schema_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS BQ_{x.replace('.', '_')}"), metadata_views))

    def task_runner(self, sync_function, workQueue:PriorityQueue):
        while not workQueue.empty():
            value = SchemaView[workQueue.get()]

            try:
                sync_function(value)
            except Exception as e:
                print(f"ERROR {value}: {e}")
            finally:
                workQueue.task_done()

    def process_queue(self, workQueue:PriorityQueue, task_function, sync_function):
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
                    
        print(f"Async metadata update complete...")

    def metadata_sync(self, view:SchemaView):
        print(f"Syncing metadata for {view}...")
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

    def sync_metadata(self):
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

        self.cleanup_metadata_cache()

        self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        self.async_bq_metadata()
        self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

        self.create_proxy_views()

    def create_proxy_views(self):
        """
        Create the user config and covering BQ information schema views
        """
        self.Metastore.create_userconfig_tables_proxy_view()        
        self.Metastore.create_userconfig_tables_cols_proxy_view()

        self.Metastore.create_autodetect_view()      

    def auto_detect_config(self):
        print("Auto-detect schema/configuration...")
        self.Metastore.auto_detect_table_profiles(self.UserConfig.ID, self.UserConfig.LoadAllTables)

        if self.UserConfig.EnableViews:
            self.Metastore.auto_detect_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllViews)

        if self.UserConfig.EnableMaterializedViews:
            self.Metastore.auto_detect_materialized_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllMaterializedViews)