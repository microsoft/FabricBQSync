from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from delta.tables import *
from datetime import datetime, date, timezone
from typing import Tuple
from queue import PriorityQueue
from threading import Thread, Lock

from FabricSync.BQ.Metastore import *
from FabricSync.BQ.Config import *
from FabricSync.DeltaTableUtility import *
from FabricSync.BQ.Enum import *

class ConfigMetadataLoader(ConfigBase):
    """
    Class handles:
     
    1. Loads the table metadata from the BigQuery information schema tables to 
        the Lakehouse Delta tables
    2. Autodetect table sync configuration based on defined metadata & heuristics
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls the parent init to load the user config JSON file
        """        
        super().__init__(context, user_config, gcp_credential)

    def sync_bq_information_schema_core(self, project:str, dataset:str, type:BigQueryObjectType):
        """
        Reads the INFORMATION_SCHEMA.TABLES|VIEWS|MATERIALIZED_VIEWS from BigQuery for the configuration project_id 
        and dataset. For TABLES it returns only BASE TABLEs. Writes the results to the configured Metadata Lakehouse
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
        Reads a child INFORMATION_SCHEMA table from BigQuery for the configuration project_id 
        and dataset. The child table is joined to the TABLES table to filter for BASE TABLEs.
        Writes the results to the configured Fabric Metadata Lakehouse.
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

    def sync_bq_metadata(self):
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

    def auto_detect_bq_schema(self):
        print("Auto-detect schema/configuration...")
        self.Metastore.auto_detect_table_profiles(self.UserConfig.ID, self.UserConfig.LoadAllTables)

        if self.UserConfig.EnableViews:
            self.Metastore.auto_detect_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllViews)

        if self.UserConfig.EnableMaterializedViews:
            self.Metastore.auto_detect_materialized_view_profiles(self.UserConfig.ID, self.UserConfig.LoadAllMaterializedViews)

class Scheduler(ConfigBase):
    """
    Class responsible for calculating the to-be run schedule based on the sync config and 
    the most recent BigQuery table metadata. Schedule is persisted to the Sync Schedule
    Delta table. When tables are scheduled but no updates are detected on the BigQuery side 
    a SKIPPED record is created for tracking purposes.
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls the parent init to load the user config JSON file
        """
        super().__init__(context, user_config, gcp_credential)

    def build_schedule(self, schedule_type:ScheduleType) -> str:
        print(f"Building Sync Schedule for {schedule_type} Schedule...")
        schedule_id = self.Metastore.get_current_schedule(self.UserConfig.ID, schedule_type)

        if not schedule_id:
            schedule_id = self.Metastore.build_new_schedule(schedule_type, self.UserConfig.ID,
                self.UserConfig.EnableViews, self.UserConfig.EnableMaterializedViews)
        
        return schedule_id

class BQDataRetention(ConfigBase):
    """
    Class responsible for BQ Table and Partition Expiration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(context, user_config, gcp_credential)
    
    def execute(self):        
        print ("Enforcing Data Expiration/Retention Policy...")
        self._enforce_retention_policy()
        print ("Updating Data Expiration/Retention Policy...")
        self.Metastore.sync_retention_config(self.UserConfig.ID)

    def _enforce_retention_policy(self):
        df = self.Metastore.get_bq_retention_policy(self.UserConfig.ID)

        for d in df.collect():
            if d["use_lakehouse_schema"]:
                table_name = f"{d['lakehouse']}.{d['lakehouse_schema']}.{d['lakehouse_table_name']}"
            else:
                table_name = f"{d['lakehouse']}.{d['lakehouse_table_name']}"

            table_maint = DeltaTableMaintenance(self.Context, table_name)

            if d["partition_id"]:
                print(f"Expiring Partition: {table_name}${d['partition_id']}")
                predicate = SyncUtil.resolve_fabric_partition_predicate(d["partition_type"], d["partition_column"], 
                    d["partition_grain"], d["partition_id"])
                table_maint.drop_partition(predicate)
            else:
                print(f"Expiring Table: {table_name}")
                table_maint.drop_table()  

class SyncUtil():
    @staticmethod
    def flatten_structs(nested_df:DataFrame) -> DataFrame:
        """
        Recurses through Dataframe and flattens columns of with datatype struct
        using '_' notation
        """
        stack = [((), nested_df)]
        columns = []

        while len(stack) > 0:        
            parents, df = stack.pop()

            flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) \
                    for c in df.dtypes if c[1][:6] != "struct"]
                
            nested_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
            
            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
            
        return nested_df.select(columns)

    @staticmethod
    def flatten_df(explode_arrays:bool, df:DataFrame) -> DataFrame:
        """
        Recurses through Dataframe and flattens complex types
        """ 
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]

        if len(array_cols) > 0 and explode_arrays:
            while len(array_cols) > 0:        
                for array_col in array_cols:            
                    cols_to_select = [x for x in df.columns if x != array_col ]            
                    df = df.withColumn(array_col, explode(col(array_col)))
                    
                df = SyncUtil.flatten_structs(df)
                
                array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        else:
            df = SyncUtil.flatten_structs(df)

        return df

    @staticmethod
    def resolve_fabric_partition_predicate(partition_type:str, partition_column:str, partition_grain:str, partition_id:str):
        if PartitionType[partition_type] == PartitionType.TIME:
            partition_dt=SyncUtil.get_derived_date_from_part_id(partition_grain, partition_id)
            proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(partition_grain)

            predicate = SyncUtil.get_fabric_partition_predicate(partition_dt, partition_column, proxy_cols) 
        else:
            predicate = f"__{partition_column}_Range='{partition_id}'"
        
        return predicate

    @staticmethod
    def get_fabric_partition_proxy_cols(partition_grain:str) -> list[str]:
        proxy_cols = list(CalendarInterval)

        match CalendarInterval[partition_grain]:
            case CalendarInterval.DAY:
                proxy_cols.remove(CalendarInterval.HOUR)
            case CalendarInterval.MONTH:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
            case CalendarInterval.YEAR:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
                proxy_cols.remove(CalendarInterval.MONTH)

        return [x.value for x in proxy_cols]
    
    @staticmethod
    def get_bq_partition_id_format(partition_grain:str) -> str:
        pattern = None

        match CalendarInterval[partition_grain]:
            case CalendarInterval.DAY:
                pattern = "%Y%m%d"
            case CalendarInterval.MONTH:
                pattern = "%Y%m"
            case CalendarInterval.YEAR:
                pattern = "%Y"
            case CalendarInterval.HOUR:
                pattern = "%Y%m%d%H"
        
        return pattern

    @staticmethod
    def get_derived_date_from_part_id(partition_grain:str, partition_id:str) -> datetime:
        dt_format = SyncUtil.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)
    
    @staticmethod
    def create_fabric_partition_proxy_cols(df:DataFrame, partition:str, partition_grain:str, proxy_cols:list[str]) -> DataFrame:  
        for c in proxy_cols:
            match CalendarInterval[c]:
                case CalendarInterval.HOUR:
                    df = df.withColumn(f"__{partition}_HOUR", \
                        date_format(col(partition), "HH"))
                case CalendarInterval.DAY:
                    df = df.withColumn(f"__{partition}_DAY", \
                        date_format(col(partition), "dd"))
                case CalendarInterval.MONTH:
                    df = df.withColumn(f"__{partition}_MONTH", \
                        date_format(col(partition), "MM"))
                case CalendarInterval.YEAR:
                    df = df.withColumn(f"__{partition}_YEAR", \
                        date_format(col(partition), "yyyy"))
        
        return df

    @staticmethod
    def get_fabric_partition_cols(partition:str, proxy_cols:list[str]):
        return [f"__{partition}_{c}" for c in proxy_cols]

    @staticmethod
    def get_fabric_partition_predicate(partition_dt:datetime, partition:str, proxy_cols:list[str]) -> str:
        partition_predicate = []

        for c in proxy_cols:
            match CalendarInterval[c]:
                case CalendarInterval.HOUR:
                    part_id = partition_dt.strftime("%H")
                case CalendarInterval.DAY:
                    part_id = partition_dt.strftime("%d")
                case CalendarInterval.MONTH:
                    part_id = partition_dt.strftime("%m")
                case CalendarInterval.YEAR:
                    part_id = partition_dt.strftime("%Y")

            partition_predicate.append(f"__{partition}_{c} = '{part_id}'")

        return " AND ".join(partition_predicate)
    
    @staticmethod
    def get_bq_range_map(tbl_ranges:str) -> DataFrame:
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
    
    @staticmethod
    def create_fabric_range_partition(df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        
        df = self.Context.createDataFrame(partition_range, ["range_name", "range_low", "range_high"]) \
            .alias("rng")

        df_bq = df_bq.alias("bq")
        df_bq = df_bq.join(df, (col(f"bq.{schedule.PartitionColumn}") >= col("rng.range_low")) & \
            (col(f"bq.{schedule.PartitionColumn}") < col("rng.range_high"))) \
            .select("bq.*", col("rng.range_name").alias(schedule.FabricPartitionColumns[0]))
        
        return df_bq

    @staticmethod
    def get_partition_range_predicate(schedule:SyncSchedule) -> str:
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]

        if not r:
            raise Exception(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"


class BQScheduleLoader(ConfigBase):
    """
    Class repsonsible for processing the sync schedule and handling data movement 
    from BigQuery to Fabric Lakehouse based on each table's configuration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(context, user_config, gcp_credential)
        self._SyncTableIndex:list[str] = []
        self._TableIndexLock = Lock()

    def appendTableIndex(self, table:str):
        """
        Thread-safe list of sync'd tables
        """
        with self._TableIndexLock:
            if not table in self._SyncTableIndex:
                self._SyncTableIndex.append(table)
    
    def isTabledSynced(self, table:str) -> bool:
        """
        Thread-safe list exists for sync'd tables
        """
        with self._TableIndexLock:
            exists = (table in self._SyncTableIndex)
        
        return exists

    def get_delta_merge_row_counts(self, schedule:SyncSchedule) -> Tuple[int, int, int]:
        """
        Gets the rows affected by merge operation, filters on partition id when table is partitioned
        """
        telemetry = self.Context.sql(f"DESCRIBE HISTORY {schedule.LakehouseTableName}")

        telemetry = telemetry \
            .filter("operation = 'MERGE' AND CAST(timestamp AS DATE) = current_date()") \
            .orderBy("version", ascending=False)

        inserts = 0
        updates = 0
        deletes = 0

        for t in telemetry.collect():
            op_metrics = None

            if schedule.FabricPartitionColumns and schedule.PartitionId:
                if "predicate" in t["operationParameters"] and \
                    schedule.PartitionId in t["operationParameters"]["predicate"]:
                        op_metrics = t["operationMetrics"]
            else:
                op_metrics = t["operationMetrics"]

            if op_metrics:
                inserts = int(op_metrics["numTargetRowsInserted"])
                updates = int(op_metrics["numTargetRowsUpdated"])
                deletes = int(op_metrics["numTargetRowsDeleted"])

                continue

        return (inserts, updates, deletes)

    def get_max_watermark(self, schedule:SyncSchedule, df_bq:DataFrame) -> str:
        """
        Get the max value for the supplied table and column
        """
        df = df_bq.select(max(col(schedule.WatermarkColumn)).alias("watermark"))

        max_watermark = None

        for r in df.collect():
            max_watermark = r["watermark"] 

        val = None

        if type(max_watermark) is date:
            val = max_watermark.strftime("%Y-%m-%d")
        elif type(max_watermark) is datetime:
            val = max_watermark.strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            val = str(max_watermark)

        return val

    def merge_table(self, schedule:SyncSchedule, tableName:str, src:DataFrame) -> SyncSchedule:
        """
        Merge into Lakehouse Table based on User Configuration. Only supports Insert/Update All
        """
        self.Context.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")

        constraints = []

        for p in schedule.Keys:
            constraints.append(f"s.{p} = d.{p}")

        if not constraints:
            raise ValueError("One or more keys must be specified for a MERGE operation")
        
        if schedule.FabricPartitionColumns and schedule.PartitionId:
            for p in schedule.FabricPartitionColumns:
                constraints.append(f"d.{p} = '{schedule.PartitionId}'")

        predicate = " AND ".join(constraints)

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        dest = DeltaTable.forName(self.Context, tableOrViewName=tableName)

        dest.alias('d') \
        .merge( \
            src.alias('s'), \
            predicate) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

        results = self.get_delta_merge_row_counts(schedule)

        schedule.UpdateRowCounts(src=0, insert=results[0], update=results[1])
        
        return schedule

    def get_bq_table(self, schedule:SyncSchedule) -> (SyncSchedule, DataFrame):
        if schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None:
            part_format = SyncUtil.get_bq_partition_id_format(schedule.PartitionGrain)

            if schedule.PartitionDataType == BQDataType.TIMESTAMP:                  
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter, True)
        elif schedule.IsRangePartitioned:
            part_filter = SyncUtil.get_partition_range_predicate(schedule)
            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter, True)
        else:
            if schedule.LoadStrategy == LoadStrategy.WATERMARK and not schedule.InitialLoad:
                if schedule.MaxWatermark.isdigit():
                    predicate = f"{schedule.WatermarkColumn} > {schedule.MaxWatermark}"
                else:
                    predicate = f"{schedule.WatermarkColumn} > '{schedule.MaxWatermark}'"

                df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, predicate, True)
            else:
                src = schedule.BQTableName     

                if schedule.SourceQuery:
                    src = schedule.SourceQuery

                df_bq = self.read_bq_to_dataframe(src, True)

        if schedule.IsPartitioned:
            if schedule.PartitionType == PartitionType.TIME:
                proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                schedule.FabricPartitionColumns = SyncUtil.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                if schedule.IsTimeIngestionPartitioned:
                    df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                df_bq = SyncUtil.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, schedule.PartitionGrain, proxy_cols)
            else:
                schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                df_bq = SyncUtil.create_fabric_range_partition(df_bq, schedule)
        
        return (schedule, df_bq)

    def save_bq_dataframe(self, schedule:SyncSchedule, df_bq:DataFrame, lock:Lock) -> SyncSchedule:
        write_config = { **schedule.TableOptions }
        table_maint = None

        #Flattening complex types (structs & arrays)
        df_bq_flattened = None
        if schedule.FlattenTable:
            if schedule.LoadType == LoadType.MERGE and schedule.ExplodeArrays:
                raise Exception("Invalid load configuration: Merge is not supported when Explode Arrays is enabed")
                
            if schedule.FlattenInPlace:
                df_bq = self.flatten_df(schedule.ExplodeArrays, df_bq)
            else:
                df_bq_flattened = self.flatten_df(schedule.ExplodeArrays, df_bq)
            
        #Schema Evolution
        if not schedule.InitialLoad:
            if schedule.AllowSchemaEvolution:
                table_maint = DeltaTableMaintenance(self.Context, schedule.LakehouseTableName)
                table_maint.evolve_schema(df_bq)
                write_config["mergeSchema"] = True

        if not schedule.LoadType == LoadType.MERGE or schedule.InitialLoad:
            if (schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None) or schedule.IsRangePartitioned:
                has_lock = False

                if schedule.InitialLoad:
                    has_lock = True
                    lock.acquire()
                else:
                    write_config["partitionOverwriteMode"] = "dynamic"
                
                try:
                    df_bq.write \
                        .partitionBy(schedule.FabricPartitionColumns) \
                        .mode(str(LoadType.OVERWRITE)) \
                        .options(**write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                    
                    if df_bq_flattened:
                       df_bq_flattened.write \
                            .partitionBy(schedule.FabricPartitionColumns) \
                            .mode(str(LoadType.OVERWRITE)) \
                            .options(**write_config) \
                            .saveAsTable(f"{schedule.LakehouseTableName}_flattened") 
                finally:
                    self.appendTableIndex(schedule.LakehouseTableName)

                    if has_lock:
                        lock.release()
            else:
                if schedule.FabricPartitionColumns is None:
                    df_bq.write \
                        .mode(schedule.Mode) \
                        .options( **write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                    
                    if df_bq_flattened:
                        df_bq_flattened.write \
                            .mode(schedule.Mode) \
                            .options( **write_config) \
                            .saveAsTable(f"{schedule.LakehouseTableName}_flattened")
                else:
                    df_bq.write \
                        .partitionBy(schedule.FabricPartitionColumns) \
                        .mode(schedule.Mode) \
                        .options( **write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                    
                    if df_bq_flattened:
                        df_bq_flattened.write \
                            .partitionBy(schedule.FabricPartitionColumns) \
                            .mode(schedule.Mode) \
                            .options( **write_config) \
                            .saveAsTable(f"{schedule.LakehouseTableName}_flattened")
                
                self.appendTableIndex(schedule.LakehouseTableName)
        else:
            schedule = self.merge_table(schedule, schedule.LakehouseTableName, df_bq)

            if df_bq_flattened:
                self.merge_table(schedule, f"{schedule.LakehouseTableName}_flattened", df_bq)

            self.appendTableIndex(schedule.LakehouseTableName)

        if not table_maint:
            table_maint = DeltaTableMaintenance(self.Context, schedule.LakehouseTableName)

        if schedule.LoadStrategy == LoadStrategy.WATERMARK:
            schedule.MaxWatermark = self.get_max_watermark(schedule, df_bq)

        src_cnt = df_bq.count()
        schedule.UpdateRowCounts(src_cnt, 0, 0)    
        schedule.SparkAppId = self.Context.sparkContext.applicationId
        schedule.DeltaVersion = table_maint.CurrentTableVersion
        schedule.EndTime = datetime.now(timezone.utc)
        schedule.Status = SyncStatus.COMPLETE
        
        df_bq.unpersist()

        return schedule

    def sync_bq_table(self, schedule:SyncSchedule, lock:Lock = None):
        """
        Sync the data for a table from BigQuery to the target Fabric Lakehouse based on configuration

        1. Determines how to retrieve the data from BigQuery
            a. PARTITION & TIME_INGESTION
                - Data is loaded by partition using the partition filter option of the spark connector
            b. FULL & WATERMARK
                - Loaded using the table name or source query and any relevant predicates
        2. Resolve BigQuery to Fabric partition mapping
            a. BigQuery supports TIME and RANGE based partitioning
                - TIME based partitioning support YEAR, MONTH, DAY & HOUR grains
                    - When the grain doesn't exist or a psuedo column is used, a proxy column is added
                        on the Fabric Lakehouse side
                - RANGE partitioning is a backlog feature
        3. Write data to the Fabric Lakehouse
            a. PARTITION write use replaceWhere to overwrite the specific Delta partition
            b. All other writes respect the configure MODE against the write destination
        4. Collect and save telemetry
        """
        schedule.SummaryLoadType = schedule.DefaultSummaryLoadType
        self.show_sync_status(schedule)

        #Get BQ table using sync config
        schedule, df_bq = self.get_bq_table(schedule)

        #Save BQ table to Lakehouse
        schedule = self.save_bq_dataframe(schedule, df_bq, lock)

        return schedule

    def show_sync_status(self, schedule:SyncSchedule):
        show_status = f"{schedule.SummaryLoadType} {schedule.ObjectType} {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}"

        if schedule.PartitionId:
            show_status = f"{show_status}${schedule.PartitionId}"

        print(f"{show_status}...")

    def save_schedule_telemetry(self, schedule:SyncSchedule):
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        """
        rdd = self.Context.sparkContext.parallelize([Row( \
            schedule_id=schedule.ScheduleId, \
            sync_id=schedule.SyncId, \
            project_id=schedule.ProjectId, \
            dataset=schedule.Dataset, \
            table_name=schedule.TableName, \
            partition_id=schedule.PartitionId, \
            status=str(schedule.Status), \
            started=schedule.StartTime, \
            completed=schedule.EndTime, \
            src_row_count=schedule.SourceRows, \
            inserted_row_count=schedule.InsertedRows, \
            updated_row_count=schedule.UpdatedRows, \
            delta_version=schedule.DeltaVersion, \
            spark_application_id=schedule.SparkAppId, \
            max_watermark=schedule.MaxWatermark, \
            summary_load=schedule.SummaryLoadType \
        )])

        self.Metastore.save_schedule_telemetry(rdd)

    def run_sequential_schedule(self, group_schedule_id:str):
        """
        Run the schedule activities sequentially based on priority order
        """
        print(f"Sequential schedule sync starting...")
        df_schedule = self.Metastore.get_schedule(group_schedule_id)

        for row in df_schedule.collect():
            schedule = SyncSchedule(row)
            self.sync_bq_table(schedule)
            self.save_schedule_telemetry(schedule)  

        self.Metastore.process_load_group_telemetry(group_schedule_id)
        print(f"Sequential schedule sync complete...")
    
    def schedule_sync(self, schedule:SyncSchedule, lock:Lock) -> SyncSchedule:
        schedule = self.sync_bq_table(schedule, lock)
        self.save_schedule_telemetry(schedule) 

        return schedule

    def task_runner(self, sync_function, workQueue:PriorityQueue, lock:Lock):
        while not workQueue.empty():
            value = workQueue.get()
            schedule = value[2]
            
            try:
                sync_function(schedule, lock)
            except Exception as e:
                print(f"ERROR with {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}: {e}")
                schedule.Status = SyncStatus.FAILED
                schedule.SummaryLoadType = f"ERROR: {e}"
                self.save_schedule_telemetry(schedule) 
            finally:
                workQueue.task_done()

    def process_queue(self, workQueue:PriorityQueue, task_function, sync_function):
        lock = Lock() 
        for i in range(self.UserConfig.Async.Parallelism):
            t=Thread(target=task_function, args=(sync_function, workQueue, lock))
            t.daemon = True
            t.start() 
            
        workQueue.join()
        
    def run_async_schedule(self, group_schedule_id:str):
        """
        Runs the schedule activities in parallel using python threading

        - Utilitizes the priority to define load groups to respect priority
        - Parallelism is control from the User Config JSON file
        """
        print(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...")
        workQueue = PriorityQueue()

        schedule = self.Metastore.get_schedule(group_schedule_id)

        load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

        if load_grps:
            for grp in load_grps:
                grp_nm = "LOAD GROUP {0}".format(grp)
                grp_df = schedule.where(f"priority = '{grp}'")

                for tbl in grp_df.collect():
                    s = SyncSchedule(tbl)
                    nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                    if s.PartitionId is not None:
                        nm = "{0}${1}".format(nm, s.PartitionId)        

                    workQueue.put((s.Priority, nm, s))

                if not workQueue.empty():
                    print(f"### Processing {grp_nm}...")
                    self.process_queue(workQueue, self.task_runner, self.schedule_sync)

            self.Metastore.process_load_group_telemetry(group_schedule_id)
       
        print(f"Async schedule sync complete...")

class BQSync(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path, clean_session=True)

        self.MetadataLoader = ConfigMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = Scheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)
        self.DataRetention = BQDataRetention(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        self.MetadataLoader.sync_bq_metadata()

        if self.UserConfig.Autodetect:
            self.MetadataLoader.auto_detect_bq_schema()
    
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = str(ScheduleType.AUTO)) -> str:
        if sync_metadata:
            self.sync_metadata()
        else:
            self.MetadataLoader.create_proxy_views()

        return self.Scheduler.build_schedule(schedule_type=ScheduleType[schedule_type])

    def run_schedule(self, group_schedule_id:str, optimize_metadata:bool=True):
        self.MetadataLoader.create_proxy_views()

        if self.UserConfig.Fabric.EnableSchemas:
            self.Metastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceId, self.UserConfig.ID)

        if self.UserConfig.Async.Enabled:
            self.Loader.run_async_schedule(group_schedule_id)
        else:
            self.Loader.run_sequential_schedule(group_schedule_id)
        
        self.Metastore.commit_table_configuration(group_schedule_id)
        
        if self.UserConfig.EnableDataExpiration:
            self.DataRetention.execute()
        
        if optimize_metadata:
            self.Metastore.optimize_metadata_tbls()

class SyncConfigurationHelper(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path, clean_session=True)
    
    def generate_user_config_json_from_metadata(self, path:str):
        bq_tables = self.Context.table(SyncConstants.SQL_TBL_SYNC_CONFIG)
        bq_tables = bq_tables.filter(col("sync_id") == self.UserConfig.ID)

        user_cfg = self.build_user_config_json(bq_tables.collect())

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(user_cfg, f, ensure_ascii=False, indent=4)

    def build_user_config_json(self, tbls:list) -> str:
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
        return [{"dataset": d.Dataset} for d in ds]

    def get_gcp_project_config_json(self, proj) -> str:
        datasets = self.get_gcp_dataset_config_json(proj.Datasets)

        return {
            "project_id":proj.ProjectID,
            "datasets":datasets
        }

    def get_table_keys_config_json(self, pk:list):
        return [{"column": k} for k in pk]

    def get_table_config_json(self, tbl:Row) -> str:
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