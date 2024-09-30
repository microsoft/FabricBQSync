from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from delta.tables import *
from datetime import datetime, date, timezone
from typing import Tuple
from queue import PriorityQueue
from threading import Thread, Lock
import uuid

from FabricSync.BQ.Metastore import *
from FabricSync.BQ.Config import *
from FabricSync.DeltaTableUtility import *

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

    def sync_bq_information_schema_core(self, project:str, dataset:str, type:BigQueryObjectType = BigQueryObjectType.BASE_TABLE):
        """
        Reads the INFORMATION_SCHEMA.TABLES|VIEWS|MATERIALIZED_VIEWS from BigQuery for the configuration project_id 
        and dataset. For TABLES it returns only BASE TABLEs. Writes the results to the configured Metadata Lakehouse
        """

        match type:
            case BigQueryObjectType.VIEW:
                view = SyncConstants.INFORMATION_SCHEMA_VIEWS
            case BigQueryObjectType.MATERIALIZED_VIEW:
                view = SyncConstants.INFORMATION_SCHEMA_MATERIALIZED_VIEWS
            case _:
                view = SyncConstants.INFORMATION_SCHEMA_TABLES
        
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

        if not self.UserConfig.LoadAllTables:
            filter_list = self.UserConfig.get_table_name_list(project, dataset, BigQueryObjectType.BASE_TABLE, True)
            df = df.filter(col("table_name").isin(filter_list))   

        self.write_lakehouse_table(df, self.UserConfig.Fabric.MetadataLakehouse, tbl_nm, SyncConstants.APPEND)

    def sync_bq_information_schema_table_dependent(self, project:str, dataset:str, dependent_tbl:str):
        """
        Reads a child INFORMATION_SCHEMA table from BigQuery for the configuration project_id 
        and dataset. The child table is joined to the TABLES table to filter for BASE TABLEs.
        Writes the results to the configured Fabric Metadata Lakehouse.
        """
        bq_table = f"{project}.{dataset}.{SyncConstants.INFORMATION_SCHEMA_TABLES}"
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

        self.write_lakehouse_table(df, self.UserConfig.Fabric.MetadataLakehouse, tbl_nm, SyncConstants.APPEND)

    def cleanup_metadata_cache(self):
        metadata_views = SyncConstants.get_information_schema_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS BQ_{x.replace('.', '_')}"), metadata_views))

    def task_runner(self, sync_function, workQueue:PriorityQueue):
        while not workQueue.empty():
            value = workQueue.get()

            try:
                sync_function(value)
            except Exception as e:
                print(f"ERROR {view}: {e}")
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
            match view:
                case SyncConstants.INFORMATION_SCHEMA_VIEWS:
                    if self.UserConfig.LoadViews:
                        workQueue.put(view)
                case SyncConstants.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                    if self.UserConfig.LoadMaterializedViews:
                        workQueue.put(view)
                case _:
                    workQueue.put(view)

        self.process_queue(workQueue, self.task_runner, self.metadata_sync)
                    
        print(f"Async metadata update complete...")

    def metadata_sync(self, view:str):
        print(f"Syncing metadata for {view}...")
        for p in self.UserConfig.GCPCredential.Projects:
            for d in p.Datasets:
                match view:
                    case SyncConstants.INFORMATION_SCHEMA_TABLES:
                        self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.BASE_TABLE)
                    case SyncConstants.INFORMATION_SCHEMA_VIEWS:
                        if self.UserConfig.LoadViews:
                            self.sync_bq_information_schema_core(project=p.ProjectID, dataset=d.Dataset, type=BigQueryObjectType.VIEW)
                    case SyncConstants.INFORMATION_SCHEMA_MATERIALIZED_VIEWS:
                        if self.UserConfig.LoadMaterializedViews:
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
        self.Metastore.auto_detect_table_profiles(self.UserConfig.LoadAllTables)

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

    def build_schedule(self, schedule_type:str) -> str:
        schedule_id = self.Metastore.get_current_schedule(self.UserConfig.ID, schedule_type)

        if not schedule_id:
            schedule_id = self.Metastore.build_new_schedule(schedule_type, self.UserConfig.ID)
        
        return schedule_id

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

    def get_fabric_partition_proxy_cols(self, partition_grain:str) -> list[str]:
        proxy_cols = ["YEAR", "MONTH", "DAY", "HOUR"]

        match partition_grain:
            case "DAY":
                proxy_cols.remove("HOUR")
            case "MONTH":
                proxy_cols.remove("HOUR")
                proxy_cols.remove("DAY")
            case "YEAR":
                proxy_cols.remove("HOUR")
                proxy_cols.remove("DAY")
                proxy_cols.remove("MONTH")

        return proxy_cols

    def get_bq_partition_id_format(self, partition_grain:str) -> str:
        pattern = None

        match partition_grain:
            case "DAY":
                pattern = "%Y%m%d"
            case "MONTH":
                pattern = "%Y%m"
            case "YEAR":
                pattern = "%Y"
            case "HOUR":
                pattern = "%Y%m%d%H"
        
        return pattern

    def get_derived_date_from_part_id(self, partition_grain:str, partition_id:str) -> datetime:
        dt_format = self.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)

    def create_fabric_partition_proxy_cols(self, df:DataFrame, partition:str, partition_grain:str, proxy_cols:list[str]) -> DataFrame:  
        for c in proxy_cols:
            match c:
                case "HOUR":
                    df = df.withColumn(f"__{partition}_HOUR", \
                        date_format(col(partition), "HH"))
                case "DAY":
                    df = df.withColumn(f"__{partition}_DAY", \
                        date_format(col(partition), "dd"))
                case "MONTH":
                    df = df.withColumn(f"__{partition}_MONTH", \
                        date_format(col(partition), "MM"))
                case "YEAR":
                    df = df.withColumn(f"__{partition}_YEAR", \
                        date_format(col(partition), "yyyy"))
                case _:
                    next
        
        return df

    def get_fabric_partition_cols(self, partition:str, proxy_cols:list[str]):
        return [f"__{partition}_{c}" for c in proxy_cols]

    def get_fabric_partition_predicate(self, partition_dt:datetime, partition:str, proxy_cols:list[str]) -> str:
        partition_predicate = []

        for c in proxy_cols:
            match c:
                case "HOUR":
                    part_id = partition_dt.strftime("%H")
                case "DAY":
                    part_id = partition_dt.strftime("%d")
                case "MONTH":
                    part_id = partition_dt.strftime("%m")
                case "YEAR":
                    part_id = partition_dt.strftime("%Y")
                case _:
                    next
            
            partition_predicate.append(f"__{partition}_{c} = '{part_id}'")

        return " AND ".join(partition_predicate)
    
    def get_bq_range_map(self, tbl_ranges:str) -> DataFrame:
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
        
    def create_fabric_range_partition(self, df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        partition_range = self.get_bq_range_map(schedule.PartitionRange)
        
        df = self.Context.createDataFrame(partition_range, ["range_name", "range_low", "range_high"]) \
            .alias("rng")

        df_bq = df_bq.alias("bq")
        df_bq = df_bq.join(df, (col(f"bq.{schedule.PartitionColumn}") >= col("rng.range_low")) & \
            (col(f"bq.{schedule.PartitionColumn}") < col("rng.range_high"))) \
            .select("bq.*", col("rng.range_name").alias(schedule.FabricPartitionColumns[0]))
        
        return df_bq

    def get_partition_range_predicate(self, schedule:SyncSchedule) -> str:
        partition_range = self.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]

        if not r:
            raise Exception(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"

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
            part_format = self.get_bq_partition_id_format(schedule.PartitionGrain)

            if schedule.PartitionDataType == SyncConstants.TIMESTAMP:                  
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter, True)
        elif schedule.IsRangePartitioned:
            part_filter = self.get_partition_range_predicate(schedule)
            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter, True)
        else:
            if schedule.LoadStrategy == SyncConstants.WATERMARK and not schedule.InitialLoad:
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
            if schedule.PartitionType == SyncConstants.TIME:
                proxy_cols = self.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                schedule.FabricPartitionColumns = self.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                if schedule.IsTimeIngestionPartitioned:
                    df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                df_bq = self.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, schedule.PartitionGrain, proxy_cols)
            else:
                schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                df_bq = self.create_fabric_range_partition(df_bq, schedule)
        
        return (schedule, df_bq)

    def save_bq_dataframe(self, schedule:SyncSchedule, df_bq:DataFrame, lock:Lock) -> SyncSchedule:
        write_config = { **schedule.TableOptions }
        table_maint = None

        #Flattening complex types (structs & arrays)
        df_bq_flattened = None
        if schedule.FlattenTable:
            if schedule.LoadType == SyncConstants.MERGE and schedule.ExplodeArrays:
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

        if not schedule.LoadType == SyncConstants.MERGE or schedule.InitialLoad:
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
                        .mode(SyncConstants.OVERWRITE) \
                        .options(**write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                    
                    if df_bq_flattened:
                       df_bq_flattened.write \
                            .partitionBy(schedule.FabricPartitionColumns) \
                            .mode(SyncConstants.OVERWRITE) \
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

        if schedule.LoadStrategy == SyncConstants.WATERMARK:
            schedule.MaxWatermark = self.get_max_watermark(schedule, df_bq)

        src_cnt = df_bq.count()
        schedule.UpdateRowCounts(src_cnt, 0, 0)    
        schedule.SparkAppId = self.Context.sparkContext.applicationId
        schedule.DeltaVersion = table_maint.CurrentTableVersion
        schedule.EndTime = datetime.now(timezone.utc)
        schedule.Status = SyncConstants.COMPLETE
        
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
        print(f"{schedule.SummaryLoadType} {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}...")

        #Get BQ table using sync config
        schedule, df_bq = self.get_bq_table(schedule)

        #Save BQ table to Lakehouse
        schedule = self.save_bq_dataframe(schedule, df_bq, lock)

        return schedule

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
            status=schedule.Status, \
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
                schedule = sync_function(schedule, lock)
            except Exception as e:
                schedule.Status = "FAILED"
                schedule.SummaryLoadType = f"ERROR: {e}"
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

class BQSync(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path, clean_session=True)

        self.MetadataLoader = ConfigMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = Scheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        self.MetadataLoader.sync_bq_metadata()

        if self.UserConfig.Autodetect:
            self.MetadataLoader.auto_detect_bq_schema()
    
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = SyncConstants.AUTO) -> str:
        if sync_metadata:
            self.sync_metadata()
        else:
            self.MetadataLoader.create_proxy_views()

        return self.Scheduler.build_schedule(schedule_type)

    def run_schedule(self, group_schedule_id:str, optimize_metadata:bool=True):
        self.MetadataLoader.create_proxy_views()

        if self.UserConfig.Fabric.EnableSchemas:
            self.Metastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceId, self.UserConfig.ID)

        if self.UserConfig.Async.Enabled:
            self.Loader.run_async_schedule(group_schedule_id)
        else:
            self.Loader.run_sequential_schedule(group_schedule_id)
        
        self.Metastore.commit_table_configuration(group_schedule_id)

        if optimize_metadata:
            self.Metastore.optimize_metadata_tbls()