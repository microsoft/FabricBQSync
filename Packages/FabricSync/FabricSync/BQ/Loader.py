from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from delta.tables import *
from datetime import datetime, date, timezone
from typing import Tuple
from queue import PriorityQueue
from threading import Thread, Lock
import traceback

from ..Config import *
from ..Core import *
from ..Admin.DeltaTableUtility import *
from ..Enum import *
from .Model.Schedule import SyncSchedule
from .SyncUtils import *

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

    def get_bq_table(self, schedule:SyncSchedule) -> tuple[SyncSchedule, DataFrame]:
        if schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None:
            part_format = SyncUtil.get_bq_partition_id_format(schedule.PartitionGrain)

            if schedule.PartitionDataType == BQDataType.TIMESTAMP:                  
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            df_bq = self.read_bq_partition_to_dataframe(schedule.ProjectId, schedule.BQTableName, part_filter)
        elif schedule.IsRangePartitioned:
            part_filter = SyncUtil.get_partition_range_predicate(schedule)
            df_bq = self.read_bq_partition_to_dataframe(schedule.ProjectId, schedule.BQTableName, part_filter)
        else:
            if schedule.LoadStrategy == LoadStrategy.WATERMARK and not schedule.InitialLoad:
                if schedule.MaxWatermark.isdigit():
                    predicate = f"{schedule.WatermarkColumn} > {schedule.MaxWatermark}"
                else:
                    predicate = f"{schedule.WatermarkColumn} > '{schedule.MaxWatermark}'"

                df_bq = self.read_bq_partition_to_dataframe(schedule.ProjectId, schedule.BQTableName, predicate)
            else:
                src = schedule.BQTableName     

                if schedule.SourceQuery:
                    src = schedule.SourceQuery

                df_bq = self.read_bq_to_dataframe(schedule.ProjectId, src)

        if schedule.IsPartitioned:
            if schedule.PartitionType == PartitionType.TIME:
                proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                schedule.FabricPartitionColumns = SyncUtil.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                if schedule.IsTimeIngestionPartitioned:
                    df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                df_bq = SyncUtil.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, proxy_cols)
            else:
                schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                df_bq = SyncUtil.create_fabric_range_partition(self.Context, df_bq, schedule)
        
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
            if schedule.IsPartitionedSyncLoad:
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
        
        with SyncTimer() as t:
            schedule.SummaryLoadType = schedule.DefaultSummaryLoadType
            self.show_sync_status(schedule, message=schedule.SummaryLoadType)

            #Get BQ table using sync config
            schedule, df_bq = self.get_bq_table(schedule)

            #On initial load, force drop the table to ensure a clean load
            if schedule.InitialLoad and not schedule.IsPartitionedSyncLoad:
                self.Context.sql(f"DROP TABLE IF EXISTS {schedule.LakehouseTableName}")
                
            #Save BQ table to Lakehouse
            schedule = self.save_bq_dataframe(schedule, df_bq, lock)

        self.show_sync_status(schedule, message="FINISHED", status=f"in {str(t)}")

        return schedule

    def show_sync_status(self, schedule:SyncSchedule, message:str, status:str=None):
        msg_header = f"{message} {schedule.ObjectType} {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}"

        if schedule.PartitionId:
            msg_header = f"{msg_header}${schedule.PartitionId}"

        if not status:
            print(f"{msg_header}...")
        else:
            print(f"{msg_header} {status}...")

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
            summary_load=schedule.SummaryLoadType, \
            source_query=schedule.SourceQuery \
        )])

        self.Metastore.save_schedule_telemetry(rdd)

    def run_sequential_schedule(self, group_schedule_id:str) -> bool:
        print(f"Sequential schedule sync starting...")

        initial_loads = False

        with SyncTimer() as t:
            df_schedule = self.Metastore.get_schedule(group_schedule_id)           

            for row in df_schedule.collect():
                schedule = SyncSchedule(row)

                if schedule.InitialLoad:
                    initial_loads = True

                self.sync_bq_table(schedule)
                self.save_schedule_telemetry(schedule)  

            self.Metastore.process_load_group_telemetry(group_schedule_id)

        print(f"Sequential schedule sync completed in {str(t)}...")
        return initial_loads
    
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
                ex_stack = traceback.format_exc()
                print(f"ERROR with {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}: {ex_stack}")
                schedule.Status = SyncStatus.FAILED
                schedule.SummaryLoadType = f"ERROR: {ex_stack}"
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

    def run_schedule(self, group_schedule_id:str) -> bool:
        if self.UserConfig.Async.Enabled:
            initial_loads = self.run_async_schedule(group_schedule_id)
        else:
            initial_loads = self.run_sequential_schedule(group_schedule_id)
        
        return initial_loads

    def run_async_schedule(self, group_schedule_id:str) -> bool:
        print(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...")

        initial_loads = False

        with SyncTimer() as tt:
            workQueue = PriorityQueue()

            schedule = self.Metastore.get_schedule(group_schedule_id)

            load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

            if load_grps:
                for grp in load_grps:
                    grp_nm = "LOAD GROUP {0}".format(grp)
                    grp_df = schedule.where(f"priority = '{grp}'").sort("total_rows")

                    for tbl in grp_df.collect():
                        s = SyncSchedule(tbl)
                        nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                        if s.PartitionId is not None:
                            nm = "{0}${1}".format(nm, s.PartitionId)        

                        if s.InitialLoad:
                            initial_loads = True

                        workQueue.put((s.Priority, nm, s))

                    if not workQueue.empty():
                        print(f"### Processing {grp_nm}...")
                        with SyncTimer() as t:                        
                            self.process_queue(workQueue, self.task_runner, self.schedule_sync)
                        print(f"### {grp_nm} completed in {str(t)}...")

                self.Metastore.process_load_group_telemetry(group_schedule_id)
       
        print(f"Async schedule sync completed in {str(tt)}...")

        return initial_loads