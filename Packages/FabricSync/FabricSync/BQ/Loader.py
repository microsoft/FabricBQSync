from pyspark.sql import SparkSession, DataFrame, Row, Observation
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

from datetime import datetime, timezone
from typing import Tuple
from threading import Lock

from .Model.Config import *
from .Core import *
from .Admin.DeltaTableUtility import *
from .Enum import *
from .Model.Schedule import SyncSchedule
from .Model.Query import *
from .SyncUtils import *
from .Logging import *
from .Exceptions import *

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
        self.TableLocks = ThreadSafeDict()

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

    def merge_table(self, schedule:SyncSchedule, tableName:str, df:DataFrame) -> tuple:
        if schedule.FlattenTable:
            df, flattened = self.flatten(schedule=schedule, df=df)

            if flattened:
                self.merge_dataframe(schedule, f"{schedule.LakehouseTableName}_flattened", flattened)

        self.merge_dataframe(schedule, tableName, df)

        results = self.get_delta_merge_row_counts(schedule)
        schedule.UpdateRowCounts(insert=results[0], update=results[1])
        
        return (schedule, df)

    def merge_dataframe(self, schedule:SyncSchedule, tableName:str, df:DataFrame):
        """
        Merge into Lakehouse Table based on User Configuration. Only supports Insert/Update All
        """
        self.Context.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")

        constraints = []

        for p in schedule.Keys:
            constraints.append(f"s.{p} = d.{p}")

        if not constraints:
            raise SyncConfigurationError("One or more keys must be specified for a MERGE operation")
        
        if schedule.FabricPartitionColumns and schedule.PartitionId:
            for p in schedule.FabricPartitionColumns:
                constraints.append(f"d.{p} = '{schedule.PartitionId}'")

        predicate = " AND ".join(constraints)

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        dest = DeltaTable.forName(self.Context, tableOrViewName=tableName)

        dest.alias('d') \
        .merge( \
            df.alias('s'), \
            predicate) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

    def get_bq_table(self, schedule:SyncSchedule) -> tuple[SyncSchedule, DataFrame, Observation]:
        qm = {
            "ProjectId": schedule.ProjectId,
            "Dataset": schedule.Dataset,
            "TableName": schedule.BQTableName,
            "Predicate": []
        }
        query_model = BQQueryModel(**qm)

        if schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None:
            part_format = SyncUtil.get_bq_partition_id_format(schedule.PartitionGrain)

            if schedule.PartitionDataType == str(BQDataType.TIMESTAMP):                  
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            query_model.PartitionFilter = part_filter
        elif schedule.IsRangePartitioned:
            part_filter = SyncUtil.get_partition_range_predicate(schedule)
            query_model.PartitionFilter = part_filter
        else:
            if schedule.Load_Strategy == str(LoadStrategy.WATERMARK) and not schedule.InitialLoad:
                if schedule.MaxWatermark.isdigit():
                    predicate = f"{schedule.WatermarkColumn} > {schedule.MaxWatermark}"
                else:
                    predicate = f"{schedule.WatermarkColumn} > '{schedule.MaxWatermark}'"

                query_model.add_predicate(predicate)
        
        if schedule.SourceQuery:
            query_model.Query = schedule.SourceQuery
                
        if schedule.SourcePredicate:
            query_model.add_predicate(schedule.SourcePredicate)

        if self.UserConfig.Optimization.UseApproximateRowCounts:
            query_model.Cached = False

            observation = Observation(name="BQSyncMetricsObservation")
        else:
            observation = None

        df_bq = self.read_bq_to_dataframe(query_model)

        if observation:
            if schedule.Load_Strategy == str(LoadStrategy.WATERMARK) and schedule.WatermarkColumn:
                df_bq = df_bq.observe(observation, count(lit(1)).alias("row_count"),
                    max(col(schedule.WatermarkColumn)).alias("watermark"))
            else:
                df_bq = df_bq.observe(observation, count(lit(1)).alias("row_count"))

        if schedule.IsPartitioned and not schedule.LakehousePartition:
            if schedule.Partition_Type == str(PartitionType.TIME):

                proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                schedule.FabricPartitionColumns = SyncUtil.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                if schedule.IsTimeIngestionPartitioned:
                    df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                df_bq = SyncUtil.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, proxy_cols)
            else:
                schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                df_bq = SyncUtil.create_fabric_range_partition(self.Context, df_bq, schedule)
        
        return (schedule, df_bq, observation)

    def save_bq_dataframe(self, schedule:SyncSchedule, df_bq:DataFrame, lock:Lock, observation:Observation = None, write_config=None) -> SyncSchedule:
        if not schedule.Load_Type == str(LoadType.MERGE) or schedule.InitialLoad:
            if schedule.IsPartitionedSyncLoad:
                has_lock = False

                if schedule.InitialLoad or schedule.LakehousePartition:
                    has_lock = True
                    lock.acquire()
                else:
                    write_config["partitionOverwriteMode"] = "dynamic"
                
                try:
                    partition_cols = self.get_lakehouse_partitions(schedule)
                    df_bq = self.save_to_lakehouse(schedule, df=df_bq, moder=str(LoadType.OVERWRITE), 
                        partition_by=partition_cols, options=write_config)
                finally:
                    if has_lock:
                        lock.release()
            else:
                if not schedule.FabricPartitionColumns and not schedule.LakehousePartition:
                    df_bq = self.save_to_lakehouse(schedule, df=df_bq, options=write_config)
                else:
                    partition_cols = self.get_lakehouse_partitions(schedule)
                    df_bq = self.save_to_lakehouse(schedule, df=df_bq, partition_by=partition_cols, options=write_config)
        else:
            schedule,df_bq = self.merge_table(schedule, schedule.LakehouseTableName, df_bq)

        return (schedule,df_bq)

    def flatten(self, schedule:SyncSchedule, df:DataFrame) -> tuple:
        if schedule.Load_Type == str(LoadType.MERGE) and schedule.ExplodeArrays:
            raise SyncConfigurationError("Invalid load configuration: Merge is not supported when Explode Arrays is enabed")
                
        if schedule.FlattenInPlace:
            df = SyncUtil.flatten_df(schedule.ExplodeArrays, df)
            flattend = None
        else:
            flattened = SyncUtil.flatten_df(schedule.ExplodeArrays, df)
        
        return (df, flattened)

    def save_to_lakehouse(self, schedule:SyncSchedule, df:DataFrame, mode:str=None, partition_by:list=None, options:dict=None):
        if not mode:
            mode = schedule.Mode

        if schedule.FlattenTable:
            df, flattened = self.flatten(schedule=schedule, df=df)

            if flattened:
                SyncUtil.self.save_dataframe(table_name=f"{schedule.LakehouseTableName}_flattened", df=flattened, 
                        mode=mode, partition_by=partition_by, options=options)
        
        SyncUtil.save_dataframe(table_name=schedule.LakehouseTableName, df=df, mode=mode, partition_by=partition_by, options=options)

        return df

    def get_source_metrics(self, schedule:SyncSchedule, df_bq:DataFrame, observation:Observation = None):
        row_count = 0
        watermark = None

        if self.UserConfig.Optimization.UseApproximateRowCounts and observation:
            observations = observation.get
            row_count = observations["row_count"]

            if "watermark" in observations:
                watermark = SyncUtil.format_watermark(observations["watermark"])
        else:
            if schedule.Load_Strategy == str(LoadStrategy.WATERMARK):
                df = df_bq.select(max(col(schedule.WatermarkColumn)).alias("watermark"), count("*").alias("row_count"))

                row = df.first()
                row_count = row["row_count"]
                watermark = SyncUtil.format_watermark(row["watermark"])
            else:
                df = df_bq.select(count("*").alias("row_count"))

                row = df.first()
                row_count = row["row_count"]
            
        return (row_count, watermark)

    def get_lakehouse_partitions(self, schedule:SyncSchedule):
        if schedule.LakehousePartition:
            return schedule.LakehousePartition.split(",")
        else:
            return schedule.FabricPartitionColumns

    def transform(self, schedule:SyncSchedule, df:DataFrame):
        df = self.map_columns(schedule, df)

        return (schedule, df)

    def map_columns(self, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        maps = schedule.get_column_map()

        if maps:
            for m in maps:
                df = SyncUtil.map_column(m, df)
        
        return df 

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
            try:
                schedule, df_bq, observation = self.get_bq_table(schedule)
            except Exception as e:
                raise SyncLoadError(msg="Failed to retrieve table from BQ", data=schedule) from e

            #Transform
            try:
                schedule, df_bq = self.transform(schedule, df_bq)
            except Exception as e:
                raise SyncLoadError(msg="Transformation failed during sync", data=schedule) from e

            #On initial load, force drop the table to ensure a clean load
            if schedule.InitialLoad and not schedule.IsPartitionedSyncLoad:
                self.Context.sql(f"DROP TABLE IF EXISTS {schedule.LakehouseTableName}")
                
            #Save BQ table to Lakehouse
            try:
                table_maint = DeltaTableMaintenance(self.Context, schedule.LakehouseTableName)
                write_config = { }

                #Schema Evolution
                if not schedule.InitialLoad:
                    if schedule.AllowSchemaEvolution:                        
                        table_maint.evolve_schema(df_bq)
                        write_config["mergeSchema"] = True

                schedule,df_bq = self.save_bq_dataframe(schedule, df_bq, lock, observation, write_config)

                if schedule.InitialLoad:
                    with lock:
                        table_maint.set_default_table_properties()

                src_cnt, schedule.MaxWatermark = self.get_source_metrics(schedule, df_bq)

                schedule.UpdateRowCounts(src=src_cnt)    
                schedule.SparkAppId = self.Context.sparkContext.applicationId
                schedule.DeltaVersion = table_maint.CurrentTableVersion
                schedule.EndTime = datetime.now(timezone.utc)
                schedule.Status = SyncStatus.COMPLETE
                
                df_bq.unpersist()
            except Exception as e:
                raise FabricLakehouseError(msg="Error writing BQ table to Lakehouse", data=schedule) from e

        self.show_sync_status(schedule, message="FINISHED", status=f"in {str(t)}")

        return schedule

    def show_sync_status(self, schedule:SyncSchedule, message:str, status:str=None):
        msg_header = f"{message} {schedule.ObjectType} {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}"

        if schedule.PartitionId:
            msg_header = f"{msg_header}${schedule.PartitionId}"

        if not status:
            self.Logger.sync_status(f"{msg_header}...")
        else:
            self.Logger.sync_status(f"{msg_header} {status}...")

    def save_schedule_telemetry(self, schedule:SyncSchedule):
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        """
        rdd = self.Context.sparkContext.parallelize([Row( 
            schedule_id=schedule.ScheduleId, 
            sync_id=schedule.SyncId, 
            project_id=schedule.ProjectId, 
            dataset=schedule.Dataset, 
            table_name=schedule.TableName, 
            partition_id=schedule.PartitionId, 
            status=str(schedule.Status), 
            started=schedule.StartTime, 
            completed=schedule.EndTime, 
            src_row_count=schedule.SourceRows, 
            inserted_row_count=schedule.InsertedRows, 
            updated_row_count=schedule.UpdatedRows, 
            delta_version=schedule.DeltaVersion, 
            spark_application_id=schedule.SparkAppId, 
            max_watermark=schedule.MaxWatermark, 
            summary_load=schedule.SummaryLoadType, 
            source_query=schedule.SourceQuery, 
            source_predicate=schedule.SourcePredicate 
        )])

        self.Metastore.save_schedule_telemetry(rdd)

    def run_sequential_schedule(self, sync_id:str, schedule_type:str) -> bool:
        self.Logger.sync_status(f"Sequential schedule sync starting...")

        initial_loads = False

        with SyncTimer() as t:
            df_schedule = self.Metastore.get_schedule(sync_id, schedule_type)           

            for row in df_schedule.collect():
                d = row.asDict()
                schedule = SyncSchedule(**d)
                schedule.StartTime = datetime.now(timezone.utc)

                if schedule.InitialLoad:
                    initial_loads = True

                self.schedule_sync(schedule)

            self.Logger.sync_status("Processing Sync Telemetry...")
            self.Metastore.process_load_group_telemetry(sync_id, schedule_type)

        self.Logger.sync_status(f"Sequential schedule sync completed in {str(t)}...")
        return initial_loads

    @Telemetry.Sync_Load    
    def schedule_sync(self, schedule:SyncSchedule, lock=None) -> SyncSchedule:
        schedule.StartTime = datetime.now(timezone.utc)
        schedule = self.sync_bq_table(schedule, lock)
        self.save_schedule_telemetry(schedule) 

        return schedule

    def schedule_sync_wrapper(self, value) -> SyncSchedule:
        schedule = value[2]
        lock = self.TableLocks.get_or_set(schedule.TableName, Lock())
        return self.schedule_sync(schedule, lock)

    def thread_exception_handler(self, value):
        schedule = value[2]

        schedule.Status = SyncStatus.FAILED
        schedule.SummaryLoadType = f"ERROR: {e}"
        self.save_schedule_telemetry(schedule) 
        logging.error(msg=f"ERROR with {schedule.ProjectId}.{schedule.Dataset}.{schedule.TableName}: {e}")

    def run_schedule(self, sync_id:str, schedule_type:str) -> bool:
        if self.UserConfig.Async.Enabled:
            return self.run_async_schedule(sync_id, schedule_type)
        else:
            return self.run_sequential_schedule(sync_id, schedule_type)

    def run_async_schedule(self, sync_id:str, schedule_type:str) -> bool:
        self.Logger.sync_status(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...")

        initial_loads = False

        with SyncTimer() as t:
            processor = QueueProcessor(num_threads=self.UserConfig.Async.Parallelism)

            schedule = self.Metastore.get_schedule(sync_id, schedule_type)

            load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

            if load_grps:
                for grp in load_grps:
                    grp_nm = "LOAD GROUP {0}".format(grp)
                    grp_df = schedule.where(f"priority = '{grp}'")

                    for tbl in grp_df.collect():
                        d = tbl.asDict()
                        s = SyncSchedule(**d)
                        nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                        if s.PartitionId is not None:
                            nm = "{0}${1}".format(nm, s.PartitionId)        

                        if s.InitialLoad:
                            initial_loads = True

                        priority = s.Priority + tbl["size_priority"]
                        processor.put((priority, nm, s))

                    if not processor.empty():
                        self.Logger.sync_status(f"### Processing {grp_nm}...")

                        with SyncTimer() as t:                        
                            processor.process(self.schedule_sync_wrapper, self.thread_exception_handler)

                        if not processor.has_exceptions:
                            self.Logger.sync_status(f"### {grp_nm} completed in {str(t)}...")
                        else:
                            self.Logger.sync_status(f"### {grp_nm} FAILED...")
                            break

                self.Logger.sync_status("Processing Sync Telemetry...")
                self.Metastore.process_load_group_telemetry(sync_id, schedule_type)
       
        self.Logger.sync_status(f"Async schedule sync finished in {str(t)}...")

        return initial_loads