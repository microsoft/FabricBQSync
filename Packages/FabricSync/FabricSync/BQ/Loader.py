from pyspark.sql import (
    DataFrame, Observation
)
from pyspark.sql.functions import (
    col, lit, count, max
)
from datetime import (
    datetime, timezone, timedelta
)
from typing import (
    Tuple, Dict
)
from threading import Lock
from delta.tables import DeltaTable

from FabricSync.BQ.Utils import SyncTimer
from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Model.Core import BQQueryModel
from FabricSync.BQ.SyncUtils import SyncUtil
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.Mirror import OpenMirror
from FabricSync.BQ.Exceptions import (
    SyncConfigurationError, SyncLoadError, FabricLakehouseError
)
from FabricSync.BQ.Threading import (
    QueueProcessor, ThreadSafeDict, SparkProcessor
)
from FabricSync.BQ.SyncCore import ConfigBase
from FabricSync.BQ.Enum import (
    SyncLoadType, SyncLoadStrategy, SyncStatus, FabricDestinationType, BQDataType, BQPartitionType
)
from FabricSync.BQ.Core import DeltaTableMaintenance

class BQScheduleLoader(ConfigBase):    
    def __init__(self) -> None:
        """
        BQ Schedule Loader
        """
        super().__init__()

        self.TableLocks:ThreadSafeDict = None
        self.MultiWrite:ThreadSafeDict = None

    def __get_delta_merge_row_counts(self, schedule:SyncSchedule, telemetry:DataFrame) -> Tuple[int, int, int]:
        """
        Retrieves the counts of inserted, updated, and deleted rows from the current day's MERGE operations.
        This function filters the provided telemetry DataFrame for MERGE operations that occurred today,
        in descending order by version. If partition-based operations are configured, it will validate
        the partition predicate based on the schedule's settings before reading the operation metrics.
        It returns the first set of non-zero row counts for inserted, updated, and deleted rows.
        Args:
            schedule (SyncSchedule): Holds the synchronization schedule and partition details.
            telemetry (DataFrame): A Spark DataFrame containing telemetry logs with 'operation',
                'timestamp', 'version', 'operationParameters', and 'operationMetrics' columns.
        Returns:
            Tuple[int, int, int]: The number of inserted, updated, and deleted rows.
        """

        telemetry = telemetry.filter("operation = 'MERGE' AND CAST(timestamp AS DATE) = current_date()") \
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

                break

        return (inserts, updates, deletes)

    def __merge_table(self, schedule:SyncSchedule, tableName:str, df:DataFrame) -> Tuple[SyncSchedule, DataFrame]:
        """
        Merges the given Spark DataFrame into a Delta table, optionally flattening the data if
        indicated by the schedule. Once the merge is completed, row insert and update counts
        are recorded in the schedule before returning the updated schedule and DataFrame.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing settings for
                flattening, table paths, and row count updates.
            tableName (str): The name of the table being merged.
            df (DataFrame): The Spark DataFrame to merge with the target table.
        Returns:
            Tuple[SyncSchedule, DataFrame]: A tuple containing the updated schedule and the
            potentially modified DataFrame.
        """

        if schedule.FlattenTable:
            df, flattened = SyncUtil.flatten(schedule=schedule, df=df)

            if flattened:
                self.__merge_dataframe(schedule, f"{schedule.LakehouseTableName}_flattened", flattened)

        delta_dest = DeltaTable.forPath(self.Context, schedule.LakehouseAbfssTablePath)

        self.__merge_dataframe(schedule, delta_dest, df)

        results = self.__get_delta_merge_row_counts(schedule, delta_dest.history())
        schedule.UpdateRowCounts(insert=results[0], update=results[1])
        
        return (schedule, df)

    def __merge_dataframe(self, schedule:SyncSchedule, delta_dest:DeltaTable, df:DataFrame) -> None:
        """
        Merge into Lakehouse Table based on User Configuration. Only supports Insert/Update All
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the merge settings.
            delta_dest (DeltaTable): The Delta table to merge into.
            df (DataFrame): The DataFrame to merge.
        Returns:
            None
        """
        self.Context.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")

        constraints = []

        if schedule.Keys:
            constraints.extend([f"s.{p} = d.{p}" for p in schedule.Keys])
        else:
            raise SyncConfigurationError("One or more keys must be specified for a MERGE operation")
        
        if schedule.FabricPartitionColumns and schedule.PartitionId:
            partition_constraint = SyncUtil.resolve_fabric_partition_predicate(schedule.Partition_Type,schedule.PartitionColumn,
                schedule.PartitionGrain, schedule.PartitionId, "d")
            self.Logger.sync_status(f"Merge with partition constraint: {partition_constraint}", verbose=True)
            constraints.append(partition_constraint)

        predicate = " AND ".join(constraints)

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        if schedule.Load_Strategy == SyncLoadStrategy.CDC:
            df = self.__get_cdc_latest_changes(schedule, df)

            delta_dest.alias('d').merge(
                    df.alias('s'), predicate
                ).whenMatchedDelete(
                    condition = "s.BQ_CDC_CHANGE_TYPE = 'DELETE'"
                ).whenMatchedUpdate(
                    set = self.__get_cdc_merge_set(schedule.TableColumns, "s")
                ).whenNotMatchedInsert(
                    condition = "s.BQ_CDC_CHANGE_TYPE != 'DELETE'",
                    values = self.__get_cdc_merge_set(schedule.TableColumns, "s")
                ).execute()
        else:
            delta_dest.alias('d').merge(df.alias('s'), predicate) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll().execute()

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

    def __get_cdc_merge_set(self, columns:str, alias:str) -> Dict[str, str]:
        """
        Builds a dictionary of column expressions for a CDC merge operation.
        This function constructs a dictionary of column expressions for a CDC merge operation
        based on the provided column names and alias. It returns the dictionary of column
        expressions for the merge operation.
        Args:
            columns (str): A comma-separated string of column names to merge.
            alias (str): The alias to use for the column names in the merge operation.
        Returns:
            Dict[str, str]: A dictionary of column expressions for the merge operation.
        """
        return {c: f"{alias}.{c}" for c in columns.split(",")}

    def __get_cdc_latest_changes(self, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        """
        Retrieves the latest changes from a CDC DataFrame based on the provided SyncSchedule.
        This function calculates the row number for each key group in the DataFrame, filters
        the DataFrame to only include the latest changes, and returns the filtered DataFrame.
        Args:
            schedule (SyncSchedule): An object that holds information about the sync configuration,
                including load strategy, table columns, and keys.
            df (DataFrame): The DataFrame to filter for the latest changes.
        Returns:
            DataFrame: A filtered DataFrame containing only the latest changes for each key group.
        """
        keys = ",".join(schedule.Keys)

        row_num_expr = f"row_number() over(partition by {keys} order by BQ_CDC_CHANGE_TIMESTAMP desc) as cdc_row_num"

        df = df.selectExpr("*", row_num_expr).filter(col("cdc_row_num") == 1) \
            .drop(col("cdc_row_num"), col("BQ_CDC_CHANGE_TIMESTAMP"))
        
        return df
    
    def __build_cdc_query(self, schedule:SyncSchedule) -> str:
        """
        Builds a CDC (Change Data Capture) or full load query string for BigQuery based on the provided SyncSchedule.
        This function calculates the time window for data capture, constructs a BigQuery SQL query using either
        CHANGES or APPENDS functions depending on whether a CDC load strategy is used, and returns the final
        query string. The time window is defined by the difference between the current timestamp and the
        schedule's watermark timestamp, adjusting logic for CDC or a full seven-day window as necessary.
        Args:
            schedule (SyncSchedule): An object that holds information about the sync configuration,
                including load strategy, table columns, BigQuery table name, and current maximum
                watermark.
        Returns:
            str: A fully constructed SQL query for retrieving changes from the specified BigQuery table
            within the calculated time interval, incorporating either CDC or full load semantics.
        """

        seven_days_seconds = 7*24*60*60
        twenty_four_hours_seconds = 24*60*60
        ten_minutes_seconds = 10*60

        cdc = (schedule.Load_Strategy == SyncLoadStrategy.CDC)
        watermark_ts = datetime.fromisoformat(schedule.MaxWatermark)

        if not self.is_timezone_aware(watermark_ts):
            watermark_ts = watermark_ts.replace(tzinfo=timezone.utc)

        now_utc = datetime.now(timezone.utc)

        window_seconds = (twenty_four_hours_seconds - ten_minutes_seconds) if cdc else seven_days_seconds
        cdc_time_remaining = window_seconds - (now_utc-watermark_ts).total_seconds()

        window_end = None
        if cdc_time_remaining < 0:
            if cdc:
                window_end = watermark_ts + timedelta(seconds=window_seconds - 1)
            else:
                window_end = watermark_ts + timedelta(seconds=window_seconds - 1)

        sql_string = "SELECT _CHANGE_TYPE AS BQ_CDC_CHANGE_TYPE, _CHANGE_TIMESTAMP AS BQ_CDC_CHANGE_TIMESTAMP, "
        sql_string += f"{schedule.TableColumns} FROM "
        sql_string += f"CHANGES(TABLE `{schedule.BQTableName}`,TIMESTAMP_ADD(TIMESTAMP'{schedule.MaxWatermark}',INTERVAL 1 SECOND)," \
            if cdc else f"APPENDS(TABLE `{schedule.BQTableName}`,TIMESTAMP_ADD(TIMESTAMP'{schedule.MaxWatermark}',INTERVAL 1 SECOND),"
        
        if window_end:
            sql_string += f"TIMESTAMP'{window_end.isoformat()}')" 
        else:
            sql_string += "TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 601 SECOND))" if cdc else "NULL)"

        return sql_string

    def is_timezone_aware(self, timestamp):
        """
        Check if the specified timestamp object is timezone-aware.
        Args:
            timestamp (datetime): The datetime object to check.
        Returns:
            bool: True if the timestamp has a timezone set (with non-null offset), 
            otherwise returns False.
        """
        return timestamp.tzinfo is not None and timestamp.tzinfo.utcoffset(timestamp) is not None

    def __get_bq_table(self, schedule:SyncSchedule) -> Tuple[SyncSchedule, DataFrame, Observation]:
        """
        Retrieves data from a BigQuery table or query based on the provided SyncSchedule.
        This method dynamically applies partition or watermark filters, constructs a query,
        and loads data from BigQuery into a Spark DataFrame. It also optionally sets up
        an Observation object for monitoring row counts and watermark values.
        Args:
            schedule (SyncSchedule): The synchronization metadata, including partitioning
                strategies, load strategies, and optional source query or predicates.
        Returns:
            Tuple[SyncSchedule, DataFrame, Observation]:
                A tuple containing the updated SyncSchedule, the loaded Spark DataFrame,
                and an optional Observation object (None if not used).
        """

        qm = {
            "ProjectId": schedule.ProjectId,
            "Dataset": schedule.Dataset,
            "TableName": schedule.BQTableName,
            "Predicate": []
        }
        query_model = BQQueryModel(**qm)

        if schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None:
            self.Logger.sync_status(f"Loading {schedule.LakehouseTableName} with time ingestion...", verbose=True)
            part_format = SyncUtil.get_bq_partition_id_format(schedule.PartitionGrain)
            if schedule.PartitionDataType == BQDataType.TIMESTAMP:        
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            self.Logger.sync_status(f"Load from BQ by time partition: {part_filter}", verbose=True)
            query_model.PartitionFilter = part_filter
        elif schedule.IsRangePartitioned:
            self.Logger.sync_status(f"Loading {schedule.LakehouseTableName} with range partitioning...", verbose=True)
            part_filter = SyncUtil.get_partition_range_predicate(schedule)
            self.Logger.sync_status(f"Load from BQ by range partition: {part_filter}", verbose=True)
            query_model.PartitionFilter = part_filter
        elif not schedule.InitialLoad:
            if schedule.Load_Strategy == SyncLoadStrategy.WATERMARK:
                self.Logger.sync_status(f"Loading {schedule.LakehouseTableName} with watermark...", verbose=True)
                if schedule.MaxWatermark.isdigit():
                    predicate = f"{schedule.WatermarkColumn} > {schedule.MaxWatermark}"
                else:
                    predicate = f"{schedule.WatermarkColumn} > '{schedule.MaxWatermark}'"

                self.Logger.sync_status(f"Load from BQ with watermark: {predicate}", verbose=True)
                query_model.add_predicate(predicate)
            elif schedule.IsCDCStrategy and schedule.MaxWatermark:
                cdc_query = self.__build_cdc_query(schedule)
                self.Logger.sync_status(f"Load {schedule.LakehouseTableName} with CDC: {cdc_query}", verbose=True)
                query_model.Query = cdc_query
        
        if schedule.SourceQuery and not query_model.Query:
            self.Logger.sync_status(f"Overriding table. Load from BQ with source query: {schedule.SourceQuery}", verbose=True)
            query_model.Query = schedule.SourceQuery
                
        if schedule.SourcePredicate:
            self.Logger.sync_status(f"Applying table predicate: {schedule.SourcePredicate}", verbose=True)
            query_model.add_predicate(schedule.SourcePredicate)

        if self.__use_dataframe_observation(schedule):
            query_model.Cached = False
            observation = Observation(name=f"OB_{schedule.TableId}_{schedule.PartitionId}")
        else:
            query_model.Cached = (self.UserConfig.Optimization.DisableDataframeCache == False)
            observation = None

        df_bq = self.read_bq_to_dataframe(query_model)

        if observation:
            if schedule.IsCDCStrategy and not schedule.InitialLoad:
                self.Logger.sync_status(f"{schedule.LakehouseTableName} - CDC Observation", verbose=True)
                df_bq = df_bq.observe(observation, 
                    count(lit(1)).alias("row_count"),
                    max(col("BQ_CDC_CHANGE_TIMESTAMP")).alias("watermark"))
            elif schedule.Load_Strategy == SyncLoadStrategy.WATERMARK and schedule.WatermarkColumn:
                self.Logger.sync_status(f"{schedule.LakehouseTableName} - Observation Watermark: {schedule.WatermarkColumn}", verbose=True)
                df_bq = df_bq.observe(
                    observation, 
                    count(lit(1)).alias("row_count"),
                    max(col(schedule.WatermarkColumn)).alias("watermark"))
            else:
                self.Logger.sync_status(f"{schedule.LakehouseTableName} - Observation Count Only", verbose=True)
                df_bq = df_bq.observe(observation, count(lit(1)).alias("row_count"))

        if not schedule.IsMirrored:
            #Ignore BQ partitioning, open-mirror does not support partitioning - 1/2025
            if schedule.IsPartitioned and not schedule.LakehousePartition:
                if schedule.Partition_Type == BQPartitionType.TIME:

                    proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                    schedule.FabricPartitionColumns = SyncUtil.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                    if schedule.IsTimeIngestionPartitioned:
                        df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                    df_bq = SyncUtil.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, proxy_cols)
                else:
                    schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                    df_bq = SyncUtil.create_fabric_range_partition(self.Context, df_bq, schedule)
        
        return (schedule, df_bq, observation)

    def __use_dataframe_observation(self, schedule:SyncSchedule) -> bool:
        """
        Determines whether to use a DataFrame observation for monitoring row counts and watermarks.
        This function checks the user configuration for the approximate row count optimization setting
        and the sync schedule for disallowed load strategies and types. If approximate row counts are
        enabled and the schedule's load strategy and type are allowed, this function returns True.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the load strategy and type.
        Returns:
            bool: True if approximate row counts are enabled and the schedule's load strategy and type
            are allowed; False otherwise.
        """
        disallowed_strategies = [SyncLoadStrategy.CDC]
        disallowed_types = [SyncLoadType.MERGE]

        if self.UserConfig.Optimization.UseApproximateRowCounts:
            if schedule.Load_Strategy not in disallowed_strategies and schedule.Load_Type not in disallowed_types:
                return True
        
        return False

    def __save_bq_to_lakehouse(self, schedule:SyncSchedule, df_bq:DataFrame, 
                            lock:Lock, write_config=None) -> Tuple[SyncSchedule, DataFrame]:
        """
        Saves a DataFrame to a Lakehouse table based on the provided SyncSchedule.
        This method handles the logic for saving a DataFrame to a Lakehouse table, including
        partitioning, schema evolution, and overwrite/append strategies. It also manages the
        acquisition and release of a lock for partitioned sync loads and returns the updated
        SyncSchedule and DataFrame.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the Lakehouse table details.
            df_bq (DataFrame): The DataFrame to save to the Lakehouse table.
            lock (Lock): A threading lock for partitioned sync loads.
            write_config (dict): A dictionary of write options for the DataFrame save operation.
        Returns:
            Tuple[SyncSchedule, DataFrame]: A tuple containing the updated SyncSchedule and the potentially
            modified DataFrame after saving to the Lakehouse table.
        """
        if not schedule.Load_Type == SyncLoadType.MERGE or schedule.InitialLoad:
            df_bq, observation = self.__apply_task_tracker_observation(schedule, df_bq)

            if schedule.IsPartitionedSyncLoad:
                has_lock = False
                
                if schedule.TotalTableTasks > 1:
                    has_lock = True
                    lock.acquire()

                if not schedule.InitialLoad and not schedule.LakehousePartition:
                    write_config["partitionOverwriteMode"] = "dynamic"
                
                try:
                    partition_cols = SyncUtil.get_lakehouse_partitions(schedule)
                    df_bq = self.__save_to_lakehouse(schedule, df=df_bq, mode="OVERWRITE", partition_by=partition_cols, options=write_config)
                    task_part, total_row_count = self.__increment_task_tracker(schedule.LakehouseTableName, observation)                    
                finally:
                    if has_lock:
                        lock.release()
            else:
                if schedule.Load_Strategy == SyncLoadStrategy.CDC_APPEND:
                    df_bq = df_bq.drop("BQ_CDC_CHANGE_TYPE", "BQ_CDC_CHANGE_TIMESTAMP")

                if not schedule.FabricPartitionColumns and not schedule.LakehousePartition:
                    df_bq = self.__save_to_lakehouse(schedule, df=df_bq, options=write_config)
                else:
                    partition_cols = SyncUtil.get_lakehouse_partitions(schedule)
                    df_bq = self.__save_to_lakehouse(schedule, df=df_bq, partition_by=partition_cols, options=write_config)
                
                task_part, total_row_count = self.__increment_task_tracker(schedule.LakehouseTableName, observation)
            
            if task_part == schedule.TotalTableTasks:
                self.Logger.sync_status(f"LAKEHOUSE - {schedule.LakehouseTableName} - Rows: {total_row_count}", verbose=True)

                if total_row_count == 0:
                    self.Logger.sync_status(f"LAKEHOUSE - Empty Sync - {schedule.LakehouseTableName}", verbose=True)
                    schedule.IsEmpty = False
        else:
            schedule,df_bq = self.__merge_table(schedule, schedule.LakehouseTableName, df_bq)

        return (schedule,df_bq)

    def __apply_task_tracker_observation(self, schedule:SyncSchedule, df:DataFrame) -> Tuple[DataFrame, Observation]:
        """
        Applies an Observation object to a DataFrame for monitoring row counts.
        This method applies an Observation object to a DataFrame for monitoring row counts
        and returns the updated DataFrame after applying the observation.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the Lakehouse table details.
            df (DataFrame): The DataFrame to apply the observation to.
        Returns:
            Tuple[DataFrame, Observation]: A tuple containing the updated DataFrame after applying
            the observation and the Observation object for monitoring row counts.
        """
        observation = Observation(name=f"Tasks_{schedule.TableId}_{schedule.PartitionId}")
        df = df.observe(observation, count(lit(1)).alias("row_count"))

        return (df, observation)

    def __increment_task_tracker(self, lakehouse_name:str, observation:Observation) -> Tuple[int, int]:
        """
        Increments the task count for a given Lakehouse table.
        This method increments the task count for a given Lakehouse table, updating the task
        count in the shared dictionary and returning the updated task count.
        Args:
            lakehouse_name (str): The name of the Lakehouse table to increment the task count for.
            observation (Observation): The Observation object containing the row count for the task.
        Returns:
            Tuple[int, int]: A tuple containing the updated task count and row count for the task.
        """
        row_count = observation.get["row_count"]

        sync_data = self.MultiWrite.get(lakehouse_name)
        sync_data["tasks"] += 1
        sync_data["observations"] += row_count
        self.MultiWrite.set(lakehouse_name, sync_data)

        return (sync_data["tasks"],sync_data["observations"])

    def __save_to_lakehouse(self, schedule:SyncSchedule, df:DataFrame, mode:str=None, partition_by:list=None, options:dict=None) -> DataFrame:
        """
        Saves a DataFrame to a Lakehouse table based on the provided SyncSchedule.
        This method saves a DataFrame to a Lakehouse table based on the provided SyncSchedule, optionally
        flattening the DataFrame and saving the flattened DataFrame if necessary. It also handles the
        logic for saving to a Lakehouse table, including partitioning, schema evolution, and overwrite/append
        strategies, and returns the saved DataFrame.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the Lakehouse table details.
            df (DataFrame): The DataFrame to save to the Lakehouse table.
            mode (str): The write mode for the DataFrame save operation.
            partition_by (list): A list of partition columns for the DataFrame save operation.
            options (dict): A dictionary of write options for the DataFrame save operation.
        Returns:
            DataFrame: The saved DataFrame after writing to the Lakehouse table.
        """
        if not mode:
            mode = schedule.Mode

        if schedule.FlattenTable:
            df, flattened = SyncUtil.flatten(schedule=schedule, df=df)

            if flattened:
                SyncUtil.save_dataframe(table_name=f"{schedule.LakehouseTableName}_flattened", 
                    path=f"{schedule.LakehouseAbfssTablePath}_flattened",
                    df=flattened, mode=mode, partition_by=partition_by, options=options)
                    
        SyncUtil.save_dataframe(table_name=schedule.LakehouseTableName, path=schedule.LakehouseAbfssTablePath,
            df=df, mode=mode, partition_by=partition_by, options=options)

        return df

    def __process_to_mirror_db(self, schedule:SyncSchedule, df:DataFrame, lock:Lock) -> Tuple[SyncSchedule, DataFrame]:
        """
        Processes a DataFrame for saving to a mirrored database landing zone.
        This method saves a DataFrame to a mirrored database landing zone based on the provided SyncSchedule,
        optionally flattening the DataFrame and saving the flattened DataFrame if necessary. It also handles
        the logic for saving to a mirrored database landing zone, including partitioning, schema evolution,
        and overwrite/append strategies, and returns the saved DataFrame.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the mirrored database details.
            df (DataFrame): The DataFrame to save to the mirrored database landing zone.
            lock (Lock): A threading lock for partitioned sync loads.
        Returns:
            Tuple[SyncSchedule, DataFrame]: A tuple containing the updated SyncSchedule and the potentially
            modified DataFrame after saving to the mirrored database landing zone.
        """
        has_lock = False

        if schedule.TotalTableTasks > 1:
            has_lock = True
            lock.acquire()

        try:
            df, observation = self.__apply_task_tracker_observation(schedule, df)
            df = OpenMirror.save_to_landing_zone(schedule, df)
            task_part, total_row_count = self.__increment_task_tracker(schedule.LakehouseTableName, observation)

            if task_part == schedule.TotalTableTasks:
                self.Logger.sync_status(f"MIRRORING - {schedule.LakehouseTableName} - Rows: {total_row_count}", verbose=True)

                if total_row_count > 0:
                    schedule.MirrorFileIndex = OpenMirror.process_landing_zone(schedule)
                    self.Logger.sync_status(f"MIRRORING - Next File Index for {schedule.LakehouseTableName}: {schedule.MirrorFileIndex}", verbose=True)
                else:
                    self.Logger.sync_status(f"MIRRORING - Empty Sync - {schedule.LakehouseTableName}", verbose=True)
                    schedule.IsEmpty = True
                    OpenMirror.cleanup_spark_files(schedule)
        finally:
            if has_lock:
                lock.release()
        
        return (schedule, df)

    def __sync_bq_table(self, schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
        """
        Synchronizes a BigQuery table with a Lakehouse table based on the provided SyncSchedule.
        This method retrieves a BigQuery table, transforms the data, saves the data to a Lakehouse table,
        and updates the SyncSchedule with row counts, watermarks, and status information. It also handles
        the logic for schema evolution, partitioning, and mirroring to a database landing zone, and returns
        the updated SyncSchedule after the synchronization process is complete.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the BigQuery and Lakehouse details.
            lock (Lock): A threading lock for partitioned sync loads.
        Returns:
            SyncSchedule: The updated SyncSchedule after the synchronization process is complete.
        Raises:
            SyncLoadError: If an error occurs during the synchronization process.
            FabricLakehouseError: If an error occurs during the Lakehouse write process.
        """
        with SyncTimer() as t:
            schedule.SummaryLoadType = schedule.get_summary_load_type()
            self.__show_sync_status(schedule)

            #Drop Mirrored Table if OVERWRITE
            if schedule.IsMirrored and not schedule.InitialLoad and schedule.Load_Type == SyncLoadType.OVERWRITE:
                self.Logger.sync_status(f"Overwriting current mirrored table: {schedule.LakehouseTableName}...", verbose=True)
                OpenMirror.drop_mirrored_table(schedule)
                schedule.MirrorFileIndex = 1
                
            #Get BQ table using sync config
            try:
                schedule, df_bq, observation = self.__get_bq_table(schedule)
            except Exception as e:
                raise SyncLoadError(msg=f"Failed to retrieve table from BQ: {e}", data=schedule)

            #Transform
            try:
                schedule, df_bq = SyncUtil.transform(schedule, df_bq)
            except Exception as e:
                raise SyncLoadError(msg=f"Transformation failed during sync: {e}", data=schedule)

            #Save BQ table
            try:              
                if not schedule.IsMirrored:
                    write_config = { }

                    #Schema Evolution
                    if schedule.AllowSchemaEvolution and not schedule.InitialLoad:
                        if schedule.Load_Type == SyncLoadType.OVERWRITE:
                            write_config["overwriteSchema"] = True
                        else:
                            write_config["mergeSchema"] = True

                    schedule,df_bq = self.__save_bq_to_lakehouse(schedule, df_bq, lock, write_config)
                    
                    table_maint = DeltaTableMaintenance(schedule.LakehouseTableName, schedule.LakehouseAbfssTablePath)
                    schedule.DeltaVersion = table_maint.CurrentTableVersion
                else:
                    self.Logger.sync_status(f"Writing {schedule.LakehouseTableName} data to mirrored DB landing zone", verbose=True)
                    schedule, df_bq = self.__process_to_mirror_db(schedule, df_bq, lock)

                if not schedule.IsEmpty:
                    src_cnt, watermark = SyncUtil.get_source_metrics(schedule, df_bq, observation)

                    if not watermark:
                        if schedule.IsCDCStrategy:
                            self.Logger.sync_status(f"No watermark defined, using BQ Table Last Modified: {schedule.BQTableLastModified}", verbose=True)
                            schedule.MaxWatermark = schedule.BQTableLastModified
                    else:
                        schedule.MaxWatermark = watermark

                    schedule.UpdateRowCounts(src=src_cnt)   

                schedule.Status = SyncStatus.COMPLETE
                schedule.SparkAppId = self.Context.sparkContext.applicationId
                schedule.EndTime = datetime.now(timezone.utc)

                df_bq.unpersist()
            except Exception as e:
                raise FabricLakehouseError(msg=f"Error writing BQ table to Lakehouse: {e}", data=schedule)

        self.__show_sync_status(schedule, status=f"in {str(t)}")

        return schedule

    def __show_sync_status(self, schedule:SyncSchedule, status:str=None) -> None:
        """
        Displays the synchronization status for a given SyncSchedule.
        This method displays the synchronization status for a given SyncSchedule, including the
        table name, partition ID, and status information. If a status is provided, it is appended
        to the status message; otherwise, the status message is displayed as "IN PROGRESS".
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the table and status details.
            status (str): An optional status message to append to the status information.
        Returns:
            None
        """
        msg = f"{schedule.SummaryLoadType} {schedule.ObjectType} {schedule.LakehouseTableName}"

        if schedule.PartitionId:
            msg = f"{msg}${schedule.PartitionId}"

        if not status:
            self.Logger.sync_status(f"{msg}...")
        else:
            self.Logger.sync_status(f"FINISHED {msg} {status}...")

    @Telemetry.Sync_Load    
    def __schedule_sync(self, schedule:SyncSchedule, lock=None) -> SyncSchedule:
        """
        Synchronizes a BigQuery table with a Lakehouse table based on the provided SyncSchedule.
        This method retrieves a BigQuery table, transforms the data, saves the data to a Lakehouse table,
        and updates the SyncSchedule with row counts, watermarks, and status information. It also handles
        the logic for schema evolution, partitioning, and mirroring to a database landing zone, and returns
        the updated SyncSchedule after the synchronization process is complete.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the BigQuery and Lakehouse details.
            lock (Lock): A threading lock for partitioned sync loads.
        Returns:
            SyncSchedule: The updated SyncSchedule after the synchronization process is complete.
        """
        schedule.StartTime = datetime.now(timezone.utc)
        schedule = self.__sync_bq_table(schedule, lock)
        SyncUtil.save_schedule_telemetry(schedule) 

        return schedule

    def __schedule_sync_wrapper(self, value) -> SyncSchedule:
        """
        Wrapper function for synchronizing a BigQuery table with a Lakehouse table.
        This method wraps the __schedule_sync method for use with the QueueProcessor, handling
        the input value and returning the updated SyncSchedule after the synchronization process
        is complete.
        Args:
            value (Any): A tuple containing the SyncSchedule object and an optional threading lock.
        Returns:
            SyncSchedule: The updated SyncSchedule after the synchronization process is complete.
        """
        schedule = value[2]
        lock = self.TableLocks.get_or_set(schedule.LakehouseTableName, Lock())
        return self.__schedule_sync(schedule, lock)

    def __thread_exception_handler(self, value) -> None:
        """
        Exception handler for thread-based synchronization processes.
        This method handles exceptions raised during thread-based synchronization processes,
        updating the SyncSchedule status to FAILED and logging the error message.
        Args:
            value (Any): A tuple containing the SyncSchedule object and an optional threading lock.
        Returns:
            None
        """
        schedule = value[2]
        err = value[3]
        
        #print(f"ERROR - {schedule.LakehouseTableName} FAILED: {err}")
        
        #if self.UserConfig.Fabric.TargetType==FabricDestinationType.MIRRORED_DATABASE:  
        #    self.Logger.sync_status(f"Sync failed cleaning up staged LZ files for {schedule.LakehouseTableName}....") 
        #    OpenMirror.cleanup_spark_files(schedule)

        schedule.Status = SyncStatus.FAILED
        schedule.SummaryLoadType = f"ERROR"
        SyncUtil.save_schedule_telemetry(schedule) 

        self.Logger.error(msg=f"ERROR - {schedule.LakehouseTableName} FAILED: {err}")

    def run_schedule(self, schedule_type:str) -> bool:
        """
        Runs a synchronization schedule based on the provided schedule type.
        This method retrieves a synchronization schedule from the Fabric Metastore, initializes
        initial loads if necessary, and processes the schedule asynchronously or synchronously
        based on the user configuration. It returns True if the schedule is successfully processed.
        Args:
            schedule_type (str): The type of synchronization schedule to run.
        Returns:
            bool: True if the schedule is successfully processed; False otherwise.
        """
        if self.UserConfig.Async.Enabled:
            return self.__run_async_schedule(schedule_type, num_threads=self.UserConfig.Async.Parallelism)
        else:
            return self.__run_async_schedule(schedule_type, num_threads=1)

    def __initialize_initial_loads(self, schedule:DataFrame) -> bool:
        """
        Initializes tables for initial load based on the provided schedule.
        This method retrieves the initial load tables from the schedule, drops the tables from the
        Lakehouse or mirrored database, and returns True if initial loads are present; otherwise, it
        returns False.
        Args:
            schedule (DataFrame): A Spark DataFrame containing the synchronization schedule.
        Returns:
            bool: True if initial loads are present; False otherwise.
        """
        initial_loads = [i for i in schedule.collect() if i["initial_load"] == True]
                
        if initial_loads:
            self.Logger.sync_status(f"Initializing tables for initial load...", verbose=True)
            if self.UserConfig.Fabric.TargetType==FabricDestinationType.LAKEHOUSE:                
                tbls = [SyncSchedule(**(tbl.asDict())).LakehouseTableName for tbl in initial_loads]
                tbls = list(set(tbls))

                self.Logger.sync_status(f"Dropping lakehouse tables: {tbls}", verbose=True)
                SparkProcessor.drop(tbls)
            else:
                tbls = [SyncSchedule(**(tbl.asDict())) for tbl in initial_loads]
                dedup = []

                for tbl in tbls:
                    if tbl.LakehouseTableName not in dedup:
                        self.Logger.sync_status(f"Dropping mirrored database tables: {tbl.LakehouseTableName}", verbose=True)
                        OpenMirror.drop_mirrored_table(tbl)
                        dedup.append(tbl.LakehouseTableName)             

            return True
        else:
            return False

    def __run_async_schedule(self, schedule_type:str, num_threads:int) -> bool:
        """
        Runs a synchronization schedule asynchronously based on the provided schedule type.
        This method retrieves a synchronization schedule from the Fabric Metastore, initializes
        initial loads if necessary, and processes the schedule asynchronously based on the user
        configuration. It returns True if the schedule is successfully processed.
        Args:
            schedule_type (str): The type of synchronization schedule to run.
            num_threads (int): The number of threads to use for the asynchronous schedule.
        Returns:
            bool: True if the schedule is successfully processed; False otherwise.
        """
        SyncUtil.ensure_sync_views()
        self.Logger.sync_status(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...", verbose=True)         

        processor = QueueProcessor(num_threads=num_threads)
        #sync_stopped_mirror = False

        with SyncTimer() as t:
            schedule = FabricMetastore.get_schedule(schedule_type)
            initial_loads = self.__initialize_initial_loads(schedule)

            load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

            if load_grps:
                self.TableLocks = ThreadSafeDict()

                #if self.UserConfig.Fabric.TargetType==FabricDestinationType.MIRRORED_DATABASE:  
                #    self.Logger.sync_status(f"Stopping Mirroring on {self.UserConfig.Fabric.TargetLakehouse} Database...") 
                #    OpenMirror.stop_mirror(self.UserConfig.Fabric.TargetLakehouseID, self.FabricAPIToken)   
                #    sync_stopped_mirror= True

                for grp in load_grps:
                    self.MultiWrite = ThreadSafeDict()

                    grp_nm = "LOAD GROUP {0}".format(grp)
                    grp_df = schedule.where(f"priority = '{grp}'")

                    group_schedule = []

                    for tbl in grp_df.collect():
                        s = SyncSchedule(**(tbl.asDict()))

                        if not self.MultiWrite.contains(s.LakehouseTableName):
                            sync_track = {
                                "tasks": 0,
                                "observations": 0
                            }
                            self.MultiWrite.set(s.LakehouseTableName, sync_track)

                        group_schedule.append(s)

                        nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                        if s.PartitionId is not None:
                            nm = "{0}${1}".format(nm, s.PartitionId)        

                        priority = s.LoadPriority
                        processor.put((priority, nm, s))

                    if not processor.empty():
                        self.Logger.sync_status(f"### Processing {grp_nm}...", verbose=True)

                        with SyncTimer() as t:                        
                            processor.process(self.__schedule_sync_wrapper, self.__thread_exception_handler)

                        if not processor.has_exceptions:                            
                            self.Logger.sync_status(f"### {grp_nm} completed in {str(t)}...")
                        else:
                            self.Logger.sync_status(f"### {grp_nm} FAILED...")
                            break

                self.Logger.sync_status("Processing Sync Telemetry...", verbose=True)
                FabricMetastore.process_load_group_telemetry(schedule_type)
       
        #if self.UserConfig.Fabric.TargetType==FabricDestinationType.MIRRORED_DATABASE and sync_stopped_mirror:  
        #    self.Logger.sync_status(f"Starting Mirroring on {self.UserConfig.Fabric.TargetLakehouse} Database...") 
        #    OpenMirror.start_mirror(self.UserConfig.Fabric.TargetLakehouseID, self.FabricAPIToken)

        self.Logger.sync_status(f"Async schedule sync finished in {str(t)}...")

        return initial_loads