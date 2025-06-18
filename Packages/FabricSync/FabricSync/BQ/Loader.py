from pyspark.sql import ( # type: ignore
    DataFrame, Observation
)
from pyspark.sql.functions import ( # type: ignore
    col, lit, count, max, date_format
)
from datetime import (
    datetime, timezone, timedelta
)
from typing import (
    Tuple, Dict
)
from threading import Lock
from delta.tables import DeltaTable # type: ignore
import json
import base64 as b64

from FabricSync.BQ.Utils import SyncTimer
from FabricSync.BQ.SessionManager import Session
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
    QueueProcessor, ThreadSafeList, ThreadSafeDict, SparkProcessor
)
from FabricSync.BQ.SyncCore import ConfigBase
from FabricSync.BQ.Enum import (
    SyncLoadType, SyncLoadStrategy, SyncStatus, FabricDestinationType, BQDataType, BQPartitionType,
    BigQueryAPI
)
from FabricSync.BQ.Core import DeltaTableMaintenance
from FabricSync.BQ.GoogleStorageAPI import BucketStorageClient
from FabricSync.BQ.BigQueryAPI import BigQueryClient

class BQDataProxy(ConfigBase):
    """
    A class to retrieve data from BigQuery based on a SyncSchedule.
    This class handles the logic for building CDC queries, applying partition filters,
    and loading data from BigQuery into a Spark DataFrame. It also manages the observation
    of row counts and watermarks for synchronization tasks.
    Attributes:
        schedule (SyncSchedule): The synchronization schedule containing the BigQuery table details.
    Methods:
        __init__(schedule:SyncSchedule) -> None:
            Initializes a new instance of the BQDataProxy class with the provided SyncSchedule.
        get_schedule_data(schedule:SyncSchedule) -> Tuple[SyncSchedule, DataFrame, Observation]:
            Retrieves data from BigQuery based on the provided SyncSchedule.
        get_bq_data() -> Tuple[SyncSchedule, DataFrame, Observation]:
            Retrieves data from a BigQuery table or query based on the provided SyncSchedule.
        __build_cdc_query() -> str:
            Builds a CDC (Change Data Capture) or full load query string for BigQuery based on the provided SyncSchedule.
        is_timezone_aware(timestamp:datetime) -> bool:
            Checks if the specified timestamp object is timezone-aware.
        __get_watermark_predicate(schedule:SyncSchedule) -> str:
            Constructs a predicate for filtering data based on the watermark column and its maximum value.
        __use_dataframe_observation() -> bool:
            Determines whether to use a DataFrame observation for monitoring row counts and watermarks.
        __get_time_ingestion_query(schedule:SyncSchedule, query_model:BQQueryModel) -> BQQueryModel:
            Constructs a query for time ingestion based on the provided SyncSchedule.
        __resolve_time_ingestion_pseudo_columns(schedule:SyncSchedule, df:DataFrame) -> DataFrame:
            Resolves pseudo columns for time ingestion in a DataFrame.
    """
    def __init__(self, schedule:SyncSchedule) -> None:
        """
        Initializes a new instance of the BQDataProxy class with the provided SyncSchedule.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the BigQuery table details.
        """
        super().__init__()
        self.__schedule = schedule

    @staticmethod
    def get_schedule_data(schedule:SyncSchedule) -> Tuple[SyncSchedule, DataFrame, Observation]:
        """
        Retrieves data from BigQuery based on the provided SyncSchedule.
        This method dynamically applies partition or watermark filters, constructs a query,
        and loads data from BigQuery into a Spark DataFrame. It also optionally sets up
        an Observation object for monitoring row counts and watermark values.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the BigQuery table details.
        Returns:
            Tuple[SyncSchedule, DataFrame, Observation]:
                A tuple containing the updated SyncSchedule, the loaded Spark DataFrame,
                and an optional Observation object (None if not used).
        Raises:
            SyncLoadError: If there is an error retrieving data from BigQuery.
        """
        try:
            proxy = BQDataProxy(schedule)
            return proxy.get_bq_data()
        except Exception as e:
            raise SyncLoadError(msg=f"Failed to retrieve table from BQ: {e}", data=schedule)
    
    def __build_cdc_query(self) -> str:
        """
        Builds a CDC (Change Data Capture) or full load query string for BigQuery based on the provided SyncSchedule.
        This function calculates the time window for data capture, constructs a BigQuery SQL query using either
        CHANGES or APPENDS functions depending on whether a CDC load strategy is used, and returns the final
        query string. The time window is defined by the difference between the current timestamp and the
        schedule's watermark timestamp, adjusting logic for CDC or a full seven-day window as necessary.
        Returns:
            str: A fully constructed SQL query for retrieving changes from the specified BigQuery table
            within the calculated time interval, incorporating either CDC or full load semantics.
        """

        seven_days_seconds = 7*24*60*60
        twenty_four_hours_seconds = 24*60*60
        ten_minutes_seconds = 10*60

        cdc = (self.__schedule.Load_Strategy == SyncLoadStrategy.CDC)
        watermark_ts = datetime.fromisoformat(self.__schedule.MaxWatermark)

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
        sql_string += f"{self.__schedule.TableColumns} FROM "
        sql_string += f"CHANGES(TABLE `{self.__schedule.BQTableName}`,TIMESTAMP_ADD(TIMESTAMP'{self.__schedule.MaxWatermark}',INTERVAL 1 SECOND)," \
            if cdc else f"APPENDS(TABLE `{self.__schedule.BQTableName}`,TIMESTAMP_ADD(TIMESTAMP'{self.__schedule.MaxWatermark}',INTERVAL 1 SECOND),"
        
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

    def get_bq_data(self) -> Tuple[SyncSchedule, DataFrame, Observation]:
        """
        Retrieves data from a BigQuery table or query based on the provided SyncSchedule.
        This method dynamically applies partition or watermark filters, constructs a query,
        and loads data from BigQuery into a Spark DataFrame. It also optionally sets up
        an Observation object for monitoring row counts and watermark values.
        Returns:
            Tuple[SyncSchedule, DataFrame, Observation]:
                A tuple containing the updated SyncSchedule, the loaded Spark DataFrame,
                and an optional Observation object (None if not used).
        """

        qm = {
            "ScheduleId": self.__schedule.ScheduleId,
            "TaskId": self.__schedule.ID,
            "ProjectId": self.__schedule.ProjectId,
            "Dataset": self.__schedule.Dataset,
            "TableName": self.__schedule.BQTableName,
            "TempTableId": self.__schedule.TempTableId,
            "API": self.__schedule.SyncAPI,
            "Predicate": []
        }
        query_model = BQQueryModel(**qm)

        if self.__schedule.IsTimePartitionedStrategy:
            self.Logger.debug(f"Loading {self.__schedule.LakehouseTableName} with time ingestion...")

            if self.__schedule.IsTimeIngestionPartitioned:
                query_model = self.__get_time_ingestion_query(self.__schedule, query_model)

            if not self.__schedule.InitialLoad or self.__schedule.RequirePartitionFilter:
                self.Logger.debug(f"Loading {self.__schedule.LakehouseTableName} by time ingestion partition...")

                part_format = SyncUtil.get_bq_partition_id_format(self.__schedule.PartitionGrain)
                if self.__schedule.PartitionDataType == BQDataType.TIMESTAMP:        
                    part_filter = f"timestamp_trunc({self.__schedule.PartitionColumn}, {self.__schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{self.__schedule.PartitionId}')"
                else:
                    part_filter = f"date_trunc({self.__schedule.PartitionColumn}, {self.__schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{self.__schedule.PartitionId}')"

                self.Logger.debug(f"{self.__schedule.LakehouseTableName} time partition: {part_filter}")
                query_model.add_predicate(part_filter)

                if self.__schedule.WatermarkColumn:
                    predicate = self.__get_watermark_predicate(self.__schedule)
                    query_model.add_predicate(predicate)
        elif self.__schedule.IsRangePartitioned:
            self.Logger.debug(f"Loading {self.__schedule.LakehouseTableName} with range partitioning...")
            part_filter = SyncUtil.get_partition_range_predicate(self.__schedule)
            self.Logger.debug(f"Load from BQ by range partition: {part_filter}")
            query_model.PartitionFilter = part_filter
        elif not self.__schedule.InitialLoad:
            if self.__schedule.Load_Strategy == SyncLoadStrategy.WATERMARK:
                self.Logger.debug(f"Loading {self.__schedule.LakehouseTableName} with watermark...")
                predicate = self.__get_watermark_predicate(self.__schedule)
                query_model.add_predicate(predicate)
            elif self.__schedule.IsCDCStrategy and self.__schedule.MaxWatermark:
                cdc_query = self.__build_cdc_query()
                self.Logger.debug(f"Load {self.__schedule.LakehouseTableName} with CDC: {cdc_query}")
                query_model.Query = cdc_query
        
        if self.__schedule.SourceQuery and not query_model.Query:
            self.Logger.debug(f"Overriding table. Load from BQ with source query: {self.__schedule.SourceQuery}")
            query_model.Query = self.__schedule.SourceQuery
                
        if self.__schedule.SourcePredicate:
            self.Logger.debug(f"Applying table predicate: {self.__schedule.SourcePredicate}")
            query_model.add_predicate(self.__schedule.SourcePredicate)

        if self.__use_dataframe_observation():
            query_model.Cached = False
            observation = Observation(name=f"OB_{self.__schedule.TableId}_{self.__schedule.PartitionId}")
        else:
            query_model.Cached = (self.UserConfig.Optimization.DisableDataframeCache == False)
            observation = None

        df_bq = self.read_bq_to_dataframe(query_model)
        self.Logger.debug(f"{self.__schedule.LakehouseTableName} - BASE Read from BQ")

        if df_bq:
            if observation:
                if self.__schedule.IsCDCStrategy and not self.__schedule.InitialLoad:
                    self.Logger.debug(f"{self.__schedule.LakehouseTableName} - CDC Observation")
                    df_bq = df_bq.observe(observation, 
                        count(lit(1)).alias("row_count"),
                        max(col("BQ_CDC_CHANGE_TIMESTAMP")).alias("watermark"))
                elif self.__schedule.WatermarkColumn:
                    self.Logger.debug(f"{self.__schedule.LakehouseTableName} - Observation Watermark: {self.__schedule.WatermarkColumn}")
                    df_bq = df_bq.observe(
                        observation, 
                        count(lit(1)).alias("row_count"),
                        max(col(self.__schedule.WatermarkColumn)).alias("watermark"))
                else:
                    self.Logger.debug(f"{self.__schedule.LakehouseTableName} - Observation Count Only")
                    df_bq = df_bq.observe(observation, count(lit(1)).alias("row_count"))

            if self.__schedule.IsPartitioned:
                if self.__schedule.Partition_Type == BQPartitionType.TIME:
                    proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(self.__schedule.PartitionGrain)
                    self.__schedule.FabricPartitionColumns = SyncUtil.get_fabric_partition_cols(self.__schedule.PartitionColumn, proxy_cols)

                    if self.__schedule.IsTimeIngestionPartitioned:
                        self.Logger.debug(f"{self.__schedule.LakehouseTableName} - Time Ingestion Pseudo Col: {self.__schedule.PartitionColumn}-{self.__schedule.PartitionId}")
                        df_bq = self.__resolve_time_ingestion_pseudo_columns(self.__schedule, df_bq)          

                    df_bq = SyncUtil.create_fabric_partition_proxy_cols(df_bq, self.__schedule.PartitionColumn, proxy_cols)
                else:
                    self.__schedule.FabricPartitionColumns = [f"__{self.__schedule.PartitionColumn}_Range"]
                    df_bq = SyncUtil.create_fabric_range_partition(self.Context, df_bq, self.__schedule)
            
            return (self.__schedule, df_bq, observation)
        else:
            self.Logger.debug(f"{self.__schedule.LakehouseTableName} - No Data Returned...")
            self.__schedule.Status = SyncStatus.NO_DATA

            return (self.__schedule, None, None)

    def __resolve_time_ingestion_pseudo_columns(self, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        """
        Resolves pseudo columns for time ingestion in a DataFrame.
        This function checks if the DataFrame contains the partition column and, if not,
        adds the partition column with the specified partition ID or formats it based on the
        partition grain. It returns the updated DataFrame with the partition column added.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the partition details.
            df (DataFrame): The DataFrame to resolve pseudo columns for.
        Returns:
            DataFrame: The updated DataFrame with the partition column added.
        """
        if schedule.PartitionColumn:
            proxy = f"__{schedule.PartitionColumn}__"

            if schedule.PartitionColumn not in df.schema.fieldNames():
                if schedule.PartitionId:
                    df = df.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))  
                else:
                    if proxy in df.schema.fieldNames():
                        dt_format = SyncUtil.get_partition_id_df_format(schedule.PartitionGrain)
                        df = df.withColumn(schedule.PartitionColumn, \
                                        date_format(col(proxy), dt_format))
            
            if proxy in df.schema.fieldNames():
                df = df.drop(proxy)
        return df

    def __get_time_ingestion_query(self, schedule:SyncSchedule, query_model:BQQueryModel) -> BQQueryModel:
        """
        Constructs a query for time ingestion based on the provided SyncSchedule.
        This function builds a query to retrieve data from a BigQuery table for time ingestion,
        including the partition column and any necessary predicates. It sets the query string
        in the BQQueryModel and returns the updated model.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the partition details.
            query_model (BQQueryModel): The model to hold the query and other parameters.
        Returns:
            BQQueryModel: The updated BQQueryModel with the constructed query for time ingestion.
        """
        if not schedule.SourceQuery:
            sql = f"SELECT *, {schedule.PartitionColumn} AS __{schedule.PartitionColumn}__ FROM `{query_model.TableName}`"

            query_model.Query = sql
            self.Logger.debug(f"{self.__schedule.LakehouseTableName} - Time Ingestion Initial Load Query: {sql}")
        
        query_model.UseForceBQJobConfig = True

        return query_model

    def __get_watermark_predicate(self, schedule:SyncSchedule) -> str:
        """
        Constructs a predicate for filtering data based on the watermark column and its maximum value.
        This function checks if the maximum watermark value is a digit or a string and constructs
        the appropriate predicate for filtering the data. It returns the predicate string to be used
        in the BigQuery query for loading data.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the watermark column and its maximum value.
        Returns:
            str: The predicate string for filtering data based on the watermark column and its maximum value.
        """
        if schedule.MaxWatermark.isdigit():
            predicate = f"{schedule.WatermarkColumn} > {schedule.MaxWatermark}"
        else:
            predicate = f"{schedule.WatermarkColumn} > '{schedule.MaxWatermark}'"
        
        self.Logger.debug(f"Load from BQ with watermark: {predicate}")

        return predicate

    def __use_dataframe_observation(self) -> bool:
        """
        Determines whether to use a DataFrame observation for monitoring row counts and watermarks.
        This function checks the user configuration for the approximate row count optimization setting
        and the sync schedule for disallowed load strategies and types. If approximate row counts are
        enabled and the schedule's load strategy and type are allowed, this function returns True.
        Returns:
            bool: True if approximate row counts are enabled and the schedule's load strategy and type
            are allowed; False otherwise.
        """
        disallowed_strategies = [SyncLoadStrategy.CDC]
        disallowed_types = [SyncLoadType.MERGE]

        if self.UserConfig.Optimization.UseApproximateRowCounts:
            if self.__schedule.Load_Strategy not in disallowed_strategies and self.__schedule.Load_Type not in disallowed_types:
                return True
        
        return False

class BQFabricWriter(ConfigBase):
    """
    A class to write data to a Fabric Lakehouse table based on a SyncSchedule.
    This class handles the logic for merging data, applying CDC strategies, and managing
    partitioned sync loads. It provides methods to merge data into Delta tables, apply CDC
    strategies, and manage the partitioned sync loads with appropriate locking mechanisms.
    Attributes:
        MultiWrite (ThreadSafeDict): A thread-safe dictionary to manage multiple write operations.
    Methods:
        __init__() -> None:
            Initializes a new instance of the BQFabricWriter class.
        __get_delta_merge_row_counts(schedule:SyncSchedule, telemetry:DataFrame) -> Tuple[int, int, int]:
            Retrieves the counts of inserted, updated, and deleted rows from the current day's MERGE operations.
        __get_cdc_merge_set(columns:str, alias:str) -> Dict[str, str]:
            Builds a dictionary of column expressions for a CDC merge operation.
        __get_cdc_latest_changes(df:DataFrame) -> DataFrame:
            Retrieves the latest changes from a CDC DataFrame based on the provided SyncSchedule.
        __merge_table(schedule:SyncSchedule, tableName:str, df:DataFrame) -> Tuple[SyncSchedule, DataFrame]:
            Merges the given Spark DataFrame into a Delta table, optionally flattening the data if indicated by the schedule.
        __merge_dataframe(schedule:SyncSchedule, delta_dest:DeltaTable, df:DataFrame) -> None:
            Merge into Lakehouse Table based on User Configuration. Only supports Insert/Update All.
        __save_bq_to_lakehouse(schedule:SyncSchedule, df_bq:DataFrame, lock:Lock, write_config=None) -> Tuple[SyncSchedule, DataFrame]:
            Saves a DataFrame to a Lakehouse table based on the provided SyncSchedule.
        __apply_task_tracker_observation(schedule:SyncSchedule, df:DataFrame) -> Tuple[DataFrame, Observation]:
            Applies an Observation object to a DataFrame for monitoring row counts.
        __write_to_lakehouse(schedule:SyncSchedule, df:DataFrame, mode:str, partition_by:list=None, options:dict=None) -> DataFrame:
            Writes a DataFrame to a Lakehouse table with the specified mode, partitioning, and options.
        __write_to_mirror(schedule:SyncSchedule, df:DataFrame, mode:str, partition_by:list=None, options:dict=None) -> DataFrame:
            Writes a DataFrame to a Fabric Mirror with the specified mode, partitioning, and options.
        __increment_task_tracker(lakehouse_table_name:str, observation:Observation) -> Tuple[int, int]:
            Increments the task tracker for the specified Lakehouse table and returns the task part and total row count.
        initialize_first_load(schedule:SyncSchedule) -> None:
            Initializes the first load for a SyncSchedule by creating a Fabric Lakehouse table and setting up the task tracker.
        save_bq_to_lakehouse(schedule:SyncSchedule, df_bq:DataFrame, lock:Lock, write_config=None) -> Tuple[SyncSchedule, DataFrame]:
            Saves a DataFrame to a Lakehouse table based on the provided SyncSchedule.
    """
    def __init__(self) -> None:
        """
        Initializes a new instance of the BQFabricWriter class.
        This class is responsible for writing data to a Fabric Lakehouse table based on a SyncSchedule.
        It handles the logic for merging data, applying CDC strategies, and managing partitioned sync loads.
        """
        super().__init__()
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
            df (DataFrame): The DataFrame to filter for the latest changes.
        Returns:
            DataFrame: A filtered DataFrame containing only the latest changes for each key group.
        """
        keys = ",".join(schedule.Keys)

        row_num_expr = f"row_number() over(partition by {keys} order by BQ_CDC_CHANGE_TIMESTAMP desc) as cdc_row_num"

        df = df.selectExpr("*", row_num_expr).filter(col("cdc_row_num") == 1) \
            .drop(col("cdc_row_num"), col("BQ_CDC_CHANGE_TIMESTAMP"))
        
        return df
    
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
            self.Logger.debug(f"Merge with partition constraint: {partition_constraint}")
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
                self.Logger.debug(f"LAKEHOUSE - {schedule.LakehouseTableName} - Partitioned Sync Load...")
                has_lock = False
                
                if schedule.TotalTableTasks > 1:
                    has_lock = True
                    lock.acquire()

                if not schedule.InitialLoad and not schedule.LakehousePartition:
                    write_config["partitionOverwriteMode"] = "dynamic"

                    if "overwriteSchema" in write_config:
                        del write_config["overwriteSchema"]
                
                try:
                    partition_cols = SyncUtil.get_lakehouse_partitions(schedule)
                    df_bq = self.__write_to_lakehouse(schedule, df=df_bq, mode="OVERWRITE", partition_by=partition_cols, options=write_config)
                    task_part, total_row_count = self.__increment_task_tracker(schedule.LakehouseTableName, observation)                    
                finally:
                    if has_lock:
                        lock.release()
            else:
                self.Logger.debug(f"LAKEHOUSE - {schedule.LakehouseTableName} - Non-Partitioned Sync Load...")

                if schedule.Load_Strategy == SyncLoadStrategy.CDC_APPEND:
                    df_bq = df_bq.drop("BQ_CDC_CHANGE_TYPE", "BQ_CDC_CHANGE_TIMESTAMP")

                partition_cols = None

                if schedule.FabricPartitionColumns or schedule.LakehousePartition:
                    partition_cols = SyncUtil.get_lakehouse_partitions(schedule)

                df_bq = self.__write_to_lakehouse(schedule, df=df_bq, partition_by=partition_cols, options=write_config)
                
                task_part, total_row_count = self.__increment_task_tracker(schedule.LakehouseTableName, observation)
            
            if task_part == schedule.TotalTableTasks:
                self.Logger.debug(f"LAKEHOUSE - {schedule.LakehouseTableName} - Rows: {total_row_count}")

                if total_row_count == 0:
                    self.Logger.debug(f"LAKEHOUSE - Empty Sync - {schedule.LakehouseTableName}")
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

    def __write_to_lakehouse(self, schedule:SyncSchedule, df:DataFrame, mode:str=None, partition_by:list=None, options:dict=None) -> DataFrame:
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

        self.Logger.debug(f"LAKEHOUSE - Writing {schedule.LakehouseTableName} to {schedule.LakehouseAbfssTablePath} with mode: {mode} - {options}") 
        
        if schedule.FlattenTable:
            df, flattened = SyncUtil.flatten(schedule=schedule, df=df)

            if flattened:
                SyncUtil.save_dataframe(table_name=f"{schedule.LakehouseTableName}_flattened", 
                    path=f"{schedule.LakehouseAbfssTablePath}_flattened",
                    df=flattened, mode=mode, partition_by=partition_by, options=options)
                    
        SyncUtil.save_dataframe(table_name=schedule.LakehouseTableName, path=schedule.LakehouseAbfssTablePath,
            df=df, mode=mode, partition_by=partition_by, options=options)

        return df

    def __write_to_mirror_db(self, schedule:SyncSchedule, df:DataFrame, lock:Lock) -> Tuple[SyncSchedule, DataFrame]:
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
                self.Logger.debug(f"MIRRORING - {schedule.LakehouseTableName} - Rows: {total_row_count}")

                if total_row_count > 0:
                    schedule.MirrorFileIndex = OpenMirror.process_landing_zone(schedule)
                    self.Logger.debug(f"MIRRORING - Next File Index for {schedule.LakehouseTableName}: {schedule.MirrorFileIndex}")
                else:
                    self.Logger.debug(f"MIRRORING - Empty Sync - {schedule.LakehouseTableName}")
                    schedule.IsEmpty = True
                    OpenMirror.cleanup_spark_files(schedule)
        finally:
            if has_lock:
                lock.release()
        
        return (schedule, df)

    def initialize_first_loads(self, schedule:DataFrame) -> bool:
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
            self.Logger.debug(f"Initializing tables for initial load...")
            if self.UserConfig.Fabric.TargetType==FabricDestinationType.LAKEHOUSE:                
                tbls = [SyncSchedule(**(tbl.asDict())).LakehouseTableName for tbl in initial_loads]
                tbls = list(set(tbls))

                self.Logger.debug(f"Dropping lakehouse tables: {tbls}")
                SparkProcessor.drop(tbls)
            else:
                tbls = [SyncSchedule(**(tbl.asDict())) for tbl in initial_loads]
                dedup = []

                for tbl in tbls:
                    if tbl.LakehouseTableName not in dedup:
                        self.Logger.debug(f"Dropping mirrored database tables: {tbl.LakehouseTableName}")
                        OpenMirror.drop_mirrored_table(tbl)
                        dedup.append(tbl.LakehouseTableName)             

            return True
        else:
            return False    
    
    def save_bq_table(self, schedule:SyncSchedule, df_bq:DataFrame, observation:Observation, lock:Lock):
        """
        Saves a DataFrame to a Lakehouse table based on the provided SyncSchedule.
        This method handles the logic for saving a DataFrame to a Lakehouse table, including
        partitioning, schema evolution, and overwrite/append strategies. It also manages the
        acquisition and release of a lock for partitioned sync loads and returns the updated
        SyncSchedule and DataFrame.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the Lakehouse table details.
            df_bq (DataFrame): The DataFrame to save to the Lakehouse table.
            observation (Observation): The Observation object for monitoring row counts.
            lock (Lock): A threading lock for partitioned sync loads.
        Returns:
            None
        Raises:
            FabricLakehouseError: If an error occurs during the Lakehouse write process.
        """
        try:              
            if not schedule.IsMirrored:
                write_config = { }

                #Schema Evolution
                if schedule.AllowSchemaEvolution and not schedule.InitialLoad:
                    if schedule.Load_Type == SyncLoadType.OVERWRITE:
                        write_config["overwriteSchema"] = True
                    else:
                        write_config["mergeSchema"] = True
                elif not schedule.IsPartitionedSyncLoad and schedule.InitialLoad:
                    write_config["overwriteSchema"] = True

                schedule,df_bq = self.__save_bq_to_lakehouse(schedule, df_bq, lock, write_config)
                    
                table_maint = DeltaTableMaintenance(schedule.LakehouseTableName, schedule.LakehouseAbfssTablePath)
                schedule.DeltaVersion = table_maint.CurrentTableVersion
            else:
                self.Logger.debug(f"Writing {schedule.LakehouseTableName} data to mirrored DB landing zone")
                schedule, df_bq = self.__write_to_mirror_db(schedule, df_bq, lock)

            if not schedule.IsEmpty:
                src_cnt, watermark = SyncUtil.get_source_metrics(schedule, df_bq, observation)

                if not watermark:
                    if schedule.IsCDCStrategy:
                        self.Logger.debug(f"No watermark defined, using BQ Table Last Modified: {schedule.BQTableLastModified}")
                        schedule.MaxWatermark = schedule.BQTableLastModified
                else:
                    schedule.MaxWatermark = watermark

                schedule.UpdateRowCounts(src=src_cnt)   

            schedule.Status = SyncStatus.COMPLETE

            df_bq.unpersist()
        except Exception as e:
            raise FabricLakehouseError(msg=f"Error writing BQ table to Lakehouse: {e}", data=schedule)

class BQScheduleLoader(ConfigBase):
    """
    A class to load and synchronize BigQuery tables with Fabric Lakehouse tables.
    This class handles the logic for synchronizing BigQuery tables, transforming data,
    saving data to Lakehouse tables, and updating synchronization schedules with row counts,
    watermarks, and status information. It also manages the logic for schema evolution,
    partitioning, and mirroring to a database landing zone.
    Attributes:
        HasScheduleErrors (bool): A flag to indicate if any synchronization schedules encountered errors.
    Methods:
        __init__() -> None:
            Initializes a new instance of the BQScheduleLoader class.
        __sync_bq_table(schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
            Synchronizes a BigQuery table with a Lakehouse table based on the provided SyncSchedule.
        __sync_cleanup(schedule:SyncSchedule) -> None:
            Cleans up temporary resources after synchronizing a BigQuery table.
        __cleanup_temp_tables() -> None:
            Cleans up temporary BigQuery tables created during the synchronization process.
        __show_sync_status(schedule:SyncSchedule, status:str=None) -> None:
            Displays the synchronization status for a given SyncSchedule.
        __schedule_sync(schedule:SyncSchedule, lock=None) -> SyncSchedule:
            Synchronizes a BigQuery table with a Lakehouse table based on the provided SyncSchedule.
        __sync_wrapper(schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
            A wrapper method for synchronizing a BigQuery table with a Lakehouse table.
        __threaded_sync_wrapper(schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
            A threaded wrapper method for synchronizing a BigQuery table with a Lakehouse table.
        run_sync(schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
            Runs the synchronization process for a given SyncSchedule, handling both threaded and non-threaded syncs.
        __run_async(schedule:SyncSchedule, lock:Lock = None) -> SyncSchedule:
            Runs the synchronization process asynchronously for a given SyncSchedule.
        """   
    def __init__(self) -> None:
        """
        Initializes a new instance of the BQScheduleLoader class.
        This class is responsible for loading and synchronizing BigQuery tables with Fabric Lakehouse tables.
        It handles the logic for synchronizing BigQuery tables, transforming data, saving data to Lakehouse tables,
        and updating synchronization schedules with row counts, watermarks, and status information.
        It also manages the logic for schema evolution, partitioning, and mirroring to a database landing zone.
        Attributes:
            __BQTempTables (ThreadSafeList): A thread-safe list to manage temporary BigQuery tables created during the sync process.
            __TableLocks (ThreadSafeDict): A thread-safe dictionary to manage table locks for partitioned sync loads.
            __FabricWriter (BQFabricWriter): An instance of the BQFabricWriter class for writing data to Lakehouse tables.
            __HasScheduleErrors (bool): A flag to indicate if any synchronization schedules encountered errors.
        """
        super().__init__()

        self.__BQTempTables:ThreadSafeList = None
        self.__TableLocks:ThreadSafeDict = None
        self.__FabricWriter = BQFabricWriter()
        self.__HasScheduleErrors = False

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
                self.Logger.debug(f"Overwriting current mirrored table: {schedule.LakehouseTableName}...")
                OpenMirror.drop_mirrored_table(schedule)
                schedule.MirrorFileIndex = 1

            #Get BQ table using sync config
            schedule, df_bq, observation = BQDataProxy.get_schedule_data(schedule)

            if df_bq:
                #Transform
                try:
                    schedule, df_bq = SyncUtil.transform(schedule, df_bq)
                except Exception as e:
                    raise SyncLoadError(msg=f"Transformation failed during sync: {e}", data=schedule)

                #Save BQ table
                self.__FabricWriter.save_bq_table(schedule, df_bq, observation, lock)

            schedule.SparkAppId = self.Context.sparkContext.applicationId
            schedule.EndTime = datetime.now(timezone.utc)

        self.__show_sync_status(schedule, status=f"in {str(t)}")

        return schedule

    def __sync_cleanup(self, schedule:SyncSchedule) -> None:
        """
        This method cleans up temporary resources after synchronizing a BigQuery table, including
        deleting temporary tables, cleaning up exported bucket data, and dropping temporary BigQuery tables
        if necessary.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the BigQuery and Lakehouse details.
        Returns:
            None
        """
        if schedule.SyncAPI == BigQueryAPI.BUCKET and self.UserConfig.GCP.Storage.EnabledCleanUp:
            self.Logger.debug(f"Cleaning up exported bucket data for {schedule.LakehouseTableName}...")
            storage_client = BucketStorageClient(self.UserConfig, self.GCPCredential)
            storage_client.delete_folder(self.UserConfig.GCP.Storage.BucketUri,
                storage_client.get_storage_prefix(schedule.ScheduleId, schedule.ID))
        
        self.__BQTempTables.append((schedule.ProjectId, schedule.Dataset, schedule.TempTableId))

    def __cleanup_temp_tables(self) -> None:
        """
        This method cleans up temporary BigQuery tables created during the synchronization process,
        ensuring that temporary tables are deleted after they are no longer needed.
        Returns:
            None
        """
        if self.__BQTempTables and self.__BQTempTables.length() > 0:
            self.Logger.debug(f"Cleaning up temporary BigQuery tables: {self.__BQTempTables.unsafe_list}")
            
            datasets = list(set([(i[0], i[1]) for i in self.__BQTempTables.unsafe_list]))
            
            if datasets:
                bq_client = BigQueryClient(self.UserConfig)
                bq_client.drop_temp_tables(datasets)

            self.__BQTempTables.clear()

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
        msg = f"{schedule.SummaryLoadType} - {schedule.LakehouseTableName}"

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

        #Cleanup
        self.__sync_cleanup(schedule)

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
        lock = self.__TableLocks.get_or_set(schedule.LakehouseTableName, Lock())
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

        schedule.Status = SyncStatus.FAILED
        schedule.SummaryLoadType = f"ERROR"
        SyncUtil.save_schedule_telemetry(schedule) 

        self.Logger.error(msg=f"ERROR - {schedule.LakehouseTableName} FAILED: {err}")

    @property
    def HasScheduleErrors(self) -> bool:
        """
        Returns whether any synchronization schedules encountered errors.
        This property checks if any synchronization schedules encountered errors during the
        synchronization process and returns a boolean value indicating the presence of errors.
        Returns:
            bool: True if any synchronization schedules encountered errors; False otherwise.
        """
        return self.__HasScheduleErrors
    
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
        self.Logger.sync_status(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...")         

        if self.UserConfig.GCP.API.EnableBigQueryExport:            
            gcp_credentials = json.loads(b64.b64decode(self.GCPCredential))
            Session.set_spark_conf("fs.gs.auth.service.account.private.key.id", gcp_credentials["private_key_id"])
            Session.set_spark_conf("fs.gs.auth.service.account.private.key", gcp_credentials["private_key"])
            Session.set_spark_conf("fs.gs.auth.service.account.email", gcp_credentials["client_email"])

        processor = QueueProcessor(num_threads=num_threads)

        with SyncTimer() as t:
            self.__BQTempTables = ThreadSafeList()
            schedule = FabricMetastore.get_schedule(schedule_type)
            initial_loads = self.__FabricWriter.initialize_first_loads(schedule)

            load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

            if load_grps:
                self.__TableLocks = ThreadSafeDict()

                for grp in load_grps:
                    self.__FabricWriter.MultiWrite = ThreadSafeDict()

                    grp_nm = "LOAD GROUP {0}".format(grp)
                    grp_df = schedule.where(f"priority = '{grp}'")

                    group_schedule = []

                    for tbl in grp_df.collect():
                        s = SyncSchedule(**(tbl.asDict()))
                        s.BQTableName = self.UserConfig.GCP.format_table_path(s.ProjectId, s.Dataset, s.TableName)

                        if not self.__FabricWriter.MultiWrite.contains(s.LakehouseTableName):
                            sync_track = {
                                "tasks": 0,
                                "observations": 0
                            }
                            self.__FabricWriter.MultiWrite.set(s.LakehouseTableName, sync_track)

                        group_schedule.append(s)

                        nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                        if s.PartitionId is not None:
                            nm = "{0}${1}".format(nm, s.PartitionId)        

                        priority = s.LoadPriority
                        processor.put((priority, nm, s))

                    if not processor.empty():
                        self.Logger.sync_status(f"### Processing {grp_nm}...")

                        with SyncTimer() as t:                        
                            processor.process(self.__schedule_sync_wrapper, self.__thread_exception_handler)

                        if not processor.has_exceptions:   
                            self.__HasScheduleErrors = False
                            self.Logger.sync_status(f"### {grp_nm} completed in {str(t)}...")
                        else:
                            self.__HasScheduleErrors = True
                            self.Logger.sync_status(f"### {grp_nm} FAILED...")
                            break

                self.Logger.sync_status("Processing Sync Telemetry...")
                FabricMetastore.process_load_group_telemetry(schedule_type)

        self.__cleanup_temp_tables()

        self.Logger.sync_status(f"Async schedule sync finished in {str(t)}...")

        return initial_loads