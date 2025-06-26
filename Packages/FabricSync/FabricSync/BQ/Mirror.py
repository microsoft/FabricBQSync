from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.FileSystem import OpenMirrorLandingZone
from FabricSync.BQ.Logging import Telemetry

from FabricSync.BQ.Enum import (
    SyncLoadStrategy, FabricCDCType, SyncLoadType
)

class OpenMirror(ContextAwareBase):
    @classmethod 
    def __get_lz(cls, schedule:SyncSchedule) -> OpenMirrorLandingZone:
        """
        Retrieves the landing zone object associated with the given synchronization schedule.
        Args:
            schedule (SyncSchedule): The synchronization schedule that identifies the landing zone.
        Returns:
            OpenMirrorLandingZone: The landing zone object for the specified schedule.
        """
        return OpenMirrorLandingZone(
            schedule.WorkspaceId, 
            schedule.LakehouseId, 
            schedule.LakehouseSchema, 
            schedule.LakehouseTable
            )

    @classmethod
    def drop_mirrored_table(cls, schedule:SyncSchedule) -> int:
        """
        Removes the mirrored table associated with the given synchronization schedule.
        Args:
            schedule (SyncSchedule): 
                The synchronization schedule that identifies the table to be removed.
        Returns:
            int: 
                A status code, typically indicating the success or failure of the table removal.
        """

        lz = cls.__get_lz(schedule)
        lz.delete_table()

    @classmethod
    def cleanup_spark_files(cls, schedule:SyncSchedule) -> None:
        """
        Removes temporary Spark files associated with the given schedule.
        This method identifies the landing zone for the provided schedule and cleans up 
        any staged files used during Spark operations.
        Args:
            schedule (SyncSchedule): Configuration details of the synchronization schedule 
                that uses Spark for data processing and staging.
        Returns:
            None
        """
        
        lz = cls.__get_lz(schedule)
        lz.cleanup_staged_lz()

    @classmethod
    @Telemetry.Mirror_DB_Sync()
    def process_landing_zone(cls, schedule:SyncSchedule) -> int:
        """
        Processes the landing zone for a given sync schedule.
        This method retrieves a landing zone (LZ) object, optionally initializes
        the mirrored table metadata for the first load, and stages Spark output
        files based on the current mirror file index.
        Args:
            schedule (SyncSchedule): The schedule configuration containing information
                about keys, initial load, and the index of the mirrored file to process.
        Returns:
            int: The next index to process after staging the Spark output.
        """

        lz = cls.__get_lz(schedule)

        cls.__log_formatted(schedule, f"Writing Mirror Metadata File")
        lz.generate_metadata_file(schedule.Keys)

        cls.__log_formatted(schedule, f"Processing Spark staging files - index: {schedule.MirrorFileIndex}")
        next_index = lz.stage_spark_output(schedule.MirrorFileIndex)

        return next_index
    
    @classmethod
    def save_to_landing_zone(cls, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        """
        Saves the provided DataFrame to the landing zone as Parquet files, inserting or updating row markers
        based on the specified load strategy.
        This method applies row markers according to the load strategy:
        • Uses specific marker values (e.g., INSERT, UPDATE, DELETE, UPSERT) to track the row-level changes. 
        • Then converts complex types in the DataFrame to JSON strings.
        • Optionally reduces the number of partitions for performance optimization.
        Parameters
        ----------
        schedule : SyncSchedule
            Configuration object containing sync and load settings (e.g., load strategy, initial load, keys).
        df : DataFrame
            The Spark DataFrame to save, which may be updated with row marker columns and transformed columns.
        Returns
        -------
        DataFrame
            The modified Spark DataFrame after applying row markers and potential partition adjustments.
        """
        cls.__log_formatted(schedule, f"Saving to Landing Zone...")        
        lz = cls.__get_lz(schedule)
        cls.__log_formatted(schedule, f"Using Landing Zone {lz._get_onelake_path(lz.SCRATCH_PATH)}...")

        df = cls.__shape_df(schedule, df)
        df = cls.__repartition_df(df)

        df.write.mode("overwrite").format("parquet").save(f"{lz._get_onelake_path(lz.SCRATCH_PATH)}/")

        return df
    
    @classmethod
    def __shape_df(cls, schedule:SyncSchedule,  df:DataFrame) -> DataFrame:
        """
        Shapes the DataFrame for the mirrored database by applying row markers and converting complex types to JSON strings.
        This method modifies the DataFrame based on the synchronization schedule's load strategy and keys.
        Args:
            df (DataFrame): The Spark DataFrame to be shaped.
            schedule (SyncSchedule): The synchronization schedule containing configuration details.
        Returns:
            DataFrame: The modified DataFrame ready for saving to the landing zone.
        """

        cls.__log_formatted(schedule, f"Shaping DF for Mirroed Database...")
        meta_cols = [SyncConstants.MIRROR_ROW_MARKER]
        df_cols = df.columns
        df_cols = [c for c in df_cols if c not in ["BQ_CDC_CHANGE_TYPE", "BQ_CDC_CHANGE_TIMESTAMP"]]

        if not schedule.InitialLoad and schedule.HasKeys:
            if schedule.Load_Strategy == SyncLoadStrategy.CDC:
                cls.__log_formatted(schedule, f"{schedule.Load_Strategy} - Row Marker - BQ CDC CHANGE TYPE")
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, 
                    when(col("BQ_CDC_CHANGE_TYPE") == "UPDATE", lit(FabricCDCType.UPDATE))
                        .when(col("BQ_CDC_CHANGE_TYPE") == "DELETE", lit(FabricCDCType.DELETE))
                        .otherwise(lit(FabricCDCType.INSERT)))
            elif schedule.Load_Strategy != SyncLoadStrategy.FULL or schedule.Load_Type == SyncLoadType.MERGE:
                cls.__log_formatted(schedule, f"Row Marker - {FabricCDCType.UPSERT}")
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.UPSERT))
            else:
                cls.__log_formatted(schedule, f"Row Marker - {FabricCDCType.INSERT}")
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.INSERT))
        else:
            cls.__log_formatted(schedule, f"Row Marker - {FabricCDCType.INSERT}")
            df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.INSERT))

        df = df.select(*meta_cols, *df_cols)
        df = Util.convert_complex_types_to_json_str(df)
        cls.__log_formatted(schedule, f"Shaped DF for Mirrored Database...")

        return df

    @classmethod
    def __get_partition_counts(cls, df:DataFrame) -> tuple[int, int]:
        """
        Counts the number of partitions in a DataFrame and the number of non-empty partitions.
        This method uses an accumulator to count non-empty partitions efficiently.
        Args:
            df (DataFrame): The Spark DataFrame to analyze.
        Returns:
            tuple[int, int]:
                A tuple containing the total number of partitions and the count of non-empty partitions.
        """
        #accumulator = cls.Context.sparkContext.accumulator(0)
        #df.foreachPartition(lambda partition: accumulator.add(1 if len(list(partition))> 0 else 0))

        # OVERRIDE: to allow for further testing
        num_partitions = df.rdd.getNumPartitions()
        return (num_partitions, num_partitions)

    @classmethod
    def __repartition_df(cls, df:DataFrame) -> DataFrame:
        """
        Resolves the number of partitions for a given DataFrame.
        This method checks the number of partitions in the DataFrame and returns
        a reduced number if it exceeds a certain threshold.
        Args:
            df (DataFrame): The Spark DataFrame to check for partitions.
        Returns:
            DataFrame:
                The DataFrame with potentially reduced partitions.
        """
        num_partitions, non_empty_partitions = cls.__get_partition_counts(df)
        has_empty_partitions = num_partitions > non_empty_partitions

        if num_partitions > 4:
            partitions = int(num_partitions // 4)
        else:
            partitions = 1

        cls.Logger.debug(f"DF has {num_partitions} Partitions ({non_empty_partitions} Not Empty)...target is {partitions} partitions...")

        if num_partitions > partitions:
            cls.Logger.debug(f"Reducing partitions from {num_partitions} to {partitions}")

            if has_empty_partitions:
                cls.Logger.debug(f"Empty partitions exist...using repartition...")
                df = df.repartition(partitions)
            else:
                cls.Logger.debug(f"No empty partitions...using coalesce...")
                df = df.coalesce(partitions)
        
        return df
    
    @classmethod
    def __log_formatted(cls, schedule:SyncSchedule, msg:str) -> None:
        """
        Logs a formatted message with the schedule's Lakehouse table name.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the Lakehouse table name.
            msg (str): The message to log.
        Returns:
            None
        """
        cls.Logger.debug(f"MIRRORING - {schedule.LakehouseTableName} - {msg}")