from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.FileSystem import OpenMirrorLandingZone
from FabricSync.BQ.APIClient import FabricAPI
from FabricSync.BQ.Logging import Telemetry

from FabricSync.BQ.Enum import (
    SyncLoadStrategy, FabricCDCType
)

class OpenMirror(ContextAwareBase):
    @classmethod
    def start_mirror(cls, mirror_id:str, wait:bool=False) -> None:
        """
        Starts a mirroring process for the specified mirror.
        This method checks the current status of the mirror using the provided API token.
        If the mirror is stopped, it initiates the mirroring process. If the mirror is already
        in a running or starting state, no action is taken. Optionally, it can wait until the
        mirror is confirmed to be running before returning.
        Args:
            mirror_id (str): Unique identifier of the mirror to be started.
            wait (bool, optional): If True, the method will wait until the mirror's status is 
                'running' before returning. Defaults to False.
        Returns:
            None: The function does not return any value.
        """

        fabric_api = FabricAPI(cls.WorkspaceID, cls.FabricAPIToken)
        cls.Logger.sync_status(f"Starting Mirror - {mirror_id}", verbose=True)

        while(True):
            status = fabric_api.OpenMirroredDatabase.get_mirroring_status(mirror_id)

            cls.Logger.sync_status(f"Starting Mirror - Mirror {mirror_id} Status: {status}", verbose=True)

            if status.lower() == "stopped":
                fabric_api.OpenMirroredDatabase.start_mirroring(mirror_id)

                if wait:
                    fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "running")
                
                break
            elif status.lower() in ["running", "starting"]:
                cls.Logger.sync_status(f"Starting Mirror - Mirror {mirror_id} - Already {status}", verbose=True)
                break
            else:
                cls.Logger.sync_status(f"Starting Mirror - Mirror {mirror_id} is currently {status} - waiting...", verbose=True)
                fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "stopped")
    
    @classmethod
    def stop_mirror(cls, mirror_id:str, wait:bool=False) -> None:
        """
        Stops an ongoing mirror operation for the specified mirror.
        This method checks the current mirroring status of the given mirror ID and stops it if it is running. If 'wait' is set to True, it will also wait until the mirroring status transitions to 'stopped'. If the mirror is already stopped or stopping, a message is logged and the method returns immediately.
        Args:
            mirror_id (str): The unique identifier of the mirror to stop.
            wait (bool, optional): Whether to wait until the mirror is fully stopped. Defaults to False.
        Returns:
            None
        """
        
        fabric_api = FabricAPI(cls.WorkspaceID, cls.FabricAPIToken)
        cls.Logger.sync_status(f"Stopping Mirror - {mirror_id}", verbose=True)

        while(True):
            status = fabric_api.OpenMirroredDatabase.get_mirroring_status(mirror_id)

            cls.Logger.sync_status(f"Stopping Mirror - Mirror {mirror_id} Status: {status}", verbose=True)

            if status.lower() == "running":
                fabric_api.OpenMirroredDatabase.stop_mirroring(mirror_id)

                if wait:
                    fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "stopped")
                
                break
            elif status.lower() in ["stopped", "stopping"]:
                cls.Logger.sync_status(f"Stopping Mirror - Mirror {mirror_id} - Already {status}", verbose=True)
                break
            else:
                cls.Logger.sync_status(f"Stopping Mirror - Mirror {mirror_id} is currently {status} - waiting...", verbose=True)
                fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "running")

    
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
        
        #if schedule.InitialLoad:
        #    cls.__log_formatted(schedule, f"Initializing Mirrored Table")
        #    lz.generate_metadata_file(schedule.Keys)

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

        lz = cls.__get_lz(schedule)
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
            elif schedule.Load_Strategy != SyncLoadStrategy.FULL:
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

        num_partitions = df.rdd.getNumPartitions()

        if num_partitions > 1:
            partitions = int(num_partitions/2)
            cls.__log_formatted(schedule, f"Reducing partitions from {num_partitions} to {partitions}")
            df = df.repartition(partitions)

        df.write.mode("overwrite").format("parquet").save(f"{lz._get_onelake_path(lz.SCRATCH_PATH)}/")

        return df
    
    @classmethod
    def __log_formatted(cls, schedule:SyncSchedule, msg:str) -> None:
        cls.Logger.sync_status(f"MIRRORING - {schedule.LakehouseTableName} - {msg}", verbose=True)