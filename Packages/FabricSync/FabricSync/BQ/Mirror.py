from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.FileSystem import OpenMirrorLandingZone
from FabricSync.BQ.APIClient import FabricAPI

from FabricSync.BQ.Enum import (
    SyncLoadStrategy, FabricCDCType
)

class OpenMirror(ContextAwareBase):
    @classmethod
    def start_mirror(cls, mirror_id:str, api_token:str, wait:bool=False) -> None:
        fabric_api = FabricAPI(cls.workspace_id, api_token)
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
    def stop_mirror(cls, mirror_id:str, api_token:str, wait:bool=False) -> None:
        fabric_api = FabricAPI(cls.workspace_id, api_token)
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
        return OpenMirrorLandingZone(
            schedule.WorkspaceId, 
            schedule.LakehouseId, 
            schedule.LakehouseSchema, 
            schedule.LakehouseTable
            )

    @classmethod
    def drop_mirrored_table(cls, schedule:SyncSchedule) -> int:
        lz = cls.__get_lz(schedule)
        lz.delete_table()

    @classmethod
    def cleanup_spark_files(cls, schedule:SyncSchedule) -> None:
        lz = cls.__get_lz(schedule)
        lz.cleanup_staged_lz()

    @classmethod
    def process_landing_zone(cls, schedule:SyncSchedule) -> int:
        lz = cls.__get_lz(schedule)

        if schedule.InitialLoad:
            cls.__log_formatted(schedule, f"Initializing Mirrored Table")
            lz.generate_metadata_file(schedule.Keys)

        cls.__log_formatted(schedule, f"Processing Spark staging files - index: {schedule.MirrorFileIndex}")
        next_index = lz.stage_spark_output(schedule.MirrorFileIndex)

        return next_index
    
    @classmethod
    def save_to_landing_zone(cls, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
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

        df.write.mode("append").format("parquet").save(lz._get_onelake_path(""))

        return df
    
    @classmethod
    def __log_formatted(cls, schedule:SyncSchedule, msg:str) -> None:
        cls.Logger.sync_status(f"MIRRORING - {schedule.LakehouseTableName} - {msg}", verbose=True)
