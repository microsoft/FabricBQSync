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
        fabric_api.OpenMirroredDatabase.start_mirroring(mirror_id)

        if wait:
            fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "running")
    
    @classmethod
    def stop_mirror(cls, mirror_id:str, api_token:str, wait:bool=False) -> None:
        fabric_api = FabricAPI(cls.workspace_id, api_token)
        fabric_api.OpenMirroredDatabase.stop_mirroring(mirror_id)

        if wait:
            fabric_api.OpenMirroredDatabase.wait_for_mirroring(mirror_id, "stopped")
    
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
    def cleanup_failed_sync(cls, schedule:SyncSchedule) -> None:
        lz = cls.__get_lz(schedule)
        lz.cleanup_staged_lz()

    @classmethod
    def process_landing_zone(cls, schedule:SyncSchedule) -> int:
        lz = cls.__get_lz(schedule)

        if schedule.InitialLoad:
            cls.__Logger.debug(f"Initializing Mirror Table: " +
                              f"{LakehouseCatalog.resolve_table_name(schedule.LakehouseSchema, schedule.LakehouseTable)}")
            lz.generate_metadata_file(schedule.keys)

        next_index = lz.stage_spark_output(schedule.MirrorFileIndex)
        
        return next_index
    
    @classmethod
    def save_to_landing_zone(cls, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        lz = cls.__get_lz(schedule)
        meta_cols = [SyncConstants.MIRROR_ROW_MARKER]
        df_cols = df.columns

        if not schedule.InitialLoad:
            if schedule.Load_Strategy == SyncLoadStrategy.CDC:
                df_cols = [c for c in df_cols if c not in ["BQ_CDC_CHANGE_TYPE", "BQ_CDC_CHANGE_TIMESTAMP"]]
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, 
                    when(col("BQ_CDC_CHANGE_TYPE") == "UPDATE", lit(FabricCDCType.UPDATE))
                        .when(col("BQ_CDC_CHANGE_TYPE") == "DELETE", lit(FabricCDCType.DELETE))
                        .otherwise(lit(FabricCDCType.INSERT)))
            elif schedule.HasKeys and schedule.Load_Strategy != SyncLoadStrategy.FULL:
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.UPSERT))
            else:
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.INSERT))
        else:
            df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, lit(FabricCDCType.INSERT))
        
        df = df.select(*meta_cols, *df_cols)

        #complex type handling
        df = Util.convert_complex_types_to_json_str(df)

        df.write.mode("append").format("parquet").save(lz._get_onelake_path(""))

        return df