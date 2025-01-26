from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Logging import SyncLogger
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Model.Core import OpenMirrorTable
from FabricSync.BQ.FileSystem import OpenMirrorLandingZone
from FabricSync.BQ.APIClient import FabricAPI

from FabricSync.BQ.Enum import (
    SyncLoadStrategy, FabricCDCType
)

class OpenMirror:
    __Logger = SyncLogger().get_logger()

    def __init__(self, table:OpenMirrorTable, api_token:str) -> None:
        self._table = table
        self._lz = OpenMirrorLandingZone(table.workspace, table.lakehouse, table.schema, table.name)
        self.fabric_api = FabricAPI(api_token)

    @classmethod
    def start_mirror(cls, table:OpenMirrorTable, api_token:str, wait:bool) -> None:
        fabric_api = FabricAPI(api_token)
        fabric_api.OpenMirroredDatabase.start_mirroring(table.lakehouse)

        if wait:
            fabric_api.OpenMirroredDatabase.wait_for_mirroring(table.lakehouse, "running")
    
    @classmethod
    def stop_mirror(cls, table:OpenMirrorTable, api_token:str, wait:bool) -> None:
        fabric_api = FabricAPI(api_token)
        fabric_api.OpenMirroredDatabase.stop_mirroring(table.lakehouse)

        if wait:
            fabric_api.OpenMirroredDatabase.wait_for_mirroring(table.lakehouse, "stopped")
    
    @classmethod
    def sync_mirrored_table(cls, table:OpenMirrorTable) -> int:
        lz = OpenMirrorLandingZone(table.workspace, table.lakehouse, table.schema, table.name)

        if table.initialize:
            cls.__Logger.debug(f"Initializing Mirror Table: " +
                              f"{LakehouseCatalog.resolve_table_name(table.schema, table.name)}")
            lz.delete_table()
            lz.generate_metadata_file(table.keys)

        next_index = lz.stage_spark_output(table.file_index)
        
        return next_index
    
    @classmethod
    def save_to_landing_zone(cls, table:OpenMirrorTable, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        lz = OpenMirrorLandingZone(table.workspace, table.lakehouse, table.schema, table.name)
        meta_cols = [SyncConstants.MIRROR_ROW_MARKER]
        df_cols = df.columns

        if not schedule.InitialLoad:
            if schedule.Load_Strategy == SyncLoadStrategy.CDC:
                df_cols = [c for c in df_cols if c not in ["BQ_CDC_CHANGE_TYPE", "BQ_CDC_CHANGE_TIMESTAMP"]]
                df = df.withColumn(SyncConstants.MIRROR_ROW_MARKER, 
                    when(col("BQ_CDC_CHANGE_TYPE") == "UPDATE", lit(FabricCDCType.UPDATE))
                        .when(col("BQ_CDC_CHANGE_TYPE") == "DELETE", lit(FabricCDCType.DELETE))
                        .otherwise(lit(FabricCDCType.INSERT)))
            elif table.has_keys and schedule.Load_Strategy != SyncLoadStrategy.FULL:
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