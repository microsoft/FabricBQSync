from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random

from .Model.Config import *
from .Core import *
from .Enum import *
from .SyncUtils import *
from .Loader import BQScheduleLoader
from .Metadata import BQMetadataLoader
from .Schedule import BQScheduler
from .Logging import *
from .Exceptions import *
from .Expiration import BQDataRetention

class BQSync(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path, clean_session=True)

        self.MetadataLoader = BQMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = BQScheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)
        self.DataRetention = BQDataRetention(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        try:
            self.MetadataLoader.sync_metadata()

            if self.UserConfig.Autodetect:
                self.MetadataLoader.auto_detect_config()
        except SyncBaseError as e:
            self.Logger.error(f"BQ Metadata Update Failed: {e}")
    
    def build_schedule(self, schedule_type:str = str(ScheduleType.AUTO), sync_metadata:bool = True):
        try:
            if sync_metadata:
                self.sync_metadata()
            else:
                self.Metastore.create_proxy_views()

            self.Scheduler.build_schedule(schedule_type=ScheduleType[schedule_type])
        except SyncBaseError as e:
            self.Logger.error(f"BQ Scheduler Failed: {e}")

    @Telemetry.Delta_Maintenance(maintainence_type="SYNC_METADATA")
    def optimize_metadata_tbls(self):
        self.Logger.sync_status("Optimizing Sync Metadata Metastore...")
        SyncUtil.optimize_bq_sync_metastore(self.Context)

    def run_schedule(self, schedule_type:str, build_schedule:bool=True, sync_metadata:bool=False, optimize_metadata:bool=True):
        try:
            if build_schedule:
                self.build_schedule(schedule_type=schedule_type, sync_metadata=sync_metadata)

            if self.UserConfig.Fabric.EnableSchemas:
                self.Metastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceId, self.UserConfig.ID)

            initial_loads = self.Loader.run_schedule(self.UserConfig.ID, schedule_type)
            
            if initial_loads:
                self.Logger.sync_status("Committing Sync Table Configuration...")
                self.Metastore.commit_table_configuration(self.UserConfig.ID, schedule_type)
            
            if self.UserConfig.EnableDataExpiration:
                self.Logger.sync_status(f"Data Expiration started...")
                with SyncTimer() as t:
                    self.DataRetention.execute()
                self.Logger.sync_status(f"Data Expiration completed in {str(t)}...")
            
            if optimize_metadata:
                self.Logger.sync_status(f"Metastore Metadata Optimization started...")
                with SyncTimer() as t:
                    self.optimize_metadata_tbls()
                self.Logger.sync_status(f"Metastore Metadata Optimization completed in {str(t)}...")
            
            self.Logger.sync_status("Run Schedule Done!!")
        except SyncBaseError as e:
            self.Logger.error(f"Run Schedule Failed: {e}")  