from pyspark.sql.functions import *
from pyspark.sql.types import *

from ..Config import *
from ..Core import *
from ..Enum import *
from .SyncUtils import *
from .Loader import BQScheduleLoader
from .Metadata import BQMetadataLoader
from .Schedule import BQScheduler


class BQSync(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path, clean_session=True)

        self.MetadataLoader = BQMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = BQScheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)
        self.DataRetention = BQDataRetention(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        metadata_loaded = self.MetadataLoader.sync_metadata()

        if self.UserConfig.Autodetect and metadata_loaded:
            self.MetadataLoader.auto_detect_config()
    
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = str(ScheduleType.AUTO)) -> str:
        if sync_metadata:
            self.sync_metadata()
        else:
            self.MetadataLoader.create_proxy_views()

        return self.Scheduler.build_schedule(schedule_type=ScheduleType[schedule_type])

    def run_schedule(self, group_schedule_id:str, optimize_metadata:bool=True):
        self.MetadataLoader.create_proxy_views()

        if self.UserConfig.Fabric.EnableSchemas:
            self.Metastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceId, self.UserConfig.ID)

        initial_loads = self.Loader.run_schedule(group_schedule_id)
        
        if initial_loads:
            self.Metastore.commit_table_configuration(group_schedule_id)
        
        if self.UserConfig.EnableDataExpiration:
            with SyncTimer() as t:
                self.DataRetention.execute()
            print(f"Data Expiration completed in {str(t)}...")
        
        if optimize_metadata:
            with SyncTimer() as t:
                self.Metastore.optimize_metadata_tbls()
            print(f"Metastore Metadata Optimization completed in {str(t)}...")