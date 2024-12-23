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
from ..Setup.FabricSetup import *

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
            self.Logger.sync_status("BQ Metadata Update Failed!")
            self.Logger.error(e)
    
    def build_schedule(self, schedule_type:str = str(ScheduleType.AUTO), sync_metadata:bool = True):
        try:
            if sync_metadata:
                self.sync_metadata()
            else:
                self.MetadataLoader.create_proxy_views()

            self.Scheduler.build_schedule(schedule_type=ScheduleType[schedule_type])
        except SyncBaseError as e:
            self.Logger.sync_status("BQ Scheduler Failed!")
            self.Logger.error(e)

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
            self.Logger.sync_status("Run Schedule  Failed!")
            self.Logger.error(e)
    
    def check_for_updates(self, download:bool=False):
        update_available, update_version = SetupUtils.check_for_update(self.Context, self.UserConfig.Version)

        if update_available:
            self.Logger.sync_status(f"A newer version ({update_version}) of BQ Sync is available..your version is {self.UserConfig.Version}...")

            if download:
                random_int = random.randint(1, 1000)
                randomizer = f"0000{random_int}"
                git_notebooks = [ \
                            {"name": f"BQ-Sync-Upgrade-v{update_version}-{randomizer[-4:]}", 
                                "url": f"{Installer.GIT_URL}/Notebooks/v{update_version}/Upgrade.ipynb",
                                "file_name": "Upgrade.ipynb"}]
                
                workspace_id = self.UserConfig.Fabric.WorkspaceID

                if not workspace_id:
                    workspace_id = self.Context.conf.get("trident.workspace.id")

                try:
                    SetupUtils.download_notebooks(workspace_id, git_notebooks)
                    self.Logger.sync_status(f"The BQ Sync Upgrade notebook has been downloaded to your workspace...")
                except SyncInstallError as e:
                    self.Logger.error(e)
                    self.Logger.sync_status(f"Failed to download the BQ Sync Upgrade notebook...")
        else:
             self.Logger.sync_status(f"BQ Sync is up-to-data (Version: {self.UserConfig.Version})...") 