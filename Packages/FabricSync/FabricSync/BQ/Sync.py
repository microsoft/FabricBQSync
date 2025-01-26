from packaging import version as pv

from FabricSync.BQ.SyncCore import SyncBase
from FabricSync.BQ.Auth import (
    Credentials, TokenProvider
)
from FabricSync.BQ.Enum import (
    SyncScheduleType, FabricDestinationType
)
from FabricSync.BQ.SyncUtils import (
    SyncUtil, SyncTimer
)
from FabricSync.BQ.Loader import BQScheduleLoader
from FabricSync.BQ.Metadata import BQMetadataLoader
from FabricSync.BQ.Schedule import BQScheduler
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Exceptions import SyncBaseError, SyncConfigurationError
from FabricSync.BQ.Expiration import BQDataRetention
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.APIClient import FabricAPI
from FabricSync.Meta import Version

class BQSync(SyncBase):
    def __init__(self, config_path:str, credentials:Credentials):
        """
        Initializes the synchronization process for BigQuery by configuring logging, scheduling, and data retention.
        Args:
            config_path (str): The path to the sync configuration file.
            credentials (Credentials): The provider used to obtain necessary authentication credentials.
        Sets:
            self.Logger: Handles sync logging and status updates.
            self.MetadataLoader: Manages metadata operations for BigQuery.
            self.Scheduler: Orchestrates schedules for data loading tasks.
            self.Loader: Executes scheduled loads into BigQuery.
            self.DataRetention: Applies data retention policies in BigQuery.
        """

        try:
            super().__init__(config_path, credentials)

            self.__validate_app_versions()

            self.MetadataLoader = BQMetadataLoader(self.UserConfig)
            self.Scheduler = BQScheduler(self.UserConfig)
            self.Loader = BQScheduleLoader(self.UserConfig, self.TokenProvider)
            self.DataRetention = BQDataRetention(self.UserConfig)
        except SyncConfigurationError as e:
            print(f"FAILED TO INITIALIZE FABRIC SYNC\r\n{e}")

    def update_user_config_for_current(self):
        self.__validate_user_config()

    def __validate_user_config(self):
        self.Logger.sync_status(f"Updating Fabric Sync user configuration...")
        self.UserConfig.Version = Version.CurrentVersion
        self.UserConfig.Fabric.WorkspaceID = self.Context.conf.get("trident.workspace.id")

        fabric_api = FabricAPI(self.UserConfig.Fabric.WorkspaceID, 
            self.TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))    

        self.UserConfig.Fabric.WorkspaceName = fabric_api.Workspace.get_name(self.UserConfig.Fabric.WorkspaceID)
        self.UserConfig.Fabric.MetadataLakehouseID = fabric_api.Lakehouse.get_id(self.UserConfig.Fabric.MetadataLakehouse)
        self.UserConfig.Fabric.TargetLakehouseID = fabric_api.Lakehouse.get_id(self.UserConfig.Fabric.TargetLakehouse)

        if not self.UserConfig.Fabric.TargetType:
            self.UserConfig.Fabric.TargetType = FabricDestinationType.LAKEHOUSE
        
        self.UserConfig.to_json(self.ConfigPath)
        self.load_user_config()

    def __validate_app_versions(self):
        runtime_version = pv.parse("0.0.0" if not self.UserConfig.Version else self.UserConfig.Version)
        current_version = pv.parse(Version.CurrentVersion)

        if current_version > runtime_version:
            self.Logger.sync_status(f"Fabric Sync Config Version: " +
                f"{'Unspecified' if not self.UserConfig.Version else self.UserConfig.Version} " +
                f"- Runtime Version: {Version.CurrentVersion}")

            self.Logger.sync_status(f"Upgrading Fabric Sync metastore to v{Version.CurrentVersion}...")
            LakehouseCatalog.upgrade_metastore(self.UserConfig.Fabric.MetadataLakehouse)
            
            self.__validate_user_config()

    def sync_metadata(self):
        """
        Synchronizes metadata in the BQ environment.
        This method triggers the metadata synchronization using the MetadataLoader instance.
        If 'Autodetect' is enabled in the user's configuration, it performs automatic
        detection of configuration settings.
        Raises:
            SyncBaseError: If a metadata update operation fails.
        """    
        try:
            self.MetadataLoader.sync_metadata()

            if self.UserConfig.Autodetect:
                self.MetadataLoader.auto_detect_config()
        except SyncBaseError as e:
            self.Logger.error(f"BQ Metadata Update Failed: {e}")
    
    def sync_autodetect(self):
        """
        Automatically updates BigQuery metadata if autodetection is enabled.
        This method checks if autodetection is enabled in the user configuration.
        If enabled, it creates the necessary proxy views and attempts to
        autodetect the metadata configuration. Logs an error if metadata
        autodetection fails.
        Raises:
            SyncBaseError: If any errors occur during metadata autodetection.
        """

        try:
            if self.UserConfig.Autodetect:
                self.MetadataLoader.auto_detect_config()
        except SyncBaseError as e:
            self.Logger.error(f"BQ Metadata Autodetect Failed: {e}")

    def build_schedule(self, schedule_type:SyncScheduleType = SyncScheduleType.AUTO, sync_metadata:bool = True):
        """
        Builds or updates the synchronization schedule in BigQuery.
        This method triggers metadata synchronization if requested, or creates
        proxy views directly if metadata syncing is skipped. Afterwards, it
        instructs the Scheduler to build the schedule based on the given schedule type.
        Args:
            schedule_type (SyncScheduleType, optional): The type of schedule to create (e.g., manual or automatic).
            sync_metadata (bool, optional): Determines whether to synchronize metadata before building the schedule.
        Raises:
            SyncBaseError: If there is a failure during metadata synchronization or schedule creation.
        """

        try:
            if sync_metadata:
                self.sync_metadata()

            self.Scheduler.build_schedule(schedule_type)
        except SyncBaseError as e:
            self.Logger.error(f"BQ Scheduler Failed: {e}")

    @Telemetry.Delta_Maintenance(maintainence_type="SYNC_METADATA")
    def optimize_metadata_tbls(self):
        """
        Optimize metadata tables in the sync metastore.
        Logs a status message indicating the optimization process
        and then invokes the SyncUtil utility to perform the actual
        optimization on the sync metadata metastore.
        """

        self.Logger.sync_status("Optimizing Sync Metadata Metastore...", verbose=True)
        LakehouseCatalog.optimize_sync_metastore()

    def run_schedule(self, schedule_type:SyncScheduleType, build_schedule:bool=True, sync_metadata:bool=False, optimize_metadata:bool=True):
        """
        Runs the data synchronization schedule, optionally building and synchronizing metadata.
        Args:
            schedule_type (SyncScheduleType): The type of schedule to run (e.g., incremental or full).
            build_schedule (bool, optional): If True, builds the schedule before running. Defaults to True.
            sync_metadata (bool, optional): If True, performs metadata synchronization. Defaults to False.
            optimize_metadata (bool, optional): If True, optimizes metadata tables after synchronization. Defaults to True.
        Raises:
            SyncBaseError: If a synchronization-related error is encountered.
        """

        try:
            if build_schedule:
                self.build_schedule(schedule_type, sync_metadata)

            if self.UserConfig.Fabric.EnableSchemas and self.UserConfig.Fabric.TargetType==FabricDestinationType.LAKEHOUSE:
                FabricMetastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceName)

            initial_loads = self.Loader.run_schedule(schedule_type)
            
            if initial_loads:
                self.Logger.sync_status("Committing Sync Table Configuration...", verbose=True)
                FabricMetastore.commit_table_configuration(schedule_type)
            
            if self.UserConfig.EnableDataExpiration:
                self.Logger.sync_status(f"Data Expiration started...", verbose=True)
                with SyncTimer() as t:
                    self.DataRetention.execute()
                self.Logger.sync_status(f"Data Expiration completed in {str(t)}...")
            
            if optimize_metadata:
                self.Logger.sync_status(f"Metastore Metadata Optimization started...", verbose=True)
                with SyncTimer() as t:
                    self.optimize_metadata_tbls()
                self.Logger.sync_status(f"Metastore Metadata Optimization completed in {str(t)}...")
            
            self.Logger.sync_status("Run Schedule Done!!")
        except SyncBaseError as e:
            self.Logger.error(f"Run Schedule Failed: {e}")