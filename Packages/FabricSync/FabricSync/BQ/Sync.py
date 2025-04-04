from packaging import version as pv

from FabricSync.BQ.SyncCore import SyncBase
from FabricSync.BQ.Auth import (
    Credentials, TokenProvider
)
from FabricSync.BQ.Enum import (
    SyncScheduleType, FabricDestinationType
)
from FabricSync.BQ.Utils import SyncTimer
from FabricSync.BQ.Loader import BQScheduleLoader
from FabricSync.BQ.Metadata import BQMetadataLoader
from FabricSync.BQ.Schedule import BQScheduler
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Exceptions import SyncBaseError, SyncConfigurationError
from FabricSync.BQ.Expiration import BQDataRetention
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.APIClient import FabricAPI
from FabricSync.BQ.ModelValidation import UserConfigurationValidation
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.SessionManager import Session

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
            Session.reset()
            super().__init__(config_path, credentials)

            if self.__requires_update():
                self.__update_sync_runtime(config_path)

            config_validation = UserConfigurationValidation.validate(self.UserConfig)

            if not config_validation:
                self.MetadataLoader = BQMetadataLoader()
                self.Scheduler = BQScheduler()
                self.Loader = BQScheduleLoader()
                self.DataRetention = BQDataRetention()
            else:
                self.UserConfig = None
                self.Logger.sync_status(f"Failed to load Fabric Sync with User Configuration errors:\r\n" +
                    "\r\n".join(config_validation))

        except SyncConfigurationError as e:
            self.Logger.sync_status(f"FAILED TO INITIALIZE FABRIC SYNC\r\n{e}")

    def update_user_config_for_current(self) -> None:
        """
        Updates the user configuration to the current version of the Fabric Sync runtime.
        This method checks if the current runtime version is greater than the version
        stored in the user configuration. If so, it updates the configuration to the
        current version and validates the configuration settings.
        Returns:
            None
        """
        if not self._is_runtime_ready():
            return
            
        self.__validate_user_config(self.UserConfigPath)

    def __validate_user_config(self, config_path:str) -> None:
        """
        Validates the user configuration and updates it to the current runtime version.
        This method updates the user configuration to the current runtime version and
        validates the configuration settings. If the configuration is invalid, it logs
        an error message and sets the UserConfig property to None.
        Args:
            config_path (str): The path to the user configuration file.
        Returns:
            None
        """
        self.Logger.sync_status(f"Updating Fabric Sync user configuration...")
        config = ConfigDataset.from_json(config_path, False)
        config.Version = str(Session.CurrentVersion)
        config.Fabric.WorkspaceID = self.Context.conf.get("trident.workspace.id")

        #Override any configured workspace and use the current context
        workspace_id = Session.Context.conf.get("trident.workspace.id")

        fabric_api = FabricAPI(workspace_id, 
            self.TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))    

        config.Fabric.WorkspaceName = fabric_api.Workspace.get_name(workspace_id)

        if self.UserConfig.Fabric.MetadataLakehouse:
            config.Fabric.MetadataLakehouseID = fabric_api.Lakehouse.get_id(self.UserConfig.Fabric.MetadataLakehouse)

        if not config.Fabric.TargetType:
            config.Fabric.TargetType = FabricDestinationType.LAKEHOUSE
        
        if self.UserConfig.Fabric.TargetLakehouse:
            if config.Fabric.TargetType  == FabricDestinationType.LAKEHOUSE:
                config.Fabric.TargetLakehouseID = fabric_api.Lakehouse.get_id(self.UserConfig.Fabric.TargetLakehouse)
            else:
                config.Fabric.TargetLakehouseID = fabric_api.OpenMirroredDatabase.get_id(self.UserConfig.Fabric.TargetLakehouse)

        config.to_json(config_path)
        self.init_sync_session(config_path)

    def __update_sync_runtime(self, config_path:str) -> None:
        """
        Updates the Fabric Sync runtime to the current version.
        This method updates the Fabric Sync runtime to the current version by
        upgrading the sync metastore and validating the user configuration.
        Args:
            config_path (str): The path to the user configuration file.
        Returns:
            None
        """
        self.Logger.sync_status(f"Upgrading Fabric Sync metastore to v{str(Session.CurrentVersion)}...")
        LakehouseCatalog.upgrade_metastore(self.UserConfig.Fabric.get_metadata_lakehouse())
        self.__apply_manual_updates()
        self.__validate_user_config(config_path)

    def __apply_manual_updates(self) -> None:
        if self.Version < pv.parse("2.1.15"):
            self.Context.sql(f"""
                WITH tbls AS (
                    SELECT table_id, project_id, dataset, table_name 
                    FROM sync_configuration
                    WHERE sync_id='{self.UserConfig.ID}'
                )

                MERGE INTO sync_schedule s
                USING tbls t ON s.project_id=t.project_id AND s.dataset=t.dataset AND s.table_name=t.table_name
                WHEN MATCHED AND s.table_id IS NULL THEN
                    UPDATE SET
                        s.table_id=t.table_id
                """)

            self.Context.sql(f"""
                WITH tbls AS (
                    SELECT table_id, project_id, dataset, table_name 
                    FROM sync_configuration
                    WHERE sync_id='{self.UserConfig.ID}'
                )

                MERGE INTO sync_schedule_telemetry s
                USING tbls t ON s.project_id=t.project_id AND s.dataset=t.dataset AND s.table_name=t.table_name
                WHEN MATCHED AND s.table_id IS NULL THEN
                    UPDATE SET
                        s.table_id=t.table_id
                """)

    def __requires_update(self) -> bool:
        """
        Determines if the Fabric Sync runtime requires an update.
        This method checks if the current runtime version is greater than the version
        stored in the user configuration. If so, it logs a status message indicating
        the version mismatch and returns True. Otherwise, it returns False.
        Returns:
            bool: True if the runtime requires an update; otherwise, False.
        """
        if Session.CurrentVersion > self.Version:
            self.Logger.sync_status(f"Fabric Sync Config Version: " +
                f"{str(self.Version)} - Runtime Version: {str(Session.CurrentVersion)}")
            return True
        else:
            return False

    def _is_runtime_ready(self) -> bool:
        """
        Checks if the Fabric Sync runtime is ready for synchronization.
        This method verifies that the user configuration has been loaded and
        that the Fabric Sync runtime is ready to perform synchronization tasks.
        Returns:
            bool: True if the runtime is ready; otherwise, False.
        """
        if not self.UserConfig:
            self.Logger.sync_status("ERROR: Fabric Sync User Configuration must be loaded first. Please reload and try again.")
            return False
        else:
            return True

    def sync_metadata(self):
        """
        Synchronizes metadata in the BQ environment.
        This method triggers the metadata synchronization using the MetadataLoader instance.
        If 'Autodetect' is enabled in the user's configuration, it performs automatic
        detection of configuration settings.
        Raises:
            SyncBaseError: If a metadata update operation fails.
        """
        if not self._is_runtime_ready():
            return

        try:
            self.MetadataLoader.sync_metadata()
            self.sync_autodetect()
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
        if not self._is_runtime_ready():
            return

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
        if not self._is_runtime_ready():
            return

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
        if not self._is_runtime_ready():
            return

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
        if not self._is_runtime_ready():
            return

        try:
            if build_schedule:
                self.build_schedule(schedule_type, sync_metadata)

            if self.UserConfig.Fabric.EnableSchemas and self.UserConfig.Fabric.TargetType==FabricDestinationType.LAKEHOUSE:
                FabricMetastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceName)

            if self.UserConfig.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                Session.set_spark_conf("spark.sql.parquet.vorder.enabled", "true")
                Session.set_spark_conf("spark.databricks.delta.optimizeWrite.enabled", "true")
                Session.set_spark_conf("spark.databricks.delta.optimizeWrite.binSize", "1gb")
                Session.set_spark_conf("spark.databricks.delta.collect.stats", "true")

            initial_loads = self.Loader.run_schedule(schedule_type)
            
            if self.UserConfig.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                Session.set_spark_conf("spark.sql.parquet.vorder.enabled", "false")
                Session.set_spark_conf("spark.databricks.delta.optimizeWrite.enabled", "false")
                Session.set_spark_conf("spark.databricks.delta.collect.stats", "false")
            
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