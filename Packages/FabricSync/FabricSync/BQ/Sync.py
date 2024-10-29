from pyspark.sql.functions import *
from pyspark.sql.types import *

from ..Config import *
from ..Core import *
from ..Enum import *
from .SyncUtils import *
from .Loader import BQScheduleLoader
from .Metadata import ConfigMetadataLoader
from .Schedule import Scheduler

class BQSync(SyncBase):
    """
    BQSync class for synchronizing metadata, building schedules, and running schedules with BigQuery.
    Methods:
        __init__(context: SparkSession, config_path: str):
        sync_metadata():
        build_schedule(sync_metadata: bool = True, schedule_type: str = str(ScheduleType.AUTO)) -> str:
        run_schedule(group_schedule_id: str, optimize_metadata: bool = True):
    """

    def __init__(self, context:SparkSession, config_path:str):
        """
        Initializes the Sync class with the given Spark session context and configuration path.
        Args:
            context (SparkSession): The Spark session context.
            config_path (str): The path to the configuration file.
        Attributes:
            MetadataLoader (ConfigMetadataLoader): Loads metadata configuration.
            Scheduler (Scheduler): Manages scheduling tasks.
            Loader (BQScheduleLoader): Loads BigQuery schedules.
            DataRetention (BQDataRetention): Manages data retention policies.
        """

        super().__init__(context, config_path, clean_session=True)

        self.MetadataLoader = ConfigMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = Scheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)
        self.DataRetention = BQDataRetention(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        """
        Synchronizes metadata using the MetadataLoader instance.
        This method performs the following actions:
        1. Calls the `sync_metadata` method of the `MetadataLoader` instance to synchronize metadata.
        2. If the `Autodetect` attribute of `UserConfig` is set to True, it calls the `auto_detect_config` method of the `MetadataLoader` instance to automatically detect the configuration.
        """

        self.MetadataLoader.sync_metadata()

        if self.UserConfig.Autodetect:
            self.MetadataLoader.auto_detect_config()
    
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = str(ScheduleType.AUTO)) -> str:
        """
        Builds a schedule for synchronization.
        Args:
            sync_metadata (bool): If True, synchronizes metadata. If False, creates proxy views. Default is True.
            schedule_type (str): The type of schedule to build. Default is 'AUTO'.
        Returns:
            str: The built schedule.
        """

        if sync_metadata:
            self.sync_metadata()
        else:
            self.MetadataLoader.create_proxy_views()

        return self.Scheduler.build_schedule(schedule_type=ScheduleType[schedule_type])

    def run_schedule(self, group_schedule_id:str, optimize_metadata:bool=True):
        """
        Executes the schedule for the given group schedule ID.
        This method performs the following steps:
        1. Creates proxy views using the MetadataLoader.
        2. Ensures schemas are set up if schema support is enabled in the user configuration.
        3. Runs the schedule using the Loader.
        4. Commits the table configuration to the Metastore.
        5. Executes data retention policies if enabled in the user configuration.
        6. Optimizes metadata tables if the optimize_metadata flag is set to True.
        Args:
            group_schedule_id (str): The ID of the group schedule to run.
            optimize_metadata (bool, optional): Flag to indicate whether to optimize metadata tables. Defaults to True.
        """

        self.MetadataLoader.create_proxy_views()

        if self.UserConfig.Fabric.EnableSchemas:
            self.Metastore.ensure_schemas(self.UserConfig.Fabric.WorkspaceId, self.UserConfig.ID)

        self.Loader.run_schedule(group_schedule_id)
        
        self.Metastore.commit_table_configuration(group_schedule_id)
        
        if self.UserConfig.EnableDataExpiration:
            self.DataRetention.execute()
        
        if optimize_metadata:
            self.Metastore.optimize_metadata_tbls()