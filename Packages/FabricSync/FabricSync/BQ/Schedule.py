
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.Model.Config import ConfigBase
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Enum import SyncScheduleType

class BQScheduler(ConfigBase):
    def __init__(self, user_config):
        """
        Initialize the Schedule instance with the provided user configuration.
        :param user_config: A dictionary or object containing user-specific settings.
        """

        super().__init__(user_config)

    @Telemetry.Scheduler
    def build_schedule(self, schedule_type:SyncScheduleType) -> None:
        """
        Builds a synchronization schedule for the given schedule type.
        This method logs the scheduling process, creates the schedule in the metastore,
        and logs the completion status. 
        Args:
            schedule_type (SyncScheduleType): The synchronization schedule type to build.
        Returns:
            None
        """

        self.Logger.sync_status(f"Scheduling {self.UserConfig.ID} for {schedule_type}...", verbose=True)
        FabricMetastore.build_schedule(schedule_type)        
        self.Logger.sync_status(f"Sync Schedule {self.UserConfig.ID}({schedule_type}) Ready...")