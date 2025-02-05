from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.SyncCore import ConfigBase
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Enum import SyncScheduleType
from FabricSync.BQ.SyncUtils import SyncUtil

class BQScheduler(ConfigBase):
    def __init__(self):
        """
        Initialize the Schedule instance with the provided user configuration.
        """
        super().__init__()

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
        SyncUtil.ensure_sync_views()
        self.Logger.sync_status(f"Scheduling {self.ID} for {schedule_type}...", verbose=True)
        FabricMetastore.build_schedule(schedule_type)        
        self.Logger.sync_status(f"Sync Schedule {self.ID}({schedule_type}) Ready...")