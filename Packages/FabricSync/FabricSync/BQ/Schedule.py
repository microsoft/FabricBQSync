from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

from .Model.Config import *
from .Core import *
from .Metastore import *

class BQScheduler(ConfigBase):
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        super().__init__(context, user_config, gcp_credential)

    @Telemetry.Scheduler
    def build_schedule(self, schedule_type:ScheduleType):
        self.Logger.sync_status(f"Scheduling {self.UserConfig.ID} for {schedule_type}...")
        self.Metastore.build_schedule(self.UserConfig.ID, schedule_type)        
        self.Logger.sync_status(f"Sync Schedule {self.UserConfig.ID}({schedule_type}) Ready...")