from pyspark.sql.functions import *
from pyspark.sql.types import *

from ..Config import *
from ..Core import *
from ..Metastore import *

class BQScheduler(ConfigBase):
    """
    Class responsible for calculating the to-be run schedule based on the sync config and 
    the most recent BigQuery table metadata. Schedule is persisted to the Sync Schedule
    Delta table. When tables are scheduled but no updates are detected on the BigQuery side 
    a SKIPPED record is created for tracking purposes.
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls the parent init to load the user config JSON file
        """
        super().__init__(context, user_config, gcp_credential)

    def build_schedule(self, schedule_type:ScheduleType) -> str:
        print(f"Building Sync Schedule for {schedule_type} Schedule...")
        schedule_id = self.Metastore.get_current_schedule(self.UserConfig.ID, schedule_type)

        if not schedule_id:
            schedule_id = self.Metastore.build_new_schedule(schedule_type, self.UserConfig.ID,
                self.UserConfig.EnableViews, self.UserConfig.EnableMaterializedViews)
        
        return schedule_id