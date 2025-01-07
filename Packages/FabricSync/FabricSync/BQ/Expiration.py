from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

from .Model.Config import *
from .Core import *
from .DeltaTableUtility import *
from .SyncUtils import SyncUtil
from .Logging import *

class BQDataRetention(ConfigBase):
    """
    Class responsible for BQ Table and Partition Expiration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        super().__init__(context, user_config, gcp_credential)
    
    def execute(self):        
        self.Logger.sync_status("Enforcing Data Expiration/Retention Policy...")
        self._enforce_retention_policy()
        self.Logger.sync_status("Updating Data Expiration/Retention Policy...")
        self.Metastore.sync_retention_config(self.UserConfig.ID)

    @Telemetry.Data_Expiration()
    def expire_data(self, row):
        if row["use_lakehouse_schema"]:
            table_name = f"{row['lakehouse']}.{row['lakehouse_schema']}.{row['lakehouse_table_name']}"
        else:
            table_name = f"{row['lakehouse']}.{row['lakehouse_table_name']}"

        table_maint = DeltaTableMaintenance(self.Context, table_name)

        if row["partition_id"]:
            self.Logger.sync_status(f"Expiring Partition: {table_name}${row['partition_id']}")
            predicate = SyncUtil.resolve_fabric_partition_predicate(row["partition_type"], row["partition_column"], 
                row["partition_grain"], row["partition_id"])
            table_maint.drop_partition(predicate)
        else:
            self.Logger.sync_status(f"Expiring Table: {table_name}")
            table_maint.drop_table()  

    def _enforce_retention_policy(self):
        df = self.Metastore.get_bq_retention_policy(self.UserConfig.ID)

        for d in df.collect():
            self.expire_data(d) 