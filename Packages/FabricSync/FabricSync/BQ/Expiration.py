from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

from .Model.Config import *
from .Core import *
from .Admin.DeltaTableUtility import *
from .SyncUtils import SyncUtil

class BQDataRetention(ConfigBase):
    """
    Class responsible for BQ Table and Partition Expiration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(context, user_config, gcp_credential)
    
    def execute(self):        
        self.Logger.Sync_Status("Enforcing Data Expiration/Retention Policy...")
        self._enforce_retention_policy()
        self.Logger.Sync_Status("Updating Data Expiration/Retention Policy...")
        self.Metastore.sync_retention_config(self.UserConfig.ID)

    def _enforce_retention_policy(self):
        df = self.Metastore.get_bq_retention_policy(self.UserConfig.ID)

        for d in df.collect():
            if d["use_lakehouse_schema"]:
                table_name = f"{d['lakehouse']}.{d['lakehouse_schema']}.{d['lakehouse_table_name']}"
            else:
                table_name = f"{d['lakehouse']}.{d['lakehouse_table_name']}"

            table_maint = DeltaTableMaintenance(self.Context, table_name)

            if d["partition_id"]:
                self.Logger.Sync_Status(f"Expiring Partition: {table_name}${d['partition_id']}")
                predicate = SyncUtil.resolve_fabric_partition_predicate(d["partition_type"], d["partition_column"], 
                    d["partition_grain"], d["partition_id"])
                table_maint.drop_partition(predicate)
            else:
                self.Logger.Sync_Status(f"Expiring Table: {table_name}")
                table_maint.drop_table()  