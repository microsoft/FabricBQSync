from pyspark.sql import Row

from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.SyncCore import ConfigBase
from FabricSync.BQ.SyncUtils import SyncUtil
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Core import DeltaTableMaintenance
class BQDataRetention(ConfigBase):
    """
    Represents a class for enforcing BigQuerydata expiration/retention policy.
    """
    def __init__(self):
        """
        Initializes a new instance of the BQDataRetention class.
        """
        super().__init__()
    
    def execute(self) -> None:   
        """
        Executes the data expiration/retention policy.
        """     
        self.Logger.debug("Enforcing Data Expiration/Retention Policy...")
        self.__enforce_retention_policy()
        self.Logger.sync_status("Updating Data Expiration/Retention Policy...")
        FabricMetastore.sync_retention_config()

    @Telemetry.Data_Expiration()
    def expire_data(self, row:Row) -> None:
        """
        Expires the data.
        Args:
            row (Row): The row.
        """
        if row["use_lakehouse_schema"]:
            table_name = f"{row['lakehouse']}.{row['lakehouse_schema']}.{row['lakehouse_table_name']}"
        else:
            table_name = f"{row['lakehouse']}.{row['lakehouse_table_name']}"

        table_maint = DeltaTableMaintenance(table_name)

        if row["partition_id"]:
            self.Logger.sync_status(f"Expiring Partition: {table_name}${row['partition_id']}")
            predicate = SyncUtil.resolve_fabric_partition_predicate(row["partition_type"], row["partition_column"], 
                row["partition_grain"], row["partition_id"])
            table_maint.drop_partition(predicate)
        else:
            self.Logger.sync_status(f"Expiring Table: {table_name}")
            table_maint.drop_table()  

    def __enforce_retention_policy(self) -> None:
        """
        Enforces the retention policy.
        """
        df = FabricMetastore.get_bq_retention_policy()

        for d in df.collect():
            self.expire_data(d) 