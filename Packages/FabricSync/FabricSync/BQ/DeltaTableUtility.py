from delta.tables import DeltaTable
from pyspark.sql import (
    SparkSession, Row, DataFrame
)
from pyspark.sql.functions import (
    col, max
)

from FabricSync.BQ.Logging import SyncLogger

class DeltaTableMaintenance:
    __detail:Row = None

    def __init__(self, table_name:str, table_path:str=None) -> None:
        """
        Initializes a new instance of the DeltaTableMaintenance class.
        Args:
            table_name (str): The table name.
            table_path (str): The table path.
        """
        self.Context = SparkSession.builder.getOrCreate()
        self.TableName = table_name

        if table_path:
            self.DeltaTable = DeltaTable.forPath(self.Context, table_path)
        else:
            self.DeltaTable = DeltaTable.forName(self.Context, table_name)

        self.Logger = SyncLogger().get_logger()
    
    @property
    def CurrentTableVersion(self) -> int:
        """
        Gets the current table version.
        Returns:
            int: The current table version.
        """
        history = self.get_table_history() \
            .select(max(col("version")).alias("delta_version"))

        return [r[0] for r in history.collect()][0]

    @property
    def Detail(self) -> DataFrame:
        """
        Gets the table detail.
        Returns:
            DataFrame: The table detail.
        """
        if not self.__detail:
            self.__detail = self.DeltaTable.detail().collect()[0]
        
        return self.__detail
        
    @property
    def OneLakeLocation(self) -> str:
        """
        Gets the OneLake location.
        Returns:
            str: The OneLake location.
        """
        return self.Detail["location"]

    def get_table_history(self, only_current_day:bool = False) -> DataFrame:
        """
        Gets the table history.
        Args:
            only_current_day (bool): Whether to get only the current day.
        Returns:
            DataFrame: The table history.
        """
        history = self.DeltaTable.history()

        if only_current_day:
            history = history.filter("CAST(timestamp AS DATE) = current_date()")

        return history
    
    def drop_partition(self, partition_filter:str) -> None:
        """
        Drops the partition.
        Args:
            partition_filter (str): The partition filter.
        """
        self.DeltaTable.delete(partition_filter)

    def drop_table(self) -> None:
        """
        Drops the table.
        """
        self.Context.sql(f"DROP TABLE IF EXISTS {self.TableName}")
    
    def optimize_and_vacuum(self, partition_filter:str = None) -> None:
        """
        Optimizes and vacuums the table.
        Args:
            partition_filter (str): The partition filter.
        """
        self.optimize(partition_filter)
        self.vacuum()
    
    def optimize(self, partition_filter:str = None) -> None:
        """
        Optimizes the table.
        Args:
            partition_filter (str): The partition filter.
        """
        if partition_filter:
            self.DeltaTable.optimize().where(partition_filter).executeCompaction()
        else:
            self.DeltaTable.optimize().executeCompaction()

    def vacuum(self) -> None:
        """
        Vacuums the table.
        """
        self.DeltaTable.vacuum(0)