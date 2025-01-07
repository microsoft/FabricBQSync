from delta.tables import *
from pyspark.sql import SparkSession, DataFrame
import warnings

from .Model.Config import *
from .Core import *
from .Logging import *

class DeltaTableMaintenance:
    __detail:Row = None

    def __init__(self, context:SparkSession, table_nm:str):
        self.Context = context
        self.TableName = table_nm
        self.DeltaTable = DeltaTable.forName(context, table_nm)
        self.Logger = SyncLogger(context).get_logger()
    
    @property
    def CurrentTableVersion(self) -> int:
        history = self.get_table_history() \
            .select(max(col("version")).alias("delta_version"))

        return [r[0] for r in history.collect()][0]

    @property
    def Detail(self) -> DataFrame:
        if not self.__detail:
            self.__detail = self.DeltaTable.detail().collect()[0]
        
        return self.__detail
    @property
    def OneLakeLocation(self) -> str:
        return self.Detail["location"]

    def get_table_history(self, only_current_day:bool = False) -> DataFrame:
        history = self.DeltaTable.history()

        if only_current_day:
            history = history.filter("CAST(timestamp AS DATE) = current_date()")

        return history
    
    def drop_partition(self, partition_filter:str):
        self.DeltaTable.delete(partition_filter)

    def drop_table(self):
        self.Context.sql(f"DROP TABLE IF EXISTS {self.TableName}")
    
    def optimize_and_vacuum(self, partition_filter:str = None):
        self.optimize(partition_filter)
        self.vacuum()
    
    def optimize(self, partition_filter:str = None):
        if partition_filter:
            self.DeltaTable.optimize().where(partition_filter).executeCompaction()
        else:
            self.DeltaTable.optimize().executeCompaction()

    def vacuum(self):
        self.DeltaTable.vacuum(0)