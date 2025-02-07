from pyspark.sql import (
    SparkSession, DataFrame, Row
)
from pyspark.sql.functions import (
    max, col
)
from logging import Logger
from packaging import version as pv
from delta.tables import DeltaTable
from pyspark.sql.functions import col

from FabricSync.BQ.Logging import SyncLogger
from FabricSync.BQ.SessionManager import Session
class classproperty(property):
    def __get__(self, owner_self, owner_cls):
        """
        Get the value of the property.
        Args:
            owner_self: The owner self.
            owner_cls: The owner class.
        Returns:
            The value of the property.
        """
        return self.fget(owner_cls)

class ContextAwareBase:   
    __context:SparkSession = None
    __logger:Logger = None

    @classproperty
    def Context(cls) -> SparkSession:
        """
        Gets the Spark context.
        Returns:
            SparkSession: The Spark context.
        """
        if not cls.__context:
            cls.__context = SparkSession.getActiveSession()

        return cls.__context
    
    @classproperty
    def Logger(cls) -> Logger:
        """
        Gets the logger.
        Returns:
            Logger: The logger.
        """
        if cls.__logger is None:
            cls.__logger = SyncLogger.getLogger()
        
        return cls.__logger
    
    @classproperty
    def ApplicationID(cls) -> str:
        return Session.ApplicationID
    
    @classproperty
    def ID(cls) -> str:
        return Session.ID
    
    @classproperty
    def Version(cls) -> pv.Version:
        return Session.Version
    
    @classproperty
    def TelemetryEndpoint(cls) -> str:
        return Session.TelemetryEndpoint
    
    @classproperty
    def LogLevel(cls) -> str:
        return Session.LogLevel
    
    @classproperty
    def LogPath(cls) -> str:
        return Session.LogPath
    
    @classproperty
    def Telemetry(cls) -> str:
        return Session.Telemetry

    @classproperty
    def WorkspaceID(cls) -> str:
        return Session.WorkspaceID
    
    @classproperty
    def MetadataLakehouse(cls) -> str:
        return Session.MetadataLakehouse
    
    @classproperty
    def MetadataLakehouseID(cls) -> str:
        return Session.MetadataLakehouseID
    
    @classproperty
    def MetadataLakehouseSchema(cls) -> str:
        return Session.MetadataLakehouseSchema
    
    @classproperty
    def TargetLakehouse(cls) -> str:
        return Session.TargetLakehouse
    
    @classproperty
    def TargetLakehouseID(cls) -> str:
        return Session.TargetLakehouseID
    
    @classproperty
    def TargetLakehouseSchema(cls) -> str:
        return Session.TargetLakehouseSchema
    
    @classproperty
    def FabricAPIToken(cls) -> str:
        return Session.FabricAPIToken

    @classproperty
    def UserConfigPath(cls) -> str:
        return Session.UserConfigPath

    @classproperty
    def EnableSchemas(cls) -> bool:
        return Session.EnableSchemas
    
    @classproperty
    def GCPCredential(cls) -> str:
        return Session.GCPCredentials

class DeltaTableMaintenance(ContextAwareBase):
    __detail:Row = None

    def __init__(self, table_name:str, table_path:str=None) -> None:
        """
        Initializes a new instance of the DeltaTableMaintenance class.
        Args:
            table_name (str): The table name.
            table_path (str): The table path.
        """
        self.TableName = table_name

        if table_path:
            self.DeltaTable = DeltaTable.forPath(self.Context, table_path)
        else:
            self.DeltaTable = DeltaTable.forName(self.Context, table_name)
    
    @property
    def CurrentTableVersion(self) -> int:
        """
        Gets the current table version.
        Returns:
            int: The current table version.
        """
        history = self.DeltaTable.history() \
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