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
from FabricSync.BQ.Utils import Util

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
    """
    Base class for context-aware classes.
    Provides access to the Spark context and logger.
    
    Attributes:
        Context (SparkSession): The Spark context.
        Logger (Logger): The logger.
        ApplicationID (str): The application ID.
        SafeID (str): A safe version of the application ID.
        ID (str): The application ID.
        Version (pv.Version): The version of the application.
        TelemetryEndpoint (str): The telemetry endpoint.
        LogLevel (str): The log level.
        LogPath (str): The log path.
        Telemetry (str): The telemetry setting.
        WorkspaceID (str): The workspace ID.
        MetadataLakehouse (str): The metadata lakehouse.
        MetadataLakehouseID (str): The metadata lakehouse ID.
        MetadataLakehouseSchema (str): The metadata lakehouse schema.
        TargetLakehouse (str): The target lakehouse.
        TargetLakehouseID (str): The target lakehouse ID.
        TargetLakehouseSchema (str): The target lakehouse schema.
        FabricAPIToken (str): The Fabric API token.
        UserConfigPath (str): The user configuration path.
        EnableSchemas (bool): True if schemas are enabled, otherwise False.
        GCPCredential (str): The GCP credentials.
    """ 
    __context:SparkSession = None

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
        return SyncLogger.getLogger()
    
    @classproperty
    def ApplicationID(cls) -> str:
        """
        Gets the application ID.
        Returns:
            str: The application ID.
        """
        return Session.ApplicationID
    
    @classproperty
    def SafeID(cls) -> str:
        """
        Gets a safe version of the application ID.
        Returns:
            str: The safe application ID.
        """
        return Util.remove_special_characters(cls.ID.lower())

    @classproperty
    def ID(cls) -> str:
        """
        Gets the application ID.
        Returns:
            str: The application ID.
        """
        return Session.ID if Session.ID else "Fabric_Sync_Default"
    
    @classproperty
    def Version(cls) -> pv.Version:
        """
        Gets the version of the application.
        Returns:
            pv.Version: The version of the application.
        """
        return Session.Version
    
    @classproperty
    def TelemetryEndpoint(cls) -> str:
        """
        Gets the telemetry endpoint.
        Returns:
            str: The telemetry endpoint.
        """
        return Session.TelemetryEndpoint
    
    @classproperty
    def LogLevel(cls) -> str:
        """
        Gets the log level.
        Returns:
            str: The log level.
        """
        return Session.LogLevel
    
    @classproperty
    def LogPath(cls) -> str:
        """
        Gets the log path.
        Returns:
            str: The log path.
        """
        return Session.LogPath
    
    @classproperty
    def Telemetry(cls) -> str:
        """
        Gets the telemetry setting.
        Returns:
            str: The telemetry setting.
        """
        return Session.Telemetry

    @classproperty
    def WorkspaceID(cls) -> str:
        """
        Gets the workspace ID.
        Returns:
            str: The workspace ID.
        """
        return Session.WorkspaceID
    
    @classproperty
    def MetadataLakehouse(cls) -> str:
        """
        Gets the metadata lakehouse.
        Returns:
            str: The metadata lakehouse.
        """
        return Session.MetadataLakehouse
    
    @classproperty
    def MetadataLakehouseID(cls) -> str:
        """
        Gets the metadata lakehouse ID.
        Returns:
            str: The metadata lakehouse ID.
        """
        return Session.MetadataLakehouseID
    
    @classproperty
    def MetadataLakehouseSchema(cls) -> str:
        """
        Gets the metadata lakehouse schema.
        Returns:
            str: The metadata lakehouse schema.
        """
        return Session.MetadataLakehouseSchema
    
    @classproperty
    def TargetLakehouse(cls) -> str:
        """
        Gets the target lakehouse.
        Returns:
            str: The target lakehouse.
        """
        return Session.TargetLakehouse
    
    @classproperty
    def TargetLakehouseID(cls) -> str:
        """
        Gets the target lakehouse ID.
        Returns:
            str: The target lakehouse ID.
        """
        return Session.TargetLakehouseID
    
    @classproperty
    def TargetLakehouseSchema(cls) -> str:
        """
        Gets the target lakehouse schema.
        Returns:
            str: The target lakehouse schema.
        """
        return Session.TargetLakehouseSchema
    
    @classproperty
    def FabricAPIToken(cls) -> str:
        """
        Gets the Fabric API token.
        Returns:
            str: The Fabric API token.
        """
        return Session.FabricAPIToken

    @classproperty
    def UserConfigPath(cls) -> str:
        """
        Gets the user configuration path.
        Returns:
            str: The user configuration path.
        """
        return Session.UserConfigPath

    @classproperty
    def EnableSchemas(cls) -> bool:
        """
        Gets the enable schemas setting.
        Returns:
            bool: True if schemas are enabled, otherwise False.
        """
        return Session.EnableSchemas
    
    @classproperty
    def GCPCredential(cls) -> str:
        """
        Gets the GCP credentials.
        Returns:
            str: The GCP credentials.
        """
        return Session.GCPCredentials

class DeltaTableMaintenance(ContextAwareBase):
    """
    Class for maintaining Delta tables.
    Provides methods to drop partitions, drop tables, optimize, and vacuum Delta tables.
    Attributes:
        TableName (str): The name of the Delta table.
        DeltaTable (DeltaTable): The Delta table object.
        __detail (Row): The detail of the Delta table.
    """
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
        Drops the Delta table.
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