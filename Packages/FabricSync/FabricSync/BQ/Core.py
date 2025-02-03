from pyspark.sql import (
    SparkSession, DataFrame, Row
)
from pyspark.sql.functions import (
    max, col
)
from logging import Logger
from packaging import version as pv
from delta.tables import DeltaTable
from typing import Any
from pyspark.sql.functions import col
import py4j

from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Enum import SparkSessionConfig
from FabricSync.Meta import Version
from FabricSync.BQ.Logging import SyncLogger

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

class Session:
    _context:SparkSession = None

    @classproperty
    def CurrentVersion(cls) -> pv.Version:
        return pv.parse(Version.CurrentVersion)
    
    @classproperty
    def Context(cls) -> SparkSession:
        """
        Gets the Spark context.
        Returns:
            SparkSession: The Spark context.
        """
        if not cls._context:
            cls._context = SparkSession.getActiveSession()

        return cls._context
    
    @classmethod
    def get_setting(cls, key:SparkSessionConfig, default:Any = None) -> str:
        """
        Get the setting.
        Args:
            key (SparkSessionConfig): The key.
        Returns:
            str: The setting.
        """
        try:
            return cls.Context.conf.get(cls._get_setting_key(key))
        except py4j.protocol.Py4JJavaError:
            return default
    
    @classmethod
    def set_setting(cls, key:SparkSessionConfig, value:Any) -> None:
        """
        Set the setting.
        Args:
            key (SparkSessionConfig): The key.
            value (str): The value.
        Returns:
            None
        """
        cls.set_spark_conf(cls._get_setting_key(key), str(value))
    
    @classmethod
    def set_spark_conf(cls, key:str, value:str) -> None:
        if value != None:
            cls.Context.conf.set(key, value)

    @classmethod
    def _get_setting_key(cls, key:SparkSessionConfig) -> str:
        """
        Get the setting key.
        Args:
            key (SparkSessionConfig): The key.
        Returns:
            str: The setting key.
        """
        return f"{SyncConstants.SPARK_CONF_PREFIX}.{key.value}"
    
    @classmethod
    def print_session_settings(cls):        
        [print(f"{cls._get_setting_key(k)}: {cls.get_setting(k)}") for k in list(SparkSessionConfig)]
    
    @classproperty
    def ApplicationID(cls) -> str:
        return Session.get_setting(SparkSessionConfig.APPLICATION_ID)
    
    @ApplicationID.setter
    def ApplicationID(cls, value:str):
        cls.set_setting(SparkSessionConfig.APPLICATION_ID, value)
    
    @classproperty
    def ID(cls) -> str:
        return Session.get_setting(SparkSessionConfig.NAME)
    
    @ID.setter
    def ID(cls, value:str):
        cls.set_setting(SparkSessionConfig.NAME, value)
    
    @classproperty
    def Version(cls) -> pv.Version:
        return pv.parse(Session.get_setting(SparkSessionConfig.VERSION, "0.0.0"))
    
    @Version.setter
    def Version(cls, value:str):
        cls.set_setting(SparkSessionConfig.VERSION, value)
    
    @classproperty
    def TelemetryEndpoint(cls) -> str:
        return Session.get_setting(SparkSessionConfig.TELEMETRY_ENDPOINT)
    
    @TelemetryEndpoint.setter
    def TelemetryEndpoint(cls, value:str):
        cls.set_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, value)
    
    @classproperty
    def LogLevel(cls) -> str:
        return Session.get_setting(SparkSessionConfig.LOG_LEVEL)
    
    @LogLevel.setter
    def LogLevel(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_LEVEL, value)

    @classproperty
    def LogPath(cls) -> str:
        return Session.get_setting(SparkSessionConfig.LOG_PATH)
    
    @LogPath.setter
    def LogPath(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_PATH, value)
    
    @classproperty
    def Telemetry(cls) -> str:
        return Session.get_setting(SparkSessionConfig.LOG_TELEMETRY)

    @Telemetry.setter
    def Telemetry(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_TELEMETRY, value)

    @classproperty
    def WorkspaceID(cls) -> str:
        if Session.get_setting(SparkSessionConfig.WORKSPACE_ID):
            return Session.get_setting(SparkSessionConfig.WORKSPACE_ID)
        else:
            return Session.Context.conf.get("trident.workspace.id")
    
    @WorkspaceID.setter
    def WorkspaceID(cls, value:str):
        cls.set_setting(SparkSessionConfig.WORKSPACE_ID, value)

    @classproperty
    def MetadataLakehouse(cls) -> str:
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE):
            return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE)
        else:
            return Session.Context.conf.get("trident.lakehouse.name")
    
    @MetadataLakehouse.setter
    def MetadataLakehouse(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE, value)

    @classproperty
    def MetadataLakehouseID(cls) -> str:
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID):
            return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID)
        else:
            return Session.Context.conf.get("trident.lakehouse.id")
    
    @MetadataLakehouseID.setter
    def MetadataLakehouseID(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID, value)

    @classproperty
    def MetadataLakehouseSchema(cls) -> str:
        return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA)
    
    @MetadataLakehouseSchema.setter
    def MetadataLakehouseSchema(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA, value)

    @classproperty
    def TargetLakehouse(cls) -> str:
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE)
    
    @TargetLakehouse.setter
    def TargetLakehouse(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE, value)

    @classproperty
    def TargetLakehouseID(cls) -> str:
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID)
    
    @TargetLakehouseID.setter
    def TargetLakehouseID(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID, value)

    @classproperty
    def TargetLakehouseSchema(cls) -> str:
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA)
    
    @TargetLakehouseSchema.setter
    def TargetLakehouseSchema(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA, value)

    @classproperty
    def FabricAPIToken(cls) -> str:
        return Session.get_setting(SparkSessionConfig.FABRIC_API_TOKEN)

    @FabricAPIToken.setter
    def FabricAPIToken(cls, value:str):
        cls.set_setting(SparkSessionConfig.FABRIC_API_TOKEN, value)

    @classproperty
    def UserConfigPath(cls) -> str:
        return Session.get_setting(SparkSessionConfig.USER_CONFIG_PATH)

    @UserConfigPath.setter
    def UserConfigPath(cls, value:str):
        cls.set_setting(SparkSessionConfig.USER_CONFIG_PATH, value)

    @classproperty
    def EnableSchemas(cls) -> bool:
        return Session.get_setting(SparkSessionConfig.SCHEMA_ENABLED, "").lower() == "true"

    @EnableSchemas.setter
    def EnableSchemas(cls, value:str):
        cls.set_setting(SparkSessionConfig.SCHEMA_ENABLED, value)

    @classproperty
    def SyncViewState(cls) -> bool:
        """
        Gets the flag for enable schemas.
        Returns:
            bool: The enable schemas.
        """
        return Session.get_setting(SparkSessionConfig.SYNC_VIEW_STATE, "").lower() == "true"
    
    @SyncViewState.setter
    def SyncViewState(cls, value:bool) -> None:
        cls.set_setting(SparkSessionConfig.SYNC_VIEW_STATE, value)

class LoggingBase:
    __logger:Logger = None
    
    @classproperty
    def Logger(cls):
        """
        Gets the logger.
        Returns:
            Logger: The logger.
        """
        if cls.__logger is None:
            cls.__logger = SyncLogger.getLogger()
        
        return cls.__logger
    
class ContextAwareBase(LoggingBase):    
    @classproperty
    def Context(cls) -> SparkSession:
        return Session.Context

    @classproperty
    def ApplicationID(cls) -> str:
        """
        Gets the application ID.
        Returns:
            """
        return Session.get_setting(SparkSessionConfig.APPLICATION_ID)
    
    @classproperty
    def ID(cls) -> str:
        """
        Gets the sync ID.
        Returns:
            str: The sync ID.
        """
        return Session.get_setting(SparkSessionConfig.NAME)
    
    @classproperty
    def Version(cls) -> pv.Version:
        """
        Gets the version.
        Returns:
            Version: The configured runtine version.
        """
        return pv.parse(Session.get_setting(SparkSessionConfig.VERSION))
    
    @classproperty
    def TelemetryEndpoint(cls) -> str:
        """
        Gets the telemetry endpoint.
        Returns:
            """
        return Session.get_setting(SparkSessionConfig.TELEMETRY_ENDPOINT)
    
    @classproperty
    def LogLevel(cls) -> str:
        """
        Gets the log level.
        Returns:
            """
        return Session.get_setting(SparkSessionConfig.LOG_LEVEL)
    
    @classproperty
    def LogPath(cls) -> str:
        """
        Gets the log path.
        Returns:
            str: The log path.
        """
        return Session.get_setting(SparkSessionConfig.LOG_PATH)
    
    @classproperty
    def Telemetry(cls) -> str:
        """
        Gets the telemetry.
        Returns:
            
            """
        return Session.get_setting(SparkSessionConfig.LOG_TELEMETRY)

    @classproperty
    def WorkspaceID(cls) -> str:
        """
        Gets the workspace ID.
        Returns:
            str: The workspace ID.
        """
        if Session.get_setting(SparkSessionConfig.WORKSPACE_ID):
            return Session.get_setting(SparkSessionConfig.WORKSPACE_ID)
        else:
            return Session.Context.conf.get("trident.workspace.id")
    
    @classproperty
    def MetadataLakehouse(cls) -> str:
        """
        Gets the metadata lakehouse.
        Returns:
            str: The metadata lakehouse.
        """
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE):
            return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE)
        else:
            return Session.Context.conf.get("trident.lakehouse.name")
    
    @classproperty
    def MetadataLakehouseID(cls) -> str:
        """
        Gets the metadata lakehouse ID.
        Returns:
            str: The metadata lakehouse ID.
        """
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID):
            return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID)
        else:
            return Session.Context.conf.get("trident.lakehouse.id")
    
    @classproperty
    def MetadataLakehouseSchema(cls) -> str:
        """
        Gets the metadata lakehouse schema.
        Returns:
            str: The metadata lakehouse schema.
        """
        return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA)
    
    @classproperty
    def TargetLakehouse(cls) -> str:
        """
        Gets the target lakehouse.
        Returns:
            str: The target lakehouse.
        """
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE)
    
    @classproperty
    def TargetLakehouseID(cls) -> str:
        """
        Gets the target lakehouse ID.
        Returns:
            str: The target lakehouse ID.
        """
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID)
    
    @classproperty
    def TargetLakehouseSchema(cls) -> str:
        """
        Gets the target lakehouse schema.
        Returns:
            str: The target lakehouse schema.
        """
        return Session.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA)
    
    @classproperty
    def FabricAPIToken(cls) -> str:
        """
        Gets the target lakehouse schema.
        Returns:
            str: The target lakehouse schema.
        """
        return Session.get_setting(SparkSessionConfig.FABRIC_API_TOKEN)

    @classproperty
    def UserConfigPath(cls) -> str:
        """
        Gets the user config path.
        Returns:
            str: The user config path.
        """
        return Session.get_setting(SparkSessionConfig.USER_CONFIG_PATH)

    @classproperty
    def EnableSchemas(cls) -> bool:
        """
        Gets the flag for enable schemas.
        Returns:
            bool: The enable schemas.
        """
        return Session.get_setting(SparkSessionConfig.SCHEMA_ENABLED, "").lower() == "true"

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