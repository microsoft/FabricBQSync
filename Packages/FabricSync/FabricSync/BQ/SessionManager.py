from pyspark.sql import SparkSession
from packaging import version as pv
from typing import Any
import py4j

from FabricSync.BQ.Enum import SparkSessionConfig, SyncLogLevel
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.Meta import Version

class StaticSessionMeta(type):
    @property
    def CurrentVersion(cls) -> pv.Version:
        return pv.parse(Version.CurrentVersion)
    
    @property
    def Context(cls) -> SparkSession:
        """
        Gets the Spark context.
        Returns:
            SparkSession: The Spark context.
        """
        if not cls._context:
            cls._context = SparkSession.getActiveSession()

        return cls._context
    
    @property
    def ApplicationID(cls) -> str:
        return cls.get_setting(SparkSessionConfig.APPLICATION_ID)
    
    @ApplicationID.setter
    def ApplicationID(cls, value:str):
        cls.set_setting(SparkSessionConfig.APPLICATION_ID, value)
    
    @property
    def ID(cls) -> str:
        return cls.get_setting(SparkSessionConfig.NAME)
    
    @ID.setter
    def ID(cls, value:str):
        cls.set_setting(SparkSessionConfig.NAME, value)
    
    @property
    def Version(cls) -> pv.Version:
        c = cls.get_setting(SparkSessionConfig.VERSION, "0.0.0")
        return pv.parse(c)
    
    @Version.setter
    def Version(cls, value:Any):
        cls.set_setting(SparkSessionConfig.VERSION, value)
    
    @property
    def TelemetryEndpoint(cls) -> str:
        return cls.get_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, "prdbqsyncinsights")
    
    @TelemetryEndpoint.setter
    def TelemetryEndpoint(cls, value:str):
        cls.set_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, value)
    
    @property
    def LogLevel(cls) -> str:
        return cls.get_setting(SparkSessionConfig.LOG_LEVEL, SyncLogLevel.SYNC_STATUS)
    
    @LogLevel.setter
    def LogLevel(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_LEVEL, value)

    @property
    def LogPath(cls) -> str:
        return cls.get_setting(SparkSessionConfig.LOG_PATH, "/lakehouse/default/Files/BQ_Sync_Process/logs/fabric_sync.log")
    
    @LogPath.setter
    def LogPath(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_PATH, value)
    
    @property
    def Telemetry(cls) -> str:
        return cls.get_setting(SparkSessionConfig.LOG_TELEMETRY, True)

    @Telemetry.setter
    def Telemetry(cls, value:str):
        cls.set_setting(SparkSessionConfig.LOG_TELEMETRY, value)

    @property
    def WorkspaceID(cls) -> str:
        if Session.get_setting(SparkSessionConfig.WORKSPACE_ID):
            return cls.get_setting(SparkSessionConfig.WORKSPACE_ID)
        else:
            return cls.Context.conf.get("trident.workspace.id")
    
    @WorkspaceID.setter
    def WorkspaceID(cls, value:str):
        cls.set_setting(SparkSessionConfig.WORKSPACE_ID, value)

    @property
    def MetadataLakehouse(cls) -> str:
        if cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE):
            return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE)
        else:
            return cls.Context.conf.get("trident.lakehouse.name")
    
    @MetadataLakehouse.setter
    def MetadataLakehouse(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE, value)

    @property
    def MetadataLakehouseID(cls) -> str:
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID):
            return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID)
        else:
            return cls.Context.conf.get("trident.lakehouse.id")
    
    @MetadataLakehouseID.setter
    def MetadataLakehouseID(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID, value)

    @property
    def MetadataLakehouseSchema(cls) -> str:
        return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA)
    
    @MetadataLakehouseSchema.setter
    def MetadataLakehouseSchema(cls, value:str):
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA, value)

    @property
    def TargetLakehouse(cls) -> str:
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE)
    
    @TargetLakehouse.setter
    def TargetLakehouse(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE, value)

    @property
    def TargetLakehouseID(cls) -> str:
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID)
    
    @TargetLakehouseID.setter
    def TargetLakehouseID(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID, value)

    @property
    def TargetLakehouseSchema(cls) -> str:
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA)
    
    @TargetLakehouseSchema.setter
    def TargetLakehouseSchema(cls, value:str):
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA, value)

    @property
    def FabricAPIToken(cls) -> str:
        return cls.get_setting(SparkSessionConfig.FABRIC_API_TOKEN)

    @FabricAPIToken.setter
    def FabricAPIToken(cls, value:str):
        cls.set_setting(SparkSessionConfig.FABRIC_API_TOKEN, value)

    @property
    def UserConfigPath(cls) -> str:
        return cls.get_setting(SparkSessionConfig.USER_CONFIG_PATH)

    @UserConfigPath.setter
    def UserConfigPath(cls, value:str):
        cls.set_setting(SparkSessionConfig.USER_CONFIG_PATH, value)

    @property
    def EnableSchemas(cls) -> bool:
        return cls.get_setting(SparkSessionConfig.SCHEMA_ENABLED, "").lower() == "true"

    @EnableSchemas.setter
    def EnableSchemas(cls, value:str):
        cls.set_setting(SparkSessionConfig.SCHEMA_ENABLED, value)

    @property
    def SyncViewState(cls) -> bool:
        """
        Gets the flag for enable schemas.
        Returns:
            bool: The enable schemas.
        """
        return cls.get_setting(SparkSessionConfig.SYNC_VIEW_STATE, "").lower() == "true"
    
    @SyncViewState.setter
    def SyncViewState(cls, value:bool) -> None:
        cls.set_setting(SparkSessionConfig.SYNC_VIEW_STATE, value)

class Session(metaclass=StaticSessionMeta):
    _context:SparkSession = None
    
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