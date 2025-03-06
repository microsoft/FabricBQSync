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
        """
        Gets the current version of the package.
        Returns:
            Version: The current version of the package.
        """
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
        """
        Gets the application ID.
        Returns:
            str: The application ID.
        """
        return cls.get_setting(SparkSessionConfig.APPLICATION_ID)
    
    @ApplicationID.setter
    def ApplicationID(cls, value:str):
        """
        Sets the application ID.
        Args:
            value (str): The application ID.
        """
        cls.set_setting(SparkSessionConfig.APPLICATION_ID, value)
    
    @property
    def ID(cls) -> str:
        """
        Gets the ID.
        Returns:
            str: The ID.
        """
        return cls.get_setting(SparkSessionConfig.NAME)
    
    @ID.setter
    def ID(cls, value:str):
        """
        Sets the ID.
        Args:
            value (str): The ID.
        """
        cls.set_setting(SparkSessionConfig.NAME, value)
    
    @property
    def Version(cls) -> pv.Version:
        """
        Gets the current runtime version.
        Returns:
            Version: The curren runtime version.
        """
        c = cls.get_setting(SparkSessionConfig.VERSION, "0.0.0")
        return pv.parse(c)
    
    @Version.setter
    def Version(cls, value:Any):
        """
        Sets the current runtime version.
        Args:
            value (Any): The current runtime version.
        """
        cls.set_setting(SparkSessionConfig.VERSION, value)
    
    @property
    def TelemetryEndpoint(cls) -> str:
        """
        Gets the telemetry endpoint.
        Returns:
            str: The telemetry endpoint.
        """
        return cls.get_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, "prdbqsyncinsights.azurewebsites.net")

    @TelemetryEndpoint.setter
    def TelemetryEndpoint(cls, value:str):
        """
        Sets the telemetry endpoint.
        Args:
            value (str): The telemetry endpoint.
        """
        cls.set_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, value)
    
    @property
    def LogLevel(cls) -> str:
        """
        Gets the log level.
        Returns:
            str: The log level.
        """
        return cls.get_setting(SparkSessionConfig.LOG_LEVEL, SyncLogLevel.SYNC_STATUS)
    
    @LogLevel.setter
    def LogLevel(cls, value:str):
        """
        Sets the log level.
        Args:
            value (str): The log level.
        """
        cls.set_setting(SparkSessionConfig.LOG_LEVEL, value)

    @property
    def LogPath(cls) -> str:
        """
        Gets the log path.
        Returns:
            str: The log path.
        """
        return cls.get_setting(SparkSessionConfig.LOG_PATH, "/lakehouse/default/Files/Fabric_Sync_Process/logs/fabric_sync.log")
    
    @LogPath.setter
    def LogPath(cls, value:str):
        """
        Sets the log path.
        Args:
            value (str): The log path.
        """
        cls.set_setting(SparkSessionConfig.LOG_PATH, value)
    
    @property
    def Telemetry(cls) -> str:
        """
        Gets the flag for logging telemetry.
        Returns:
            bool: Log telemetry.
        """
        return cls.get_setting(SparkSessionConfig.LOG_TELEMETRY, True)

    @Telemetry.setter
    def Telemetry(cls, value:str):
        """
        Sets the flag for logging telemetry.
        Args:
            value (str): Log telemetry.
        """
        cls.set_setting(SparkSessionConfig.LOG_TELEMETRY, value)

    @property
    def WorkspaceID(cls) -> str:
        """
        Gets the workspace ID. If not set, it will return the workspace ID from the context.
        Returns:
            str: The workspace ID.
        """
        if Session.get_setting(SparkSessionConfig.WORKSPACE_ID):
            return cls.get_setting(SparkSessionConfig.WORKSPACE_ID)
        else:
            return cls.Context.conf.get("trident.workspace.id")
    
    @WorkspaceID.setter
    def WorkspaceID(cls, value:str):
        """
        Sets the workspace ID.
        Args:
            value (str): The workspace ID.
        """
        cls.set_setting(SparkSessionConfig.WORKSPACE_ID, value)

    @property
    def MetadataLakehouse(cls) -> str:
        """
        Gets the metadata lakehouse name. If not set, it will return the metadata lakehouse from the context.
        Returns:
            str: The metadata lakehouse name.
        """
        if cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE):
            return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE)
        else:
            return cls.Context.conf.get("trident.lakehouse.name")
    
    @MetadataLakehouse.setter
    def MetadataLakehouse(cls, value:str):
        """
        Sets the metadata lakehouse name.
        Args:
            value (str): The metadata lakehouse name.
        """
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE, value)

    @property
    def MetadataLakehouseID(cls) -> str:
        """
        Gets the metadata lakehouse ID. If not set, it will return the metadata lakehouse ID from the context.
        Returns:
            str: The metadata lakehouse ID.
        """
        if Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID):
            return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID)
        else:
            return cls.Context.conf.get("trident.lakehouse.id")
    
    @MetadataLakehouseID.setter
    def MetadataLakehouseID(cls, value:str):
        """
        Sets the metadata lakehouse ID.
        Args:
            value (str): The metadata lakehouse ID.
        """
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID, value)

    @property
    def MetadataLakehouseSchema(cls) -> str:
        """
        Gets the metadata lakehouse schema.
        Returns:
            str: The metadata lakehouse schema.
        """
        return cls.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA)
    
    @MetadataLakehouseSchema.setter
    def MetadataLakehouseSchema(cls, value:str):
        """
        Sets the metadata lakehouse schema.
        Args:
            value (str): The metadata lakehouse schema.
        """
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_SCHEMA, value)

    @property
    def TargetLakehouse(cls) -> str:
        """
        Gets the target lakehouse name.
        Returns:
            str: The target lakehouse name.
        """
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE)
    
    @TargetLakehouse.setter
    def TargetLakehouse(cls, value:str):
        """
        Sets the target lakehouse name.
        Args:
            value (str): The target lakehouse name.
        """
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE, value)

    @property
    def TargetLakehouseID(cls) -> str:
        """
        Gets the target lakehouse ID.
        Returns:
            str: The target lakehouse ID.
        """
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID)
    
    @TargetLakehouseID.setter
    def TargetLakehouseID(cls, value:str):
        """
        Sets the target lakehouse ID.
        Args:
            value (str): The target lakehouse ID.
        """
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID, value)

    @property
    def TargetLakehouseSchema(cls) -> str:
        """
        Gets the target lakehouse schema.
        Returns:
            str: The target lakehouse schema.
        """
        return cls.get_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA)
    
    @TargetLakehouseSchema.setter
    def TargetLakehouseSchema(cls, value:str):
        """
        Sets the target lakehouse schema.
        Args:
            value (str): The target lakehouse schema.
        """
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_SCHEMA, value)

    @property
    def FabricAPIToken(cls) -> str:
        """
        Gets the Fabric API token.
        Returns:
            str: The Fabric API token.
        """
        return cls.get_setting(SparkSessionConfig.FABRIC_API_TOKEN)

    @FabricAPIToken.setter
    def FabricAPIToken(cls, value:str):
        """
        Sets the Fabric API token.
        Args:
            value (str): The Fabric API token.
        """
        cls.set_setting(SparkSessionConfig.FABRIC_API_TOKEN, value)

    @property
    def UserConfigPath(cls) -> str:
        """
        Gets the user config path.
        Returns:
            str: The user config path.
        """
        return cls.get_setting(SparkSessionConfig.USER_CONFIG_PATH)

    @UserConfigPath.setter
    def UserConfigPath(cls, value:str):
        """
        Sets the user config path.
        Args:
            value (str): The user config path.
        """
        cls.set_setting(SparkSessionConfig.USER_CONFIG_PATH, value)

    @property
    def EnableSchemas(cls) -> bool:
        """
        Gets the flag for enabling schemas.
        Returns:
            bool: Enable schemas.
        """
        return cls.get_setting(SparkSessionConfig.SCHEMA_ENABLED, "").lower() == "true"

    @EnableSchemas.setter
    def EnableSchemas(cls, value:str):
        """
        Sets the flag for enabling schemas.
        Args:
            value (str): Enable schemas.
        """
        cls.set_setting(SparkSessionConfig.SCHEMA_ENABLED, value)

    @property
    def SyncViewState(cls) -> bool:
        """
        Gets the flag for to indicate whether temporary views have already been created.
        Returns:
            bool: Temporary views have already been created.
        """
        return cls.get_setting(SparkSessionConfig.SYNC_VIEW_STATE, "").lower() == "true"
    
    @SyncViewState.setter
    def SyncViewState(cls, value:bool) -> None:
        """
        Sets the flag for to indicate whether temporary views have already been created.
        """
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
        """
        Set the Spark configuration.
        Args:
            key (str): The key.
            value (str): The value.
        Returns:
            None
        """
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
        """
        Print the session settings.
        """
        [print(f"{cls._get_setting_key(k)}: {cls.get_setting(k)}") for k in list(SparkSessionConfig)]
    
    @classmethod
    def reset(cls):
        for k in list(SparkSessionConfig):
            cls.Context.conf.unset(cls._get_setting_key(k))