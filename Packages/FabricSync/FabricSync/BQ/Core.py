from pyspark.sql import SparkSession
from logging import Logger
from packaging import version as pv
from typing import Any

from FabricSync.Meta import Version
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Logging import SyncLogger
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Enum import SparkSessionConfig
from FabricSync.BQ.Constants import SyncConstants

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
    def get_setting(cls, key:SparkSessionConfig) -> str:
        """
        Get the setting.
        Args:
            key (SparkSessionConfig): The key.
        Returns:
            str: The setting.
        """
        try:
            return cls.Context.conf.get(cls._get_setting_key(key))
        except Exception:
            return ""
    
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
        cls.Context.conf.set(cls._get_setting_key(key), str(value))
    
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
    def initialize_spark_session(cls, 
            config:ConfigDataset, user_config_path:str=None, fabric_api_token:str=None) -> None:
        """
        Initialize a Spark session and configure Spark settings based on the provided config.
        This function retrieves the current SparkSession (or creates one if it does not exist)
        and updates various configuration settings such as Delta Lake properties, partition
        overwrite mode, and custom application/logging metadata.
        Parameters:
            config (ConfigDataset): Contains application, logging, and telemetry settings.
            user_config_path (str): The path to the user configuration file.
            fabric_api_token (str): The Fabric API token.
        Returns:
            None
        """
        cls.Context.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        cls.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        cls.Context.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "7")
        cls.Context.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "3")
        cls.Context.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        cls.set_setting(SparkSessionConfig.APPLICATION_ID, config.ApplicationID)
        cls.set_setting(SparkSessionConfig.NAME, config.ID)
        cls.set_setting(SparkSessionConfig.LOG_PATH, config.Logging.LogPath)
        cls.set_setting(SparkSessionConfig.LOG_LEVEL, config.Logging.LogLevel)
        cls.set_setting(SparkSessionConfig.LOG_TELEMETRY, config.Logging.Telemetry)
        cls.set_setting(SparkSessionConfig.TELEMETRY_ENDPOINT, f"{config.Logging.TelemetryEndPoint}.azurewebsites.net")

        cls.set_setting(SparkSessionConfig.WORKSPACE_ID, config.Fabric.WorkspaceID)
        cls.set_setting(SparkSessionConfig.VERSION, "0.0.0" if not config.Version else config.Version)
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE, config.Fabric.MetadataLakehouse)
        cls.set_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID , config.Fabric.MetadataLakehouseID)
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE, config.Fabric.TargetLakehouse)
        cls.set_setting(SparkSessionConfig.TARGET_LAKEHOUSE_ID, config.Fabric.TargetLakehouseID)
        cls.set_setting(SparkSessionConfig.SCHEMA_ENABLED, config.Fabric.EnableSchemas)

        if fabric_api_token:
            cls.set_setting(SparkSessionConfig.FABRIC_API_TOKEN, fabric_api_token)

        if user_config_path:
            cls.set_setting(SparkSessionConfig.USER_CONFIG_PATH, user_config_path)

class ContextAwareBase():
    _Logger:Logger = None
    
    @classproperty
    def Logger(cls):
        """
        Gets the logger.
        Returns:
            Logger: The logger.
        """
        if cls._Logger is None:
            cls._Logger = SyncLogger().get_logger()
        
        return cls._Logger
    
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
        return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE)
    
    @classproperty
    def MetadataLakehouseID(cls) -> str:
        """
        Gets the metadata lakehouse ID.
        Returns:
            str: The metadata lakehouse ID.
        """
        return Session.get_setting(SparkSessionConfig.METADATA_LAKEHOUSE_ID)
    
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
        return Session.get_setting(SparkSessionConfig.SCHEMA_ENABLED).lower() == "true"