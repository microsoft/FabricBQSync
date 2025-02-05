from enum import Enum

class BaseEnum(Enum):
    """
    Base Enum class.
    """
    def __str__(self):
        """
        Returns the string representation of the enum.
        Returns:
            str: The string representation of the enum.
        """
        return str(self.value)

class SparkSessionConfig(str, BaseEnum):
    """
    Represents the possible Spark session configurations.
    Attributes:
        APPLICATION_ID (str): The application ID.
        VERSION (str): The version.
        NAME (str): The application name.
        LOG_PATH (str): The log path.
        LOG_LEVEL (str): The log level.
        LOG_TELEMETRY (str): The log telemetry.
        TELEMETRY_ENDPOINT (str): The telemetry endpoint.
        WORKSPACE_ID (str): The workspace ID.
        METADATA_LAKEHOUSE (str): The metadata lakehouse.
        METADATA_LAKEHOUSE_SCHEMA (str): The metadata lakehouse schema.
        METADATA_LAKEHOUSE_ID (str): The metadata lakehouse ID.
        TARGET_LAKEHOUSE (str): The target lakehouse.
        TARGET_LAKEHOUSE_SCHEMA (str): The target lakehouse schema.
        TARGET_LAKEHOUSE_ID (str): The target lakehouse ID.
        SCHEMA_ENABLED (str): The schema enabled.
        FABRIC_API_TOKEN (str): The Fabric API token.
        USER_CONFIG_PATH (str): The user config path.
    """
    APPLICATION_ID = "application_id"
    VERSION = "version"
    NAME = "name"
    LOG_PATH = "log_path"
    LOG_LEVEL = "log_level"
    LOG_TELEMETRY = "log_telemetry"
    TELEMETRY_ENDPOINT = "telemetry_endpoint"
    WORKSPACE_ID = "workspace_id"
    METADATA_LAKEHOUSE = "metadata_lakehouse"
    METADATA_LAKEHOUSE_SCHEMA = "metadata_lakehouse_schema"
    METADATA_LAKEHOUSE_ID = "metadata_lakehouse_id"
    TARGET_LAKEHOUSE = "target_lakehouse"
    TARGET_LAKEHOUSE_SCHEMA = "target_lakehouse_schema"
    TARGET_LAKEHOUSE_ID = "target_lakehouse_id"
    SCHEMA_ENABLED = "schema_enabled"
    FABRIC_API_TOKEN = "fabric_api_token"
    USER_CONFIG_PATH = "user_config_path"
    SYNC_VIEW_STATE = "sync_view_state"

class FileSystemType(str, BaseEnum):
    """
    FileSystemType is an enumeration of supported file system types.
    Attributes:
        ABFSS (str): Azure Blob File System Storage.
        DBFS (str): Databricks File System.
        HDFS (str): Hadoop Distributed File System.
        S3 (str): Amazon S3.
        LOCAL (str): Local file system.
        UNKNOWN (str): Unknown file system.
    """
    ABFSS = "ABFSS"
    DBFS = "DBFS"
    HDFS = "HDFS"
    S3 = "S3"
    LOCAL = "LOCAL"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _from_pattern(cls, pattern: str) -> "FileSystemType":
        """
        Returns the file system type based on the provided pattern.
        Args:
            pattern (str): The file system pattern.
        Returns:
            FileSystemTypes: The file system type.
        """
        return {
            "abfss://": cls.ABFSS,
            "s3://": cls.S3,
            "s3a://": cls.S3,
            "dbfs://": cls.DBFS,
            "hdfs://": cls.HDFS,
            "file://": cls.LOCAL,
        }.get(pattern, cls.UNKNOWN)
    
class FabricItemType(str, BaseEnum):
    """
    A string-based enumeration representing different item types in Fabric.
    Attributes:
        WORKSPACE: Represents a Fabric workspace.
        ENVIRONMENT: Represents a Fabric environment.
        LAKEHOUSE: Represents a Fabric lakehouse.
        MIRRORED_DATABASE: Represents a mirrored database within Fabric.
        NOTEBOOK: Represents a Fabric notebook.
    """
    
    WORKSPACE = "workspaces"
    ENVIRONMENT = "environments"
    LAKEHOUSE = "lakehouses"
    MIRRORED_DATABASE = "mirroredDatabases"
    NOTEBOOK = "notebooks"

class LakehouseFileFormat(str, BaseEnum):
    """
    The LakehouseFileFormat class enumerates supported file formats within a lakehouse environment.
    Attributes:
        DELTA (str): Represents the Delta file format, often used for transactional data storage.
        PARQUET (str): Represents the Parquet file format, commonly used for efficient, columnar data storage.
    """
    DELTA = "delta"
    PARQUET = "parquet"

class FabricDestinationType(str, BaseEnum):
    """
    FabricDestinationType is an enumeration that defines different possible destinations for data in
    Fabric applications. The enumerations include:
        LAKEHOUSE: Represents a lakehouse environment for data storage and analytics.
        MIRRORED_DATABASE: Represents a mirrored database environment for data synchronization.
    Use these enum values to configure the destination type for Fabric pipelines or integrations.
    """

    LAKEHOUSE = "LAKEHOUSE"
    MIRRORED_DATABASE = "MIRRORED_DATABASE"

class FabricCDCType(str, BaseEnum):
    """
    FabricCDCType is an integer-based enumeration defining possible change data capture (CDC) operations.
    Attributes:
        INSERT (int): Indicates that a record was inserted.
        UPDATE (int): Indicates that a record was updated.
        DELETE (int): Indicates that a record was deleted.
        UPSERT (int): Indicates that a record was inserted or updated (upsert operation).
    """

    INSERT = "0"
    UPDATE = "1"
    DELETE = "2"
    UPSERT = "4"

class MaintenanceStrategy(str, BaseEnum):
    """
    MaintenanceStrategy is an enumeration of possible maintenance strategies.
    Attributes:
        SCHEDULED (str): Indicates that maintenance tasks are performed on a set schedule.
        INTELLIGENT (str): Indicates that maintenance tasks are performed based on intelligent heuristics or conditions.
    """
    SCHEDULED="SCHEDULED"
    INTELLIGENT="INTELLIGENT"

class SupportedTypeConversion(str, BaseEnum):
    """
    Represents the set of supported type conversions for configuring data transformations in BigQuery.
    Each member corresponds to a specific source-to-target type mapping, such as:
    - Converting a STRING value to a DATE or TIMESTAMP for date/time operations.
    - Converting a STRING value to an INT, LONG, or DECIMAL for numerical operations.
    - Converting a DATE or TIMESTAMP back into a STRING for readability.
    - Converting numeric types (INT, LONG, DECIMAL) to STRING for flexible data processing.
    This enumeration helps ensure consistent data handling across different stages of data engineering
    pipelines, allowing for reliable and predictable type casting in BigQuery.
    """
    STRING_TO_DATE = "STRING_TO_DATE"
    STRING_TO_TIMESTAMP = "STRING_TO_TIMESTAMP"
    STRING_TO_INT = "STRING_TO_INT"
    STRING_TO_LONG = "STRING_TO_LONG"
    STRING_TO_DECIMAL = "STRING_TO_DECIMAL"
    DATE_TO_STRING = "DATE_TO_STRING"
    TIMESTAMP_TO_STRING = "TIMESTAMP_TO_STRING"
    INT_TO_STRING = "INT_TO_STRING"
    DECIMAL_TO_STRING = "DECIMAL_TO_STRING"
    LONG_TO_STRING = "LONG_TO_STRING"

class ObjectFilterType(str, BaseEnum):
    """
    Defines possible filter behaviors for objects.
    INCLUDE: Indicates that the matching objects should be included.
    EXCLUDE: Indicates that the matching objects should be excluded.
    """
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"

class PredicateType(str, BaseEnum):
    """
    Represents a string-based predicate type enumeration.
    This enumeration extends both str and BaseEnum, allowing its members
    to behave like strings while also providing Enum-like semantics.
    Attributes:
        AND: Logical AND predicate.
        OR: Logical OR predicate.
    """
    AND = "AND"
    OR = "OR"

class SyncLogLevelName(str, BaseEnum):
    """
    Represents the possible log level names used for synchronization events.
    Attributes:
        DEBUG (str): Logs detailed debug information.
        INFO (str): Logs general information about system operations.
        WARNING (str): Logs warnings that may signal potential issues.
        SYNC_STATUS (str): Logs the current status of a synchronization process.
        TELEMETRY (str): Logs telemetry data for analysis.
        ERROR (str): Logs errors that disrupt normal operation.
        CRITICAL (str): Logs critical conditions affecting the system's stability.
    """
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    SYNC_STATUS = "SYNC_STATUS"
    TELEMETRY = "TELEMETRY"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class SyncLogLevel(int, BaseEnum):
    """
    Represents the possible log levels used for synchronization events.
    Attributes:
        DEBUG (int): Logs detailed debug information.
        INFO (int): Logs general information about system operations.
        WARNING (int): Logs warnings that may signal potential issues.
        SYNC_STATUS (int): Logs the current status of a synchronization process.
        TELEMETRY (int): Logs telemetry data for analysis.
        ERROR (int): Logs errors that disrupt normal operation.
        CRITICAL (int): Logs critical conditions affecting the system's stability.
    """
    DEBUG = 10
    INFO = 20
    WARNING = 30
    SYNC_STATUS = 34
    TELEMETRY = 35
    ERROR = 40
    CRITICAL = 50

class SyncLogFileInterval(str, BaseEnum):
    """
    Represents the possible log file intervals used for synchronization events.
    Attributes:
        MINUTE (str): Logs are stored in minute intervals.
        HOUR (str): Logs are stored in hourly
        DAY (str): Logs are stored in daily intervals.
        WEEK (str): Logs are stored in weekly intervals.
        MONTH (str): Logs are stored in monthly intervals.
    """
    HOUR = "H"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
    
class BigQueryObjectType(str, BaseEnum):
    """
    Represents the possible object types in BigQuery.
    Attributes:
        TABLE (str): Represents a standard table in BigQuery.
        VIEW (str): Represents a view in BigQuery.
        MATERIALIZED_VIEW (str): Represents a materialized view in BigQuery.
    """
    BASE_TABLE = "BASE_TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"

class SyncLoadStrategy(str, BaseEnum):
    """
    Represents the possible load strategies for synchronization tasks.
    Attributes:
        OVERWRITE (str): Overwrites existing data with new data.
        APPEND (str): Appends new data to existing data.
        MERGE (str): Merges new data with existing data.
        OPEN_MIRROR (str): Opens a mirror of the source data.
    """
    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"
    CDC_APPEND = "CDC_APPEND"
    CDC = "CDC"

class BQPartitionType(str, BaseEnum):
    """
    Represents the possible partition types in BigQuery.
    Attributes:
        DAY (str): Represents a daily partition.
        MONTH (str): Represents a monthly partition.
        YEAR (str): Represents a yearly partition.
    """
    TIME = "TIME"
    RANGE = "RANGE"
    TIME_INGESTION = "TIME_INGESTION"

class BQDataType(str, BaseEnum):
    """
    Represents the possible data types in BigQuery.
    Attributes:
        STRING (str): Represents a string data type.
        FLOAT64 (str): Represents a float64 data type.
        NUMERIC (str): Represents a numeric data type.
        BIGNUMERIC (str): Represents a bignumeric data type.
        DECIMAL (str): Represents a decimal data type.
        BIGDECIMAL (str): Represents a bigdecimal data type.
        TIMESTAMP (str): Represents a timestamp data type.
        DATE (str): Represents a date data type.
        TIME (str): Represents a time data type.
        INT64 (str): Represents an int64 data type.
        INT (str): Represents an int data type.
        SMALLINT (str): Represents a smallint data type.
        INTEGER (str): Represents an integer data type.
        BIGINT (str): Represents a bigint data type.
        TINYINT (str): Represents a tinyint data type.
        BYTEINT (str): Represents a byteint data type.
    """
    STRING = "STRING"
    FLOAT64 = "FLOAT64"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    DECIMAL = "DECIMAL"
    BIGDECIMAL = "BIGDECIMAL"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    INT64 = "INT64"
    INT = "INT"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    TINYINT = "TINYINT"
    BYTEINT = "BYTEINT"

class SyncLoadType(str, BaseEnum):
    """
    Represents the possible load types for synchronization tasks.
    Attributes:
        OVERWRITE (str): Overwrites existing data with new data.
        APPEND (str): Appends new data to existing data.
        MERGE (str): Merges new data with existing data.
        OPEN_MIRROR (str): Opens a mirror of the source data.
    """
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

class SyncStatus(str, BaseEnum):
    """
    Represents the possible statuses for synchronization tasks.
    Attributes:
        RUNNING (str): Indicates that the synchronization task is currently running.
        COMPLETE (str): Indicates that the synchronization task has completed successfully.
        SKIPPED (str): Indicates that the synchronization task was skipped.
        FAILED (str): Indicates that the synchronization task has failed.
        EXPIRED (str): Indicates that the synchronization task has expired.
        SCHEDULED (str): Indicates that the synchronization task is scheduled to run.
    """
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    SCHEDULED = "SCHEDULED"

class MaintenanceInterval(str, BaseEnum):
    """
    Represents the possible maintenance intervals for scheduled maintenance tasks.
    Attributes:
        YEARLY (str): Indicates that the maintenance task should run yearly.
        MONTHLY (str): Indicates that the maintenance task should run monthly.
        WEEKLY (str): Indicates that the maintenance task should run weekly.
        DAILY (str): Indicates that the maintenance task should run daily.
        AUTO (str): Indicates that the maintenance task should run automatically.
    """
    YEAR = "YEAR"
    MONTH = "MONTH"
    WEEK = "WEEK"
    DAY = "DAY"
    HOUR = "HOUR"
    AUTO = "AUTO"

class CalendarInterval(str, BaseEnum):
    """
    Represents the possible calendar intervals for scheduled tasks.
    Attributes:
        DAILY (str): Indicates that the task should run daily.
        WEEKLY (str): Indicates that the task should run weekly.
        MONTHLY (str): Indicates that the task should run monthly.
    """
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"

class SyncScheduleType(str, BaseEnum):
    """
    Represents the possible schedule types for synchronization tasks.
    Attributes:
        AUTO (str): Indicates that the synchronization task should run automatically.
    """
    AUTO = "AUTO"

class SchemaView(str, BaseEnum):
    """
    Represents the possible schema views for BigQuery.
    Attributes:
        INFORMATION_SCHEMA_TABLES (str): Represents the INFORMATION_SCHEMA.TABLES view.
        INFORMATION_SCHEMA_PARTITIONS (str): Represents the INFORMATION_SCHEMA.PARTITIONS view.
        INFORMATION_SCHEMA_COLUMNS (str): Represents the INFORMATION_SCHEMA.COLUMNS view.
        INFORMATION_SCHEMA_TABLE_CONSTRAINTS (str): Represents the INFORMATION_SCHEMA.TABLE_CONSTRAINTS view.
        INFORMATION_SCHEMA_TABLE_OPTIONS (str): Represents the INFORMATION_SCHEMA.TABLE_OPTIONS view.
        INFORMATION_SCHEMA_KEY_COLUMN_USAGE (str): Represents the INFORMATION_SCHEMA.KEY_COLUMN_USAGE view.
        INFORMATION_SCHEMA_VIEWS (str): Represents the INFORMATION_SCHEMA.VIEWS view.
        INFORMATION_SCHEMA_MATERIALIZED_VIEWS (str): Represents the INFORMATION_SCHEMA.MATERIALIZED_VIEWS view
    """
    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_TABLE_OPTIONS = "INFORMATION_SCHEMA.TABLE_OPTIONS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    INFORMATION_SCHEMA_VIEWS = "INFORMATION_SCHEMA.VIEWS"
    INFORMATION_SCHEMA_MATERIALIZED_VIEWS = "INFORMATION_SCHEMA.MATERIALIZED_VIEWS"

class BigQueryAPI(str, BaseEnum):
    """
    Represents the possible BigQuery API endpoints.
    Attributes:
        STORAGE (str): Represents the BigQuery Storage API.
        STANDARD (str): Represents the BigQuery API.
    """
    STANDARD = "STANDARD",
    STORAGE = "STORAGE"

class SyncConfigState(str, BaseEnum):
    """
    Represents the possible states for a synchronization configuration.
    Attributes:
        INIT (str): Indicates that the configuration is in the initialization state.
        COMMIT (str): Indicates that the configuration has been committed.
    """
    INIT = "INIT"
    COMMIT = "COMMIT"