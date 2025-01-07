from enum import Enum

class BaseEnum(Enum):
    """
    BaseEnum is a subclass of the Enum class that provides a custom string representation.
    Methods:
        __str__(): Returns the string representation of the enum value.
    """

    pass

    def __str__(self):
        return str(self.value)

class MaintenanceStrategy(BaseEnum):
    SCHEDULED="SCHEDULED"
    INTELLIGENT="INTELLIGENT"

class SupportedTypeConversion(BaseEnum):
    """
    Enum class for supported type conversions.
    Attributes:
        STRING_TO_DATE (str): Conversion from string to date.
        STRING_TO_TIMESTAMP (str): Conversion from string to timestamp.
        STRING_TO_INT (str): Conversion from string to integer.
        STRING_TO_LONG (str): Conversion from string to long integer.
        STRING_TO_DECIMAL (str): Conversion from string to decimal.
        DATE_TO_STRING (str): Conversion from date to string.
        TIMESTAMP_TO_STRING (str): Conversion from timestamp to string.
        INT_TO_STRING (str): Conversion from integer to string.
        DECIMAL_TO_STRING (str): Conversion from decimal to string.
        LONG_TO_STRING (str): Conversion from long integer to string.
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

class ObjectFilterType(BaseEnum):
    """
    Enum class representing the type of object filter.
    Attributes:
        INCLUDE (str): Represents an inclusion filter type.
        EXCLUDE (str): Represents an exclusion filter type.
    """

    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"

class PredicateType(BaseEnum):
    """
    Enum class representing predicate types for logical operations.
    Attributes:
        AND (str): Represents the logical AND operation.
        OR (str): Represents the logical OR operation.
    """

    AND = "AND"
    OR = "OR"
    
class SyncLogLevel(BaseEnum):
    """
    SyncLogLevel is an enumeration that defines various levels of logging for synchronization processes.
    Attributes:
        DEBUG (int): Debug-level messages, typically used for development and troubleshooting (value: 10).
        INFO (int): Informational messages that highlight the progress of the application (value: 20).
        WARNING (int): Warning messages that indicate a potential issue or important situation (value: 30).
        SYNC_STATUS (int): Messages specifically related to the status of synchronization (value: 34).
        TELEMETRY (int): Messages related to telemetry data (value: 35).
        ERROR (int): Error messages indicating a failure in the application (value: 40).
        CRITICAL (int): Critical messages indicating a severe failure that may cause the application to terminate (value: 50).
    """

    DEBUG = 10
    INFO = 20
    WARNING = 30
    SYNC_STATUS = 34
    TELEMETRY = 35
    ERROR = 40
    CRITICAL = 50

class SyncLogFileInterval(BaseEnum):
    """
    Enum representing the intervals for sync log files.
    Attributes:
        HOUR (str): Represents an hourly interval.
        DAY (str): Represents a daily interval.
        WEEK (str): Represents a weekly interval.
        MONTH (str): Represents a monthly interval.
    """

    HOUR = "H"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
    
class BigQueryObjectType(BaseEnum):
    """
    BigQueryObjectType is an enumeration that represents different types of BigQuery objects.
    Attributes:
        BASE_TABLE (str): Represents a base table in BigQuery.
        VIEW (str): Represents a view in BigQuery.
        MATERIALIZED_VIEW (str): Represents a materialized view in BigQuery.
    """

    BASE_TABLE = "BASE_TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"

class LoadStrategy(BaseEnum):
    """
    LoadStrategy is an enumeration that defines different strategies for loading data.
    Attributes:
        FULL (str): Represents a full data load strategy.
        PARTITION (str): Represents a partition-based data load strategy.
        WATERMARK (str): Represents a watermark-based data load strategy.
        TIME_INGESTION (str): Represents a time ingestion-based data load strategy.
    """

    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"

class PartitionType(BaseEnum):
    """
    PartitionType is an enumeration that defines different types of partitioning strategies.
    Attributes:
        TIME (str): Represents partitioning by time.
        RANGE (str): Represents partitioning by range.
        TIME_INGESTION (str): Represents partitioning by time of ingestion.
    """

    TIME = "TIME"
    RANGE = "RANGE"
    TIME_INGESTION = "TIME_INGESTION"

class BQDataType(BaseEnum):
    """
    BQDataType is an enumeration that represents various BigQuery data types.
    Attributes:
        TIMESTAMP (str): Represents a timestamp data type.
        DATE (str): Represents a date data type.
        TIME (str): Represents a time data type.
        INT64 (str): Represents a 64-bit integer data type.
        INT (str): Represents an integer data type.
        SMALLINT (str): Represents a small integer data type.
        INTEGER (str): Represents an integer data type.
        BIGINT (str): Represents a big integer data type.
        TINYINT (str): Represents a tiny integer data type.
        BYTEINT (str): Represents a byte-sized integer data type.
    """

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

class LoadType(BaseEnum):
    """
    LoadType is an enumeration that defines different types of load operations.
    Attributes:
        OVERWRITE (str): Represents an operation that overwrites existing data.
        APPEND (str): Represents an operation that appends new data to existing data.
        MERGE (str): Represents an operation that merges new data with existing data.
    """

    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

class SyncStatus(BaseEnum):
    """
    SyncStatus is an enumeration representing the various states of a synchronization process.
    Attributes:
        COMPLETE (str): Indicates that the synchronization process has completed successfully.
        SKIPPED (str): Indicates that the synchronization process was skipped.
        FAILED (str): Indicates that the synchronization process has failed.
        EXPIRED (str): Indicates that the synchronization process has expired.
        SCHEDULED (str): Indicates that the synchronization process is scheduled to run.
    """

    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    SCHEDULED = "SCHEDULED"

class CalendarInterval(BaseEnum):
    """
    Enum class representing different calendar intervals.
    Attributes:
        YEAR (str): Represents a yearly interval.
        MONTH (str): Represents a monthly interval.
        DAY (str): Represents a daily interval.
        HOUR (str): Represents an hourly interval.
    """

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"

class ScheduleType(BaseEnum):
    """
    Enum class representing different types of schedules.
    Attributes:
        AUTO (str): Represents an automatic schedule type.
    """

    AUTO = "AUTO"

class SchemaView(BaseEnum):
    """
    SchemaView is an enumeration class that extends BaseEnum. It contains constants representing various 
    INFORMATION_SCHEMA views in BigQuery.
    Attributes:
        INFORMATION_SCHEMA_TABLES (str): Represents the INFORMATION_SCHEMA.TABLES view.
        INFORMATION_SCHEMA_PARTITIONS (str): Represents the INFORMATION_SCHEMA.PARTITIONS view.
        INFORMATION_SCHEMA_COLUMNS (str): Represents the INFORMATION_SCHEMA.COLUMNS view.
        INFORMATION_SCHEMA_TABLE_CONSTRAINTS (str): Represents the INFORMATION_SCHEMA.TABLE_CONSTRAINTS view.
        INFORMATION_SCHEMA_TABLE_OPTIONS (str): Represents the INFORMATION_SCHEMA.TABLE_OPTIONS view.
        INFORMATION_SCHEMA_KEY_COLUMN_USAGE (str): Represents the INFORMATION_SCHEMA.KEY_COLUMN_USAGE view.
        INFORMATION_SCHEMA_VIEWS (str): Represents the INFORMATION_SCHEMA.VIEWS view.
        INFORMATION_SCHEMA_MATERIALIZED_VIEWS (str): Represents the INFORMATION_SCHEMA.MATERIALIZED_VIEWS view.
    """

    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_TABLE_OPTIONS = "INFORMATION_SCHEMA.TABLE_OPTIONS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    INFORMATION_SCHEMA_VIEWS = "INFORMATION_SCHEMA.VIEWS"
    INFORMATION_SCHEMA_MATERIALIZED_VIEWS = "INFORMATION_SCHEMA.MATERIALIZED_VIEWS"

class BigQueryAPI(BaseEnum):
    """
    Enum representing different BigQuery API types.
    Attributes:
        STANDARD (str): Represents the standard BigQuery API.
        STORAGE (str): Represents the BigQuery storage API.
    """

    STANDARD = "STANDARD",
    STORAGE = "STORAGE"