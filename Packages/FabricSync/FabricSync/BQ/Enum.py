from enum import Enum

class BaseEnum(Enum):
    pass

    def __str__(self):
        return str(self.value)

class BigQueryObjectType(BaseEnum):
    BASE_TABLE = "BASE_TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"

class LoadStrategy(BaseEnum):
    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"

class PartitionType(BaseEnum):
    TIME = "TIME"
    RANGE = "RANGE"
    TIME_INGESTION = "TIME_INGESTION"

class BQDataType(BaseEnum):
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
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

class SyncStatus(BaseEnum):
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"
    SCHEDULED = "SCHEDULED"

class CalendarInterval(BaseEnum):
    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"

class ScheduleType(BaseEnum):
    AUTO = "AUTO"

class SchemaView(BaseEnum):
    INFORMATION_SCHEMA_TABLES = "INFORMATION_SCHEMA.TABLES"
    INFORMATION_SCHEMA_PARTITIONS = "INFORMATION_SCHEMA.PARTITIONS"
    INFORMATION_SCHEMA_COLUMNS = "INFORMATION_SCHEMA.COLUMNS"
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS = "INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
    INFORMATION_SCHEMA_TABLE_OPTIONS = "INFORMATION_SCHEMA.TABLE_OPTIONS"
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE = "INFORMATION_SCHEMA.KEY_COLUMN_USAGE"
    INFORMATION_SCHEMA_VIEWS = "INFORMATION_SCHEMA.VIEWS"
    INFORMATION_SCHEMA_MATERIALIZED_VIEWS = "INFORMATION_SCHEMA.MATERIALIZED_VIEWS"