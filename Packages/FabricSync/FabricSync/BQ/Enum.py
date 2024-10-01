from enum import Enum

class BigQueryObjectType(Enum):
    BASE_TABLE = "BASE_TABLE"
    VIEW = "VIEW"
    MATERIALIZED_VIEW = "MATERIALIZED_VIEW"

class LoadStrategy(Enum):
    FULL = "FULL"
    PARTITION = "PARTITION"
    WATERMARK = "WATERMARK"
    TIME_INGESTION = "TIME_INGESTION"

class PartitionType(Enum):
    TIME = "TIME"
    RANGE = "RANGE"
    TIME_INGESTION = "TIME_INGESTION"

class BQDataType(Enum):
    TIMESTAMP = "TIMESTAMP"

class LoadType(Enum):
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"
    MERGE = "MERGE"

class SyncStatus(Enum):
    COMPLETE = "COMPLETE"
    SKIPPED = "SKIPPED"