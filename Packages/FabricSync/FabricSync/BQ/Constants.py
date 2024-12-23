from typing import List
from .Enum import *

class SyncConstants: 
    SPARK_CONF_PREFIX = "microsoft.gbb.fabric.sync"
    THREAD_PREFIX = "BQ_ASYNC_WORKER"
    
    def enum_to_list(enum_obj)->list[str]:
        return [x.name for x in list(enum_obj)]

    def get_load_strategies () -> List[str]:

        return SyncConstants.enum_to_list(LoadStrategy)

    def get_load_types() -> List[str]:
        return SyncConstants.enum_to_list(LoadType)

    def get_partition_grains() -> List[str]:
        return SyncConstants.enum_to_list(CalendarInterval)
    
    def get_information_schema_views() -> List[str]:
        return SyncConstants.enum_to_list(SchemaView)
    
    def get_metadata_tables() -> List[str]:
        return ["bq_sync_configuration", "bq_sync_schedule", 
            "bq_sync_schedule_telemetry", "bq_sync_data_expiration"]