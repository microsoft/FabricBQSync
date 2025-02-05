from typing import List

from FabricSync.BQ.Enum import (
    SyncLoadStrategy, SyncLoadType, CalendarInterval, SchemaView
)

class SyncConstants: 
    FABRIC_LOG_NAME = "FABRIC_SYNC_LOG"
    SPARK_CONF_PREFIX = "microsoft.gbb.fabric.sync"
    THREAD_PREFIX = "BQ_ASYNC_WORKER"
    OPEN_MIRROR_METADATA_FILE_NAME = "_metadata.json"
    MIRROR_ROW_MARKER = "__rowMarker__"
    
    def enum_to_list(enum_obj)->list[str]:
        """
        Converts an enum to a list of strings.
        Args:
            enum_obj: The enum object.
        Returns:
            list[str]: The list of strings.
        """
        return [x.name for x in list(enum_obj)]

    def get_load_strategies () -> List[str]:
        """
        Gets the list of load strategies.
        Returns:
            List[str]: The list of load strategies.
        """
        return SyncConstants.enum_to_list(SyncLoadStrategy)

    def get_load_types() -> List[str]:
        """
        Gets the list of load types.
        Returns:
            List[str]: The list of load types.
        """
        return SyncConstants.enum_to_list(SyncLoadType)

    def get_partition_grains() -> List[str]:
        """
        Gets the list of partition grains.
        Returns:
            List[str]: The list of partition grains.
        """
        return SyncConstants.enum_to_list(CalendarInterval)
    
    def get_information_schema_views() -> List[str]:
        """
        Gets the list of information schema views.
        Returns:
            List[str]: The list of information schema views.
        """
        return SyncConstants.enum_to_list(SchemaView)
    
    def get_sync_temp_views() -> List[str]:
        """
        Gets the list of sync temp views.
        Returns:
            List[str]: The list of sync temp views.
        """
        return ["table_metadata_autodetect", "user_config_json", "user_config_table_keys", "user_config_tables"]
    
    def get_metadata_tables() -> List[str]:
        """
        Gets the list of metadata tables.
        Returns:
            List[str]: The list of metadata tables.
        """
        return ["sync_configuration", "sync_schedule", "sync_schedule_telemetry", "sync_data_expiration", "sync_maintenance"]

    def get_information_schema_tables() -> List[str]:
        """
        Gets the list of information schema tables.
        Returns:
            List[str]: The list of information schema tables.
        """
        return [v.lower() for v in SyncConstants.get_information_schema_views()]

    def get_inventory_temp_tables() -> List[str]:
        """
        Gets the list of inventory temp tables.
        Returns:
            List[str]: The list of inventory temp tables.
        """
        return  ["tmpFiles", "tmpHistory"]
    
    def get_inventory_tables() -> List[str]:
        """
        Gets the list of inventory tables.
        Returns:
            List[str]: The list of inventory tables.
        """
        return ["storage_inventory_tables","storage_inventory_table_files","storage_inventory_table_partitions",
            "storage_inventory_table_history","storage_inventory_table_snapshot"]