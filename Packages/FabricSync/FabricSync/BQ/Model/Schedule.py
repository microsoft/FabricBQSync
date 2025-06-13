import json

from datetime import (
    datetime, timezone
)
from pydantic import (
    TypeAdapter, Field
)
from typing import (
    List, Optional
)
from datetime import (
    datetime, timezone
)

from FabricSync.BQ.Enum import (
    BQDataType, BQPartitionType, BigQueryObjectType, CalendarInterval, 
    FabricDestinationType, SyncLoadStrategy, SyncLoadType, SyncStatus,
    BigQueryAPI
)
from FabricSync.BQ.Model.Config import (
    SyncBaseModel, MappedColumn
)

class SyncSchedule(SyncBaseModel):
    """
    Represents a BigQuery Sync Schedule with various attributes and methods to manage sync operations.
    This model includes fields for tracking sync status, load strategies, partitioning, and more.
    It also provides methods for telemetry data extraction and row count updates.
    
    Attributes:
        - BQTableName: The name of the BigQuery table in the format 'project.dataset.table'.
        - EndTime: The time when the sync operation was completed.
        - SourceRows: The number of rows in the source data.
        - InsertedRows: The number of rows inserted during the sync operation.
        - UpdatedRows: The number of rows updated during the sync operation.
        - DeltaVersion: The version of the delta sync.
        - SparkAppId: The ID of the Spark application used for the sync.
        - Status: The current status of the sync operation.
        - FabricPartitionColumns: List of partition columns used in the sync.
        - SummaryLoadType: The type of load summary for the sync.
        - TableId: The ID of the BigQuery table.
        - StartTime: The time when the sync operation started.
        - SyncId: The unique identifier for the sync operation.
        - GroupScheduleId: The ID of the group schedule for the sync.
        - ScheduleId: The ID of the specific schedule for the sync.
        - Load_Strategy: The strategy used for loading data in the sync.
        - Load_Type: The type of load operation (e.g., MERGE, OVERWRITE).
        - InitialLoad: Indicates if this is the initial load for the sync.
        - LastScheduleLoadDate: The date of the last scheduled load.
        - Priority: The priority of the sync operation.
        - ProjectId: The ID of the Google Cloud project.
        - Dataset: The name of the BigQuery dataset.
        - TableName: The name of the BigQuery table.
        - ObjectType: The type of BigQuery object (e.g., TABLE, VIEW).        
        - SourceQuery: The SQL query used to fetch the source data.
        - SourcePredicate: The predicate used to filter source data.        
        - MaxWatermark: The maximum watermark value for the sync.
        - WatermarkColumn: The column used for watermarking in the source data.
        - IsPartitioned: Indicates if the sync is partitioned.
        - PartitionColumn: The column used for partitioning.
        - Partition_Type: The type of partitioning (e.g., TIME, RANGE).
        - PartitionGrain: The grain of the partitioning (e.g., DAILY, MONTHLY).
        - PartitionId: The ID of the partition.
        - PartitionDataType: The data type of the partition column.
        - PartitionRange: The range of the partition.
        - RequirePartitionFilter: Indicates if a partition filter is required.
        - WorkspaceId: The ID of the Fabric workspace.
        - WorkspaceName: The name of the Fabric workspace.
        - LakehouseType: The type of lakehouse destination (e.g., MIRRORED_DATABASE).
        - LakehouseId: The ID of the lakehouse.
        - Lakehouse: The name of the lakehouse.
        - LakehouseSchema: The schema of the lakehouse.
        - LakehouseTable: The name of the table in the lakehouse.
        - LakehousePartition: The partition in the lakehouse.
        - UseLakehouseSchema: Indicates if the lakehouse schema should be used.
        - EnforcePartitionExpiration: Indicates if partition expiration should be enforced.
        - AllowSchemaEvolution: Indicates if schema evolution is allowed.
        - EnableTableMaintenance: Indicates if table maintenance is enabled.
        - TableMaintenanceInterval: The interval for table maintenance.
        - FlattenTable: Indicates if the table should be flattened.
        - FlattenInPlace: Indicates if flattening should be done in place.
        - ExplodeArrays: Indicates if arrays should be exploded.
        - UseBigQueryExport: Indicates if BigQuery export should be used.
        - EnableBigQueryExport: Indicates if BigQuery export is enabled at the app level.
        - UseStandardAPI: Indicates if the standard API should be used.
        - EnableStandardAPI: Indicates if the standard API is enabled at the app level.
        - AutoSelectAPI: Indicates if the API should be auto-selected based on conditions.
        - SyncAPI: The API used for the sync operation (e.g., STORAGE, STANDARD, BUCKET).
        - Keys: List of primary keys for the sync.
        - TotalRows: The total number of rows in the table.
        - TotalLogicalMb: The total logical size of the table in MB.
        - SizePriority: The size priority for the sync operation.
        - BQTableLastModified: The last modified timestamp of the BigQuery table.
        - MirrorFileIndex: The index of the mirror file used in the sync.
        - ColumnMap: A JSON string mapping source columns to destination columns.
        - TableColumns: A JSON string representing the columns in the table.
        - TotalTableTasks: The total number of tasks for the table.
        - IsEmpty: Indicates if the sync operation was skipped (no data to process).
        - HasComplexTypes: Indicates the source data contains complex data types
        - TempTableId: The temporary table ID used during the sync operation.
        - BQTableName: The name of the BigQuery table, typically in the format 'project.dataset.table'.
    Methods:
        - to_telemetry() -> dict:
            Returns a dictionary representation of the model excluding sensitive information for telemetry purposes.    
        - get_column_map() -> List[MappedColumn]:
            Returns a list of mapped columns based on the ColumnMap JSON string.
        - ID() -> str:
            Returns a unique identifier for the sync schedule based on the SyncId and PartitionId.
        - IsInitialLoad() -> bool:
            Determines if the sync schedule is for an initial load.
        - get_summary_load_type() -> str:
            Returns a summary load type string based on the sync schedule's properties.
        - Mode() -> str:
            Returns the mode of the sync load based on whether it is an initial load or not.
        - PrimaryKey() -> str:
            Returns the first primary key from the Keys list if available, otherwise returns None.
        - HasKeys() -> bool:
            Checks if the sync schedule has any primary keys defined.
        - IsMirrored() -> bool:
            Checks if the sync schedule is for a mirrored database.
        - LakehouseHttpUri() -> str:
            Returns the HTTP URI for the lakehouse
        - LakehouseAbfssUri() -> str:
            Returns the ABFSS URI for the lakehouse
        - LakehouseAbfssTablePath() -> str:
            Returns the ABFSS path for the lakehouse table.
            The path is constructed based on whether the lakehouse schema is used or not.
        - LakehouseTableName() -> str:
            Returns the full name of the lakehouse table, including the lakehouse and schema if applicable.
        - IsCDCStrategy() -> bool:
            Checks if the sync schedule uses a Change Data Capture (CDC) strategy.  
        - IsTimeIngestionPartitioned() -> bool:
            Checks if the sync schedule uses a time ingestion partitioning strategy.
        - IsRangePartitioned() -> bool:
            Checks if the sync schedule uses a range partitioning strategy.
        - IsTimePartitionedStrategy() -> bool:
            Checks if the sync schedule uses a time partitioning strategy.
        - IsPartitionedSyncLoad() -> bool:
            Checks if the sync schedule is partitioned.
        - LoadPriority() -> int:
            Calculates the load priority of the sync schedule based on its priority and size priority.
        - SyncAPI() -> BigQueryAPI:
            Determines the API to be used for the sync operation based on the configuration settings.
        - UpdateRowCounts(src:int = 0, insert:int = 0, update:int = 0) -> None:
            Updates the row counts for the sync schedule.
        - __lt__(other) -> bool:
            Compares the load priority of this sync schedule with another sync schedule.
        - __gt__(other) -> bool:
            Compares the load priority of this sync schedule with another sync schedule.
    """
    EndTime:Optional[datetime] = Field(alias="completed", default=None)
    SourceRows:Optional[int] = Field(alias="source_rows", default=0)
    InsertedRows:Optional[int] = Field(alias="inserted_rows", default=0)
    UpdatedRows:Optional[int] = Field(alias="updated_rows", default=0)
    DeltaVersion:Optional[int] = Field(alias="delta_version", default=None)
    SparkAppId:Optional[str] = Field(alias="spark_app_id", default=None)
    Status:Optional[SyncStatus] = Field(alias="sync_status", default=None)
    FabricPartitionColumns:list[str] = Field(alias="fabric_partition_columns", default=[])
    SummaryLoadType:Optional[str] = Field(alias="summary_load_type", default=None)
    TableId:Optional[str] = Field(alias="table_id", default=None)
    StartTime:Optional[datetime] = Field(alias="started", default=datetime.now(timezone.utc))
    SyncId:Optional[str] = Field(alias="sync_id", default=None)
    GroupScheduleId:Optional[str] = Field(alias="group_schedule_id", default=None)
    ScheduleId:Optional[str] = Field(alias="schedule_id", default=None)
    Load_Strategy:Optional[SyncLoadStrategy] = Field(alias="load_strategy", default=None)
    Load_Type:Optional[SyncLoadType] = Field(alias="load_type", default=None)
    InitialLoad:Optional[bool] = Field(alias="initial_load", default=False)
    LastScheduleLoadDate:Optional[datetime] = Field(alias="last_schedule_dt", default=None)
    Priority:Optional[int] = Field(alias="priority", default=0)
    ProjectId:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    TableName:Optional[str] = Field(alias="table_name", default=None)
    ObjectType:Optional[BigQueryObjectType] = Field(alias="object_type", default=None)
    SourceQuery:Optional[str] = Field(alias="source_query", default=None)
    SourcePredicate:Optional[str] = Field(alias="source_predicate", default=None)
    MaxWatermark:Optional[str] = Field(alias="max_watermark", default=None)
    WatermarkColumn:Optional[str] = Field(alias="watermark_column", default=None)
    IsPartitioned:Optional[bool] = Field(alias="is_partitioned", default=False)
    PartitionColumn:Optional[str] = Field(alias="partition_column", default=None)
    Partition_Type:Optional[BQPartitionType] = Field(alias="partition_type", default=None)
    PartitionGrain:Optional[CalendarInterval] = Field(alias="partition_grain", default=None)
    PartitionId:Optional[str] = Field(alias="partition_id", default=None)    
    PartitionDataType:Optional[BQDataType] = Field(alias="partition_data_type", default=None) 
    PartitionRange:Optional[str] = Field(alias="partition_range", default=None)
    RequirePartitionFilter:Optional[bool] = Field(alias="require_partition_filter", default=None)
    WorkspaceId:Optional[str] = Field(alias="workspace_id", default=None)
    WorkspaceName:Optional[str] = Field(alias="workspace_name", default=None)
    LakehouseType:Optional[FabricDestinationType] = Field(alias="lakehouse_type", default=None)
    LakehouseId:Optional[str] = Field(alias="lakehouse_id", default=None)
    Lakehouse:Optional[str] = Field(alias="lakehouse", default=None)
    LakehouseSchema:Optional[str] = Field(alias="lakehouse_schema", default=None)
    LakehouseTable:Optional[str] = Field(alias="lakehouse_table_name", default=None)
    LakehousePartition:Optional[str] = Field(alias="lakehouse_partition", default=None)
    UseLakehouseSchema:Optional[bool] = Field(alias="use_lakehouse_schema", default=False)
    EnforcePartitionExpiration:Optional[bool] = Field(alias="enforce_partition_expiration", default=False)
    AllowSchemaEvolution:Optional[bool] = Field(alias="allow_schema_evolution", default=False)
    EnableTableMaintenance:Optional[bool] = Field(alias="enable_table_maintenance", default=False)
    TableMaintenanceInterval:Optional[bool] = Field(alias="table_maintenance_inteval", default=False)
    FlattenTable:Optional[bool] = Field(alias="flatten_table", default=False)
    FlattenInPlace:Optional[bool] = Field(alias="flatten_inplace", default=False)
    ExplodeArrays:Optional[bool] = Field(alias="explode_arrays", default=False)    
    UseBigQueryExport:Optional[bool] = Field(alias="use_bigquery_export", default=False)
    EnableBigQueryExport:Optional[bool] = Field(alias="enable_bigquery_export", default=False)
    UseStandardAPI:Optional[bool] = Field(alias="use_standard_api", default=False)
    EnableStandardAPI:Optional[bool] = Field(alias="enable_standard_api", default=False)
    AutoSelectAPI:Optional[bool] = Field(alias="auto_select_api", default=False)    
    Keys:Optional[List[str]] = Field(alias="primary_keys", default=[])
    TotalRows:Optional[int] = Field(alias="total_rows", default=0)
    TotalLogicalMb:Optional[int] = Field(alias="total_logical_mb", default=0)
    SizePriority:Optional[int] = Field(alias="size_priority", default=0)
    BQTableLastModified:Optional[str] = Field(alias="bq_tbl_last_modified", default=None)
    MirrorFileIndex:Optional[int] = Field(alias="mirror_file_index", default=1)
    ColumnMap:Optional[str] = Field(alias="column_map", default=None)
    TableColumns:Optional[str] = Field(alias="table_columns", default=None)   
    TotalTableTasks:Optional[int] = Field(alias="total_table_tasks", default=0) 
    HasComplexTypes:Optional[bool] = Field(alias="has_complex_types", default=False)
    TempTableId:Optional[str] = Field(alias="temp_table_id", default=None)
    BQTableName:Optional[str] = Field(alias="bq_table_name", default=None)

    IsEmpty:bool = Field(alias="is_skipped", default=False) 

    def to_telemetry(self) -> dict:
        """
        Returns a dictionary representation of the model excluding sensitive information for telemetry purposes.
        Sensitive keys are removed to protect privacy and security.
        Returns:            
            dict: A dictionary containing the model's data without sensitive information.
        """
        sensitive_keys = ["project_id","dataset","table_name","source_query","source_predicate","watermark_column","max_watermark",
            "partition_column","partition_id","partition_range","mirror_file_index",
            "workspace_id", "workspace_name", "lakehouse_id", "lakehouse","lakehouse_schema","lakehouse_table_name",
            "lakehouse_partition","primary_keys","size_priority","fabric_partition_columns", "column_map", "table_columns"]
        
        telemetry_data = json.loads(self.model_dump_json())

        for key in sensitive_keys:
            if key in telemetry_data:
                del telemetry_data[key]
        
        return telemetry_data

    def get_column_map(self) -> List[MappedColumn]:
        """
        Returns a list of mapped columns based on the ColumnMap JSON string.
        If ColumnMap is not set, returns None.
        Returns:
            List[MappedColumn] or None: A list of MappedColumn objects if ColumnMap is set, otherwise None.
        """
        if self.ColumnMap:
            d = json.loads(self.ColumnMap)
            maps = TypeAdapter(List[MappedColumn]).validate_python(d)

            return [m for m in maps if m.Source]

        return None

    @property
    def ID(self) -> str:
        """
        Returns a unique identifier for the sync schedule based on the SyncId and PartitionId.
        If PartitionId is not set, it returns just the SyncId.
        Returns:
            str: A unique identifier for the sync schedule.
        """
        if self.PartitionId:
            return f"{self.BQTableName}${self.PartitionId}"
        else:
            return self.BQTableName

    @property
    def IsInitialLoad(self) -> bool:
        """
        Determines if the sync schedule is for an initial load.
        An initial load is defined as one that is not partitioned, not range partitioned, and does not require a partition filter.
        Returns:
            bool: True if it is an initial load, False otherwise.
        """
        return self.InitialLoad and not self.IsTimeIngestionPartitioned and \
            not self.IsRangePartitioned and not (self.IsPartitioned and self.RequirePartitionFilter)

    def get_summary_load_type(self) -> str:
        """
        Returns a summary load type string based on the sync schedule's properties.
        """
        prefix = "MIRRORED" if self.IsMirrored else "LAKEHOUSE"

        if self.IsInitialLoad:
            return f"{prefix} - INITIAL LOAD - {self.SyncAPI} API"
        else:
            return f"{prefix} - {self.Load_Strategy} ({self.Load_Type}) - {self.SyncAPI} API"
    
    @property
    def Mode(self) -> str:
        """
        Returns the mode of the sync load based on whether it is an initial load or not.
        """
        if self.InitialLoad:
            return SyncLoadType.OVERWRITE
        else:
            return self.Load_Type
    
    @property
    def PrimaryKey(self) -> str:
        """
        Returns the first primary key from the Keys list if available, otherwise returns None.
        Returns:
            str or None: The first primary key if available, otherwise None.
        """   
        if self.Keys:
            return self.Keys[0]
        else:
            return None

    @property
    def HasKeys(self) -> bool:
        """
        Checks if the sync schedule has any primary keys defined.
        Returns:
            bool: True if there are primary keys defined, False otherwise.
        """
        return self.Keys and len(self.Keys) > 0
        
    @property
    def IsMirrored(self) -> bool:
        """
        Checks if the sync schedule is for a mirrored database.
        A mirrored database is defined as one where the LakehouseType is MIRRORED_DATABASE.
        Returns:
            bool: True if it is a mirrored database, False otherwise.
        """
        return self.LakehouseType == FabricDestinationType.MIRRORED_DATABASE

    @property
    def LakehouseHttpUri(self) -> str:
        """
        Returns the HTTP URI for the lakehouse in the format:
            https://onelake.dfs.fabric.microsoft.com/{WorkspaceId}/{LakehouseId}
        Returns:
            str: The HTTP URI for the lakehouse.
        """ 
        return f"https://onelake.dfs.fabric.microsoft.com/{self.WorkspaceId}/{self.LakehouseId}"
    
    @property
    def LakehouseAbfssUri(self) -> str:
        """
        Returns the ABFSS URI for the lakehouse
        """
        return f"abfss://{self.WorkspaceId}@onelake.dfs.fabric.microsoft.com/{self.LakehouseId}"

    @property
    def LakehouseAbfssTablePath(self) -> str:
        """
        Returns the ABFSS path for the lakehouse table.
        The path is constructed based on whether the lakehouse schema is used or not.
        If UseLakehouseSchema is True, it includes the schema in the path.
        Otherwise, it only includes the lakehouse table name.
        Returns:
            str: The ABFSS path for the lakehouse table.
        """
        base_path = f"{self.LakehouseAbfssUri}/Tables"
        if self.UseLakehouseSchema:
            return f"{base_path}/{self.LakehouseSchema}/{self.LakehouseTable}"
        else:
            return f"{base_path}/{self.LakehouseTable}"

    @property
    def LakehouseTableName(self) -> str:
        """
        Returns the full name of the lakehouse table, including the lakehouse and schema if applicable.
        The format is:
            {Lakehouse}.{LakehouseSchema}.{LakehouseTable} if UseLakehouseSchema is True
            {Lakehouse}.{LakehouseTable} otherwise
        Returns:
            str: The full name of the lakehouse table.
        """
        if self.UseLakehouseSchema:
            table_nm = f"{self.Lakehouse}.{self.LakehouseSchema}.{self.LakehouseTable}"
        else:
            table_nm = f"{self.Lakehouse}.{self.LakehouseTable}"

        return table_nm

    @property
    def IsCDCStrategy(self) -> bool:
        """
        Checks if the sync schedule uses a Change Data Capture (CDC) strategy.
        A CDC strategy is defined as one of the following load strategies:
            - SyncLoadStrategy.CDC
            - SyncLoadStrategy.CDC_APPEND
        Returns:
            bool: True if it is a CDC strategy, False otherwise.
        """
        return self.Load_Strategy in [SyncLoadStrategy.CDC, SyncLoadStrategy.CDC_APPEND]
    
    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        """
        Checks if the sync schedule uses a time ingestion partitioning strategy.
        A time ingestion partitioning strategy is defined as one where the Load_Strategy is TIME_INGESTION.
        Returns:
            bool: True if it is a time ingestion partitioning strategy, False otherwise.
        """
        return self.Load_Strategy == SyncLoadStrategy.TIME_INGESTION

    @property
    def IsRangePartitioned(self) -> bool:
        """
        Checks if the sync schedule uses a range partitioning strategy.
        A range partitioning strategy is defined as one where the Load_Strategy is PARTITION and the Partition_Type is RANGE.
        Returns:
            bool: True if it is a range partitioning strategy, False otherwise.
        """
        return self.Load_Strategy == SyncLoadStrategy.PARTITION and self.Partition_Type == BQPartitionType.RANGE
    
    @property
    def IsTimePartitionedStrategy(self) -> bool:
         """
            Checks if the sync schedule uses a time partitioning strategy.
            A time partitioning strategy is defined as one where the Load_Strategy is PARTITION and the Partition_Type is TIME,
            or the Load_Strategy is TIME_INGESTION.
            Returns:
                bool: True if it is a time partitioning strategy, False otherwise.
        """
         return ((self.Load_Strategy == SyncLoadStrategy.PARTITION and 
            self.Partition_Type == BQPartitionType.TIME) or self.Load_Strategy == SyncLoadStrategy.TIME_INGESTION)
    
    @property
    def IsPartitionedSyncLoad(self) -> bool:
        """
        Checks if the sync schedule is partitioned.
        A sync schedule is considered partitioned if it is either time partitioned or range partitioned.
        Specifically, it checks if:
            - It is a time partitioned strategy and has a PartitionId set, or
            - It is a range partitioned strategy.
        Returns:
            bool: True if it is a partitioned sync load, False otherwise.
        """
        if self.IsPartitioned:
            if (self.IsTimePartitionedStrategy and self.PartitionId) or self.IsRangePartitioned:
                return True
        return False

    @property
    def LoadPriority(self) -> int:
        """
        Calculates the load priority of the sync schedule based on its priority and size priority.
        The load priority is the sum of the Priority and SizePriority attributes.
        Returns:
            int: The load priority of the sync schedule.
        """
        return self.Priority + self.SizePriority

    @property
    def SyncAPI(self) -> BigQueryAPI:
        """
        Determines the API to be used for the sync operation based on the configuration settings.
        It checks the EnableBigQueryExport and UseBigQueryExport flags to decide if the BigQuery export API should be used.
        If the EnableStandardAPI and UseStandardAPI flags are set, it uses the Standard API.
        If neither of these conditions are met, it defaults to using the Storage API.
        Returns:
            BigQueryAPI: The API to be used for the sync operation.
        """
        if not self.HasComplexTypes:
            if self.EnableBigQueryExport and self.UseBigQueryExport:
                return BigQueryAPI.BUCKET
            elif self.EnableStandardAPI and self.UseStandardAPI:
                return BigQueryAPI.STANDARD
        
        return BigQueryAPI.STORAGE

    def UpdateRowCounts(self, src:int = 0, insert:int = 0, update:int = 0) -> None:
        """
        Updates the row counts for the sync schedule.
        This method updates the SourceRows, InsertedRows, and UpdatedRows attributes based on the provided counts.
        """
        self.SourceRows += src

        if not self.Load_Type == SyncLoadType.MERGE or self.IsMirrored:
            self.InsertedRows += src            
            self.UpdatedRows = 0
        else:
            self.InsertedRows += insert
            self.UpdatedRows += update
    
    def __lt__(self, other) -> bool:
        """
        Compares the load priority of this sync schedule with another sync schedule.
        Returns True if this schedule has a lower load priority than the other, otherwise False.
        Args:
            other (SyncSchedule): The other sync schedule to compare with.
        Returns:
            bool: True if this schedule has a lower load priority than the other, otherwise False.
        """
        if not isinstance(other, SyncSchedule):
            return NotImplemented
        
        return self.LoadPriority < other.LoadPriority

    def __gt__(self, other) -> bool:
        """
        Compares the load priority of this sync schedule with another sync schedule.
        Returns True if this schedule has a higher load priority than the other, otherwise False.
        Args:
            other (SyncSchedule): The other sync schedule to compare with.
        Returns:
            bool: True if this schedule has a higher load priority than the other, otherwise False.
        """
        if not isinstance(other, SyncSchedule):
            return NotImplemented
        
        return self.LoadPriority > other.LoadPriority