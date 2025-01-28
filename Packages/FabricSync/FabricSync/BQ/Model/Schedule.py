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
    FabricDestinationType, SyncLoadStrategy, SyncLoadType, SyncStatus
)
from FabricSync.BQ.Model.Config import (
    SyncBaseModel, MappedColumn
)

class SyncSchedule(SyncBaseModel):
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
    Keys:Optional[List[str]] = Field(alias="primary_keys", default=[])
    TotalRows:Optional[int] = Field(alias="total_rows", default=0)
    TotalLogicalMb:Optional[int] = Field(alias="total_logical_mb", default=0)
    SizePriority:Optional[int] = Field(alias="size_priority", default=0)
    BQTableLastModified:Optional[str] = Field(alias="bq_tbl_last_modified", default=None)
    MirrorFileIndex:Optional[int] = Field(alias="mirror_file_index", default=1)
    ColumnMap:Optional[str] = Field(alias="column_map", default=None)
    TableColumns:Optional[str] = Field(alias="table_columns", default=None)   
    TotalTableTasks:Optional[int] = Field(alias="total_table_tasks", default=0) 

    IsEmpty:bool = Field(alias="is_skipped", default=False) 

    def to_telemetry(self) -> dict:
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
        if self.ColumnMap:
            d = json.loads(self.ColumnMap)
            maps = TypeAdapter(List[MappedColumn]).validate_python(d)

            return [m for m in maps if m.Source]

        return None

    @property
    def ID(self) -> str:
        if self.PartitionId:
            return f"{self.BQTableName}${self.PartitionId}"
        else:
            return self.BQTableName

    @property
    def IsInitialLoad(self) -> bool:
        return self.InitialLoad and not self.IsTimeIngestionPartitioned and \
            not self.IsRangePartitioned and not (self.IsPartitioned and self.RequirePartitionFilter)

    def get_summary_load_type(self) -> str:
        prefix = "MIRRORED" if self.IsMirrored else "LAKEHOUSE"

        if self.IsInitialLoad:
            return f"{prefix} - INITIAL FULL (OVERWRITE)"
        else:
            return f"{prefix} - {self.Load_Strategy} ({self.Load_Type})"
    
    @property
    def Mode(self) -> str:
        if self.InitialLoad:
            return SyncLoadType.OVERWRITE
        else:
            return self.Load_Type
    
    @property
    def PrimaryKey(self) -> str:      
        if self.Keys:
            return self.Keys[0]
        else:
            return None

    @property
    def HasKeys(self) -> bool:
        return self.Keys and len(self.Keys) > 0
        
    @property
    def IsMirrored(self) -> bool:
        return self.LakehouseType == FabricDestinationType.MIRRORED_DATABASE

    @property
    def LakehouseHttpUri(self) -> str:
        return f"https://onelake.dfs.fabric.microsoft.com/{self.WorkspaceId}/{self.LakehouseId}"
    
    @property
    def LakehouseAbfssUri(self) -> str:
        return f"abfss://{self.WorkspaceId}@onelake.dfs.fabric.microsoft.com/{self.LakehouseId}"

    @property
    def LakehouseAbfssTablePath(self) -> str:
        base_path = f"{self.LakehouseAbfssUri}/Tables"
        if self.UseLakehouseSchema:
            return f"{base_path}/{self.LakehouseSchema}/{self.LakehouseTable}"
        else:
            return f"{base_path}/{self.LakehouseTable}"

    @property
    def LakehouseTableName(self) -> str:
        if self.UseLakehouseSchema:
            #table_nm = f"`{self.WorkspaceName}.{self.Lakehouse}.{self.LakehouseSchema}.{self.LakehouseTable}"
            table_nm = f"{self.Lakehouse}.{self.LakehouseSchema}.{self.LakehouseTable}"
        else:
            table_nm = f"{self.Lakehouse}.{self.LakehouseTable}"

        return table_nm

    @property
    def BQTableName(self) -> str:
        return f"{self.ProjectId}.{self.Dataset}.{self.TableName}"

    @property
    def IsCDCStrategy(self) -> bool:
        return self.Load_Strategy in [SyncLoadStrategy.CDC, SyncLoadStrategy.CDC_APPEND]
    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        return self.Load_Strategy == SyncLoadStrategy.TIME_INGESTION

    @property
    def IsRangePartitioned(self) -> bool:
        return self.Load_Strategy == SyncLoadStrategy.PARTITION and self.Partition_Type == BQPartitionType.RANGE
    
    @property
    def IsTimePartitionedStrategy(self) -> bool:
         return ((self.Load_Strategy == SyncLoadStrategy.PARTITION and 
            self.Partition_Type == BQPartitionType.TIME) or self.Load_Strategy == SyncLoadStrategy.TIME_INGESTION)
    
    @property
    def IsPartitionedSyncLoad(self) -> bool:
        if self.IsPartitioned:
            if (self.IsTimePartitionedStrategy and self.PartitionId) or self.IsRangePartitioned:
                return True
        return False

    @property
    def LoadPriority(self) -> int:
        return self.Priority + self.SizePriority

    def UpdateRowCounts(self, src:int = 0, insert:int = 0, update:int = 0) -> None:
        self.SourceRows += src

        if not self.Load_Type == SyncLoadType.MERGE or self.IsMirrored:
            self.InsertedRows += src            
            self.UpdatedRows = 0
        else:
            self.InsertedRows += insert
            self.UpdatedRows += update
    
    def __lt__(self, other) -> bool:
        return self.LoadPriority < other.LoadPriority

    def __gt__(self, other) -> bool:
        return self.LoadPriority > other.LoadPriority