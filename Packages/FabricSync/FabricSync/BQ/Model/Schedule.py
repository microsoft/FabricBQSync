from datetime import datetime, timezone
from pydantic import TypeAdapter, Field
from typing import List, Optional
from datetime import datetime, timezone
import json

from ..Enum import *
from .Config import *

class SyncSchedule(SyncBaseModel):
    EndTime:Optional[datetime] = Field(alias="completed", default=None)
    SourceRows:int = Field(alias="source_rows", default=0)
    InsertedRows:int = Field(alias="inserted_rows", default=0)
    UpdatedRows:int = Field(alias="updated_rows", default=0)
    DeltaVersion:Optional[int] = Field(alias="delta_version", default=None)
    SparkAppId:Optional[str] = Field(alias="spark_app_id", default=None)
    Status:Optional[str] = Field(alias="sync_status", default=None)
    FabricPartitionColumns:list[str] = Field(alias="fabric_partition_columns", default=[])
    SummaryLoadType:str = Field(alias="summary_load_type", default=None)
    TableId:str = Field(alias="table_id", default=None)
    StartTime:Optional[datetime] = Field(alias="started", default=datetime.now(timezone.utc))
    SyncId:str = Field(alias="sync_id", default=None)
    GroupScheduleId:str = Field(alias="group_schedule_id", default=None)
    ScheduleId:str = Field(alias="schedule_id", default=None)
    Load_Strategy:str = Field(alias="load_strategy", default=str(LoadStrategy.FULL))
    Load_Type:str = Field(alias="load_type", default=str(LoadType.OVERWRITE))
    InitialLoad:bool = Field(alias="initial_load", default=False)
    LastScheduleLoadDate:Optional[datetime] = Field(alias="last_schedule_dt", default=None)
    Priority:int = Field(alias="priority", default=0)
    ProjectId:str = Field(alias="project_id", default=None)
    Dataset:str = Field(alias="dataset", default=None)
    TableName:str = Field(alias="table_name", default=None)
    ObjectType:str = Field(alias="object_type", default=str(BigQueryObjectType.BASE_TABLE))
    SourceQuery:Optional[str] = Field(alias="source_query", default=None)
    SourcePredicate:Optional[str] = Field(alias="source_predicate", default=None)
    MaxWatermark:Optional[str] = Field(alias="max_watermark", default=None)
    WatermarkColumn:Optional[str] = Field(alias="watermark_column", default=None)
    IsPartitioned:bool = Field(alias="is_partitioned", default=False)
    PartitionColumn:Optional[str] = Field(alias="partition_column", default=None)
    Partition_Type:Optional[str] = Field(alias="partition_type", default=None)
    PartitionGrain:Optional[str] = Field(alias="partition_grain", default=None)
    PartitionId:Optional[str] = Field(alias="partition_id", default=None)    
    PartitionDataType:Optional[str] = Field(alias="partition_data_type", default=None) 
    PartitionRange:Optional[str] = Field(alias="partition_range", default=None)
    RequirePartitionFilter:Optional[bool] = Field(alias="require_partition_filter", default=None)
    Lakehouse:str = Field(alias="lakehouse", default=None)
    LakehouseSchema:Optional[str] = Field(alias="lakehouse_schema", default=None)
    LakehouseTable:str = Field(alias="lakehouse_table_name", default=None)
    LakehousePartition:Optional[str] = Field(alias="lakehouse_partition", default=None)
    UseLakehouseSchema:bool = Field(alias="use_lakehouse_schema", default=False)
    EnforcePartitionExpiration:bool = Field(alias="enforce_partition_expiration", default=False)
    AllowSchemaEvolution:bool = Field(alias="allow_schema_evolution", default=False)
    EnableTableMaintenance:bool = Field(alias="enable_table_maintenance", default=False)
    TableMaintenanceInterval:bool = Field(alias="table_maintenance_inteval", default=False)
    FlattenTable:bool = Field(alias="flatten_table", default=False)
    FlattenInPlace:bool = Field(alias="flatten_inplace", default=False)
    ExplodeArrays:bool = Field(alias="explode_arrays", default=False)
    Keys:Optional[List[str]] = Field(alias="primary_keys", default=[])
    TotalRows:int = Field(alias="total_rows", default=0)
    TotalLogicalMb:int = Field(alias="total_logical_mb", default=0)
    SizePriority:int = Field(alias="size_priority", default=0)
    ColumnMap:Optional[str] = Field(alias="column_map", default=None)

    def to_telemetry(self):
        sensitive_keys = ["project_id","dataset","table_name","source_query","source_predicate","watermark_column","max_watermark",
            "partition_column","partition_id","partition_range","lakehouse","lakehouse_schema","lakehouse_table_name",
            "lakehouse_partition","primary_keys","size_priority","fabric_partition_columns", "column_map"]
        
        telemetry_data = json.loads(self.model_dump_json())

        for key in sensitive_keys:
            if key in telemetry_data:
                del telemetry_data[key]
        
        return telemetry_data

    def get_column_map(self) -> list[MappedColumn]:
        if self.ColumnMap:
            d = json.loads(self.ColumnMap)
            maps = TypeAdapter(List[MappedColumn]).validate_python(d)

            return [m for m in maps if m.Source]

        return None

    @property
    def DefaultSummaryLoadType(self) -> str:
        if self.InitialLoad and not self.IsTimeIngestionPartitioned and \
            not self.IsRangePartitioned and not (self.IsPartitioned and self.RequirePartitionFilter):
            return "INITIAL FULL_OVERWRITE"
        else:
            return f"{self.Load_Strategy}_{self.Load_Type}"
    
    @property
    def Mode(self) -> str:
        if self.InitialLoad:
            return str(LoadType.OVERWRITE)
        else:
            return str(self.Load_Type)
    
    @property
    def PrimaryKey(self) -> str:      
        if self.Keys:
            return self.Keys[0]
        else:
            return None

    @property
    def LakehouseTableName(self) -> str:
        if self.UseLakehouseSchema:
            table_nm = f"{self.Lakehouse}.{self.LakehouseSchema}.{self.LakehouseTable}"
        else:
            table_nm = f"{self.Lakehouse}.{self.LakehouseTable}"

        return table_nm

    @property
    def BQTableName(self) -> str:
        return f"{self.ProjectId}.{self.Dataset}.{self.TableName}"


    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        return self.Load_Strategy == str(LoadStrategy.TIME_INGESTION)

    @property
    def IsRangePartitioned(self) -> bool:
        return self.Load_Strategy == str(LoadStrategy.PARTITION) and self.Partition_Type == str(PartitionType.RANGE)
    
    @property
    def IsTimePartitionedStrategy(self) -> bool:
         return ((self.Load_Strategy == str(LoadStrategy.PARTITION) and 
            self.Partition_Type == str(PartitionType.TIME)) or self.Load_Strategy == str(LoadStrategy.TIME_INGESTION))
    
    @property
    def IsPartitionedSyncLoad(self) -> bool:
        if self.IsPartitioned:
            if (self.IsTimePartitionedStrategy and self.PartitionId) or self.IsRangePartitioned:
                return True
        return False

    @property
    def LoadPriority(self) -> int:
        return self.Priority + self.SizePriority

    def UpdateRowCounts(self, src:int = 0, insert:int = 0, update:int = 0):
        self.SourceRows += src

        if not self.Load_Type == str(LoadType.MERGE):
            self.InsertedRows += src            
            self.UpdatedRows = 0
        else:
            self.InsertedRows += insert
            self.UpdatedRows += update
    
    def __lt__(self, other):
        return self.LoadPriority < other.LoadPriority

    def __gt__(self, other):
        return self.LoadPriority > other.LoadPriority