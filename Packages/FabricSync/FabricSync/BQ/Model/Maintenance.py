from pydantic import Field
from typing import Optional
from datetime import datetime

from FabricSync.BQ.Model.Config import SyncBaseModel
from FabricSync.BQ.Enum import (
    BQPartitionType, CalendarInterval, MaintenanceInterval
)

class MaintenanceSchedule(SyncBaseModel):
    SyncId:Optional[str] = Field(alias="sync_id", default=None)
    TableId:Optional[str] = Field(alias="table_id", default=None)
    ProjectId:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    Table:Optional[str] = Field(alias="table_name", default=None)

    PartitionId:Optional[str] = Field(alias="partition_id", default=None)
    PartitionIndex:Optional[int] = Field(alias="partition_index", default=None)
    PartitionColumn:Optional[str] = Field(alias="partition_column", default=None)
    PartitionType:Optional[BQPartitionType] = Field(alias="partition_type", default=None)
    PartitionGrain:Optional[CalendarInterval] = Field(alias="partition_grain", default=None)

    TableMaintenanceInterval:Optional[MaintenanceInterval] = Field(alias="last_maintenance_interval", default=None)
    Lakehouse:Optional[str] = Field(alias="lakehouse", default=None)
    LakehouseSchema:Optional[str] = Field(alias="lakehouse_schema", default=None)
    LakehouseTable:Optional[str] = Field(alias="lakehouse_table_name", default=None)
    LakehousePartition:Optional[str] = Field(alias="lakehouse_partition", default=None)

    RowCount:Optional[int] = Field(alias="row_count", default=None)
    TablePartitionSize:Optional[int] = Field(alias="table_partition_size", default=None)

    Strategy:Optional[str] = Field(alias="last_maintenance_type", default=None)
    TrackHistory:Optional[bool] = Field(alias="track_history", default=None)
    RetentionHours:Optional[int] = Field(alias="retention_hours", default=None)

    RowsChanged:Optional[float] = Field(alias="rows_changed", default=None)
    TableSizeGrowth:Optional[float] = Field(alias="table_size_growth", default=None)
    FileFragmentation:Optional[float] = Field(alias="file_fragmentation", default=None)
    OutOfScopeSize:Optional[float] = Field(alias="out_of_scope_size", default=None)

    LastStatus:Optional[str] = Field(alias="last_maintenance_status", default=None)
    LastMaintenance:Optional[datetime] = Field(alias="last_maintenance", default=None)
    LastOptimize:Optional[datetime] = Field(alias="last_optimize", default=None)
    LastVacuum:Optional[datetime] = Field(alias="last_vacuum", default=None)
    NextMaintenance:Optional[datetime] = Field(alias="next_maintenance", default=None)

    RunOptimize:Optional[bool] = Field(alias="run_optimize", default=None)
    RunVacuum:Optional[bool] = Field(alias="run_vacuum", default=None)
    FullTableMaintenance:Optional[bool] = Field(alias="full_table_maintenance", default=None)

    CreatedDt:Optional[datetime] = Field(alias="created_dt", default=None)
    LastUpdatedDt:Optional[datetime] = Field(alias="last_updated_dt", default=None)

    @property
    def FabricLakehousePath(self) -> str:
        if self.LakehouseSchema:
            return f"{self.Lakehouse}.{self.LakehouseSchema}.{self.LakehouseTable}"
        else:
            return f"{self.Lakehouse}.{self.LakehouseTable}"

    @property
    def Id(self) -> str:
        if self.PartitionId:
            return f"{self.FabricLakehousePath}${self.PartitionId}"
        else:
            return self.FabricLakehousePath