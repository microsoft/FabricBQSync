from pydantic import (
    BaseModel, Field, ConfigDict
)
from typing import (
    List, Optional, Any
)

import json
import os

from FabricSync.BQ.Enum import (
    SyncLogLevelName, FabricDestinationType, MaintenanceStrategy, 
    MaintenanceInterval, CalendarInterval, BQPartitionType, BQDataType, 
    BigQueryObjectType, SyncLoadStrategy, SyncLoadType, SyncScheduleType, 
    ObjectFilterType
)

class SyncBaseModel(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True, 
        use_enum_values = True,
        arbitrary_types_allowed=True,
        exclude_unset=True        
        )
    
    def model_dump(self, **kwargs) -> dict[str, Any]:
        return super().model_dump(by_alias=True, **kwargs)
    
    def model_dump_json(self, **kwargs) -> dict[str, Any]:
        return super().model_dump_json(by_alias=True, **kwargs)
    
    def is_field_set(self, item) -> bool:
        return item in self.model_fields_set

    def get_field_default(self, item) -> Any:
        meta = self._get_field_meta(item)
        
        if not meta:
            return None
        
        return meta.default

    def _get_field_meta(self, item):
        for _, meta in self.model_fields.items():
            if meta.alias == item:
                return meta
        
        return None
    
    def __getattr__(self, item):
        for field, meta in self.model_fields.items():
            if meta.alias == item:
                return getattr(self, field)
        return super().__getattr__(item)
    
    def _get_alias(self, item_name):
        for field, meta in self.model_fields.items():
            if field == item_name:
                return meta.alias
        
        return None

class ConfigLogging(SyncBaseModel):
    LogLevel:Optional[SyncLogLevelName] = Field(alias="log_level", default=SyncLogLevelName.SYNC_STATUS)
    Telemetry:Optional[bool] = Field(alias="telemetry", default=True)
    TelemetryEndPoint:Optional[str]=Field(alias="telemetry_endpoint", default="prdbqsyncinsights")
    LogPath:Optional[str] = Field(alias="log_path", default="/lakehouse/default/Files/Fabric_Sync_Process/logs/fabric_sync.log")

class ConfigIntelligentMaintenance(SyncBaseModel):
    RowsChanged:Optional[float] = Field(alias="rows_changed", default=float(0.10))
    TableSizeGrowth:Optional[float] = Field(alias="table_size_growth", default=float(0.10))
    FileFragmentation:Optional[float] = Field(alias="file_fragmentation", default=float(0.10))
    OutOfScopeSize:Optional[float] = Field(alias="out_of_scope_size", default=float(0.10))

class ConfigMaintenance(SyncBaseModel):
    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    TrackHistory:Optional[bool] = Field(alias="track_history", default=False)
    RetentionHours:Optional[int] = Field(alias="retention_hours", default=168)
    Strategy:Optional[MaintenanceStrategy] = Field(alias="strategy", default=MaintenanceStrategy.SCHEDULED)
    Thresholds:Optional[ConfigIntelligentMaintenance] = Field(alias="thresholds", default=ConfigIntelligentMaintenance())
    Interval:Optional[str] = Field(alias="interval", default="AUTO")

class ConfigTableMaintenance(SyncBaseModel):
    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    Interval:Optional[MaintenanceInterval] = Field(alias="interval", default=MaintenanceInterval.AUTO)
                
class ConfigAsync(SyncBaseModel):
    Enabled:Optional[bool] = Field(alias="enabled", default=True)
    Parallelism:Optional[int] = Field(alias="parallelism", default=10)

class ConfigLakehouseTarget(SyncBaseModel):
    LakehouseID:Optional[str] = Field(alias="lakehouse_id", default=None)
    Lakehouse:Optional[str] = Field(alias="lakehouse", default=None)
    Schema:Optional[str] = Field(alias="schema", default=None)
    Table:Optional[str] = Field(alias="table_name", default=None)
    PartitionBy:Optional[str] = Field(alias="partition_by", default=None)
                
class ConfigPartition(SyncBaseModel):
    Enabled:Optional[bool] = Field(alias="enabled", default=None)
    PartitionType:Optional[BQPartitionType] = Field(alias="type", default=None)
    PartitionColumn:Optional[str] = Field(alias="column", default=None)
    Granularity:Optional[CalendarInterval] = Field(alias="partition_grain", default=None)
    PartitionDataType:Optional[BQDataType] = Field(alias="partition_data_type", default=None)
    PartitionRange:Optional[str] = Field(alias="partition_range", default=None)

class ConfigFabric(SyncBaseModel):
    WorkspaceID:Optional[str] = Field(alias="workspace_id", default=None)
    WorkspaceName:Optional[str] = Field(alias="workspace_name", default=None)
    MetadataLakehouse:Optional[str] = Field(alias="metadata_lakehouse", default=None)
    MetadataSchema:Optional[str] = Field(alias="metadata_schema", default="dbo")
    MetadataLakehouseID:Optional[str] = Field(alias="metadata_lakehouse_id", default=None)
    TargetType:Optional[FabricDestinationType] = Field(alias="target_type", default=FabricDestinationType.LAKEHOUSE)
    TargetLakehouse:Optional[str] = Field(alias="target_lakehouse", default=None)
    TargetLakehouseID:Optional[str] = Field(alias="target_lakehouse_id", default=None)
    TargetLakehouseSchema:Optional[str] = Field(alias="target_schema", default=None)
    EnableSchemas:Optional[bool] = Field(alias="enable_schemas", default=False)

    def get_metadata_namespace(self) -> str:
        if self.EnableSchemas:
            return f"`{self.WorkspaceName}`.{self.MetadataLakehouse}.dbo"
        else:
            return f"`{self.WorkspaceName}`.{self.MetadataLakehouse}"

    def get_metadata_lakehouse(self) -> str:
        if self.EnableSchemas:
            return f"{self.MetadataLakehouse}.dbo"
        else:
            return f"{self.MetadataLakehouse}"
    
    def get_target_namespace(self) -> str:
        if self.EnableSchemas:
            return f"`{self.WorkspaceName}`.{self.TargetLakehouse}.{self.TargetLakehouseSchema}"
        else:
            return f"`{self.WorkspaceName}`.{self.TargetLakehouse}"

    def get_target_lakehouse(self) -> str:
        if self.EnableSchemas:
            return f"{self.TargetLakehouse}.{self.TargetLakehouseSchema}"
        else:
            return f"{self.TargetLakehouse}"
    
    def get_target_table_path(self, table:str) -> str:
        if self.EnableSchemas:
            return f"{self.TargetLakehouse}.{self.TargetLakehouseSchema}.{table}"
        else:
            return table

    def get_metadata_table_path(self, table:str) -> str:
        if self.EnableSchemas:
            return f"{self.MetadataLakehouse}.dbo.{table}"
        else:
            return table

class ConfigGCPDataset(SyncBaseModel):
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    
class ConfigGCPProject(SyncBaseModel):
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Datasets:Optional[List[ConfigGCPDataset]] = Field(alias="datasets", default=[ConfigGCPDataset()])

class ConfigGCPCredential(SyncBaseModel):
    CredentialPath:Optional[str] = Field(alias="credential_path", default=None)
    AccessToken:Optional[str] = Field(alias="access_token", default=None)
    Credential:Optional[str] = Field(alias="credential", default=None)
    CredentialSecretKeyVault:Optional[str] = Field(alias="credential_secret_key_vault", default=None)
    CredentialSecretKey:Optional[str] = Field(alias="credential_secret_key", default=None)

class ConfigGCPAPI(SyncBaseModel):
    UseStandardAPI:Optional[bool] = Field(alias="use_standard_api", default=False)
    AutoSelect:Optional[bool] = Field(alias="auto_select", default=False)
    UseCDC:Optional[bool] = Field(alias="use_cdc", default=True)
    MaterializationProjectID:Optional[str] = Field(alias="materialization_project_id", default=None)
    MaterializationDataset:Optional[str] = Field(alias="materialization_dataset", default=None)
    BillingProjectID:Optional[str] = Field(alias="billing_project_id", default=None)

class ConfigGCP(SyncBaseModel):
    API:Optional[ConfigGCPAPI] = Field(alias="api", default=ConfigGCPAPI())
    Projects:List[ConfigGCPProject] = Field(alias="projects", default=[ConfigGCPProject()])
    GCPCredential:ConfigGCPCredential = Field(alias="gcp_credentials", default=ConfigGCPCredential())

    @property
    def DefaultProjectID(self) -> str:
        if self.Projects:
            return self.Projects[0].ProjectID
        
        return None
    
    @property
    def DefaultDataset(self) -> str:
        if self.Projects:
            if self.Projects[0].Datasets:
                return self.Projects[0].Datasets[0].Dataset
        
        return None

class ConfigTableColumn(SyncBaseModel):
    Column:Optional[str] = Field(alias="column", default=None)

class TypedColumn(SyncBaseModel):
    Name:Optional[str] = Field(alias="name", default=None)
    Type:Optional[str] = Field(alias="type", default=None)

class MappedColumn(SyncBaseModel):
    Source:Optional[TypedColumn] = Field(alias="source", default=None)
    Destination:Optional[TypedColumn] = Field(alias="destination", default=None)
    Format:Optional[str] = Field(alias="format", default=None)
    DropSource:Optional[bool] = Field(alias="drop_source", default=None)

    @property
    def IsTypeConversion(self) -> bool:
        return self.Source.Type != self.Destination.Type
    
    @property
    def IsRename(self) -> bool:
        return self.Source.Name != self.Destination.Name

class ConfigBQTableDefault (SyncBaseModel):
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    ObjectType:Optional[BigQueryObjectType] = Field(alias="object_type", default=None)
    Priority:Optional[int] = Field(alias="priority", default=100)
    LoadStrategy:Optional[SyncLoadStrategy] = Field(alias="load_strategy", default=None)
    LoadType:Optional[SyncLoadType] = Field(alias="load_type", default=None)
    Interval:Optional[SyncScheduleType] = Field(alias="interval", default=SyncScheduleType.AUTO)
    Enabled:Optional[bool] = Field(alias="enabled", default=True)
    EnforceExpiration:Optional[bool] = Field(alias="enforce_expiration", default=False)
    AllowSchemaEvolution:Optional[bool] = Field(alias="allow_schema_evolution", default=False)
    FlattenTable:Optional[bool] = Field(alias="flatten_table", default=False)
    FlattenInPlace:Optional[bool] = Field(alias="flatten_inplace", default=True)
    ExplodeArrays:Optional[bool] = Field(alias="explode_arrays", default=True)

    TableMaintenance:Optional[ConfigTableMaintenance] = Field(alias="table_maintenance", default=ConfigTableMaintenance())

class ConfigBQTable (SyncBaseModel):
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    ObjectType:Optional[BigQueryObjectType] = Field(alias="object_type", default=None)
    Priority:Optional[int] = Field(alias="priority", default=None)
    LoadStrategy:Optional[SyncLoadStrategy] = Field(alias="load_strategy", default=None)
    LoadType:Optional[SyncLoadType] = Field(alias="load_type", default=None)
    Interval:Optional[SyncScheduleType] = Field(alias="interval", default=None)
    Enabled:Optional[bool] = Field(alias="enabled", default=None)
    EnforceExpiration:Optional[bool] = Field(alias="enforce_expiration", default=None)
    AllowSchemaEvolution:Optional[bool] = Field(alias="allow_schema_evolution", default=None)
    FlattenTable:Optional[bool] = Field(alias="flatten_table", default=None)
    FlattenInPlace:Optional[bool] = Field(alias="flatten_inplace", default=None)
    ExplodeArrays:Optional[bool] = Field(alias="explode_arrays", default=None)

    TableMaintenance:Optional[ConfigTableMaintenance] = Field(alias="table_maintenance", default=ConfigTableMaintenance())

    TableName:Optional[str] = Field(alias="table_name", default=None)
    SourceQuery:Optional[str] = Field(alias="source_query", default=None)
    Predicate:Optional[str] = Field(alias="predicate", default=None)

    ColumnMap:Optional[List[MappedColumn]] = Field(alias="column_map", default=[MappedColumn()])

    LakehouseTarget:Optional[ConfigLakehouseTarget] = Field(alias="lakehouse_target", default=ConfigLakehouseTarget())
    BQPartition:Optional[ConfigPartition] = Field(alias="bq_partition", default=ConfigPartition())
    Keys:Optional[List[ConfigTableColumn]] = Field(alias="keys", default=[ConfigTableColumn()])
    Watermark:Optional[ConfigTableColumn] = Field(alias="watermark", default=ConfigTableColumn()) 

    @property
    def BQ_FQName(self) -> str:
        return f"{self.ProjectID}.{self.Dataset}.{self.TableName}"

    def get_table_keys(self) -> list[str]:
        keys = []

        if self.Keys:
            keys = [k.Column for k in self.Keys]
        
        return keys

class ConfigOptimization(SyncBaseModel):
    UseApproximateRowCounts:Optional[bool] = Field(alias="use_approximate_row_counts", default=True)
    DisableDataframeCache:Optional[bool] = Field(alias="disable_dataframe_cache", default=False)

class ConfigObjectFilter(SyncBaseModel):
    pattern:Optional[str] = Field(alias="pattern", default=None)
    type:Optional[ObjectFilterType] = Field(alias="type", default=None)

class ConfigDiscoverObject(SyncBaseModel):
    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    LoadAll:Optional[bool] = Field(alias="load_all", default=False) 
    Filter:Optional[ConfigObjectFilter] = Field(alias="filter", default=None) 

class ConfigAutodiscover(SyncBaseModel):
    Autodetect:Optional[bool] = Field(alias="autodetect", default=True)

    Tables:Optional[ConfigDiscoverObject] = Field(alias="tables", default=ConfigDiscoverObject())
    Views:Optional[ConfigDiscoverObject] = Field(alias="views", default=ConfigDiscoverObject())
    MaterializedViews:Optional[ConfigDiscoverObject] = Field(alias="materialized_views", default=ConfigDiscoverObject())

class ConfigDataset(SyncBaseModel):
    ApplicationID:Optional[str] = Field(alias="correlation_id", default=None)
    ID:Optional[str] = Field(alias='id', default="FABRIC_SYNC_LOADER")    
    Version:Optional[str] = Field(alias='version', default=None)    
    EnableDataExpiration:Optional[bool] = Field(alias="enable_data_expiration", default=False)
    Optimization:Optional[ConfigOptimization] = Field(alias="optimization", default=ConfigOptimization())
    Maintenance:Optional[ConfigMaintenance] = Field(alias="maintenance", default=ConfigMaintenance())

    AutoDiscover:Optional[ConfigAutodiscover] = Field(alias="autodiscover", default=ConfigAutodiscover())
    Logging:Optional[ConfigLogging] = Field(alias="logging", default=ConfigLogging())
    Fabric:Optional[ConfigFabric] = Field(alias="fabric", default=ConfigFabric())
    
    GCP:Optional[ConfigGCP] = Field(alias="gcp", default=ConfigGCP())
    
    Async:Optional[ConfigAsync] = Field(alias="async", default=ConfigAsync())
    TableDefaults:Optional[ConfigBQTableDefault] = Field(alias="table_defaults", default=ConfigBQTable())
    Tables:Optional[List[ConfigBQTable]] = Field(alias="tables", default=[ConfigBQTable()])

    @property
    def Autodetect(self) -> bool:
        return self.AutoDiscover.Autodetect
    
    @property
    def LoadAllTables(self) -> bool:
        return self.AutoDiscover.Tables.LoadAll
    
    @property
    def EnableTables(self) -> bool:
        return self.AutoDiscover.Tables.Enabled
    
    @property
    def LoadAllViews(self) -> bool:
        return self.AutoDiscover.Views.LoadAll
    
    @property
    def EnableViews(self) -> bool:
        return self.AutoDiscover.Views.Enabled
    
    @property
    def LoadAllMaterializedViews(self) -> bool:
        return self.AutoDiscover.MaterializedViews.LoadAll
    
    @property
    def EnableMaterializedViews(self) -> bool:
        return self.AutoDiscover.MaterializedViews.Enabled

    def get_table_config(self, bq_table:str) -> ConfigBQTable:
        if self.Tables:
            return next((t for t in self.Tables if t.BQ_FQName == bq_table), None)
        else:
            return None

    def get_table_name_list(self, project:str, dataset:str, obj_type:BigQueryObjectType, only_enabled:bool = False) -> list[str]:
        """
        Returns a list of table names from the user configuration
        """
        tables = [t for t in self.Tables if t.ProjectID == project and t.Dataset == dataset and t.ObjectType == obj_type]

        if only_enabled:
            tables = [t for t in tables if t.Enabled == True]

        return [str(x.TableName) for x in tables]
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def apply_defaults(self) -> None:
        if self.Fabric.EnableSchemas:
            if not self.Fabric.TargetLakehouseSchema:
                self.Fabric.TargetLakehouseSchema = self.GCP.DefaultDataset

        self.__apply_table_defaults()


    def __apply_table_defaults(self) -> None:
        if self.is_field_set("TableDefaults") and self.is_field_set("Tables"):
            for table in self.Tables:
                default_fields = [f for f in table.model_fields 
                    if not table.is_field_set(f) 
                        and f in self.TableDefaults.model_fields 
                        and self.TableDefaults.is_field_set(f)]
                
                for f in default_fields:
                    setattr(table, f, getattr(self.TableDefaults, f))
        
    @classmethod
    def from_json(cls, path:str, with_defaults:bool=True) -> 'ConfigDataset':
        data = cls.__read_json_config(path)

        config = ConfigDataset(**data)      
         
        if with_defaults:     
            config.apply_defaults()

        return config

    @classmethod
    def __read_json_config(self, path) -> 'ConfigDataset':
        if os.path.exists(path):
            with open(path, 'r', encoding="utf-8") as f:
                data = json.load(f)
            return data
        else:
            raise Exception(f"Configuration file not found ({path})")
    
    def to_json(self, path:str) -> None:
        try:
            with open(path, 'w', encoding="utf-8") as f:
                json.dump(self.model_dump(exclude_none=True, exclude_unset=True), f)
        except IOError as e:
            raise Exception(f"Unable to save Configuration file to path ({path}): {e}")