from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Any
from uuid import UUID, uuid4
import json

from ..Enum import *
from ...Meta import Version

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
    LogLevel:str = Field(alias="log_level", default="SYNC_STATUS")
    Telemetry:bool = Field(alias="telemetry", default=True)
    TelemetryEndPoint:str=Field(alias="telemetry_endpoint", default="prdbqsyncinsights")
    LogPath:str = Field(alias="log_path", default="/lakehouse/default/Files/BQ_Sync_Process/logs/fabric_sync.log")

class ConfigGCPDataset(SyncBaseModel):
    Dataset:Optional[str] = Field(alias="dataset", default=None)
                   
class ConfigTableMaintenance(SyncBaseModel):
    Enabled:bool = Field(alias="enabled", default=False)
    Interval:Optional[str] = Field(alias="interval", default="MONTH")
                
class ConfigAsync(SyncBaseModel):
    Enabled:bool = Field(alias="enabled", default=True)
    Parallelism:int = Field(alias="parallelism", default=10)
    NotebookTimeout:int = Field(alias="notebook_timeout", default=3600)
    CellTimeout:int = Field(alias="cell_timeout", default=1800)

class ConfigLakehouseTarget(SyncBaseModel):
    Lakehouse:Optional[str] = Field(alias="lakehouse", default=None)
    Schema:Optional[str] = Field(alias="schema", default=None)
    Table:Optional[str] = Field(alias="table_name", default=None)
    PartitionBy:Optional[str] = Field(alias="partition_by", default=None)
                
class ConfigPartition(SyncBaseModel):
    Enabled:bool = Field(alias="enabled", default=False)
    PartitionType:Optional[str] = Field(alias="type", default=None)
    PartitionColumn:Optional[str] = Field(alias="column", default=None)
    Granularity:Optional[str] = Field(alias="partition_grain", default=None)
    PartitionDataType:Optional[str] = Field(alias="partition_data_type", default=None)
    PartitionRange:Optional[str] = Field(alias="partition_range", default=None)

class ConfigFabric(SyncBaseModel):
    WorkspaceID :Optional[str] = Field(alias="workspace_id", default=None)
    MetadataLakehouse:Optional[str] = Field(alias="metadata_lakehouse", default=None)
    TargetLakehouse:Optional[str] = Field(alias="target_lakehouse", default=None)
    TargetLakehouseSchema:Optional[str] = Field(alias="target_schema", default=None)
    EnableSchemas:bool = Field(alias="enable_schemas", default=False)

class ConfigGCPProject(SyncBaseModel):
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Datasets:Optional[List[ConfigGCPDataset]] = Field(alias="datasets", default=[ConfigGCPDataset()])

class ConfigGCPCredential(SyncBaseModel):
    CredentialPath:Optional[str] = Field(alias="credential_path", default=None)
    AccessToken:Optional[str] = Field(alias="access_token", default=None)
    Credential:Optional[str] = Field(alias="credential", default=None)

class ConfigGCPAPI(SyncBaseModel):
    UseStandardAPI:bool = Field(alias="use_standard_api", default=False)
    AutoSelect:bool = Field(alias="auto_select", default=False)
    MaterializationProjectID:Optional[str] = Field(alias="materialization_project_id", default=None)
    MaterializationDataset:Optional[str] = Field(alias="materialization_dataset", default=None)
    BillingProjectID:Optional[str] = Field(alias="billing_project_id", default=None)

class ConfigGCP(SyncBaseModel):
    API:ConfigGCPAPI = Field(alias="api", default=ConfigGCPAPI())
    Projects:List[ConfigGCPProject] = Field(alias="projects", default=[ConfigGCPProject()])
    GCPCredential:ConfigGCPCredential = Field(alias="gcp_credentials", default=ConfigGCPCredential())

class ConfigTableColumn(SyncBaseModel):
    Column:Optional[str] = Field(alias="column", default=None)

class ConfigWatermarkColumn(SyncBaseModel):
    Column:Optional[str] = Field(alias="column", default=None)

class TypedColumn(SyncBaseModel):
    Name:Optional[str] = Field(alias="name", default=None)
    Type:Optional[str] = Field(alias="type", default=None)

class MappedColumn(SyncBaseModel):
    Source:Optional[TypedColumn] = Field(alias="source", default=None)
    Destination:Optional[TypedColumn] = Field(alias="destination", default=None)
    Format:Optional[str] = Field(alias="format", default=None)
    DropSource:bool = Field(alias="drop_source", default=False)

    @property
    def IsTypeConversion(self):
        return self.Source.Type != self.Destination.Type
    
    @property
    def IsRename(self):
        return self.Source.Name != self.Destination.Name

class ConfigBQTableDefault (SyncBaseModel):
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[str] = Field(alias="dataset", default=None)
    ObjectType:Optional[str] = Field(alias="object_type", default="BASE_TABLE")
    Priority:int = Field(alias="priority", default=100)
    LoadStrategy:Optional[str] = Field(alias="load_strategy", default="FULL")
    LoadType:Optional[str] = Field(alias="load_type", default="OVERWRITE")
    Interval:Optional[str] = Field(alias="interval", default="AUTO")
    Enabled:bool = Field(alias="enabled", default=True)
    EnforceExpiration:bool = Field(alias="enforce_expiration", default=False)
    AllowSchemaEvolution:bool = Field(alias="allow_schema_evolution", default=False)
    FlattenTable:bool = Field(alias="flatten_table", default=False)
    FlattenInPlace:bool = Field(alias="flatten_inplace", default=True)
    ExplodeArrays:bool = Field(alias="explode_arrays", default=True)

    TableMaintenance:Optional[ConfigTableMaintenance] = Field(alias="table_maintenance", default=ConfigTableMaintenance())

class ConfigBQTable (ConfigBQTableDefault):
    TableName:Optional[str] = Field(alias="table_name", default=None)
    SourceQuery:Optional[str] = Field(alias="source_query", default=None)
    Predicate:Optional[str] = Field(alias="predicate", default=None)

    ColumnMap:Optional[List[MappedColumn]] = Field(alias="column_map", default=[MappedColumn()])

    LakehouseTarget:Optional[ConfigLakehouseTarget] = Field(alias="lakehouse_target", default=ConfigLakehouseTarget())
    BQPartition:Optional[ConfigPartition] = Field(alias="bq_partition", default=ConfigPartition())
    Keys:Optional[List[ConfigTableColumn]] = Field(alias="keys", default=[ConfigTableColumn()])
    Watermark:Optional[ConfigTableColumn] = Field(alias="watermark", default=ConfigTableColumn()) 

    @property
    def BQ_FQName(self):
        return f"{self.ProjectID}.{self.Dataset}.{self.TableName}"

    def get_table_keys(self):
        keys = []

        if self.Keys:
            keys = [k.Column for k in self.Keys]
        
        return keys
    
    def apply_defaults(self, cfg:list[str], default:ConfigBQTableDefault):
        for td in vars(default).items():
            alias = self._get_alias(td[0])
            if alias and alias not in cfg:
                setattr(self, td[0], td[1])

class ConfigOptimization(SyncBaseModel):
    UseApproximateRowCounts:bool = Field(alias="use_approximate_row_counts", default=False)
    DisableDataframeCache:bool = Field(alias="disable_dataframe_cache", default=False)

class ConfigObjectFilter(SyncBaseModel):
    pattern:Optional[str] = Field(alias="pattern", default=None)
    type:Optional[str] = Field(alias="type", default=None)

class ConfigDiscoverObject(SyncBaseModel):
    Enabled:bool = Field(alias="enabled", default=False)
    LoadAll:bool = Field(alias="load_all", default=False) 
    Filter:Optional[ConfigObjectFilter] = Field(alias="filter", default=None) 

class ConfigAutodiscover(SyncBaseModel):
    Autodetect:bool = Field(alias="autodetect", default=True)

    Tables:ConfigDiscoverObject = Field(alias="tables", default=ConfigDiscoverObject())
    Views:Optional[ConfigDiscoverObject] = Field(alias="views", default=ConfigDiscoverObject())
    MaterializedViews:Optional[ConfigDiscoverObject] = Field(alias="materialized_views", default=ConfigDiscoverObject())

class ConfigDataset(SyncBaseModel):
    ApplicationID:UUID = Field(alias="correlation_id", default=uuid4())
    ID:str = Field(alias='id', default="BQ_SYNC_LOADER")    
    Version:str = Field(alias='version', default=Version.CurrentVersion) 
    EnableDataExpiration:bool = Field(alias="enable_data_expiration", default=False)
    Optimization:ConfigOptimization = Field(alias="optimization", default=ConfigOptimization())

    AutoDiscover:ConfigAutodiscover = Field(alias="autodiscover", default=ConfigAutodiscover())
    Logging:ConfigLogging = Field(alias="logging", default=ConfigLogging())
    Fabric:ConfigFabric = Field(alias="fabric", default=ConfigFabric())
    
    GCP:ConfigGCP = Field(alias="gcp", default=ConfigGCP())
    
    Async:ConfigAsync = Field(alias="async", default=ConfigAsync())
    TableDefaults:Optional[ConfigBQTableDefault] = Field(alias="table_defaults", default=ConfigBQTable())
    Tables:Optional[List[ConfigBQTable]] = Field(alias="tables", default=[ConfigBQTable()])

    @property
    def Autodetect(self):
        return self.AutoDiscover.Autodetect
    
    @property
    def LoadAllTables(self):
        return self.AutoDiscover.Tables.LoadAll
    
    @property
    def EnableTables(self):
        return self.AutoDiscover.Tables.Enabled
    
    @property
    def LoadAllViews(self):
        return self.AutoDiscover.Views.LoadAll
    
    @property
    def EnableViews(self):
        return self.AutoDiscover.Views.Enabled
    
    @property
    def LoadAllMaterializedViews(self):
        return self.AutoDiscover.MaterializedViews.LoadAll
    
    @property
    def EnableMaterializedViews(self):
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
        tables = [t for t in self.Tables if t.ProjectID == project and t.Dataset == dataset and t.ObjectType == str(obj_type)]

        if only_enabled:
            tables = [t for t in tables if t.Enabled == True]

        return [str(x.TableName) for x in tables]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def apply_table_defaults(self, config_json:str):
        if self.TableDefaults and self.Tables:
            json_data = json.loads(config_json)

            for t in self.Tables: 
                tbl_cfg = self._get_table_json_config(json_data, t.TableName)

                if tbl_cfg:
                    t.apply_defaults(list(tbl_cfg.keys()), self.TableDefaults)
    
    def _get_table_json_config(self, json_data, tbl_nm:str):
        if "tables" in json_data:
            t = [t for t in json_data["tables"] if t["table_name"]==tbl_nm]

            if t:
                return t[0]
        
        return None