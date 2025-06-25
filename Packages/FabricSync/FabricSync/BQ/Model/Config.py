from pydantic import (
    BaseModel, Field, ConfigDict
)
from typing import (
    List, Optional, Any
)

import json
import hashlib
import os
import shutil
from pathlib import Path
from datetime import datetime

from FabricSync.BQ.Enum import (
    SyncLogLevelName, FabricDestinationType, MaintenanceStrategy, 
    MaintenanceInterval, CalendarInterval, BQPartitionType, BQDataType, 
    BigQueryObjectType, SyncLoadStrategy, SyncLoadType, SyncScheduleType, 
    ObjectFilterType
)

class SyncBaseModel(BaseModel):
    """
    SyncBaseModel is a base class for synchronization configuration models.
    It extends the BaseModel from Pydantic and provides additional functionality
    for handling model fields, aliases, and serialization.
    This class is designed to be used as a base for other configuration models 
    that require consistent handling of field names and serialization behavior.
    Attributes:
        model_config (ConfigDict): Configuration for the Pydantic model, including options for field population,
            enum value usage, and allowing arbitrary types.
        model_fields_set (set): A set of fields that have been explicitly set in the model instance.
    Methods:
        model_dump(**kwargs) -> dict[str, Any]:
            Generates a dictionary representation of the model, using aliases for field names.  
        model_dump_json(**kwargs) -> dict[str, Any]:
            Serializes the current model instance to a JSON-compatible dictionary.
        is_field_set(item) -> bool:
            Checks if the specified item is among the fields that have been set.
        get_field_default(item) -> Any:
            Retrieve the default value defined for a given field.
        _get_field_meta(item):
            Retrieves the metadata object for a given item by matching its alias in the model_fields dictionary.
        __getattr__(item):
            Retrieves an attribute by its alias from the model_fields dictionary, returning
            the corresponding attribute if found. If no matching alias exists, defers to the superclass __getattr__.
        _get_alias(item_name):
            Retrieves the alias associated with a given field name in the model.
    This class provides a foundation for building configuration models with consistent
    serialization and field management capabilities, making it easier to work with
    synchronization configurations in a structured manner.
    """

    model_config = ConfigDict(
        populate_by_name=True, 
        use_enum_values = True,
        arbitrary_types_allowed=True,
        exclude_unset=True        
        )
    
    @property
    def hash_sha256(self):
        """
        Generates a SHA-256 hash of the model's JSON representation.
        This hash is based on the model's data, excluding any unset or None values.
        Returns:
            str: The SHA-256 hash of the model's JSON representation.
        """
        data = json.dumps(self.model_dump_json(exclude_none=True, exclude_unset=True), sort_keys=True)
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    def model_dump(self, **kwargs) -> dict[str, Any]:
        """
        Generates a dictionary representation of the model, using aliases for field names.
        This method allows for additional keyword arguments to be passed, which can modify the behavior of the
        serialization process, such as excluding unset fields or formatting options.
        Args:
            **kwargs: Additional keyword arguments to customize the serialization.
        Returns:
            dict[str, Any]: A dictionary representation of the model, with field names as aliases.
        """
        return super().model_dump(by_alias=True, **kwargs)
    
    def model_dump_json(self, **kwargs) -> dict[str, Any]:
        """
        Serializes the current model instance to a JSON-compatible dictionary.
        This method uses the model's aliases for field names and allows for additional keyword arguments
        to customize the serialization process, such as excluding unset fields or formatting options.
        Args:
            **kwargs: Additional keyword arguments to customize the serialization.
        Returns:
            dict[str, Any]: A JSON-compatible dictionary representation of the model, with field names as aliases.
        """
        return super().model_dump_json(by_alias=True, **kwargs)
    
    def is_field_set(self, item) -> bool:
        """
        Checks if the specified item is among the fields that have been set in the model instance.
        This method checks against the model_fields_set, which contains the names of fields that have been
        explicitly set in the model.
        Args:
            item (str): The name of the field to check.
        Returns:
            bool: True if the field is set, False otherwise.
        """
        return item in self.model_fields_set

    def get_field_default(self, item) -> Any:
        """
        Retrieve the default value defined for a given field in the model.
        This method checks the model's metadata for the specified item and returns its default value.
        Args:
            item (str): The name of the field for which to retrieve the default value.
        Returns:
            Any: The default value for the specified field, or None if the field is not found
        """
        meta = self._get_field_meta(item)
        
        if not meta:
            return None
        
        return meta.default

    def _get_field_meta(self, item):
        """
        Retrieves the metadata object for a given item by matching its alias in the model_fields dictionary.
        Args:
            item (str): The name of the field for which to retrieve the metadata.
        Returns:
            Field: The metadata object for the specified field, or None if not found.
        """
        for _, meta in self.model_fields.items():
            if meta.alias == item:
                return meta
        
        return None
    
    def __getattr__(self, item):
        """
        Retrieves an attribute by its alias from the model_fields dictionary.
        If the alias matches a field in the model, it returns the corresponding attribute.
        If no matching alias exists, it defers to the superclass __getattr__ method.
        Args:
            item (str): The name of the attribute to retrieve.
        Returns:
            Any: The value of the attribute if found, otherwise defers to the superclass __getattr__.
        """
        for field, meta in self.model_fields.items():
            if meta.alias == item:
                return getattr(self, field)
        return super().__getattr__(item)
    
    def _get_alias(self, item_name):
        """
        Retrieves the alias associated with a given field name in the model.
        Args:
            item_name (str): The name of the field for which to retrieve the alias.
        Returns:
            str or None: The alias for the given field name if found, otherwise None.
        """
        for field, meta in self.model_fields.items():
            if field == item_name:
                return meta.alias
        
        return None

class ConfigLogging(SyncBaseModel):
    """
    Represents the logging configuration for the synchronization process.
    Attributes:
        LogLevel (Optional[SyncLogLevelName]): The level of logging to use.
        Telemetry (Optional[bool]): Indicates whether telemetry is enabled.
        TelemetryEndPoint (Optional[str]): The endpoint for sending telemetry data.
        LogPath (Optional[str]): The file path where logs are saved.
    """

    LogLevel:Optional[SyncLogLevelName] = Field(alias="log_level", default=SyncLogLevelName.SYNC_STATUS)
    Telemetry:Optional[bool] = Field(alias="telemetry", default=True)
    TelemetryEndPoint:Optional[str]=Field(alias="telemetry_endpoint", default="prdbqsyncinsights")
    LogPath:Optional[str] = Field(alias="log_path", default="/lakehouse/default/Files/Fabric_Sync_Process/logs/fabric_sync.log")

class ConfigIntelligentMaintenance(SyncBaseModel):
    """
    ConfigIntelligentMaintenance is a model that contains configuration thresholds
    for maintenance operations in BigQuery.
    Attributes:
        RowsChanged (float): The threshold factor of changed rows before a maintenance task is triggered.
        TableSizeGrowth (float): The threshold factor of table size increase indicating potential need for optimization.
        FileFragmentation (float): The threshold factor measuring file fragmentation to determine if defragmentation is needed.
        OutOfScopeSize (float): The threshold factor of out-of-scope data size prompting possible cleanup or archiving.
    """

    RowsChanged:Optional[float] = Field(alias="rows_changed", default=float(0.10))
    TableSizeGrowth:Optional[float] = Field(alias="table_size_growth", default=float(0.10))
    FileFragmentation:Optional[float] = Field(alias="file_fragmentation", default=float(0.10))
    OutOfScopeSize:Optional[float] = Field(alias="out_of_scope_size", default=float(0.10))

class ConfigMaintenance(SyncBaseModel):
    """
    ConfigMaintenance is a configuration model used to define maintenance-related settings for a synchronization process.
    Attributes:
        Enabled (bool):
            Indicates whether the maintenance process is enabled.
        TrackHistory (bool):
            Specifies if history tracking is enabled during maintenance.
        RetentionHours (int):
            Number of hours to retain data, defaults to one week (168 hours).
        Strategy (MaintenanceStrategy):
            Defines the maintenance strategy, such as scheduled or on-demand.
        Thresholds (ConfigIntelligentMaintenance):
            Holds threshold configurations for intelligent maintenance, if applicable.
        Interval (str):
            Time interval or schedule for the maintenance process, defaults to "AUTO".
    """

    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    TrackHistory:Optional[bool] = Field(alias="track_history", default=False)
    RetentionHours:Optional[int] = Field(alias="retention_hours", default=168)
    Strategy:Optional[MaintenanceStrategy] = Field(alias="strategy", default=MaintenanceStrategy.SCHEDULED)
    Thresholds:Optional[ConfigIntelligentMaintenance] = Field(alias="thresholds", default=ConfigIntelligentMaintenance())
    Interval:Optional[str] = Field(alias="interval", default="AUTO")

class ConfigTableMaintenance(SyncBaseModel):
    """
    ConfigTableMaintenance manages the configuration settings for table maintenance.
    Attributes:
        Enabled (bool): 
            Indicates whether periodic maintenance is enabled.
        Interval (MaintenanceInterval): 
            Defines how often maintenance should occur, with the default set to AUTO.
    """

    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    Interval:Optional[MaintenanceInterval] = Field(alias="interval", default=MaintenanceInterval.AUTO)
                
class ConfigAsync(SyncBaseModel):
    """
    The ConfigAsync class provides configuration settings for asynchronous operations.
    Attributes:
        Enabled (bool): Flag indicating if asynchronous operations are enabled (default: True).
        Parallelism (int): Number of concurrent operations allowed (default: 10).
    """

    Enabled:Optional[bool] = Field(alias="enabled", default=True)
    Parallelism:Optional[int] = Field(alias="parallelism", default=10)

class ConfigLakehouseTarget(SyncBaseModel):
    """
    ConfigLakehouseTarget serves as a data model for specifying lakehouse-related
    target configurations when synchronizing data. Each field corresponds to a
    relevant configuration parameter for defining or identifying a specific lakehouse 
    destination within a synchronization process.
    Attributes:
        LakehouseID (str): Identifier of the target lakehouse. Maps to 'lakehouse_id'.
        Lakehouse (str): The name of the lakehouse resource. Maps to 'lakehouse'.
        Schema (str): The schema name within the lakehouse. Maps to 'schema'.
        Table (str): The name of the target table. Maps to 'table_name'.
        PartitionBy (str): Optional partition strategy for the target table. Maps to 'partition_by'.
    """

    LakehouseID:Optional[str] = Field(alias="lakehouse_id", default=None)
    Lakehouse:Optional[str] = Field(alias="lakehouse", default=None)
    Schema:Optional[str] = Field(alias="schema", default=None)
    Table:Optional[str] = Field(alias="table_name", default=None)
    PartitionBy:Optional[str] = Field(alias="partition_by", default=None)
                
class ConfigPartition(SyncBaseModel):
    """
    ConfigPartition represents the configuration for partitioning in BigQuery.
    Attributes:
        Enabled (bool): Indicates whether the partitioning is enabled.
        PartitionType (BQPartitionType): Specifies the type of partitioning to use.
        PartitionColumn (str): The name of the column used for partitioning.
        Granularity (CalendarInterval): The partition granularity (e.g., DAILY, MONTHLY).
        PartitionDataType (BQDataType): The data type of the partitioned column.
        PartitionRange (str): An optional range for partitioning.
    """

    Enabled:Optional[bool] = Field(alias="enabled", default=None)
    PartitionType:Optional[BQPartitionType] = Field(alias="type", default=None)
    PartitionColumn:Optional[str] = Field(alias="column", default=None)
    Granularity:Optional[CalendarInterval] = Field(alias="partition_grain", default=None)
    PartitionDataType:Optional[BQDataType] = Field(alias="partition_data_type", default=None)
    PartitionRange:Optional[str] = Field(alias="partition_range", default=None)

class ConfigFabric(SyncBaseModel):
    """
    ConfigFabric is a configuration class that holds metadata and destination settings
    for syncing operations in a Fabric environment. It includes both static information
    (e.g., workspace details) and dynamic options (e.g., whether to enable schema-based
    namespaces). The class methods build string paths and namespaces for use in various
    sync operations.
    Attributes:
        WorkspaceID (str, optional): The unique identifier of the workspace.
        WorkspaceName (str, optional): The display name of the workspace.
        MetadataLakehouse (str, optional): The name of the lakehouse that holds metadata.
        MetadataSchema (str, optional): The default schema for metadata tables.
        MetadataLakehouseID (str, optional): The unique identifier of the metadata lakehouse.
        TargetType (FabricDestinationType, optional): The target destination type for sync.
        TargetLakehouse (str, optional): The name of the target lakehouse for sync.
        TargetLakehouseID (str, optional): The unique identifier of the target lakehouse.
        TargetLakehouseSchema (str, optional): The schema name used within the target lakehouse.
        EnableSchemas (bool, optional): Flag indicating whether schema usage is enabled.
    Methods:
        get_metadata_namespace():
            Returns a string path including or excluding the schema, based on EnableSchemas,
            for referencing the metadata tables in the workspace.
        get_metadata_lakehouse():
            Returns the metadata lakehouse string including or excluding the schema, based
            on the EnableSchemas flag.
        get_target_namespace():
            Returns the namespace path for target tables, taking into account the workspace
            name, target lakehouse name, and schema usage settings.
        get_target_lakehouse():
            Returns the target lakehouse reference, including the schema if schemas are enabled.
        get_target_table_path(table):
            Builds a fully qualified path (including lakehouse and schema if enabled) for a
            specific table in the target.
        get_metadata_table_path(table):
            Builds a fully qualified path (including lakehouse and schema if enabled) for a
            specific metadata table.
    """

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
        """
        Returns the full metadata namespace for the current workspace.
        The returned namespace includes the workspace name, the metadata lakehouse,
        and optionally appends the .dbo schema segment if schemas are enabled.
        Returns:
            str: The formatted metadata namespace.
        """

        if self.EnableSchemas:
            return f"`{self.WorkspaceName}`.{self.MetadataLakehouse}.dbo"
        else:
            return f"`{self.WorkspaceName}`.{self.MetadataLakehouse}"

    def get_metadata_lakehouse(self) -> str:
        """
        Returns the metadata lakehouse path as a string.
        If schemas are enabled (EnableSchemas is True), appends '.dbo' to the lakehouse name.
        Otherwise, returns the lakehouse name unchanged.
        Returns:
            str: The constructed metadata lakehouse path.
        """

        if self.EnableSchemas:
            return f"{self.MetadataLakehouse}.dbo"
        else:
            return f"{self.MetadataLakehouse}"
    
    def get_target_namespace(self) -> str:
        """
        Retrieves the fully qualified target namespace string.
        If EnableSchemas is True, the returned string includes the WorkspaceName,
        TargetLakehouse, and TargetLakehouseSchema in dotted format with backticks.
        If EnableSchemas is False, schema information is omitted.
        Returns:
            str: The formatted target namespace string.
        """

        if self.EnableSchemas:
            return f"`{self.WorkspaceName}`.{self.TargetLakehouse}.{self.TargetLakehouseSchema}"
        else:
            return f"`{self.WorkspaceName}`.{self.TargetLakehouse}"

    def get_target_lakehouse(self) -> str:
        """
        Returns the target lakehouse reference, including the schema if schemas are enabled.
        If EnableSchemas is True, the schema is appended to the lakehouse name.
        Otherwise, it returns just the lakehouse name.
        Returns:
            str: The target lakehouse reference, potentially including the schema.
        """
        if self.EnableSchemas:
            return f"{self.TargetLakehouse}.{self.TargetLakehouseSchema}"
        else:
            return f"{self.TargetLakehouse}"
    
    def get_target_table_path(self, table:str) -> str:
        """
        Constructs the fully qualified path for a target table.
        If EnableSchemas is True, the path includes the TargetLakehouse and TargetLakehouseSchema.
        Otherwise, it returns just the table name.
        Args:
            table (str): The name of the table for which to construct the path.
        Returns:
            str: The fully qualified path for the target table.
        """
        if self.EnableSchemas:
            return f"{self.TargetLakehouse}.{self.TargetLakehouseSchema}.{table}"
        else:
            return table

    def get_metadata_table_path(self, table:str) -> str:
        """
        Constructs the fully qualified path for a metadata table.
        If EnableSchemas is True, the path includes the MetadataLakehouse and MetadataSchema.
        Otherwise, it returns just the table name.
        Args:
            table (str): The name of the metadata table for which to construct the path.
        Returns:
            str: The fully qualified path for the metadata table.
        """
        if self.EnableSchemas:
            return f"{self.MetadataLakehouse}.dbo.{table}"
        else:
            return table

class ConfigBQDataset(SyncBaseModel):
    """
    ConfigDataset is a configuration model for datasets in BigQuery.
    It includes fields for specifying the dataset name and location.
    Attributes:
        Dataset (Optional[str]): The name of the dataset in BigQuery.
        Location (Optional[str]): The location of the dataset in BigQuery.
    """
    Dataset:Optional[str] = Field(alias="dataset_id", default=None)
    Location:Optional[str] = Field(alias="location", default=None)

class ConfigGCPDataset(ConfigBQDataset):
    """
    ConfigGCPDataset is a configuration model for Google Cloud Platform (GCP) datasets.
    It extends the ConfigDataset model to include additional fields specific to GCP datasets.
    Attributes:
        Dataset (Optional[str]): The name of the dataset in GCP.
        Location (Optional[str]): The location of the dataset in GCP.
        MaterializationDataset (Optional[ConfigDataset]):
            The dataset used for materialization in GCP, if applicable.
    """
    MaterializationDataset:Optional[ConfigBQDataset] = Field(alias="materialization_dataset", default=ConfigBQDataset())

class ConfigDefaultMaterialization(SyncBaseModel):
    """
    ConfigDefaultMaterialization is a configuration model for default materialization settings in BigQuery.
    It includes fields for specifying the dataset, location, and other materialization options.
    Attributes:
        ProjectID (Optional[str]): The GCP Project ID for the materialization.
        Dataset (Optional[ConfigDataset]):
            The dataset configuration for materialization, including dataset name and location.
    """
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Dataset:Optional[ConfigBQDataset] = Field(alias="dataset", default=ConfigBQDataset())

class ConfigGCPProject(SyncBaseModel):
    """
    ConfigGCPProject is a configuration model for Google Cloud Platform (GCP) projects.
    It includes fields for specifying the project ID and a list of datasets associated with the project.
    Attributes:
        ProjectID (Optional[str]): The unique identifier for the GCP project.
        Datasets (Optional[List[ConfigGCPDataset]]): A list of datasets associated with the project.
    """
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    Datasets:Optional[List[ConfigGCPDataset]] = Field(alias="datasets", default=[ConfigGCPDataset()])

class ConfigGCPCredential(SyncBaseModel):
    """
    ConfigGCPCredential is a configuration model for Google Cloud Platform (GCP) credentials.
    It includes fields for specifying the credential path, access token, and other related parameters.
    Attributes:
        CredentialPath (Optional[str]): The file path to the GCP credentials JSON file.
        AccessToken (Optional[str]): The access token for GCP authentication.
        Credential (Optional[str]): The GCP credential string, if provided.
        CredentialSecretKeyVault (Optional[str]): The secret key vault for storing GCP credentials.
        CredentialSecretKey (Optional[str]): The secret key for accessing the GCP credentials in the vault.
    """
    CredentialPath:Optional[str] = Field(alias="credential_path", default=None)
    AccessToken:Optional[str] = Field(alias="access_token", default=None)
    Credential:Optional[str] = Field(alias="credential", default=None)
    CredentialSecretKeyVault:Optional[str] = Field(alias="credential_secret_key_vault", default=None)
    CredentialSecretKey:Optional[str] = Field(alias="credential_secret_key", default=None)

class ConfigGCPAPI(SyncBaseModel):
    """
    ConfigGCPAPI is a configuration model for Google Cloud Platform (GCP) API settings.
    It includes fields for specifying the API endpoint, whether to use the standard API, and options for BigQuery export.
    Attributes:
        UseStandardAPI (Optional[bool]): Flag indicating whether to use the standard API (default: False).
        EnableBigQueryExport (Optional[bool]): Flag indicating whether to enable BigQuery export (default: False).
        ForceBQJobConfig (Optional[bool]): Flag indicating whether to force BigQuery job configuration (default: False).
        AutoSelect (Optional[bool]): Flag indicating whether to automatically select the API (default: False).
        UseCDC (Optional[bool]): Flag indicating whether to use Change Data Capture (CDC) (default: True).
        DefaultMaterialization (Optional[ConfigDefaultMaterialization]): The default materialization configuration for datasets.
        BillingProjectID (Optional[str]): The project ID for billing purposes, if applicable.
    """
    UseStandardAPI:Optional[bool] = Field(alias="use_standard_api", default=False)
    EnableBigQueryExport:Optional[bool] = Field(alias="enable_bigquery_export", default=False)
    ForceBQJobConfig:Optional[bool] = Field(alias="force_bq_job_config", default=False)
    AutoSelect:Optional[bool] = Field(alias="auto_select", default=False)
    UseCDC:Optional[bool] = Field(alias="use_cdc", default=True)
    DefaultMaterialization:Optional[ConfigDefaultMaterialization] = Field(alias="materialization_default", default=ConfigDefaultMaterialization())
    BillingProjectID:Optional[str] = Field(alias="billing_project_id", default=None)

class ConfigGCPStorage(SyncBaseModel):
    """
    ConfigGCPStorage is a configuration model for Google Cloud Platform (GCP) storage settings.
    It includes fields for specifying the bucket URI, prefix path, and whether cleanup is enabled.
    Attributes:
        ProjectID (Optional[str]): The GCP Project ID of the GCP storage bucket.
        BucketUri (Optional[str]): The URI of the GCP storage bucket.
        PrefixPath (Optional[str]): The prefix path within the bucket for storing data.
        EnabledCleanUp (Optional[bool]): Flag indicating whether to enable cleanup of old data (default: True).
    """
    ProjectID:Optional[str] = Field(alias="project_id", default=None)
    BucketUri:Optional[str] = Field(alias="bucket_uri", default=None)
    PrefixPath:Optional[str] = Field(alias="prefix_path", default=None)
    EnabledCleanUp:Optional[bool] = Field(alias="enable_cleanup", default=True)

class ConfigGCP(SyncBaseModel):
    """
    ConfigGCP is a configuration model for Google Cloud Platform (GCP) settings.
    It includes fields for specifying API settings, projects, credentials, and storage configurations.
    Attributes:
        API (Optional[ConfigGCPAPI]): The API configuration for GCP services.
        Projects (List[ConfigGCPProject]): A list of GCP projects, each with its own ID and datasets.
        GCPCredential (ConfigGCPCredential): The GCP credential configuration for authentication.
        GCPStorage (ConfigGCPStorage): The GCP storage configuration for managing data storage.
    Methods:
        DefaultProjectID (str): Returns the project ID of the first project in the Projects list.
        DefaultDataset (str): Returns the dataset name of the first dataset in the first project.
        get_project_config(project_id:str) -> ConfigGCPProject: Retrieves the configuration for a specific GCP project by its project ID.
        get_dataset_config(project_id:str, dataset:str) -> ConfigGCPDataset: Retrieves the configuration for a specific dataset within a GCP project.
        format_table_path(project_id:str, dataset:str, table_name:str) -> str: Formats the table path for a given project, dataset, and table name.
        resolve_dataset_path(project_id:str, dataset:str) -> tuple[str,str,str]: Resolves the dataset path for a given project and dataset.
    """
    API:Optional[ConfigGCPAPI] = Field(alias="api", default=ConfigGCPAPI())
    Projects:List[ConfigGCPProject] = Field(alias="projects", default=[ConfigGCPProject()])
    GCPCredential:ConfigGCPCredential = Field(alias="gcp_credentials", default=ConfigGCPCredential())
    Storage:ConfigGCPStorage = Field(alias="gcp_storage", default=ConfigGCPStorage())

    @property
    def DefaultProjectID(self) -> str:
        """
        Returns the default project ID from the first project in the Projects list.
        If no projects are defined, it returns None.
        Returns:
            str: The project ID of the first project in the Projects list, or None if no projects exist.
        """
        if self.Projects:
            return self.Projects[0].ProjectID
        
        return None
    
    @property
    def DefaultDataset(self) -> str:
        """
        Returns the default dataset from the first dataset in the first project of the Projects list.
        If no projects or datasets are defined, it returns None.
        Returns:
            str: The dataset name of the first dataset in the first project, or None if no datasets exist.
        """
        if self.Projects:
            if self.Projects[0].Datasets:
                return self.Projects[0].Datasets[0].Dataset
        
        return None
    
    def get_project_config(self, project_id:str) -> ConfigGCPProject:
        """
        Retrieves the configuration for a specific GCP project by its project ID.
        Args:
            project_id (str): The ID of the GCP project to retrieve.
        Returns:
            ConfigGCPProject: The configuration object for the specified project, or None if not found.
        """
        return next((p for p in self.Projects if p.ProjectID == project_id), None)

    def get_dataset_config(self, project_id:str, dataset:str) -> ConfigGCPDataset:
        """
        Retrieves the configuration for a specific dataset within a GCP project.
        Args:
            project_id (str): The ID of the GCP project containing the dataset.
            dataset (str): The name of the dataset to retrieve.
        Returns:
            ConfigGCPDataset: The configuration object for the specified dataset, or None if not found.
        """
        project = self.get_project_config(project_id)
        if project:
            return next((d for d in project.Datasets if d.Dataset == dataset), None)
        
        return None
    
    def format_table_path(self, project_id:str, dataset:str, table_name:str) -> str:
        """
        Formats the table path for a given project, dataset, and table name.
        Args:
            project_id (str): The ID of the GCP project.
            dataset (str): The name of the dataset containing the table.
            table_name (str): The name of the table to format.
        Returns:
            str: The formatted table path, including the project ID, dataset, and table name.
        """
        return f"{project_id}.{dataset}.{table_name}"

    def get_dataset_location(self, project_id:str, dataset:str) -> str:
        """
        Retrieves the location of a dataset within a GCP project.
        Args:
            project_id (str): The ID of the GCP project.
            dataset (str): The name of the dataset for which to retrieve the location.
        Returns:
            str: The location of the dataset, or None if no location is specified.
        """
        p, l, d = self.resolve_dataset_path(project_id, dataset)
        return l

    def resolve_materialization_path(self, project_id:str, dataset:str) -> tuple[str,str, str]:
        """
        Resolves the materialization path for a given project and dataset.
        Args:
            project_id (str): The ID of the GCP project.
            dataset (str): The name of the dataset for which to resolve the materialization path.
        Returns:
            tuple[str, str, str]: A tuple containing the project ID, dataset location, and dataset name.
            If the dataset is not found, it returns None for the location and dataset name.
        """
        config_datatset = self.get_dataset_config(project_id, dataset)

        if config_datatset.MaterializationDataset and config_datatset.MaterializationDataset.Dataset:
            return (project_id, config_datatset.MaterializationDataset.Location, config_datatset.MaterializationDataset.Dataset)
        
        if self.API.DefaultMaterialization.Dataset.Dataset:
            return (self.API.DefaultMaterialization.ProjectID, 
                    self.API.DefaultMaterialization.Dataset.Location, 
                    self.API.DefaultMaterialization.Dataset.Dataset)

        return (None, None, None)

    def resolve_dataset_path(self, project_id:str, dataset:str) -> tuple[str,str,str]:
        """
        Resolves the dataset path for a given project and dataset.
        Args:
            project_id (str): The ID of the GCP project.
            dataset (str): The name of the dataset to resolve.
        Returns:
            tuple[str, str, str]: A tuple containing the project ID, dataset location, and dataset name.
            If the dataset is not found, it returns None for the location and dataset name.
        """
        config_datatset = self.get_dataset_config(project_id, dataset)
        
        return (project_id, config_datatset.Location, config_datatset.Dataset)

class ConfigTableColumn(SyncBaseModel):
    """
    ConfigTableColumn represents a column configuration for a table in BigQuery.
    It includes the column name and other optional attributes.
    Attributes:
        Column (Optional[str]): An alias for the column name, if applicable.
    """
    Column:Optional[str] = Field(alias="column", default=None)

class TypedColumn(SyncBaseModel):
    """
    TypedColumn represents a column with its name and type in BigQuery.
    It is used to define the structure of a column in a table, including its name and data type.
    Attributes:
        Name (Optional[str]): The name of the column.
        Type (Optional[str]): The data type of the column, such as STRING, INTEGER, etc.
    """
    Name:Optional[str] = Field(alias="name", default=None)
    Type:Optional[str] = Field(alias="type", default=None)

class MappedColumn(SyncBaseModel):
    """
    MappedColumn represents a mapping between a source column and a destination column in BigQuery.
    It includes the source and destination columns, the format for the mapping, and whether to drop the source column after mapping.
    Attributes:
        Source (Optional[TypedColumn]): The source column being mapped, including its name and type.
        Destination (Optional[TypedColumn]): The destination column where the source column is mapped, including its name and type.
        Format (Optional[str]): The format string used for the mapping, if applicable.
        DropSource (Optional[bool]): A flag indicating whether to drop the source column after mapping (default: None).
    Properties:
        IsTypeConversion (bool): Indicates if the source and destination columns have different types.
        IsRename (bool): Indicates if the source and destination columns have different names.
    """
    Source:Optional[TypedColumn] = Field(alias="source", default=None)
    Destination:Optional[TypedColumn] = Field(alias="destination", default=None)
    Format:Optional[str] = Field(alias="format", default=None)
    DropSource:Optional[bool] = Field(alias="drop_source", default=None)

    @property
    def IsTypeConversion(self) -> bool:
        """
        Checks if the source and destination columns have different types.
        Returns:
            bool: True if the source and destination types are different, otherwise False.
        """
        return self.Source.Type != self.Destination.Type
    
    @property
    def IsRename(self) -> bool:
        """
        Checks if the source and destination columns have different names.
        Returns:
            bool: True if the source and destination names are different, otherwise False.
        """
        return self.Source.Name != self.Destination.Name

class ConfigBQTableDefault (SyncBaseModel):
    """
    ConfigBQTableDefault is a configuration model for default settings applied to BigQuery tables.
    It includes fields for specifying the project ID, dataset, object type, load strategy, load type,
    sync interval, and various table properties such as priority, enabled status, and schema evolution options.
    Attributes:
        ProjectID (Optional[str]): The ID of the GCP project where the table resides.
        Dataset (Optional[str]): The name of the dataset containing the table.
        ObjectType (Optional[BigQueryObjectType]): The type of object (e.g., TABLE, VIEW) in BigQuery.
        Priority (Optional[int]): The priority level for loading data into the table, default is 100.
        LoadStrategy (Optional[SyncLoadStrategy]): The strategy for loading data into the table.
        LoadType (Optional[SyncLoadType]): The type of load operation to perform on the table.
        Interval (Optional[SyncScheduleType]): The synchronization interval for the table, default is AUTO.
        Enabled (Optional[bool]): Indicates whether the table is enabled for synchronization, default is True.
        EnforceExpiration (Optional[bool]): Whether to enforce data expiration policies on the table, default is False.
        AllowSchemaEvolution (Optional[bool]): Whether to allow schema evolution during data loads, default is False.
        FlattenTable (Optional[bool]): Whether to flatten nested structures in the table, default is False.
        FlattenInPlace (Optional[bool]): Whether to flatten nested structures in place, default is True.
        ExplodeArrays (Optional[bool]): Whether to explode array fields in the table, default is True.
        UseBigQueryExport (Optional[bool]): Whether to use BigQuery export features, default is False.
        UseStandardAPI (Optional[bool]): Whether to use BigQuery Standard API, default is False.
        TableMaintenance (Optional[ConfigTableMaintenance]): Configuration for table maintenance operations.
    """
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
    UseBigQueryExport:Optional[bool] = Field(alias="use_bigquery_export", default=False)
    UseStandardAPI:Optional[bool] = Field(alias="use_standard_api", default=False)
    TableMaintenance:Optional[ConfigTableMaintenance] = Field(alias="table_maintenance", default=ConfigTableMaintenance())

class ConfigBQTable (SyncBaseModel):
    """
    ConfigBQTable is a configuration model for BigQuery tables used in synchronization processes.
    Attributes:
        ProjectID (Optional[str]): The ID of the GCP project where the table resides.
        Dataset (Optional[str]): The name of the dataset containing the table.
        ObjectType (Optional[BigQueryObjectType]): The type of object in BigQuery (e.g., TABLE, VIEW).
        Priority (Optional[int]): The priority level for loading data into the table, default is None.
        LoadStrategy (Optional[SyncLoadStrategy]): The strategy for loading data into the table.
        LoadType (Optional[SyncLoadType]): The type of load operation to perform on the table.
        Interval (Optional[SyncScheduleType]): The synchronization interval for the table, default is None.
        Enabled (Optional[bool]): Indicates whether the table is enabled for synchronization, default is None.
        EnforceExpiration (Optional[bool]): Whether to enforce data expiration policies on the table, default is None.
        AllowSchemaEvolution (Optional[bool]): Whether to allow schema evolution during data loads, default is None.
        FlattenTable (Optional[bool]): Whether to flatten nested structures in the table, default is None.
        FlattenInPlace (Optional[bool]): Whether to flatten nested structures in place, default is None.
        ExplodeArrays (Optional[bool]): Whether to explode array fields in the table, default is None.
        UseBigQueryExport (Optional[bool]): Whether to use BigQuery export features, default is False.
        UseStandardAPI (Optional[bool]): Whether to use BigQuery Standard API, default is False.
        TableMaintenance (Optional[ConfigTableMaintenance]): Configuration for table maintenance operations.
        TableName (Optional[str]): The name of the BigQuery table.
        SourceQuery (Optional[str]): The SQL query used to populate the table.
        Predicate (Optional[str]): An optional predicate for filtering data in the table.
        ColumnMap (Optional[List[MappedColumn]]): A list of mapped columns defining the source and destination mappings.
        LakehouseTarget (Optional[ConfigLakehouseTarget]): Configuration for the lakehouse target.
        BQPartition (Optional[ConfigPartition]): Configuration for BigQuery partitioning.
        Keys (Optional[List[ConfigTableColumn]]): A list of keys for the table, used for identifying unique records.
        Watermark (Optional[ConfigTableColumn]): A column used for watermarking, typically for incremental loads. 
    Methods:
        BQ_FQName (str): Returns the fully qualified name of the BigQuery table in the format "ProjectID.Dataset.TableName".
        get_table_keys() -> list[str]: Returns a list of column names that are defined as keys for the table.  
    """
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
    UseBigQueryExport:Optional[bool] = Field(alias="use_bigquery_export", default=False)
    UseStandardAPI:Optional[bool] = Field(alias="use_standard_api", default=False)
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
        """
        Returns the fully qualified name of the BigQuery table in the format "ProjectID.Dataset.TableName".
        This property constructs the fully qualified name using the ProjectID, Dataset, and TableName attributes.
        Returns:
            str: The fully qualified name of the BigQuery table.
        """
        return f"{self.ProjectID}.{self.Dataset}.{self.TableName}"

    def get_table_keys(self) -> list[str]:
        """
        Returns a list of column names that are defined as keys for the table.
        If no keys are defined, it returns an empty list.
        Returns:
            list[str]: A list of column names that are keys for the table.
        """
        keys = []

        if self.Keys:
            keys = [k.Column for k in self.Keys]
        
        return keys

class ConfigStandardAPIExportConfig(SyncBaseModel):
    """
    ConfigStandardAPIExportConfig is a configuration model for standard API export settings in BigQuery synchronization.
    It includes options for result partitions and page size, which control how data is exported from BigQuery.
    Attributes:
        ResultPartitions (Optional[int]): The number of partitions to use for the result set, default is 1.
        PageSize (Optional[int]): The size of each page of results, default is 100000.
    """
    ResultPartitions:Optional[int] = Field(alias="result_partitions", default=1)
    PageSize:Optional[int] = Field(alias="page_size", default=100000)

class ConfigOptimization(SyncBaseModel):
    """
    ConfigOptimization is a configuration model for optimization settings in BigQuery synchronization.
    It includes options for using approximate row counts, disabling the dataframe cache, and other optimization parameters.
    Attributes:
        UseApproximateRowCounts (Optional[bool]): Flag indicating whether to use approximate row counts for optimization (default: True).
        DisableDataframeCache (Optional[bool]): Flag indicating whether to disable the dataframe cache (default: False).
    """
    UseApproximateRowCounts:Optional[bool] = Field(alias="use_approximate_row_counts", default=True)
    DisableDataframeCache:Optional[bool] = Field(alias="disable_dataframe_cache", default=False)
    StandardAPIExport:Optional[ConfigStandardAPIExportConfig] = Field(alias="standard_api_export", default=ConfigStandardAPIExportConfig())

class ConfigObjectFilter(SyncBaseModel):
    """
    ConfigObjectFilter is a configuration model for filtering objects in BigQuery synchronization.
    It allows specifying patterns and types for filtering tables, views, and materialized views.
    Attributes:
        pattern (Optional[str]): A regex pattern to filter objects by name.
        type (Optional[ObjectFilterType]): The type of object to filter (e.g., TABLE, VIEW, MATERIALIZED_VIEW).
    """
    pattern:Optional[str] = Field(alias="pattern", default=None)
    type:Optional[ObjectFilterType] = Field(alias="type", default=None)

class ConfigDiscoverObject(SyncBaseModel):
    """
    ConfigDiscoverObject is a configuration model for discovering objects in BigQuery synchronization.
    It includes options for enabling discovery, loading all objects, and applying filters.
    Attributes:
        Enabled (Optional[bool]): Flag indicating whether discovery is enabled for the object (default: False).
        LoadAll (Optional[bool]): Flag indicating whether to load all objects of this type (default: False).
        Filter (Optional[ConfigObjectFilter]): An optional filter to apply when discovering objects.
    """
    Enabled:Optional[bool] = Field(alias="enabled", default=False)
    LoadAll:Optional[bool] = Field(alias="load_all", default=False) 
    Filter:Optional[ConfigObjectFilter] = Field(alias="filter", default=None) 

class ConfigAutodiscover(SyncBaseModel):
    """
    ConfigAutodiscover is a configuration model for automatic discovery of BigQuery objects.
    It includes options for enabling autodiscovery, loading all objects, and configuring tables, views, and materialized views.
    Attributes:
        Autodetect (Optional[bool]): Flag indicating whether to automatically detect objects (default: True).
        Tables (Optional[ConfigDiscoverObject]): Configuration for discovering tables.
        Views (Optional[ConfigDiscoverObject]): Configuration for discovering views.
        MaterializedViews (Optional[ConfigDiscoverObject]): Configuration for discovering materialized views.
    """
    Autodetect:Optional[bool] = Field(alias="autodetect", default=True)
    Tables:Optional[ConfigDiscoverObject] = Field(alias="tables", default=ConfigDiscoverObject())
    Views:Optional[ConfigDiscoverObject] = Field(alias="views", default=ConfigDiscoverObject())
    MaterializedViews:Optional[ConfigDiscoverObject] = Field(alias="materialized_views", default=ConfigDiscoverObject())

class ConfigDataset(SyncBaseModel):
    """
    ConfigDataset is a configuration model for managing dataset synchronization settings in a Fabric environment.
    It includes metadata about the dataset, synchronization options, and configurations for various components such as GCP, logging, and fabric settings.
    Attributes:
        ApplicationID (Optional[str]): The correlation ID for the application, used for tracking and logging purposes.
        ID (Optional[str]): The unique identifier for the dataset configuration, default is "FABRIC_SYNC_LOADER".
        Version (Optional[str]): The version of the dataset configuration.
        EnableDataExpiration (Optional[bool]): Flag indicating whether data expiration is enabled, default is False.
        Optimization (Optional[ConfigOptimization]): Configuration for optimization settings.
        Maintenance (Optional[ConfigMaintenance]): Configuration for maintenance settings.
        AutoDiscover (Optional[ConfigAutodiscover]): Configuration for automatic discovery of objects in BigQuery.
        Logging (Optional[ConfigLogging]): Configuration for logging settings.
        Fabric (Optional[ConfigFabric]): Configuration for fabric-related settings.
        GCP (Optional[ConfigGCP]): Configuration for Google Cloud Platform settings.
        Async (Optional[ConfigAsync]): Configuration for asynchronous operations.
        TableDefaults (Optional[ConfigBQTableDefault]): Default settings applied to BigQuery tables.
        Tables (Optional[List[ConfigBQTable]]): A list of configurations for individual BigQuery tables.
    Methods:
        Autodetect (bool): Returns whether automatic detection of objects is enabled in the autodiscovery configuration.
        LoadAllTables (bool): Returns whether all tables should be loaded based on the autodiscovery configuration.
        EnableTables (bool): Returns whether table discovery is enabled based on the autodiscovery configuration.
        LoadAllViews (bool): Returns whether all views should be loaded based on the autodiscovery configuration.
        EnableViews (bool): Returns whether view discovery is enabled based on the autodiscovery configuration.
        LoadAllMaterializedViews (bool): Returns whether all materialized views should be loaded based on the autodiscovery configuration.
        EnableMaterializedViews (bool): Returns whether materialized view discovery is enabled based on the autodiscovery configuration.
        get_table_config(bq_table:str) -> ConfigBQTable: Returns the configuration for a specific BigQuery table based on its fully qualified name.
        get_table_name_list(project:str, dataset:str, obj_type:BigQueryObjectType, only_enabled:bool = False) -> list[str]:
            Returns a list of table names based on the specified project, dataset, and object type.
    """
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
        """
        Returns whether automatic detection of objects is enabled in the autodiscovery configuration.
        This property checks the Autodetect field in the AutoDiscover configuration.
        Returns:
            bool: True if autodetection is enabled, otherwise False.
        """
        return self.AutoDiscover.Autodetect
    
    @property
    def LoadAllTables(self) -> bool:
        """
        Returns whether all tables should be loaded based on the autodiscovery configuration.
        This property checks the LoadAll field in the Tables configuration of AutoDiscover.
        Returns:
            bool: True if all tables should be loaded, otherwise False.
        """
        return self.AutoDiscover.Tables.LoadAll
    
    @property
    def EnableTables(self) -> bool:
        """
        Returns whether table discovery is enabled based on the autodiscovery configuration.
        This property checks the Enabled field in the Tables configuration of AutoDiscover.
        Returns:
            bool: True if table discovery is enabled, otherwise False.
        """
        return self.AutoDiscover.Tables.Enabled
    
    @property
    def LoadAllViews(self) -> bool:
        """
        Returns whether all views should be loaded based on the autodiscovery configuration.
        This property checks the LoadAll field in the Views configuration of AutoDiscover.
        Returns:
            bool: True if all views should be loaded, otherwise False.
        """
        return self.AutoDiscover.Views.LoadAll
    
    @property
    def EnableViews(self) -> bool:
        """
        Returns whether view discovery is enabled based on the autodiscovery configuration.
        This property checks the Enabled field in the Views configuration of AutoDiscover.
        Returns:
            bool: True if view discovery is enabled, otherwise False.
        """
        return self.AutoDiscover.Views.Enabled
    
    @property
    def LoadAllMaterializedViews(self) -> bool:
        """
        Returns whether all materialized views should be loaded based on the autodiscovery configuration.
        This property checks the LoadAll field in the MaterializedViews configuration of AutoDiscover.
        Returns:
            bool: True if all materialized views should be loaded, otherwise False.
        """
        return self.AutoDiscover.MaterializedViews.LoadAll
    
    @property
    def EnableMaterializedViews(self) -> bool:
        """
        Returns whether materialized view discovery is enabled based on the autodiscovery configuration.
        This property checks the Enabled field in the MaterializedViews configuration of AutoDiscover.
        Returns:
            bool: True if materialized view discovery is enabled, otherwise False.
        """
        return self.AutoDiscover.MaterializedViews.Enabled

    def get_table_config(self, bq_table:str) -> ConfigBQTable:
        """
        Returns the configuration for a specific BigQuery table based on its fully qualified name.
        If the table is not found in the configuration, it returns None.
        Args:
            bq_table (str): The fully qualified name of the BigQuery table in the format "ProjectID.Dataset.TableName".
        Returns:
            ConfigBQTable: The configuration for the specified BigQuery table, or None if not found.
        """
        if self.Tables:
            return next((t for t in self.Tables if t.BQ_FQName == bq_table), None)
        else:
            return None

    def get_table_name_list(self, project:str, dataset:str, obj_type:BigQueryObjectType, only_enabled:bool = False) -> list[str]:
        """
        Returns a list of table names based on the specified project, dataset, and object type.
        If only_enabled is True, it filters the tables to include only those that are enabled.
        Args:
            project (str): The ID of the GCP project.
            dataset (str): The name of the dataset within the project.
            obj_type (BigQueryObjectType): The type of object to filter by (e.g., TABLE, VIEW).
        Returns:
            list[str]: A list of table names that match the specified criteria.
        """
        tables = [t for t in self.Tables if t.ProjectID == project and t.Dataset == dataset and t.ObjectType == obj_type]

        if only_enabled:
            tables = [t for t in tables if t.Enabled == True]

        return [str(x.TableName) for x in tables]
    
    def __init__(self, **kwargs) -> None:
        """
        Initializes the ConfigDataset instance with the provided keyword arguments.
        This constructor allows for dynamic configuration of the dataset settings.
        Args:
            **kwargs: Arbitrary keyword arguments to set the attributes of the ConfigDataset instance.
        """
        super().__init__(**kwargs)

    def apply_defaults(self) -> None:
        """
        Applies default values to the ConfigDataset instance based on the provided configuration.
        This method sets default values for various fields, including the GCP project and dataset,
        and ensures that the TargetLakehouseSchema is set if EnableSchemas is True.
        It also applies default settings to each table in the Tables list based on the TableDefaults configuration.
        Returns:
            None
        """
        if self.Fabric.EnableSchemas:
            if not self.Fabric.TargetLakehouseSchema:
                self.Fabric.TargetLakehouseSchema = self.GCP.DefaultDataset

        self.__apply_table_defaults()


    def __apply_table_defaults(self) -> None:
        """
        Applies default values to each table in the Tables list based on the TableDefaults configuration.
        This method iterates through each table and sets any fields that are not already set
        to the corresponding default values from TableDefaults.
        It ensures that each table has the necessary default settings applied, such as project ID, dataset, and other properties.
        Returns:
            None
        """
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
        """
        Reads a JSON configuration file from the specified path and returns an instance of ConfigDataset.
        If with_defaults is True, it applies default values to the configuration.
        Args:
            path (str): The file path to the JSON configuration file.
            with_defaults (bool): Whether to apply default values to the configuration (default: True).
        Returns:
            ConfigDataset: An instance of ConfigDataset populated with the data from the JSON file.
        Raises:
            Exception: If the configuration file is not found or cannot be read.
        """
        data = cls.__read_json_config(path)

        config = ConfigDataset(**data)      
         
        if with_defaults:     
            config.apply_defaults()

        return config

    @classmethod
    def __read_json_config(self, path) -> 'ConfigDataset':
        """
        Reads a JSON configuration file from the specified path and returns the parsed data.
        Args:
            path (str): The file path to the JSON configuration file.
        Returns:
            dict: The parsed JSON data as a dictionary.
        Raises:
            Exception: If the configuration file does not exist or cannot be read.
        """
        if os.path.exists(path):
            with open(path, 'r', encoding="utf-8") as f:
                data = json.load(f)
            return data
        else:
            raise Exception(f"Configuration file not found ({path})")
    
    def to_json(self, path:str, backup:bool=False) -> None:
        """
        Saves the current configuration to a JSON file at the specified path.
        If backup is True, it creates a backup of the existing configuration file before saving.
        Args:
            path (str): The file path where the configuration should be saved.
            backup (bool): Whether to create a backup of the existing configuration file (default: False).
        Returns:
            None
        Raises:
            Exception: If the configuration file cannot be saved or if there is an IOError.
        """
        try:
            if backup:
                self.backup_config(path)

            with open(path, 'w', encoding="utf-8") as f:
                json.dump(self.model_dump(exclude_none=True, exclude_unset=True), f)
        except IOError as e:
            raise Exception(f"Unable to save Configuration file to path ({path}): {e}")
    
    def backup_config(self, path:str) -> None:
        """
        Creates a backup of the configuration file at the specified path.
        The backup file is created in the same directory as the original file, with a timestamp appended
        to the filename.
        Args:
            path (str): The file path to the configuration file to be backed up.
        Returns:
                None
        Raises:
            Exception: If the configuration file does not exist or cannot be backed up.
        """
        if not os.path.exists(path):
            return

        config_path = Path(path)
        backup_path = os.path.join(config_path.parent, f"{config_path.stem}_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")

        shutil.copyfile(path, backup_path)