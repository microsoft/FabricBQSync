import builtins as b
from typing import List, Any

from FabricSync.BQ.Model.Config import (
    ConfigDataset, ConfigLogging, ConfigMaintenance, ConfigAutodiscover, ConfigDiscoverObject,
    ConfigBQTable, ConfigTableColumn, ConfigLakehouseTarget, ConfigPartition,
    ConfigGCP, ConfigTableMaintenance, MappedColumn, ConfigFabric, ConfigGCPAPI,
    ConfigGCPDataset, ConfigGCPProject, ConfigGCPCredential
)
from FabricSync.BQ.Enum import (
    SyncLoadStrategy, SyncLoadType, FabricDestinationType, BQPartitionType
)

class UserConfigurationValidation:
    _config_context:ConfigDataset = None

    @classmethod
    def validate(cls, config:ConfigDataset) -> List[str]:
        """
        Validates the provided configuration dataset, checking for required fields and
        verifying sub-configuration sections (AutoDiscover, Fabric, GCP, Logging, etc.).
        Args:
            config (ConfigDataset): The configuration dataset to be validated.
        Returns:
            List[str]: A list of error messages describing any validation failures. 
            If all validations pass, the returned list is empty.
        """

        cls._config_context = config

        errors = []

        errors.append(cls.__required_field(config, "id"))
        errors.append(cls.__required_field(config, "correlation_id"))
        errors.append(cls.__required_field(config, "version"))

        errors.extend(cls._validate_autodiscovery(config.AutoDiscover))
        errors.extend(cls._validate_fabric_config(config.Fabric))
        errors.extend(cls._validate_gcp_config(config.GCP))
        errors.extend(cls._validate_logging(config.Logging))

        if config.is_field_set("Maintenance"):
            errors.extend(cls._validate_maintenance(config.Maintenance))

        if config.is_field_set("Tables"):
            errors.extend(cls._validate_table_configuration(config.Tables))
        
        return [e for e in errors if e]

    @classmethod
    def _validate_logging(cls, log:ConfigLogging) -> List[str]:
        """
        Validates the logging configuration by checking required fields.
        Args:
            log (ConfigLogging): The logging configuration to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        """

        errors = []
        
        errors.append(cls.__required_field(log, "log_level"))
        errors.append(cls.__required_field(log, "telemetry"))
        errors.append(cls.__required_field(log, "telemetry_endpoint"))
        errors.append(cls.__required_field(log, "log_path"))

        return [f"logging.{e}" for e in errors if e]
    
    @classmethod
    def _validate_maintenance(cls, maintenance:ConfigMaintenance) -> List[str]:
        """
        Validates the given maintenance configuration by checking required fields and optional
        threshold fields if they are set.
        Args:
            maintenance (ConfigMaintenance): The maintenance configuration to validate.
        Returns:
            List[str]: A list of error messages describing any validation findings.
            Returns an empty list if there are no errors.
        """

        errors = []

        errors.append(cls.__required_field(maintenance, "enabled"))
        errors.append(cls.__required_field(maintenance, "track_history"))
        errors.append(cls.__required_field(maintenance, "retention_hours"))
        errors.append(cls.__required_field(maintenance, "strategy"))
        errors.append(cls.__required_field(maintenance, "interval"))

        if maintenance.is_field_set("Thresholds"):
            errors.append(cls.__required_field(maintenance.Thresholds, "rows_changed"))
            errors.append(cls.__required_field(maintenance.Thresholds, "table_size_growth"))
            errors.append(cls.__required_field(maintenance.Thresholds, "file_fragmentation"))
            errors.append(cls.__required_field(maintenance.Thresholds, "out_of_scope_size"))

        return [f"maintenance.{e}" for e in errors if e]
    
    @classmethod
    def _validate_autodiscovery(cls, auto_discover:ConfigAutodiscover) -> List[str]:
        """
        Validates the autodiscovery configuration.
        Ensures that:
        1) The 'autodetect' field is present.
        2) At least one of the following attributes is configured: 'tables', 'views', or 'materialized_views'.
        3) Any optional fields ('Tables', 'Views', or 'MaterializedViews') conform to the discovery object validation.
        Args:
            auto_discover (ConfigAutodiscover): The configuration object that contains autodiscovery settings.
        Returns:
            List[str]: A list of error messages, each prefixed with "autodiscover." if any validation fails.
        """

        errors = []

        errors.append(cls.__required_field(auto_discover, "autodetect"))
        errors.append(cls.__at_least_one_attr(auto_discover, ["tables", "views", "materialized_views"]))

        if auto_discover.is_field_set("Tables"):
            errors.extend(cls._validate_discovery_object(auto_discover.Tables, "tables"))

        if auto_discover.is_field_set("Views"):
            errors.extend(cls._validate_discovery_object(auto_discover.Views, "views"))
        
        if auto_discover.is_field_set("MaterializedViews"):
            errors.extend(cls._validate_discovery_object(auto_discover.MaterializedViews, "materialized_views"))

        return [f"autodiscover.{e}" for e in errors if e]
    
    @classmethod
    def _validate_discovery_object(cls, discovery_obj:ConfigDiscoverObject, prefix:str) -> List[str]:
        """
        Validates the provided discovery object, checking that required fields are set.
        This function verifies if the fields "enabled" and "load_all" are specified.
        If a filter is present, it additionally verifies "pattern" and "type".
        Any validation errors are collected as strings, each prefixed with the given prefix.
        Args:
            cls: The class in which this validator is defined.
            discovery_obj (ConfigDiscoverObject): The discovery object containing fields to validate.
            prefix (str): A string to prepend to each validation error message.
        Returns:
            List[str]: A list of validation error messages, or an empty list if there are none.
        """

        errors = []

        errors.append(cls.__required_field(discovery_obj, "enabled"))
        errors.append(cls.__required_field(discovery_obj, "load_all"))

        if discovery_obj.is_field_set("Filter"):
            errors.append(cls.__required_field(discovery_obj.Filter, "pattern"))
            errors.append(cls.__required_field(discovery_obj.Filter, "type"))

        return [f"{prefix}.{e}" for e in errors if e]
    
    @classmethod
    def _validate_table_configuration(cls, tables:List[ConfigBQTable]) -> List[str]:
        """
        Validates the configuration of BigQuery tables.
        Args:
            tables (List[ConfigBQTable]): A list of ConfigBQTable instances representing 
                the tables to be validated.
        Returns:
            List[str]: A list of error strings describing any validation issues found. 
                Each error is prefixed with its corresponding table identifier.
        Notes:
            - Ensures that required fields (table_name, project_id, dataset, etc.) 
              are provided.
            - Checks whether certain integer fields (e.g., priority) fall within 
              valid ranges.
            - Validates additional dependent configurations (e.g., partition, column map, 
              load configuration, table maintenance, keys, watermark).
        """

        errors = []

        if tables:
            index = 1

            for table in tables:
                tbl_errors = []

                id = f"Table-{index}" if cls.__is_null_or_empty(table, "table_name") else cls.__get_value(table, "table_name")

                tbl_errors.append(cls.__required_field(table, "table_name"))
                tbl_errors.append(cls.__required_field(table, "project_id"))
                tbl_errors.append(cls.__required_field(table, "dataset"))
                tbl_errors.append(cls.__required_field(table, "object_type"))
                tbl_errors.append(cls.__is_in_int_range(table, "priority", 1, 10000))
                tbl_errors.append(cls.__required_field(table, "enabled"))

                tbl_errors.extend(cls.__dependents(table, ["load_strategy", "load_type"], 
                                                   "load_strategy or load_type", False))

                if table.is_field_set("BQPartition"):
                    tbl_errors.extend(cls._validate_bq_partition(table.BQPartition))
                
                if table.is_field_set("LakehouseTarget"):
                    tbl_errors.extend(cls._validate_lakehouse_target(table.BQPartition))
                
                if table.is_field_set("ColumnMap"):
                    tbl_errors.extend(cls._validate_column_map(table.ColumnMap))

                if table.is_field_set("TableMaintenance"):    
                    tbl_errors.extend(cls._validate_table_maintenance(table.TableMaintenance))

                if table.is_field_set("Keys"):
                    for key in table.Keys:
                        tbl_errors.append(cls._validate_table_column(key, "keys"))

                if table.is_field_set("Watermark"):
                    tbl_errors.append(cls._validate_table_column(table.Watermark, "watermark"))

                tbl_errors.extend(cls._validate_table_load_config(table))

                errors.extend([f"{id}.{e}" for e in tbl_errors if e])
                index += 1

        return [f"tables.{e}" for e in errors if e]

    @classmethod
    def _validate_table_load_config(cls, table:ConfigBQTable) -> List[str]:
        """
        Validates the table load configuration for a given ConfigBQTable. It enforces
        the correct SyncLoadStrategy, ensures required fields are present, and checks
        for minimum list lengths where applicable. Any violated validation rules are
        accumulated and returned as error messages.
        Args:
            table (ConfigBQTable): The table configuration to validate.
        Returns:
            List[str]: A list of error messages describing any validation failures.
        Notes:
            - Ensures that the load strategy is compatible with the load type.
            - Validates the presence of required fields for certain load strategies.
            - Checks for minimum list lengths where applicable (e.g., keys).
        """

        errors = []

        if table.LoadStrategy == SyncLoadStrategy.CDC:
            errors.append(cls.__in_list(table, "load_type", [SyncLoadType.MERGE]))
            errors.append(cls.__equals_value(cls._config_context.GCP.API, "use_cdc", True,
                        "Load Type CDC requires {} to be '{}' but got '{}'"))
            
        if table.LoadStrategy == SyncLoadStrategy.CDC_APPEND:
            errors.append(cls.__in_list(table, "load_type", [SyncLoadType.APPEND, SyncLoadType.MERGE]))
            errors.append(cls.__equals_value(cls._config_context.GCP.API, "use_cdc", True,
                        "Load Type CDC_APPEND requires {} to be '{}' but got '{}'"))
            
        if table.LoadStrategy == SyncLoadStrategy.WATERMARK:
            errors.append(cls.__in_list(table, "load_type", [SyncLoadType.APPEND, SyncLoadType.MERGE]))
            errors.append(cls.__required_field(table, "watermark"))
            
        if table.LoadStrategy == SyncLoadStrategy.PARTITION:
            errors.append(cls.__in_list(table, "load_type", 
                        [SyncLoadType.OVERWRITE, SyncLoadType.APPEND, SyncLoadType.MERGE]))

            errors.append(cls.__required_field(table, "bq_partition"))
            errors.append(cls.__equals_value(table.BQPartition, "enabled", True,
                        "Load Type PARTITION requires {} to be '{}' but got '{}'"))
            
        if table.LoadStrategy == SyncLoadStrategy.FULL:
            errors.append(cls.__in_list(table, "load_type", [SyncLoadType.OVERWRITE, SyncLoadType.APPEND, SyncLoadType.MERGE]))

        if table.LoadType == SyncLoadType.MERGE:
            errors.append(cls.__check_min_list_length(table, "keys", 1))

        return [e for e in errors if e]
    
    @classmethod
    def _validate_table_column(cls, target:ConfigTableColumn, context:str) -> str:
        """
        Validates the specified table column within a given context.
        This method checks for required fields in the provided column
        and returns an error message if any are missing.
        Parameters:
            target (ConfigTableColumn):
                The table column to validate.
            context (str):
                The descriptive context or identifier for this validation operation.
        Returns:
            str:
                An error message if a required field is missing, otherwise None.
        """

        error = cls.__required_field(target, "column")

        return f"{context}.{error}" if error else None
    
    @classmethod
    def _validate_lakehouse_target(cls, target:ConfigLakehouseTarget) -> List[str]:
        """
        Validates the given Lakehouse target configuration.
        Args:
            target (ConfigLakehouseTarget):
                The configuration object representing a Lakehouse target.
        Returns:
            List[str]:
                A list of validation error messages. If the list is empty,
                it indicates that no errors were found.
        """

        errors = []

        return [f"lakehouse_target.{e}" for e in errors if e]
    
    @classmethod
    def _validate_bq_partition(cls, partition:ConfigPartition) -> List[str]:
        """
        Validates the given BigQuery partition configuration. This method checks for 
        required fields and ensures that dependent fields are set when necessary.
        Args:
            partition (ConfigPartition): The partition configuration to validate.
        Returns:
            List[str]: A list of error messages describing any validation issues found.
        Notes:
            - Ensures that required fields (enabled, type, column, partition_data_type) are provided.
            - Validates dependent fields based on the partition type (e.g., partition_grain, partition_range).
        """
        errors = []

        errors.append(cls.__required_field(partition, "enabled"))
        errors.append(cls.__required_field(partition, "type"))
        errors.append(cls.__required_field(partition, "column"))
        errors.append(cls.__required_field(partition, "partition_data_type"))
            
        if cls.__is_in_list(partition, "type", [BQPartitionType.TIME, BQPartitionType.TIME_INGESTION]):
            errors.append(cls.__required_field(partition, "partition_grain"))

        if cls.__is_equal(partition, "type", BQPartitionType.RANGE):
            errors.append(cls.__required_field(partition, "partition_range"))

        return [f"bq_partition.{e}" for e in errors if e]
    
    @classmethod
    def _validate_column_map(cls, column_mapping:List[MappedColumn]) -> List[str]:
        """
        Validates the column mapping configuration for a given table. This method checks
        for required fields in each mapped column and returns a list of error messages if
        any are missing.
        Args:
            column_mapping (List[MappedColumn]): A list of MappedColumn instances representing
                the column mappings to validate.
        Returns:
            List[str]: A list of error messages describing any validation issues found.
        Notes:
            - Ensures that required fields (source, destination, drop_source) are provided.
            - Validates the source and destination fields for each mapped column.
        """
        errors = []

        for map in column_mapping:
            errors.append(cls.__required_field(map, "source"))
            errors.append(cls.__required_field(map.Source, "name"))
            errors.append(cls.__required_field(map.Source, "type"))
            errors.append(cls.__required_field(map, "destination"))
            errors.append(cls.__required_field(map.Destination, "name"))
            errors.append(cls.__required_field(map.Destination, "type"))
            errors.append(cls.__required_field(map, "drop_source"))

        return [f"column_map.{e}" for e in errors if e]
    
    @classmethod
    def _validate_table_maintenance(cls, maintenance:ConfigTableMaintenance) -> List[str]:
        """
        Validates the table maintenance configuration for a given table. This method checks
        for required fields in the maintenance configuration and returns a list of error
        messages if any are missing.
        Args:
            maintenance (ConfigTableMaintenance): The table maintenance configuration to validate.
        Returns:
            List[str]: A list of error messages describing any validation issues found.
        Notes:
            - Ensures that required fields (enabled, interval) are provided.
        """
        errors = []

        errors.append(cls.__required_field(maintenance, "enabled"))
        errors.append(cls.__required_field(maintenance, "interval"))

        return [f"table_maintenance.{e}" for e in errors if e]

    @classmethod
    def _validate_gcp_config(cls, gcp:ConfigGCP) -> List[str]:
        """
        Validates the GCP configuration by checking required fields and verifying sub-configuration
        sections (GCPCredential, GCPAPI, GCPProjects, GCPDatasets).
        Args:
            gcp (ConfigGCP): The GCP configuration to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        Notes:
            - Ensures that required fields (credentials, api, projects) are provided.
            - Validates dependent configurations (e.g., projects, datasets).
        """
        errors = []

        errors.extend(cls._validate_gcp_credentials_config(gcp.GCPCredential))
        errors.extend(cls._validate_gcp_api_config(gcp.API))
        errors.append(cls.__check_min_list_length(gcp, "projects", 1))
        errors.extend(cls._validate_gcp_projects_config(gcp.Projects))
        
        return [f"gcp.{e}" for e in errors if e]

    @classmethod
    def _validate_gcp_credentials_config(cls, credentials:ConfigGCPCredential) -> List[str]:
        """
        Validates the GCP credentials configuration by checking required fields.
        Args:
            credentials (ConfigGCPCredential): The GCP credentials configuration to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        Notes:
            - Ensures that at least one of the following attributes is configured: 'credential', 'credential_path'.
        """
        errors = []

        errors.append(cls.__at_least_one_attr(credentials, ["credential", "credential_path", "credential_secret_key"]))

        if not cls.__is_null_or_empty(credentials, "credential_secret_key"):
            errors.append(cls.__required_field(credentials, "credential_secret_key_vault"))

        return [f"gcp_credentials.{e}" for e in errors if e]

    @classmethod
    def _validate_gcp_api_config(cls, api:ConfigGCPAPI) -> List[str]:
        """
        Validates the GCP API configuration by checking required fields and verifying dependent fields.
        Args:
            api (ConfigGCPAPI): The GCP API configuration to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        Notes:
            - Ensures that required fields (use_standard_api, use_cdc) are provided.
            - Validates dependent configurations (e.g., materialization_project_id, materialization_dataset).
        """
        errors = []

        errors.append(cls.__required_field(api, "use_standard_api"))
        errors.append(cls.__required_field(api, "use_cdc"))
        errors.extend(cls.__dependents(api, ["materialization_project_id", "materialization_dataset"], 
                "materialization_project_id or materialization_dataset"))
        
        return [f"api.{e}" for e in errors if e]

    @classmethod
    def _validate_gcp_projects_config(cls, projects:List[ConfigGCPProject]) -> List[str]:
        """
        Validates the GCP project configuration by checking required fields and verifying dependent fields.
        Args:
            projects (List[ConfigGCPProject]): A list of GCP project configurations to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        Notes:
            - Ensures that required fields (project_id, datasets) are provided.
            - Validates dependent configurations (e.g., datasets).
        """
        errors = []

        for p in projects:
            errors.append(cls.__required_field(p, "project_id"))
            errors.append(cls.__check_min_list_length(p, "datasets", 1))
            errors.extend(cls._validate_gcp_datasets_config(p.Datasets))
        
        return [f"projects.{e}" for e in errors if e]
    
    @classmethod
    def _validate_gcp_datasets_config(cls, datasets:List[ConfigGCPDataset]) -> List[str]:
        """
        Validates the GCP dataset configuration by checking required fields.
        Args:
            datasets (List[ConfigGCPDataset]): A list of GCP dataset configurations to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        """
        errors = []

        for d in datasets:
            errors.append(cls.__required_field(d, "dataset"))
        
        return [f"datasets.{e}" for e in errors if e]

    @classmethod
    def _validate_fabric_config(cls, fabric:ConfigFabric) -> List[str]:
        """
        Validates the Fabric configuration by checking required fields and verifying dependent fields.
        Args:
            fabric (ConfigFabric): The Fabric configuration to validate.
        Returns:
            List[str]: A list of error messages indicating any missing or invalid fields.
        Notes:
            - Ensures that required fields (workspace_id, metadata_lakehouse, metadata_lakehouse_id, target_lakehouse, target_lakehouse_id, enable_schemas, target_type) are provided.
            - Validates dependent configurations (e.g., metadata_schema, target_schema).
        """
        errors = []

        errors.append(cls.__required_field(fabric, "workspace_id"))
        errors.append(cls.__required_field(fabric, "metadata_lakehouse"))
        errors.append(cls.__required_field(fabric, "metadata_lakehouse_id"))
        errors.append(cls.__required_field(fabric, "target_lakehouse"))
        errors.append(cls.__required_field(fabric, "target_lakehouse_id"))
        errors.append(cls.__required_field(fabric, "enable_schemas"))
        errors.append(cls.__required_field(fabric, "target_type"))
        
        if fabric.TargetType == FabricDestinationType.MIRRORED_DATABASE:
            errors.append(cls.__equals_value(fabric, "enable_schemas", True, 
                               "Mirror Database destination requires '{}' to be {} but got {}"))
            
        if fabric.EnableSchemas:
            errors.append(cls.__required_field(fabric, "metadata_schema"))
            errors.append(cls.__required_field(fabric, "target_schema"))
        
        return [f"fabric.{e}" for e in errors if e]
    
    @classmethod
    def __is_in_int_range(cls, obj:Any, attr:str, min:int, max:int):
        """
        Validates that the given attribute is an integer within the specified range.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            min (int): The minimum value allowed for the attribute.
            max (int): The maximum value allowed for the attribute.
        Returns:
            str: An error message if the attribute is not an integer within the specified range.
        """
        if not cls.__is_null_or_empty(obj, attr):
            if cls.__get_value(obj, attr) >= min and cls.__get_value(obj, attr) <= max:
                return None
        
        return f"{attr} must be a value between {min} and {max}"

    @classmethod
    def __check_min_list_length(cls, obj:Any, attr:str, length:int) -> str:
        """
        Validates that the given attribute is a list with at least the specified number of elements.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            length (int): The minimum number of elements required in the list.
        Returns:
            str: An error message if the attribute is not a list with at least the specified number of elements.
        """
        if cls.__is_null_or_empty(obj, attr) or len(cls.__get_value(obj, attr)) < length:
            return f"{attr} is required"
        
        return None

    @classmethod
    def __at_least_one_attr(cls, obj:Any, attrs:List[str]) -> str:
        """
        Validates that at least one of the specified attributes is configured.
        Args:
            obj (Any): The object containing the attributes to validate.
            attrs (List[str]): A list of attribute names to check for configuration.
        Returns:
            str: An error message if none of the specified attributes are configured.
        """
        non_empty = [attr for attr in attrs if not cls.__is_null_or_empty(obj, attr)]

        if len(non_empty) == 0:
            return f"At least one of the following must be configured: {','.join(attrs)}"
        
        return None

    @classmethod
    def __dependents(cls, obj:Any, attrs:List[str], depedency:str, require_all:bool=False) -> List[str]:
        """
        Validates that the specified attributes are configured when a dependency is set.
        Args:
            obj (Any): The object containing the attributes to validate.
            attrs (List[str]): A list of attribute names to check for configuration.
            depedency (str): The name of the dependency attribute that triggers the validation.
            require_all (bool): A flag indicating whether all attributes are required.
        Returns:
            List[str]: A list of error messages indicating any missing attributes.
        """
        non_empty = [attr for attr in attrs if not cls.__is_null_or_empty(obj, attr)]

        if (len(non_empty) == len(attrs)) or (len(non_empty) == 0 and not require_all):
            return []
        
        return [f"{attr} required when {depedency} is configured" 
                    for attr in [attr for attr in attrs if attr not in non_empty]]

    @classmethod
    def __in_list(cls, obj:Any, attr:str, values:List[Any]) -> str:
        """
        Validates that the given attribute is one of the specified values.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            values (List[Any]): A list of valid values for the attribute.
        Returns:
            str: An error message if the attribute is not one of the specified values.
        """
        if not cls.__is_in_list(obj, attr, values):
            return f"{attr} expects one of '{','.join(values)}' got '{str(cls.__get_value(obj, attr))}'"
    
        return None

    @classmethod
    def __equals_value(cls, obj:Any, attr:str, value:Any, msg:str = None) -> str:
        """
        Validates that the given attribute is equal to the specified value.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            value (Any): The value to compare against the attribute.
            msg (str): An optional error message to return if the attribute is not equal to the value.
        Returns:
            str: An error message if the attribute is not equal to the specified value.
        """
        if cls.__is_equal(obj, attr, value):
            return None
        
        if msg:
            return msg.format(attr, value, cls.__get_value(obj, value))
        else:
            return f"{attr} expects '{str(value)}' got '{str(cls.__get_value(obj, value))}'"
        
    @classmethod
    def __required_field(cls, obj:Any, attr:str) -> str:
        """
        Validates that the given attribute is not null or empty.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
        Returns:
            str: An error message if the attribute is null or empty.
        """
        if cls.__is_null_or_empty(obj, attr):
            return f"{attr} is required"
        
        return None
    
    @classmethod
    def __is_equal(cls, obj:Any, attr:str, value:Any, msg:str = None) -> str:
        """
        Validates that the given attribute is equal to the specified value.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            value (Any): The value to compare against the attribute.
            msg (str): An optional error message to return if the attribute is not equal to the value.
        Returns:
            str: An error message if the attribute is not equal to the specified value.
        """
        return cls.__get_value(obj, attr) == value
    
    @classmethod
    def __is_in_list(cls, obj:Any, attr:str, values:List[Any]) -> bool:
        """
        Validates that the given attribute is one of the specified values.
        Args:
            obj (Any): The object containing the attribute to validate.
            attr (str): The name of the attribute to validate.
            values (List[Any]): A list of valid values for the attribute.
        Returns:
            bool: True if the attribute is one of the specified values, otherwise False.
        """
        return cls.__get_value(obj, attr) in values

    @classmethod
    def __get_value(cls, obj:Any, attr:str) -> Any:
        """
        Gets the value of the specified attribute from the given object.
        Args:
            obj (Any): The object containing the attribute to retrieve.
            attr (str): The name of the attribute to retrieve. 
        Returns:
            Any: The value of the specified attribute, or None if the attribute is not set.
        """
        value = getattr(obj, attr) if hasattr(obj, attr) else None

        if value == None:
            value = obj.get_field_default(attr)
        
        return value
    
    @classmethod
    def __is_null_or_empty(cls, obj:Any, attr:str) -> bool:   
        """
        Checks if the given attribute is null or empty.
        Args:
            obj (Any): The object containing the attribute to check.
            attr (str): The name of the attribute to check.
        Returns:
            bool: True if the attribute is null or empty, otherwise False.
        """     
        return cls.__get_value(obj, attr) == None