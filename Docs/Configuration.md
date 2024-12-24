# Fabric BQ Sync User Config Map
The BQ Sync accelerator offers robust user configuration for full control and optimization over the sync process. Multiple configurations are supported with the path to the in-scope configuration file provided at runtime.

Configuration is broken down into the following sections:

- [default (main)](#default)
- [autodiscover](#autodiscover)
- [logging](#logging)
- [fabric](#fabric)
- [gcp](#gcp)
- [async](#async)
- [optimization](#optimization)
- [table_defaults](#table_defaults)
- [tables](#tables)


## default

<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>correlation_id</td>
        <td>UUID</td>
        <td>Assigned automatically</td>
        <td>System assigned id</td>
    </tr>
    <tr>
        <td>id</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. User-defined loader name provided during set-up. Used within the metastore to differentation when multiple configurations are used.</td>
    </tr>
    <tr>
        <td>version</td>
        <td>STRING</td>
        <td>Assigned automatically</td>
        <td>BQ Sync version</td>
    </tr>
    <tr>
        <td>enable_data_expiration</td>
        <td>Boolean</td>
        <td>False</td>
        <td>Parent-level switch to control whether data expiration is enforced </td>
    </tr>
</table>
_____

## autodiscover
Controls the behavior of the autodiscovery process when enabled. For more information, please see the Autodiscovery docs.
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>autodetect</td><td>BOOLEAN</td><td>True</td><td>Enable/disable the auto-detect/auto-discovery prcoess</td>
    </tr>
    <tr>
    <td colspan="4" class="level1"><b>tables</b></td></tr>
    <tr>
        <td class="level1-item">enabled</td><td> BOOLEAN</td><td>True</td><td>Enable/disable BigQuery BASE TABLES for autodiscover</td>
    </tr>
    <tr>
        <td class="level1-item">load_all</td><td>BOOLEAN</td><td>True</td><td>Flag to control whether all BASE TABLES are discovered/loaded</td>
    </tr>
    <tr>
        <td colspan="4" class="level2">tables.filter</td>
    </tr>
    <tr>
        <td class="level2-item">pattern</td><td>STRING</td><td>None</td><td>Any wildcard-based '*'  pattern used to filter BASE TABLE name</td>
    </tr>
    <tr>
        <td class="level2-item">type</td><td>INCLUDE \| EXCLUDE</td><td>None</td><td>Determines whether pattern-match includes or excludes the BQ object from discovery</td></tr>
    <tr>
        <td colspan="4" class="level1"><b>views</b></td>
    </tr>
    <tr>
        <td class="level1-item">enabled</td><td> BOOLEAN</td><td>True</td><td>Enable/disable BigQuery VIEWS for autodiscover</td>
    </tr>
    <tr>
        <td class="level1-item">load_all</td><td>BOOLEAN</td><td>True</td><td>Flag to control whether all VIEWS are discovered/loaded</td></tr>
    <tr>
        <td colspan="4" class="level2">views.filter</td>
    </tr>
    <tr>
        <td class="level2-item">pattern</td><td>STRING</td><td>None</td><td>Any wildcard-based '*'  pattern used to filter VIEW name</td>
    </tr>
    <tr>
        <td class="level2-item">type</td><td>INCLUDE \| EXCLUDE</td><td>None</td><td>Determines whether pattern-match includes or excludes the BQ object from discovery</td>
    </tr>
    <tr>
        <td colspan="4" class="level1"><b>materialized_views</b></td>
    </tr>
    <tr>
        <td class="level1-item">enabled</td><td> BOOLEAN</td><td>True</td><td>Enable/disable BigQuery MATERIALIZED VIEWS for autodiscover</td>
    </tr>
    <tr>
        <td class="level1-item">load_all</td><td>BOOLEAN</td><td>True</td><td>Flag to control whether all MATERIALIZED VIEWS are discovered/loaded</td>
    </tr>
    <tr>
        <td colspan="4" class="level2">materialized_views.filter</td>
    </tr>
    <tr>
        <td class="level2-item">pattern</td><td>STRING</td><td>None</td><td>Any wildcard-based '*'  pattern used to filter MATERIALIZED VIEW name</td>
    </tr>
    <tr>
        <td class="level2-item">type</td><td>INCLUDE \| EXCLUDE</td><td>None</td><td>Determines whether pattern-match includes or excludes the BQ object from discovery</td>
    </tr>
</table>
____

## logging
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>log_level</td>
        <td>SYNC_STATUS | TELEMETRY</td>
        <td>SYNC_STATUS</td>
        <td>Custom log levels used within accelerator. SYNC STATUS records application events to sync logs. TELEMETRY suppresses application events and sends only required telemetry to logs.</td>
    </tr>
    <tr>
        <td>log_path</td>
        <td>STRING</td>
        <td>Application Lakehouse file API path to logs</td>
        <td>Valid path within the lakehouse. Path must exists and must use the File API path.</td>
    </tr>
</table>

_____

## fabric
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>workspace_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>Fabric Workspace Id (UUID)</td>
    </tr>
    <tr>
        <td>metadata_lakehouse</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. Fabric Lakehouse name for BQ Sync Metadata</td>
    </tr>
    <tr>
        <td>target_lakehouse</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. Fabric Lakehouse name for mirrored/sync'd data</td>
    </tr>
    <tr>
        <td>enable_schemas</td>
        <td>BOOLEAN</td>
        <td>False</td>
        <td>Flag to enable/disable three-part naming schema support. Schemas are currently in preview in Fabric. This capability may change based on the execution of the Fabric roadmap.</td>
    </tr> 
    <tr>
        <td>target_schema</td>
        <td>STRING</td>
        <td>None</td>
        <td>Lakehouse schema name. REQUIRED when enable_schemas is TRUE</td>
    </tr>
</table>

_____

## gcp
The configuration specified here affects billing and perissions required on the BigQuery side. Most of the configuration provided are passed directly to the BigQuery APIs. Note that BQ Sync supports targeting of multiple projects and multiple datasets when required. For more information, please see the BigQuery API docs.
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td colspan="4" class="level1">api</td>
    </tr>
    <tr>
        <td class="level1-item">use_standard_api</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Enables the Standard Big Query API for Metadata Sync operations. See Optimization docs for more details.</td>
    </tr>
    <tr>
        <td class="level1-item">auto_select</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>RESERVED FOR FUTURE USE</td>
    </tr>
    <tr>
        <td class="level1-item">materialization_project_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>When not set, the first project in the Projects config is used as the materialization project id for the BigQuery Spark connector.</td>
    </tr>
    <tr>
        <td class="level1-item">materialization_dataset</td>
        <td>STRING</td>
        <td>None</td>
        <td>When not set, the first dataset of the first project in the Projects config is used as the materialization dataset for the BigQuery Spark connector.</td>
    </tr>
    <tr>
        <td class="level1-item">billing_project_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>When not set, the first project in the Projects config is used as the billing project id for the BigQuery Spark connector.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">projects &lt;ARRAY&gt;</td>
    </tr>
    <tr>
        <td class="level1-item">project_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. GCP Project Id. Minimum of one instance.</td>
    </tr>
    <tr>
        <td colspan="4" class="level2">projects.datasets &lt;ARRAY&gt;</td>
    </tr>
    <tr>
        <td class="level2-item">datasets.dataset</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. BigQuery Dataset Name. Minimum of one instance.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">gcp_credentials</td>
    </tr>
    <tr>
        <td class="level1-item">credential_path</td>
        <td>STRING</td>
        <td>None</td>
        <td>Fabric Lakehouse path to GCP Service Account credential json file. Path should be File Path API to default lakehouse.</td>
    </tr>
    <tr>
        <td class="level1-item">access_token</td>
        <td>STRING</td>
        <td>None</td>
        <td>GCP API Access Token - RESERVED FOR FUTURE US</td>
    </tr>
    <tr>
        <td class="level1-item">credential</td>
        <td>STRING</td>
        <td>None</td>
        <td>Base-64 encoded GCP Service Credential, created by default during BQ Sync install. At least one of either, credentials or credential_path must be provided. If both are provided, credentials will cause the credential_path to be ignored.</td>
    </tr>
</table>

_____

## async
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>enabled</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Flag to enable/disable native thread-based parallelism within BQ Sync.</td>
    </tr>
    <tr>
        <td>parallelism</td>
        <td>INT</td>
        <td>10</td>
        <td>Degree of parallelism used by BQ Sync when the enabled flag is TRUE. This setting should be optimized based on the size of workload and the configuration of your Spark environment.</td>
    </tr>
</table>

_____

## optimization
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>use_approximate_row_counts</td>
        <td>BOOLEAN</td>
        <td>False</td>
        <td>Enable/Disable the approximate row count optimization. Applies to all tables with the configuration scope. See Optimization docs for more details.</td>
    </tr>
</table>

_____

## table_defaults
Table defaults are provided as a mechanism to simplify configuration through abstraction of frequently repeating configurations (i.e. defaults). By defining a default, tables will inherit the default configuration value unless overridden by a table-specific configuration. 
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>project_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>Default GCP project id</td>
    </tr>
    <tr>
        <td>dataset</td>
        <td>STRING</td>
        <td>None</td>
        <td>Default Big Query dataset</td>
    </tr>
    <tr>
        <td>object_type</td>
        <td>BASE_TABLE | VIEW | MATERIALIZED_VIEW</td>
        <td>BASE_TABLE</td>
        <td>Default BigQuery Object Type</td>
    </tr>
    <tr>
        <td>priority</td>
        <td>INT</td>
        <td>100</td>
        <td>Used to define load groups and create sync dependencies between objects or groups of objects. Objects with the same priority are grouped together and synced in parallel. Object order within the group is not guaranteed. Lower priority groups run first starting with zero. For more information see the Load Groups doc.</td>
    </tr>
    <tr>
        <td>load_strategy</td>
        <td>FULL | WATERMARK | PARTITION</td>
        <td>FULL</td>
        <td>Default load strategy</td>
    </tr>
    <tr>
        <td>load_type</td>
        <td>OVERWRITE | APPEND | MERGE</td>
        <td>OVERWRITE</td>
        <td>Default load type</td>
    </tr>
    <tr>
        <td>interval</td>
        <td>STRING</td>
        <td>AUTO</td>
        <td>Default schedule interval.</td>
    </tr>
    <tr>
        <td>enabled</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Default flag to enable/disable object synch</td>
    </tr>
    <tr>
        <td>enforce_expiration</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Default flag to enable/disable table-level data expiration</td>
    </tr>
    <tr>
        <td>allow_schema_evolution</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Default flag to enable/disable table schema evolution. For more information see Schema Evolution doc.</td>
    </tr>
    <tr>
        <td>flatten_table</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Default flag to enable/disable flattening of complex (STRUCT) types during the sync process. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td>flatten_inplace</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Default flag to determine where flatten data is stored within the Fabric Lakehouse. When TRUE, STRUCTs are flattened into existing lakehouse table. When FALSE, STRUCTs are flattened into a parallel lakehouse table. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td>explode_arrays</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Default flag to enable/disable flattening or explosion of complex (ARRAY) types during the sync process. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">table_maintenance</td>
    </tr>
    <tr>
        <td class="level1-item">enabled</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Default flag to enable/disable table maintenance. For more information see Table Maintainence doc.</td>
    </tr>
    <tr>
        <td class="level1-item">interval</td>
        <td>DAY | WEEK | MONTH | QUARTER | YEAR</td>
        <td>MONTH</td>
        <td>Default maintainence interval.Ignored when the table_maintainence enabled flag is FALSE. For more information see Table Maintainence doc</td>
    </tr>
</table>

_____

## tables
The core of configuration is tables. When the load all option is not used, the in-scope tables must be provided through configuration. Table configuration always overrides auto-discovered configuration, when provided and is not validated for correctness against the BigQuery schema.
<table>
    <tr><th>JSON Setting</th><th>Value Type</th><th>Default</th><th>Description</th></tr>
    <tr>
        <td>project_id</td>
        <td>STRING</td>
        <td>None</td>
        <td>**REQUIRED. GCP Project Id. BigQuery Project Id will cascade down through configuration when not provided at table-level.</td>
    </tr>
    <tr>
        <td>dataset</td>
        <td>STRING</td>
        <td>None</td>
        <td>**REQUIRED. Big Query dataset. BigQuery Dataset will cascade down through configuration when not provided at table-level.</td>
    </tr>
    <tr>
        <td>table_name</td>
        <td>STRING</td>
        <td>None</td>
        <td>REQUIRED. BigQuery Table/View/Materialized View name.</td>
    </tr>
    <tr>
        <td>source_query</td>
        <td>STRING</td>
        <td>None</td>
        <td>Optional BigQuery SQL query to override default object source.</td>
    </tr>
    <tr>
        <td>predicate</td>
        <td>STRING</td>
        <td>None</td>
        <td>Optional BigQuery SQL predicate condition to be applied to object source.</td>
    </tr>
    <tr>
        <td>object_type</td>
        <td>BASE_TABLE | VIEW | MATERIALIZED_VIEW</td>
        <td>BASE_TABLE</td>
        <td>BigQuery Object Type</td>
    </tr>
    <tr>
        <td>priority</td>
        <td>INT</td>
        <td>100</td>
        <td>Used to define load groups and create sync dependencies between tables or groups of tables. Objects with the same priority are group together and sync in parallel. Order within the group is not guaranteed. Lower priority groups run first starting with zero. For more information see the Load Groups doc.</td>
    </tr>
    <tr>
        <td>load_strategy</td>
        <td>FULL | WATERMARK | PARTITION</td>
        <td>FULL</td>
        <td>Load strategy</td>
    </tr>
    <tr>
        <td>load_type</td>
        <td>OVERWRITE | APPEND | MERGE</td>
        <td>OVERWRITE</td>
        <td>Load type</td>
    </tr>
    <tr>
        <td>interval</td>
        <td>STRING</td>
        <td>AUTO</td>
        <td>Schedule interval.</td>
    </tr>
    <tr>
        <td>enabled</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Flag to enable/disable table synch</td>
    </tr>
    <tr>
        <td>enforce_expiration</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to enable/disable table-level data expiration. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td>allow_schema_evolution</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to enable/disable table schema evolution. For more information see Schema Evolution doc.</td>
    </tr>
    <tr>
        <td>flatten_table</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to enable/disable flattening of complex (STRUCT) types during the sync process. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td>flatten_inplace</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Flag to determine where flatten data is stored within the Fabric Lakehouse. When TRUE, STRUCTs are flattened into existing lakehouse table. When FALSE, STRUCTs are flattened into a parallel lakehouse table. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td>explode_arrays</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to enable/disable flattening or explosion of complex (ARRAY) types during the sync process. For more information see Complex Type Handling doc.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">table_maintenance</td>
    </tr>
    <tr>
        <td class="level1-item">enabled</td>
        <td>BOOLEAN</td>
        <td>TRUE</td>
        <td>Flag to enable/disable table maintenance. For more information see Table Maintainence doc.</td>
    </tr>
    <tr>
        <td class="level1-item">interval</td>
        <td>DAY | WEEK | MONTH | QUARTER | YEAR</td>
        <td>MONTH</td>
        <td>Table maintainence interval.Ignored when the table_maintainence enabled flag is FALSE. For more information see Table Maintainence doc</td>
    </tr>    
    <tr>
        <td colspan="4" class="level1">column_map &lt;ARRAY&gt;</td>
    </tr>
    <tr>
        <td class="level1-item">format</td>
        <td>STRING</td>
        <td>None</td>
        <td>Python format string when required for by data type transformation. For more information, see Column Map doc.</td>
    </tr>
    <tr>
        <td class="level1-item">drop_source</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to allow for dropping source column when a column is renamed. Ignored when the column is not renamed. For more information, see Column Map doc.</td>
    </tr>
    <tr>
        <td colspan="4" class="level2">column_map.source</td>
    </tr>
    <tr>
        <td class="level2-item">name</td>
        <td>STRING</td>
        <td>None</td>
        <td>BigQuery column name for source</td>
    </tr>
    <tr>
        <td class="level2-item">type</td>
        <td>STRING</td>
        <td>None</td>
        <td>Spark mapped data-type for source column</td>
    </tr>
    <tr>
        <td colspan="4" class="level2">column_map.destination</td>
    </tr>
    <tr>
        <td class="level2-item">name</td>
        <td>STRING</td>
        <td>None</td>
        <td>Mapping output column name</td>
    </tr>
    <tr>
        <td class="level2-item">type</td>
        <td>STRING</td>
        <td>None</td>
        <td>Spark mapped data-type for mapping output column</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">lakehouse_target</td>
    </tr>
    <tr>
        <td class="level1-item">lakehouse</td>
        <td>STRING</td>
        <td>None</td>
        <td>OPTIONAL. Target Fabric Lakehouse.</td>
    </tr>
    <tr>
        <td class="level1-item">schema</td>
        <td>STRING</td>
        <td>None</td>
        <td>OPTIONAL. Fabric Lakehouse Schema when enable_schemas is TRUE.</td>
    </tr>
    <tr>
        <td class="level1-item">table_name</td>
        <td>STRING</td>
        <td>Defaults to BigQuery table name when None</td>
        <td>OPTIONAL. Override to rename tables within Lakehouse during sync process.</td>
    </tr>
    <tr>
        <td class="level1-item">partition_by</td>
        <td>STRING</td>
        <td>None</td>
        <td>OPTIONAL. Allows for override of BigQuery partitioning within Lakehouse. Provided as a comma-separated list of column names when more than one partition column is required.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">bq_partition</td>
    </tr>
    <tr>
        <td class="level1-item">enabled</td>
        <td>BOOLEAN</td>
        <td>FALSE</td>
        <td>Flag to enable BigQuery table partitioning. Required when load_strategy is PARTITION and is usually discovered automatically if autodetect is TRUE. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td class="level1-item">type</td>
        <td>TIME | RANGE | TIME_INGESTION</td>
        <td>None</td>
        <td>Big Query Table Partitioning Strategy. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td class="level1-item">column</td>
        <td>STRING</td>
        <td>None</td>
        <td>Physical or virtual column BigQuery table is partitioned on. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td class="level1-item">partition_grain</td>
        <td>HOUR | DAY | MONTH | YEAR</td>
        <td>None</td>
        <td>BigQuery partition granularity when partitioning strategy is time-based. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td class="level1-item">partition_data_type</td>
        <td>STRING</td>
        <td>None</td>
        <td>BigQuery column data type for table partition column. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td class="level1-item">partition_range</td>
        <td>STRING</td>
        <td>None</td>
        <td>BigQuery range values for RANGE partitioned tables. Only applicable for BASE TABLES.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">keys &lt;ARRAY&gt;</td>
    </tr>
    <tr>
        <td class="level1-item">column</td>
        <td>STRING</td>
        <td>None</td>
        <td>Object column name(s) used for join condition when load_type=MERGE.</td>
    </tr>
    <tr>
        <td colspan="4" class="level1">watermark</td>
    </tr>
    <tr>
        <td class="level1-item">column</td>
        <td>STRING</td>
        <td>None</td>
        <td>BigQuery column name used to calculate watermark when load_strategy=WATERMARK. Column must be a TIMESTAMP, DATE or NUMERIC derived data type.</td>
    </tr>
</table>
_____