# Configuration

The core of the accelerator is metadata, and this metadata drives the synchronization configuration and the overall approach for the BQ Sync process. Accelerator configuration happens first through auto-discovery using metadata from BigQuery and second through user-supplied JSON configuration which allows for overriding or refining of the discovered configuration.

Load specific configurations (load_strategy, load_type, partitioning, etc), whether auto discovered or provided through user config, happens only on the initial run and whenever new tables are added to the dataset, if the accelerator is configured for auto discovery. Load configuration can only be changed prior to the initial data load. Once data has been loaded, the accelerator locks the load configuration to guarantee the fidelity of the synchronized data. If changes are required for locked configurations after data synchronization has started, manual intervention is required.

Some configuration settings can be changed at any point, these setting are:
 - (table) enabled
 - (schedule) interval
 - (load) priority
 - source_query
 - source_predicate
 - enforce_expiration
 - column_map
 - allow_schema_evolution
 - table_maintenance_enabled
 - table_maintenance_interval

For more information on the all the settings available for configuration, please see the [Configuration Settings](ConfigurationSettings.md) index.

### Configuration through AutoDiscovery
The autodiscovery process starts with a snapshot of the existing dataset metadata from the BigQuery INFORMATION_SCHEMA tables. This metadata defines each table’s structure, schema and BigQuery configuration. This accelerator defines a set of heuristics that when are applied, discover a probable best approach to table load strategy and load type.

For more information on AutoDiscovery, please see the [AutoDiscovery](Autodiscovery.md) documentation.

### User Config
While autodiscovery intends to simplify the process, we recognize that in some cases it’s not possible to optimize the synchronization process without domain knowledge of the dataset. To accommodate these scenarios, the discovery process can be overridden through a user-supplied JSON configuration.

##### Initial Config
An initial user-config file is created by the Installer and saved to your Metadata Lakehouse. The initial config includes only the minimally required information that the BQ Sync accelerator needs to run. Additionally, the default behaviors for this configuration are:
    - Discover and sync all tables using the autodiscovered config (within the in-scope Project/Dataset)
    - View discovery is disabled
    - Materialized View discovery is disabled

##### Customizing Config
Modifying the user-config starts by extending the system generated user-config. Download the config from your Metadata Lakehouse at the following path: <code>Files/BQ_Sync_Process</code>.

