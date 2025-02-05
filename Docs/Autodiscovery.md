# Fabric Sync AutoDiscovery

The autodiscovery process is responsible for applying a set of built-in huerstics to the available BigQuery metadata to discover the most optimal approach for syncing data between BigQuery and the Fabric Lakehouse.

### Load Stratgies
Load strategies determine how data is going to be load from BigQuery. The following strategies can be either auto discovered or specified in user-configuration:
- *Full Load* – Default load strategy where the entire table and all of its data are synchronized from BigQuery. This strategy, while being least optimal, is used only when an alternate more optimal strategy can’t be resolved.
- *Watermark* – When a table is defined with a single primary-key that is either date/time or integer-derived data-typed, data can be loaded incrementally. The key column is used as a watermark to determine the delta between BQ and Fabric. This strategy can be combined with the partitioning strategy below to super-charge how data is incrementally synced in well optimized BigQuery environments.
- *Partition* – Partition tables can be loaded by partition. Inactive (archived) partitions or partitions where no changes have occurred are skipped and are not evaluated. The accelerator is capable of handle all types of partitioning that BigQuery supports including tables that use time ingestion or range partition startegies.
    - The autodiscovery process will also discover when BQ tables are configured to require partition filters. While the sync process adapts for this constraint it may have reduce performance during the initial load process. To optimize the initial load of historical data, consider removing the <code>Require Partition Filter</code> constraint from the table during the initial loading, if possible.

### Load Types
The accelerator also resolves load type based largely based on load strategy. The Load type defines how newly arriving data is loaded tp the Fabric Lakehouse. These types are:
- Overwrite – Data for each table or partition, overwrites any existing data in the destination. This type is used for Full and Partition-based load strategies.
- Append – Data is appended to existing data in the Fabric Lakehouse. This is the default for the Watermark load strategy but can be applied to the Partition load strategy through user-configuration override.
- Merge – The “upsert” pattern is never autodiscovered but can be defined within user configuration. Merge is used to either insert new data or update existing data in the destination based on a set of matching criteria. 