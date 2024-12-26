# Fabric BQ Synchronization

Synchronization runs at object-level. Where an object can be a table, view, materialized view or even a table partition depending on previously described configuration process. Currently both sequential and asynchronous options are available for the synchronization process.
The sequential option synchronizes table-by-table, individually and is primarily provided as dev/test mechanism or when the process is run in an environment where data or resources are extremely constrained.

The asynchronous option allows for parallelism within the synchronization process, where multiple objects (tables, view, partitions, etc) can be synchronized concurrently. The degree of parallelism that can be achieved is largely dictated by Spark environment configuration but can be constrained or controlled through user-configuration.

Regardless of the approach used, the synchronization process follows these steps:
- Loads data from BigQuery using the BigQuery Storage API based on the configured load strategy.
- For partitioned tables, the partitioning strategy is mapped from the BigQuery support partitioning strategy to one optimized for the Lakehouse using Delta-Parquet as the underlying store.
- Writes the data to the configured Lakehouse based on the configured load type.
- Collects and stores telemetry about the synchronization process for visibility and auditability.

Once the data is synchronized, it is immediately available for downstream consumption in PowerBI and by any compute engine capable of talking directly to OneLake.