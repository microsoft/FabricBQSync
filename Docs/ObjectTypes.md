# BigQuery Object Types

The Fabric Sync Accelerator uses three BigQuery object types as datasources for the synchronization process. Each object type behaves differently and supports different capabilities within the accelerator.

### Base Tables
BigQuery Base Tables are the standard table-type in which structured data is stored directly in BigQuery storage. Data stored in Base Tables can use all of the functions and capabilities provided by the accelerator.

For data synchronization into Fabric, Base Tables support all load strategies including <code>FULL</code>, <code>WATERMARK</code>, <code>PARTITION</code> and <code>TIME_INGESTION</code>.

### Views
BigQuery Views are logical tables defined by a SQL Query. Data can be synchronized from Views into Fabric using a limited-set of the Fabric Sync Accelerator capabilities and are treated as virtualized tables with minimal metadata.

---
***Note:***
Views cannot be accessed directly by the BigQuery Storage API. The BigQuery Spark Connector handles this limitation by materializing the View to a temporary table in BigQuery. The GCP Project and BigQuery Dataset used for materialization can be configured using User Configuration. Please see the [BigQuery API & Spark Connector documentation](BigQuery.md).

---

For data synchronization, Views support only <code>FULL</code> and <code>WATERMARK</code> load strategies. Additionally, Views are synchronized on every scheduled run since no metadata is available to avoid unnecessary runs.

Other unsupported capabilities for Views are:
- Data Expiration
- Partitions

### Materialized Views
Like Views, Materialized Views are defined by a SQL Query and are differentiated in that they are pre-computed or materialized to BigQuery storage.

Materialized Views have the same limitations as Views within the Fabric Sync Accelerator except that each materializes view provides a <code>last_refresh_time</code> in the metadata which can be used to avoid unnecessary schedule runs