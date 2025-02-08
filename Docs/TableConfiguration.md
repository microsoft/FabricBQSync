# Table Configuration

Table Configuration applies to BigQuery Object Types including Base Tables, Views and Materialized Views.

---

***Note:***
Table Configuration can be influenced and optimized through the use of Table Defaults. For more information see the documentation on Table Defaults.

---

### Required Configuration

GCP Project Id, Dataset, Table Name and Object Type are all required configuration that must be supplied either directly or through the Table Defaults. Required configuration is used for matching and mapping configuration to BigQuery metadata.

Table Configuration that cannot be matched to BigQuery metadata are ignored.

### Source Shaping and Filtering

In some cases you may need to shape the BigQuery data results before synchronizing it into the Fabric Lakehouse. Some examples of this could be:

- Altering the query results project (Adding/Removing Columns)
- Renaming columns
- Changing data types
- Enriching through joins or other BQ SQL capabilities

You can shape the results returned for the BigQuery object using the <code>source_query</code> setting. This setting accepts a complete, valid BigQuery SQL query with fully-qualified table names. The fully-qualified table name is provided in the <code>project_id.dataset.table</code> format. The query is not validated before being passed to BigQuery. Invalid queries will raise a <code>BQConnectorError</code> exception.

In some cases it may be necessary to filter the BigQuery source. A common example of this occurs when the BigQuery source has more historical data than is required for the synchronized workload. In this case it is not necessary to specify the full SQL query. Instead, a filter can be provided using the <code>predicate</code> setting.

The predicate setting accepts any valid BigQuery WHERE clause SQL-constraint (see example below). WHERE should not be provided as part of the predicate. The accelerator handles query-building and will compose the query to ensure the predicate is applied correctly.

```
"source_query": "SELECT * FROM example_project.sales.transactions WHERE type='WEB'"

"predicate": "country_region='APAC'"
```

### Destination Mapping
By default, tables are mapped to the default lakehouse using the source table name. If schemas are enabled, the dataset name is used as the schema. You can override this behavior configuring the <code>lakehouse_target</code>:

- <code>lakehouse_id</code> - the UUID of the Fabric Lakehouse/Mirrored Database
- <code>lakehouse</code> - the display name of the Fabric Lakehouse/Mirrored Database
- <code>schema</code> - lakehouse schema
- <code>table_name</code> - the destination table name

### Fabric Partitioning

The default behavior for the Fabric Sync Accelerator is to reproduce the BigQuery partitioning-schema within the Fabric Lakehouse, when applicable. If you need to alter or change the partitioning schema in the Lakehouse, you can override the BigQuery partitioning schema using the <code>lakehouse_target</code>.<code>partition_by</code> setting.

One or more columns can be provided as a comma-separated list of names. Column names should match the destination schema.

Fabric Partitioning can be applied to Base Tables, Views and Materialized View. The BigQuery does not need to be partitioned to use this feature.

### Schema Evolution

Schema evolution allows for columns to be added, removed, renamed and even data types changes. By default schema evolution is disabled but can be enabled by setting the <code>allow_schema_evolution</code> to TRUE.

When enabled, the accelerator uses the built-in Delta schema evolution capabilities. This allows for:
- Addition of new columns
- Data type conversions when the data type is widened (see [Delta Type Widening documentation](https://docs.delta.io/latest/delta-type-widening.html))

Columns can also be dropped or renamed automatically when <code>'delta.columnMapping.mode' = 'name'</code> property is manually added to the Delta table <code>TBLPROPERTIES</code>. Please see the example below.

```
ALTER TABLE example_lakehouse.transactions SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
)
```

For data type conversions that are not supported by type-widening, manual adjusts are required to support the new data type. This will also require, re-writing the all the destination data.

### Complex-Type Handling

Google BigQuery has robust support for complex-types that are often use to increase data density and optimize BigQuery workloads. Complex-types can be synchronized as is and are support natively by the Delta storage format.

To date however, the Fabric Lakehouse does not support complex-types. Working around this current limitation, involves flattening data out of complex-types.

Flattening can be done on the BigQuery side using a View or SQL query or by the Fabric Sync Accelerator directly when the complex-types are either <code>STRUCT</code> or <code>ARRAY</code> types. 

The relevent settings for complex-type handling are:
- <code>flatten_table</code> - when TRUE, <code>STRUCT</code> types are flattened using dot-notation for column names.
- <code>explode_arrays</code> - when TRUE, <code>ARRAY</codes> types are exploded out to rows within the table.
- <code>flatten_inplace</code> - when TRUE, complex types are flattened in-place into the existing table. Setting to false creates a parallel table with the flattened types. The naming schema for the parallel table follows the: <code>[TABLE NAME]_flattened</code> format.

### Column Mapping

Columning mapping allows for column-level in-flight changes to the BigQuery object data schema. For more information please see the [Column Mapping documentation](ColumnMapping.md).