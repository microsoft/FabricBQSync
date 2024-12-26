# Fabric BQ Sync Optimizations

One of the projects guiding pillars is to optimize each step of the sync process with a focus on both overall total cost and end-to-end performance. Most of these optimization are baked in by default while some must be enabled by the user. These optimizations generally have some form of trade-off that each user will have to weigh versus the optimizations gained.

### Approximate Row Counts

During the sync process from BigQuery to the Fabric Lakehouse, the accelerator calculate metrics like row counts and aggregates like evaluating the <code>max()</code> for watermark columns when configured.

The default mechanism for these operations involves caching the BigQuery dataset within Spark to avoid multiple roundtrips back to BigQuery and then calculating then required aggregates using standard Spark capabilities.

Approximate row counts provides a new path to calculate these metrics in a single pass using the native Spark SQL Observations capability. The observations API allows for standard-aggregation metrics to be calculated in-flight, eliminating the need to cache datasets and make multiple passes over the data.

In testing, this optimization substantiallly speeds up the sync (40-60% faster) with larger tables benefitting more from this optimization than smaller tables.

The potential side-effect of this optimization is that the row counts captured within the Fabric BQ Sync telemetry could be artifically inflated. This happens when Spark has tasks that fail or when a Spark task is re-run. 

If precision of the source row counts in the sync telemetry for auditing purposes is not important for your use-case, this optimization is recommended.

This optimization is enabled in the user config (see below) and applies to all tables within the the scope of the configuration.

**Configuration Example:**

<code>
    "optimization": {
        "use_approximate_row_counts": true
    }
</code>

### Big Query Standard API

The Fabric BQ Sync accelerator is built using the BigQuery Spark connector which leverages the storage api as the primary mechanism for data access. The storage api while unparallelled in the data throughput runs at a higher costs financially than the standard api and has inefficiencies when data is:

- Accessed through a View
- Requires a SQL query
- Filtered with a non-partition column

In the cases above, the storage api must first materialize the query or view result on the BigQuery side in temp tables before serving it through the api. When data volumes are small (<150mb) the standard api is not only more cost effective it also out performs the storage api. 

This occurs within the accelerator during the metadata sync where the BigQuery Information Schema tables are exposes as Views requiring them to be materialized into temp tables before serving. 

This optimization addresses this scenario by using the standard api instead of the storage api for metadata sync operations.

In testing, this optimization cut the time required for metadata sync in half while using slots and avoiding storage api costs.

This optimization is suitable for all users but those who have run syncs will see larger benefits in offsetting more frequent metadata sync.

To enable this optimize, enable <code>use_standard_api</code> flag in the user configuration file (see below). Note that the same GCP Service Account credentials are used for both apis and that all required permissions must be configured before enabling the feature.

**Configuration Example:**

<code>
    "gcp": {
    "api": {
            "use_standard_api": false
        }
    }
</code>

### Disable Spark Dataframe Caching
The accelerator makes use of Spark dataframe caching to avoid multiple roundtrips to BigQuery. In a limited number of cases when syncing very large datasets, it is beneficial for a performance perspective to disable caching on the Spark side and allow the roundtrip back to BigQuery when required.
This is most common during initial syncs for very large datasets. To use this optimization, set the <code>disable_dataframe_cache</code> flag as seen below.

**Configuration Example:**

<code>
    "optimization": {
        "use_approximate_row_counts": false,
        "disable_dataframe_cache": false
    }
</code>
