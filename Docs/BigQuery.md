# BigQuery & GCP Requirements
There are only a few requirements needed to enable Fabric Sync.
1. BigQuery Storage API - enabled by default for all GCP Projects where BigQuery is enabled.
2. GCP Service Account 
3. Service Account Permissions
4. GCP Storage Bucket (when using BQ EXPORT DATA to sync data)

### GCP Service Account
The Fabric Sync accelerator uses GCP Service Accounts for authentication and authorization to GCP and required BigQuery data and APIs. 

Authentication for Service Accounts credentials are provided through a GCP issued JSON-formatted credential file. The Fabric Sync installer, encodes and embeds the JSON crendential file into the Fabric Sync configuration file using Base 64 encoding. 

If you require key-rotation or have other requirements don't allow for embedding the credential file into the accelerator config, Fabric Sync supports loading the Service Acoount credential file from a local path within the Fabric Lakehouse.

### IAM Roles & Permissions
Foundationally the Service Account will need permissions to the datasets amd data within scope for Fabric Sync.

Additionally, the Service Account needs the following:
- BigQuery Job permissions
- Access to the <code>INFORMATION_SCHEMA</code> tables for each in-scope datasets
- Write permissions to atleast one BigQuery Dataset. This dataset can be separate from the data being synced and is used to materialize Views to BigQuery temp tables.
    - When a separate dataset is used for View materialization, update or set the <code>materialization_project_id</code> and <code>materialization_dataset</code> user configuration settings with the correct values.

### APIs
Fabric Sync is capable of using both the BigQuery Storage API and the standard BigQuery API. The Storage API is the default API used for all BQ operations. To learn about how and when the standard BigQuery API can be used by the accelerator please see the [Optimization](Optimizations.md) documentation.

### BQ EXPORT DATA and GCP Buckets
The BigQuery EXPORT DATA statement exports query results using a BigQuery Job to a storage bucket (and other cloud stores). The Fabric Sync Accelerator uses EXPORT DATA to sync incremental data to cloud storage in PARQUET format. The PARQUET data is then moved to either a OneLake Lakehouse or Fabric Mirrored Database (depending on configuration).

Using EXPORT DATA requires a GCP Storage Bucket where with the following requirements:
1. The Fabric Sync Service Account has the <code>roles/storage.objectAdmin</code> and <code>roles/storage.admin</code> roles.
2. The Protection Policy is set to disable disable Soft Deletes.

### BigQuery Spark Connector

##### Spark Connector API Optimization
The Spark connector is highly-configurable through settings. The following settings can be tuned to optimize the sync of data using the accelerator.

<table>
    <tr>
        <th>Setting</th>
        <th>Vaue</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>preferredMinParallelism</td>
        <td>600</td>
        <td>Preferred min. number of partitions data will be split into. May be less for small datasets or if resource constraints exists.</td>
    </tr>
    <tr>
        <td>responseCompressionCodec</td>
        <td>RESPONSE_COMPRESSION_CODEC_LZ4</td>
        <td>Read response compression, default is no compression.</td>
    </tr>
    <tr>
        <td>bqChannelPoolSize</td>
        <td>80 (see description)</td>
        <td>Size of the gRPC channel pool created by the BigQueryReadClient. Should atleast be set to the # of executor cores configured for your Spark cluster.</td>
    </tr>
</table>

For big data syncs which are expected to run for long periods of time, consider applying the following settings to avoid BigQuery connection and/or read timeouts.
<table>
    <tr>
        <th>Setting</th>
        <th>Vaue</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>httpConnectTimeout</td>
        <td>0</td>
        <td>BigQuery Connection timeout. 0 sets to infinite.</td>
    </tr>
    <tr>
        <td>httpReadTimeout</td>
        <td>0</td>
        <td>BigQuery Read timeout. 0 sets to infinite.</td>
    </tr>
</table>

For an extended look at all the BigQuery Spark Connector settings, please visit the connector [documentation](https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#properties).