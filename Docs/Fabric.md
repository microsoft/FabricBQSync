# Fabric Workspace

The Fabric Sync accelerator deploys into a Fabric Workspace. It is designed and built on standard Fabric capabilities with no requirement for special permissions.

### Fabric Lakehouse
The accelerator requires two Lakehouse in the Fabric Workspace:
- Metadata Lakehouse - Contains metadata and the configuration tables required to run Fabric Sync. In addition to configuration, telemetry and metrics are capture in this lakehouse which are useful for managing and monitoring the syncing of data between BigQuery and Fabric.
- Mirror Lakehouse - The initial target for your synced BigQuery data. Once synced, the data can be used directly as required or you can use Fabric Shortcuts to more broadly distribute the data.

<mark>Note: The metadata lakehouse and target lakehouse can be shared when the Fabric Destination tyoe is <code>LAKEHOUSE</code> and <code>enable_schemas</code> is <code>TRUE</code>. By default the metadata lakehouse tables with use the dbo schema and data mirrored from BQ will land in schemas that mirror the BQ dataset name.</mark>

### Fabric Spark
The accelerator runs on anysize Spark compute from 1-node to the max that your Fabric capacity allows based on your environment and workload. It tested and benchmarked using the out-of-the-box default Spark Starter Pool but can be tuned and optimized to sync data an efficiently as possible. While tuning is part art, part science a few guidelines to help you get started are provided below.

#### Spark Environment Optimizations
    - Larger clusters of smaller nodes, generally out perform smaller clusters of larger nodes
    - Disabled Autoscale. Consider a dedicate spark configuration with a fixed cluster size based on your workload needs
    - Disabled Dynamic Executor Allocation

### Notebook and Job Definition

The core of the accelerator runs on Fabric Spark compute with the default deployment being Notebook based. A Fabric Spark Job Definition is also available for those that want a more streamlined deployment option. The scripts required to deploy Fabric Sync as a Job Definition in your Workspace are availabled on [Github](https://github.com/microsoft/FabricBQSync).