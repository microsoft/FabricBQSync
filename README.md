# BigQuery (BQ) Sync v2.0

This project is provided as an accelerator to help synchronize or migrate data from Google BigQuery to Fabric. The primary use cases for this accelerator are:
 - BigQuery customers who wish to continue to leverage their existing investments and data estate while optimizing their PowerBI experience and reducing overall analytics TCO
 - BigQuery customers who wish to migrate all or part of their data estate to Microsoft Fabric

# Getting Started for New Installs

The accelerator includes an automated installer that can set up your Fabric workspace and install all required dependencies automatically. To use the installer:
1. Download the current version Installer notebook
2. Import the installer into your Fabric Workspace
3. Attach the installer to a Lakehouse within the Workspace
4. Upload your GCP Service Account credential json file to OneLake
5. Update the configuration parameters:
	- <code>loader_name</code> – custom name for the sync operation used in dashboards/reports (ex: HR Data Sync, BQ Sales Transaction Mirror)
	- <code>metadata_lakehouse</code> - name of the lakehouse used to drive the BQ Sync process
	- <code>target_lakehouse</code> - name of the lakehouse where the BQ Sync data will be landed
	- <code>gcp_project_id</code> - the GCP billing project id that contains the in-scope dataset
	- <code>gcp_dataset_id</code> - the target BQ dataset name/id
	- <code>gcp_credential_path</code> - the File API Path to your JSON credential file (Example: /lakehouse/default/Files/my-credential-file.json")
6. Run the installer notebook

The installer performs the following actions:
- Create the required Lakehouses, if they do not exists
- Creates the metadata tables and required metadata
- Downloads the correct version of your BQ Spark connector based on your configured spark runtime
- Downloads the current BQ Sync python package
- Creates an initial default user configuration file based on your config parameters
- Installs a fully configured and ready to run BQ-Sync-Notebook into your workspace

# Upgrading to Current Version

The accelerator now includes an upgrade utility to simplify the process of upgrading your existing BQ Sync instance to the most current version. The upgrade utility handles major and minor updates. To use the upgrade utility:
1. Download the current version Upgrade notebook
2. Import the Upgrade notebook into your Fabric Workspace
3. Attach the Upgrade notebook to your BQ Sync metadata Lakehouse
4. Update the notebook parameters to point to your current configuration file.
5. Run te upgrade process

The upgrade process performs the following actions:
- Migrates your current configuration file (when necessary). Note that new features/capabilities are turned off by default. Your current configuration file is cloned and is not overwritten.
- Updates the BQ Sync metastore (when necessary). When schema changes are made to the BQ Sync metastore, the metastore is Optimized as part of the upgrade process.
- Downloads the current version of the BQ Sync package. If you are using environments or have otherwise optimized your environment it may be necessary to manually update your package repository.
- Downloads the current version of the Big Query spark connector (when available/necessary)
- Installed a new version of the BQ Sync notebook, mapped to the new configuration, python package and spark connector
  
# Project Overview

For many of our customers, the native mirroring capabilities in Fabric are one of the most exciting features of the platform. While Fabric currently supports a growing number of different mirroring sources, BigQuery is not yet supported. This current gap in capabilities is the foundation of this accelerator.

The goal of this accelerator is to simplify the process of synchronizing data from Google BigQuery to Microsoft Fabric with an emphasis on reliability, performance, and cost optimization. The accelerator is implemented using Spark (PySpark) using many concepts common to an ETL framework. The accelerator is more than just an ETL framework however in that it uses BigQuery metadata to solve for the most optimal way to synchronize data between the two platforms.

To best understand the accelerator, we can break it down into three logical parts: Configuration, Scheduling & Synchronization

# Configuration

The core of the accelerator is metadata, and this metadata drives the synchronization configuration and the overall approach for the BQ Sync process. Accelerator configuration happens first through auto-discovery using metadata from BigQuery and second through user-supplied configuration which allows for overriding or refining of the discovered configuration.

The auto-discovery process starts with a snapshot of the existing dataset metadata from the BigQuery INFORMATION_SCHEMA tables. This metadata defines each table’s structure, schema and BigQuery configuration. This accelerator defines a set of heuristics that when are applied, discover a probable best approach to table load strategy and load type.

While auto-discovery intends to simplify the process, we recognize that in some cases it’s not possible to optimize the synchronization process without domain knowledge of the dataset. To accommodate these scenarios, the discovery process can be overridden through user-supplied configuration.

The load strategies supported by the accelerator that can be either auto discovered or user-configured are:
- Full Load – Default load strategy where the entire table is synchronized from BigQuery. This strategy, while being least optimal, is used only when an alternate more optimal strategy can’t be resolved.
- Watermark – When a table is defined with a single primary-key that is either date/time or integer-derived data-typed, data can be loaded incrementally using the column as a watermark to determine the differential delta. This strategy can be combined with the partitioning strategy below to super-charge how data is incrementally synced in well optimized BigQuery environments.
- Partition – Partition tables can be loaded by partition with inactive (archived) partitions or partitions where no changes have occurred are skipped and not evaluated. The accelerator is capable of handle all types of partitioning that BigQuery supports. This includes Time and range and has special partitioning capabilities like time ingestion.

The accelerator also resolves load type based largely based on load strategy. The Load type defines how newly arriving data is added to previously synchronized data. These types are:
- Overwrite – Data for each table or partition, overwrites any existing data in the destination. This type is only used for Full and Partition load strategies.
- Append – Data is appended to existing data in the destination. This type is the default for the Watermark load strategy but can be applied to the Partition load strategy through user-configuration.
- Merge – The “upsert” pattern is used to either insert new data or update existing data in the destination based on a set of matching criteria. This type is only supported through user-configuration and is never auto discovered.

This configuration, whether auto discovered or user configuration, happens on the initial run and whenever new tables are added to the dataset if the accelerator is configured for auto discovery. Table configuration can be changed at any point prior to the initial data load. Once data has been loaded, the accelerator locks the table configuration going forward. If configuration changes are required after data synchronization has started, manual intervention is required.

# Scheduling

Longer term the accelerator will support robust scheduling capabilities that allow table-level schedule definition. Currently, only AUTO scheduling is supported. AUTO scheduling is evaluated on every active (enabled) table and looks at the BigQuery metadata to determine if there have been any changes to either the table or partition since the last successful synchronization. If the table or partition has not changed or it is using long-term (archived) BigQuery storage the scheduled run for the table/partition is skipped.

The schedule is driven by a user-configured table priority which determines the order in which tables are synchronized. Tables are synchronized in ascending order of priority starting with zero. Tables that have the same priority (i.e. the default priority which is 100) are grouped together into synchronization groups that control the flow of synchronization and allow for parallelism within the asynchronous synchronization process.
Load groups function as a mechanism to create dependencies when data is synchronized. An example of this using traditional data warehouse constructs would be to ensure that all dimensions are synchronized prior to loading any fact tables.

# Synchronization

Synchronization runs at object-level. Where an object can be a table, view, materialized view or even a table partition depending on previously described configuration process. Currently both sequential and asynchronous options are available for the synchronization process.
The sequential option synchronizes table-by-table, individually and is primarily provided as dev/test mechanism or when the process is run in an environment where data or resources are extremely constrained.

The asynchronous option allows for parallelism within the synchronization process, where multiple objects (tables, view, partitions, etc) can be synchronized concurrently. The degree of parallelism that can be achieved is largely dictated by Spark environment configuration but can be constrained or controlled through user-configuration.

Regardless of the approach used, the synchronization process follows these steps:
- Loads data from BigQuery using the BigQuery Storage API based on the configured load strategy.
- For partitioned tables, the partitioning strategy is mapped from the BigQuery support partitioning strategy to one optimized for the Lakehouse using Delta-Parquet as the underlying store.
- Writes the data to the configured Lakehouse based on both the defined load strategy and load type.
- Collects and stores telemetry about the synchronization process for visibility and auditability.

Once the data is synchronized, it is immediately available for downstream consumption in PowerBI and by any compute engine capable of talking directly to OneLake.

# Features & Capabilities

Within the accelerator there is an ever-growing set of capabilities that either offer feature parity or enhance & optimize the overall synchronization process. Below is an overview of some of the core capabilities:
- Multi-Project/Multi-Dataset sync support
- Table pattern-match filters to filter (include/exclude) during discovery
- Table & Partition expiration based on BigQuery configuration
- Synching support for Views & Materialized Views
- Support for handling tables with required partition filters
- BigQuery connector configuration for alternative billing and materialization targets
- Rename BigQuery tables and map to specific Lakehouse targets
- Rename or convert data types using table-level column mapping
- Shape BigQuery source with an alternate source sql query and/or source predicate
- Complex-type (STRUCT/ARRAY) handling/flattening
- Support for Delta schema evolution for evolving BigQuery table/view schemas
- Override BigQuery native partitioning with a partitioning schema optimized for the Lakehouse (Delta partitioning)
- Automatic Lakehouse table maintenance on synced tables
- Detailed process telemetry that tracks data movement and pairs with native Delta Time Travel capabilities

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
