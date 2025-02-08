# Fabric Sync for BigQuery

This project is provided as an accelerator to help mirror or synchronize data from Google BigQuery to Fabric. The primary use cases for this accelerator are:
 - BigQuery customers who wish to continue to leverage their existing investments and data estate while optimizing their PowerBI experience and reducing overall analytics TCO
 - BigQuery customers who wish to migrate all or part of their data estate to Microsoft Fabric

# Project Overview

For many of our customers, the native mirroring capabilities in Fabric are one of the most exciting features of the platform. While Fabric currently supports a growing number of different mirroring sources, BigQuery is not yet supported. This current gap in capabilities is the foundation of this accelerator.

The goal of this accelerator is to simplify the process of synchronizing data from Google BigQuery to Microsoft Fabric with an emphasis on reliability, performance, and cost optimization. The accelerator is implemented using Spark (PySpark) using many concepts common to an ETL framework. The accelerator is more than just an ETL framework however in that it uses BigQuery metadata to solve for the most optimal way to synchronize data between the two platforms.

# v2.1 
Version 2.1 adds support for:
- Lakehouse Schemas (Fabric Preview)
- Mirrored Database/Open Mirroring (Fabric Preview)
- CDC APPEND and CDC Load Strategies (BigQuery Preview)
- Shared metastore support
- Installer Updates
	- Automatic mapping Notebook configuration
	- Create/Configure Fabric Spark Environment
- Inline/Automatic Upgrades for v2.0 and greater
- New defaults:
	- <code>use_cdc</code> enabled by default
	- GCP API <code>use_standard_api</code> enabled by default
	- Optimization <code>use_approximate_row_counts</code> enabled by default

# Getting Started

The accelerator includes an automated installer that can set up your Fabric workspace and install all required dependencies automatically. To use the installer:

1. Download the current version [Installer notebook](./Notebooks/v2.0.0/Fabric-Sync-Installer.ipynb)
2. Import the installer into your Fabric Workspace
3. Attach the installer to a Lakehouse within the Workspace
4. Upload your GCP Service Account credential json file to OneLake
5. Update the configuration parameters:
	- <code>loader_name</code> – custom name for the sync operation used in dashboards/reports (ex: HR Data Sync, BQ Sales Transaction Mirror)
	- <code>metadata_lakehouse</code> - name of the lakehouse used to drive the Fabric Sync process
	- <code>target_lakehouse</code> - name of the lakehouse where your BQ data will be synced to
	- <code>gcp_project_id</code> - the GCP billing project id that contains the in-scope dataset
	- <code>gcp_dataset_id</code> - the target BQ dataset name/id
	- <code>gcp_credential_path</code> - the File API Path to your JSON credential file (Example: /lakehouse/default/Files/my-credential-file.json")
	- <code>enable_schemas</code> - flag to enable Fabric lakehouse schemas (Schemas REQUIRED for Mirrored Databases)
	- <code>target_type</code> - Fabric LAKEHOUSE or MIRRORED_DATABASE
	- <code>create_spark_environment</code> - flag to create a Fabric Spark environment as part of installation
	- <code>spark_environment_name</code> - name for Fabric Spark Environment item
6. Run the installer notebook

The installer performs the following actions:
- Creates the Fabric Sync metadata Lakehouse, if it does not exist
- Creates the Fabric Sync mirror target (LAKEHOUSE or MIRRORED_DATABASE), if it does not exist
- Creates the metadata tables and required metadata
- Downloads the correct version of your BQ Spark connector based on your configured spark runtime
- Creates an initial default user configuration file based on your config parameters
- Creates a Fabric Spark Environment with required libraries, if configured
- Installs a fully configured and ready-to-run Fabric-Sync-Notebook into your workspace

# Automatic Version Upgrades
The Fabric Sync Accelerator will automatically upgrade itself as new runtime versions are added. If you are using PyPi to load the FabricSync package and allow the latest version of package to be pulled.The accelerator will keep your metastore and configuration up-to-date automatically.


<mark>Note that behaviors and defaults for existing configurations do not change. Any updates to default behaviors will only apply to new configurations or when manually changed. Performance optimizations will apply to  all configurations. Please see the [Release Log](Docs/ReleaseLog.md) for the latest.</mark>


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