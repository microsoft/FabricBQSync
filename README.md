# Fabric BQ (BigQuery) Sync

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
