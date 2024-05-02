# BigQuery (BQ) Sync

> This is a beta-release.

This project is provided as an accelerator to help synchronize or migrate data from Google BigQuery to Fabric. The primary target use cases for this accelerator are:
 - BigQuery customers who wish to continue to leverage their existing data estate while optimizing their PowerBI experience and reducing overall analytics TCO
 - BigQuery customers who wish to migrate all or part of their data estate to Microsoft Fabric

# Project Overview

For many of our customers, mirroring capabilities in Fabric are one of the most exciting features of the platform. While Fabric currently supports a growing number of different mirroring sources, BigQuery is not yet supported. This current gap in capabilities is the foundation of this accelerator.

The goal of this accelerator is to simplify the process of synchronizing data from Google BigQuery to Microsoft Fabric with an emphasis on reliability, performance, and cost optimization. The accelerator is implemented using Fabric Spark (PySpark) and many concepts common to an ETL framework with the added ability to resolve the most optimal way to synchronize data between the platforms. 

To best understand the accelerator, we can break it down into three logical parts: Configuration, Scheduling & Synchronization

> Before diving into the details, the scope for each instance of an accelerator is a single BigQuery dataset within a GCP Project. As we discuss configuration, scheduling, and synchronization, we are always constrained within a single BigQuery dataset. The accelerator can load multiple datasets but does not rationalize or recognize any dependencies beyond a dataset’s boundaries.

## Configuration

The core of the accelerator is metadata, and this metadata drives the configuration and approach for the BQ Sync process end-to-end. Accelerator configuration happens first through auto-discovery using metadata from BigQuery and second through user-supplied configuration which allows for overriding of refining of the discovered configuration.

The auto-discovery process starts with a snapshot of the existing dataset metadata from the BigQuery INFORMATION_SCHEMA tables. This metadata defines each table’s structure and schema and when heuristics are applied, we can discover a probable best approach to both table load strategy and load type.

While auto-discovery intends to simplify the process, we recognize that in some cases it’s not possible to optimize the synchronization process without domain knowledge of the dataset. To accommodate these scenarios, the discovery process can be overridden through user-supplied configuration.

The load strategies supported by the accelerator that can be either auto discovered or user-configured are:
-	Full Load – Default load strategy where the entire table is loaded from BigQuery. This strategy while being least optimal is used only whenever another strategy can’t be resolved.
-	Watermark – When a table is defined with a single primary-key that is either date/time or integer-derived data-typed, data can be loaded incrementally using the column as a watermark to determine the differential delta.
-	Partition – Partition tables can be loaded by partition with inactive (archived) partitions or partitions where no changes have occurred are skipped and not evaluated. 

The accelerator also resolves load type based largely based on load strategy. The Load type defines how newly arriving data is added to previously synchronized data. These types are:
-	Overwrite – Data for each table or partition, overwrites any existing data in the destination. This type is only used for Full and Partition load strategies.
-	Append – Data is appended to existing data in the destination. This type is the default for the Watermark load strategy but can be applied to the Partition load strategy through user-configuration.
-	Merge – The “up-sert” pattern is used to either insert new data or update existing data in the destination based on a set of matching criteria. This type is only supported through user-configuration and is never auto discovered.

This configuration, whether auto discovered or user configuration, happens on the initial run and whenever new tables are added to the dataset if the accelerator is configured to load all tables. Table configuration can be changed at any point prior to the initial data load. Once data has been loaded, the accelerator locks the table configuration going forward.

## Scheduling

Longer term the accelerator will support robust scheduling capabilities that allow table-level schedule definition. Currently, only AUTO scheduling is supported. AUTO scheduling is evaluated on every active (enabled) table and looks at the BigQuery metadata to determine if there have been any changes to either the table or partition since the successful last synchronization. If the table or partition has not changed or it is using long-term (archived) BigQuery storage the schedule run is skipped.

The schedule is driven by a user-configured table priority which determines the order in which tables are synchronized. Tables are synchronized in ascending order of priority starting with zero. Tables that have the same priority (i.e. the default priority which is 100) are grouped together into synchronization groups which controls the flow of synchronization and allows for parallelism within the  asynchronous synchronization process.


## Synchronization

Synchronization runs at either the table or partition-level depending on previously described configuration process. Currently both sequential and asynchronous options are available for the synchronization process.

The sequential option synchronizes table-by-table, individually and is primarily provided as dev/test mechanism or when the process is run in an environment where data or resources are extremely constrained.

The asynchronous option allows for parallelism within the synchronization process, where multiple tables and/or partitions can be synchronized at the same time. The degree of parallelism that can be achieved is largely dictated by Fabric Spark environment configuration but can be constrained or controlled through user-configuration.

Regardless of the approach used, the synchronization process follows these steps:
-	Loads data from BigQuery using the BigQuery Storage API based on the configured load strategy.
-	For partitioned tables, the partitioning strategy is mapped from the BigQuery support partitioning strategy to one optimized for the Fabric Lakehouse which uses Delta-Parquet as the underlying store.
-	Writes the data to the configured Fabric Lakehouse based on both the defined load strategy and load type.
-	Collects and stores telemetry about the synchronization process for visibility and auditability.

Once the data is synchronized to the Fabric Lakehouse it is immediately available for downstream consumption in PowerBI and by any other compute engines capable of talking directly to OneLake.



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
