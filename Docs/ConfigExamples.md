# Fabric Sync Configuration Examples

Example Index

1. [Disable Autodiscover for Production](#disable-autodiscover-for-production)
2. [Multiple GCP Projects/Multiple GCP Datasets](#multiple-gcp-projectsmultiple-gcp-datasets)
3. [Set Materialization Project/Dataset](#set-materialization-projectdataset)
4. [Set GCP Project for Billing](#set-gcp-project-for-billing)
5. [BigQuery API Selection (Standard/Storage)](#bigquery-api-selection)
6. [Configuring BQ Object for Watermark Loads](#configuring-bq-object-for-watermark-loads)
7. [Configuring BQ Object for Merge Load Type](#configuring-bq-object-for-upsertmerge-load-type)
8. [Configuring Base Table for CDC](#configuring-base-table-for-cdc)
9. [Fabric Mapping](#fabric-mapping)

#### Disable Autodiscover for Production
After initial set-up and testing, the autodiscover process can be disabled when the Fabric Sync configuration is stable and further changes are not expected. Disabling autodiscovery, optimizes metadata operations when the sync process is schedule to run frequently.

<code>
{
    ...
    "autodiscover": {
        "autodetect": false,
        ...
    }
    ...
}
</code>

#### Multiple GCP Projects/Multiple GCP Datasets
The Fabric Sync accelerator can mirror data from multiple GCP projects and multiple BigQuery datasets within a single configuration instance. In cases where multiple projects/datasets are configured, the first project and first dataset are treated as the default.

In the example below, two projects and three datasets are configured for discover. If the configuration is set to <code>load_all</code> all three datasets across both projects would be in scope. In this example, one table from each dataset is defined in the table configuration.

<code>
{
    ...
    "gcp": {
	    "projects": [
        {
            "project_id": "Project1",
                "datasets": [
                    { "dataset": "DatasetA" },
		            { "dataset": "DatasetB" }
                ]
            },
            "project_id": "Project2",
                "datasets": [
                    { "dataset": "DatasetC" }
                ]
            }
        ],
        ...
    },
    ...
    "tables": [
        {
            "project_id": "Project1",
            "dataset"; "DatasetA",
		    "table_name": "TableA"
        },
        {
            "project_id": "Project1",
            "dataset"; "DatasetA",
		    "table_name": "TableB"
        },
        {
            "project_id": "Project2",
            "dataset"; "DatasetC",
		    "table_name": "TableC"
        }
    ]
}
</code>

#### Set Materialization Project/Dataset
When mirroring Views or Materialized Views through the BigQuery Storage API, the data must be materialized first into BigQuery temporary tables. In instances where the GCP credentials don't or can't have write permissions to the Project or Datasets where the data lives, an alternate location for materialization can be configured.

<code>
{
    ...
    "gcp": {
        ...
	    "api": {
		    "materialization_project_id": "ALTERNATE_PROJECT_ID",
		    "materialization_dataset": "ALTERNATE_DATASET_ID",
            ...
	    },
        ...
    }
    ...
}
</code>

#### Set GCP Project for Billing
By default the various BigQuery transactions and API costs is billed to the default project. The transactional GCP costs associated with mirroring can be billed to a specific GCP Project through GCP configuration.

<code>
{
    ...
    "gcp": {
        ...
	    "api": {
            ...
		    "billing_project_id": "BILLING_PROJECT_ID",
            ...
        },
        ...
    },
    ...
}
</code>

#### BigQuery API Selection
The Fabric Sync accelerator used both the Standard and Storage BigQuery APIs. By default, the Standard API is used for metadata operations and the Storage API is used for all mirrored data movements. The Standard API can be enabled or disabled through GCP configuration. Setting the <code>use_standard_api</code> flag to <code>True</code> turns on the Standard API, <code>False</code> disables it.

<code>
{
    ...
    "gcp": {
        ...
	    "api": {
		    "use_standard_api": false
	    },
        ...
    },
}
</code>

#### Configuring BQ Object for Watermark Loads
A BigQuery object can be manually configured for incremental synchronization when the autodiscovery process was not able to automatically resolve it. After defining the <code>load_strategy</code> and <code>load_type</code>, configure the column to be used for defining the watermark value. Only a single watermark column is supported and it must be an integer-derived, Timestamp or Date/Time datatype.

In this example, the OnlineCatalogOrders view is configured for incremental synchronization. The ETLLoadDate column will be used as the watermark column. During the mirroring process, records which are more current then the last recorded ETLLoadDate will be appended to the mirrored data.

<code>
{
    ...
    "tables": [
		{
			"table_name": "OnlineCatalogOrders",
            "object_type": "VIEW",
            "load_strategy": "WATERMARK",
            "load_type": "APPEND",
            "watermark": {
                "column": "ETLLoadDate"
            }
        }
    ]
}
</code>

#### Configuring BQ Object for Upsert/Merge Load Type
For instances where BigQuery source data should be merged into the mirrored data, manual configuration is required. The merge load type can be used with any load strategy and requires one or more keys to be defined for the BigQuery object. The keys are used as the matching criteria for the merge operation. Currently, only UPSERT is supported for merge operations except when full change data capture (CDC) is configured.

In this example the Customers table is configured with a watermark load strategy using the ETLLoadDate column and a load type of Merge. During synchronization, records that have a more current ETLLoadDate than the last recorded watermark will the UPSERT'd into the mirrored data.

The UPSERT uses the CustomerKey and RegionKey columns as a compsite key. When a record is not matched by the destination it is inserted as a new record. When a record matches the key, an update is performed.

<mark>Note that outside of the CDC load strategy deletes are never applied by the MERGE load type.</mark>

<code>
{
    ...
    "tables": [
		{
		    "table_name": "Customers",
			"load_strategy": "WATERMARK",
			"load_type": "MERGE",
			"keys": [
                {"column": "CustomerKey"},
                {"column": "RegionKey"}
            ],
			"watermark": {
				"column": "ETLLoadDate"
			}
		}
    ]
}
</code>

#### Configuring Base Table for CDC
While CDC can be autodiscovered, it can also be enabled manually for BigQuery Base Tables in the Fabric Sync accelerator. First, one the BigQuery side, the <code>enable_change_history</code> option must be set on the table.

<code>
ALTER TABLE Project1.Sales.OnlineCatalogOrders SET OPTIONS(enable_change_history=TRUE);
</code>

After the BigQuery table option is configured. The user configuration for the accelerator can be updated. The <code>load_strategy</code> is set to <code>CDC</code>, <code>load type</code> is <code>MERGE</code> and one or more keys are defined for table.

When CDC transactions are synchronized from BigQuery, records are de-dup based on the keys provided. The last CDC transaction is appplied to the mirrored data.

<code>
{
    ...
    "tables": [
		{
		    "table_name": "OnlineCatalogOrders",
			"load_strategy": "CDC",
			"load_type": "MERGE",
			"keys": [
                {"column": "CustomerKey"},
                {"column": "RegionKey"}
            ]
		}
    ]
}
</code>

#### Fabric Mapping
When a BigQuery object is mirrored the BigQuery dataset and table name are mapped to the schema and destination table name, by default. Further, for partitioned Base Tables the BigQuery partitioning schema is mirrored within Fabric. The default behavior can be overriden and custom mapping and alternate partitioning can be applied during the mirroring process through table configuration.

In the example below, the OnlineCatalogOrders table is mapped to the SalesMirror Lakehouse, using the OnlineSales schema and the mirrored table name is defined as Catalog_Sales. The mirrored table is also configured to be using the InvoiceDateMonth as the partition key which overrides any previous partitioning strategy.

<code>
{
    ...
    "tables": [
		{
            "dataset": "CompanySales",
		    "table_name": "OnlineCatalogOrders",
            "lakehouse_target": 
			{
                "lakehouse_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef",                                
                "lakehouse": "SalesMirror",
                "schema": "OnlineSales",
                "table_name": "Catalog_Sales",
				"partition_by": "InvoiceDateMonth"
			},
			...
		}
    ]
}
</code>
