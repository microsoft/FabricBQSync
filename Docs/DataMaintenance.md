# Fabric & OneLake Data Maintenance

While BigQuery automatically handles many of the routine data management tasks required, some thought and consideration is required for managing your synced or mirrored data stored in a Fabric Lakehouse (OneLake).

### Delta
The native-format for data written to OneLake is the Linux Foundation Open-Source Delta format ([https://delta.io/](https://delta.io/)).

The Delta format provides many features which Fabric Sync utilizes including ACID transactions, DML operations, schema evolution and versioning through time-travel as a mechanism for auditing and rollbacks.

While your Delta table provides these and many other useful features, at its core, is parquet files. Parquet by natue is immutable, meaning onces its written it is never updated.

Instead for caoabilities like DML operations where data is frequently added, updated or deleted, new parquet files are added to your table but the files are never re-opened or re-appended to. 

In case where data has changed or is deleted, some parquet files may become out of scope. The out of scope files are not automatically cleaned-up and instead provide the basis for the Delta time-travel capabilities. 

Both of these behaviors which derived from parquet's immutable property can lead to file fragmentation, skew in file sizes and unexpected data bloat if not managed.

To address these scenarios, Delta has built-in capabilites to <code>OPTIMIZE</code> or compact the underlying parquet files and <code>VACUUM</code> to clean-up out of scope files that are no longer needed.

### Data Maintenance for Fabric Lakehouse
Theere are a number of options and approaches for manual or semi-automatic data maintenance at the table-level within Fabric. The Fabric Sync accelerator provides two built-in options for handling data maintenance as part of your BigQuery sync process.

1. Schedule Maintenance
    
    Scheduled maintenance is simple time-based maintenance where OPTIMIZE and VACUUM are run for your table at pre-defined intervals. The schedule-based intervals available are:

        - AUTO - Every schedule run
        - DAY - 24 hours
        - WEEK - 7 days
        - MONTH - 30 days
        - QUARTER - 90 days
        - YEAR - 365 days
    
    The intervals are time-period based and maintenance is calculated from the last time maintenance was performed. For example, if maintenance was last run on January 1st @ 8:00 AM then with a DAY scheduled it would be eligible to run again January 2nd @ 8:00am. If maintenance has never been run for a table, it will run immediately in the next maintenance window.

2. Intelligent Maintenance

    Intelligent maintenance utilizes an inventory of the Delta log and configurable threshholds to determine if maintenance should be run for any given table. 
    
    This adaptive process looks at data growth, number of files, file size overall and table size including out-of-scope files to be more selective for maintenance operations. 
    
    Intelligent maintenance can specify either <code>OPTIMIZE</code>, <code>VACUUM</code> or both when configured thresholds are exceeded. Thresholds that influence or trigger this smart maintenance process are:

    - <code>rows_changed</code> - ratio of rows that changed (inserted, updated or delete) versus the total table rows. This triggers an <code>OPTIMIZE</code>.
    - <code>table_size_growth</code> - percentage growth in overall table size. This triggers an <code>OPTIMIZE</code>.
    - <code>file_fragmentation</code> - ratio of files that are not optimally sized. This triggers an <code>OPTIMIZE</code>.
    - <code>out_of_scope_size</code>- ratio of out of scope data to total table size. This triggers an <code>VACUUM</code>. 
    
    Intelligent maintenance runs a storage inventory process against the Delta Log to run and collect the Delta metadata for each table in you mirrored Lakehouse. This process runs as part of the Fabric Sync Data Maintenance process.

    <mark><b><u>Note:</u></b> The storage inventory data is collected in your Fabric Sync Metadata Lakehouse and can be used for further analysis about your OneLake storage usage.</mark>

### Enabling Fabric Sync Data Maintenance

Data Maintenance is disabled by default and must be enabled in the User Configuration file.

To enable maintenance at the application level, set the <code>maintenance</code>.<code>enabled</code> setting to <code>true</code>. Individual Fabric Lakehouse tables are <code>enabled</code> using <code>table_defaults</code> or in the <code>tables</code> config.

#### Intelligent Maintenance

The default maintenance <code>strategy</code> is <code>SCHEDULED</code>. To enable Intelligent Maintenance, set the <code>strategy</code> setting to <code>INTELLIGENT</code>. 

Setting the strategy to <code>INTELLIGENT</code> enables the required storage inventory sync. The <code>interval</code> setting and any defined interval defaults should be set to <code>NONE</code>.

#### Time-based Maintenance

The default maintenance <code>interval</code> for time-based maintenance is <code>MONTH</code>. This default value can be overridden using the <code>table_defaults</code> or directly in the <code>tables</code> config. Valid values are <code>DAY</code>, <code>WEEK</code>, <code>QUARTER</code>, <code>MONTH</code>, <code>YEAR</code> and <code>NONE</code>.

#### Overlapping Strategies

The Fabric Sync Data Maintenance process is designed such that its possible for the time-based and intelligent data maintenance startegies to overlap. This allows the system to maintain tables based on thresholds while allowing for semi-regular schedule (i.e. quarterly, yearly) maintenance if required.

To overlap strategies, it is only necessary to define the desired <code>interval</code> setting at the desired level of your User Configuration files.

### Optimize
The <code>OPTIMIZE</code> operation re-writes a table's or partition's files to compact them down to an optimal size (250MB by default). Referred to as bin packing, this operation reduces fragmention of data and compacts smaller files into fewer larger files.

 - Time-based maintenence runs <code>OPTIMIZE</code> at the table-level for all Lakehouse Tables sourced from non-partitioned BigQuery Base Tables, Views and Materialized Views. It runs at the partition-level when the BigQuery Base Table is partitioned and the native source partitioning is not overridden.
 - Intelligent maintenance runs <code>OPTIMIZE</code> at the partition when applicable regardless of the BigQuery source. If the Lakehouse table is not partitioned the <code>OPTIMIZE</code> operation runs at the table-level.

 ### Vacuum
The <code>VACUUM</code> operation prunes all files from the Delta table directory that are out of scope. Out of scope is defined as a file that has been logically removed from the Delta transaction log and whose retention period has passed.

The Fabric Sync accelerator uses a default <code>retention_hours</code> of <code>0</code>. This prevents any history from being preserved while minimizing the overall TCO of the mirrored data.

If you want to leverage Delta Time Travel capabilities or has support for versioning and rollbacks, it is recommended that you adjust the <code>retention_hours</code> setting to <code>168</code> for 7 days for history.

<mark><b><u>Note: </u></b> Defining a large retention peiod can significant degrade overall performance for large tables. It could also substantially increase the overall OneLake storage cost</mark>

### Example Configurations
#### Scheduled Maintenance

Scheduled maintenance with Week default and a history retention default of 0 hours. Table 1 use the default configuration. Table 2 overrides the default to a Monthly period. Table 3 disables maintenance.

```
{
  "maintenance": {
	"enabled": true,
	"interval": "WEEK",
	"strategy": "SCHEDULED",
	"retention_hours": 0,
  },
  "tables": [
		{
			"table_name": "<<TABLE 1>>"
        },
        {
			"table_name": "<<TABLE 2>>",
			"table_maintenance": { 
			  "enabled": true,
			  "interval": "MONTH"
			}
        },
        {
			"table_name": "<<TABLE 3>>",
			"table_maintenance": { 
			  "enabled": false
			}
        }
	]
}
```
#### Intelligent Maintenance

Intelligent maintenance with history retention set for 7-days (168 hours). The thresholds are set to allow table rows, table size in MB and fragmentation to reach 50% before <code>OPTIMIZE</code> is triggered. <code>VACUUM</code> is triggered when the out-of-scope file size is 3x the table size.

In this example, Table 2 ensures regular monthly maintenance is performed if the thresholds are never met.
```
{
  "maintenance": {
	"enabled": true,
	"interval": "AUTO",
	"strategy": "INTELLIGENT",
	"retention_hours": 168,
    "thresholds": {
		"rows_changed": 0.5,
        "table_size_growth": 0.5,
        "file_fragmentation": 0.5,
        "out_of_scope_size": 3.0
    }
  },
  "table_defaults": {
    "table_maintenance": { 
	  "enabled": true,
      "interval": "AUTO"
    }
  },
  "tables": [
		{
			"table_name": "<<TABLE 1>>"
        },
        {
			"table_name": "<<TABLE 2>>",
			"table_maintenance": { 
			  "enabled": true,
			  "interval": "MONTH"
			}
        }
	]
}
```
