# Data Expiration Policy Enforcement

Fabric Sync accelerator supports BigQuery data expiration policies for tables and partitions as part of a suite of data management capabilities. By default, this feature is off unless enabled within your User Configuration file.

<mark><b><u>Note</u></b>: Data expiration policy enforcement only applies to BigQuery Base Tables</mark>

### BigQuery Expiration Policies
- Tables can be configured with a expiration time after which both the table and its data is deleted. This policy can be set at the Dataset level and inherits down unless overrriden.
- Partitions that are time-unit based also support expiration policies where the policy is defined by a configured numbr of days calculated from the partition boundary. Like tables, this policy can be set at the Dataset level and is inherited.
- Table expiration policies override partition expiration policies.

### Enabling Data Expiration Policy Sync
Sync your Fabric environment with your BigQuery data expiration policies with the following steps:

1. Update the <code>enable_data_expiration</code> setting to <code>true</code>.
2. To enforce data expiration across all tables by default, update the <code>enforce_expiration</code> setting in the <code>table_defaults</code> to <code>true</code>.
3. For table-level control, adjust the <code>enforce_expiration</code> setting for each table in the <code>tables</code> config.

### Technical Overview
Fabric Sync will automatically track and sync your mirrored data to configured BigQuery data expiration policies. 

The sync happens in two steps, when enabled. 

1. As part of the metadata sync, the configured expiration policies are retrieved for all in-scope tables.
    - For tables, this metadata comes in the form of a fixed expiration timestamp.
    - Partition expiration must be calculated using the configured expiration days and the partition boundary.

    Expiration policies are tracked and updated with every metadata sync operation within the <code>sync_data_expiration</code> metadata table.
2. After the schedule load completes successfully, expiration policies are enforced if required.
    - Tables that expired are dropped from the Fabric Lakehouse. A dropped table requires no further maintenance consideration since the table and its underlying files are deleted.
    - Partitions that expired are deleted from the table. When partitions are deleted, table maintenance should be consider. The partition is removed from the table but OneLake storage is not recovered until a <code>VACUUM</code> is performed where the operation falls outside the retention window. For more information, please see the Data Maintenance docs.

### Example Configuration

Data expiration is enabled and a default is turned on by default for all tables using the <code>table_defaults</code>. Table 1 is turned on by default. Table 2 overrides the default behavior, and disables data expiration.
```
{
  "enable_data_expiration": true,
  "table_defaults": {
	"enforce_expiration": true,
  },
  "tables": [
		{
			"table_name": "<<TABLE 1>>"
        },
        {
			"table_name": "<<TABLE 2>>",
			"enforce_expiration": false
        }
	]
}
```