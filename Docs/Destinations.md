# Fabric Mirror Destinations

Fabric Sync supports both Lakehouse and Mirrored Databases for mirrored data.

### Lakehouse
Lakehouse is the default destination for Fabric Sync. The Lakehouse can be pre-provisioned or is created by the Fabric Sync Installer.

#### Lakehouse Schemas
Lakehouse schemas are supported and can be enabled from the Fabric Sync Installer. 

When schema support is enabled, the BigQuery dataset is used as the schema, by default. To override this behavior, configure the <code>lakehouse_target</code> for the table configuration.

### Mirrored Database
The Mirrored Database destination is supported through Fabric Open Mirroring. BigQuery data is mirrored to the Open Mirror Landing Zone and then moved by the mirroring services to the mirrored database. The loading process for Mirrored Databases stops the mirroring service when a synch is initiated, writes the mirrored data directly to the landing zone, restructures the data files to comply with the landing zone file naming requirements and then restarts the mirroring service at the completion of the synch operation.

Mirrored Databases has the following requirements:
- Requires <code>enable_schemas</code> set to <code>True</code>
- Does not support BigQuery or Fabric partitions
- Does not support the <code>MERGE</code> load type