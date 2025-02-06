# Installation & Upgrades

The accelerator includes an automated installer and an upgrade utility that can set up your Fabric workspace and install all required dependencies  when you start while also providing a friction-free pathway for you to stay current with new versions as they are availble. 

### Installer
To use the installer:
1. Download the most current version of the Installer Notebook from the [FabricSync Github repository](https://github.com/microsoft/FabricBQSync/tree/main/Notebooks)
2. Import the installer into your Fabric Workspace
3. Attach the installer to a Lakehouse within the Workspace
4. Upload your GCP Service Account credential json file to your Lakehouse
5. Update the installer notebook configuration parameters:
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

Open the notebook in your Fabric workspace, attach to your Metadata Lakehouse and your ready to start syncing data between BigQuery and Fabric

### Upgrades
Beginning with version 2.0 and greater, Fabric Sync upgrades itself inline. When a new package runtime is detected, the Fabric Sync accelerator will automatically, update the environment to accommodate the new runtime.

