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
6. Run the installer notebook

The installer performs the following actions:
- Create the required Lakehouses, if they do not exists
- Creates the metadata tables and required metadata
- Downloads the correct version of your BQ Spark connector based on your configured spark runtime
- Creates an initial default user configuration file based on your config parameters
- Installs a fully configured and ready-to-run BQ-Sync-Notebook into your workspace

Open the notebook in your Fabric workspace, attach to your Metadata Lakehouse and your ready to start syncing data between BigQuery and Fabric

### Upgrades
The upgrade utility updates your current Fabric Sync implementation to the latest version. While its never anticipated, the upgrade process is non-destructive when couple with the native Delta time travel capabilities, making it easy to rollback changes should the need arise. To use the upgrade utility:
1. Download the most current version of the Upgrade Notebook from the [FabricSync Github repository](https://github.com/microsoft/FabricBQSync/tree/main/Notebooks)
2. Import the upgtade notebook into your Fabric Workspace
3. Attach the upgrade notebook to your current Metadata Lakehouse
4. Update the upgrade notebook configuration parameters:
    - <code>path</code> - Path to your current configuration file
    - <code>path</code> - Flag to run the upgrade on the configuration file only. This is only used when your environment runs multiple configuration files.
5. Run the upgrade notebook

The upgrade utility is version aware and performs only the actions required which may include:
- Configuration file upgrades
- Modifications to the metadata tables and required metadata
- Downloads the most ucrrent version of the BQ Spark connector based your configured spark runtime
- Deploys a new fully configured and ready-to-run BQ-Sync-Notebook into your workspace

**Note on Upgrades**
The upgrade process does not change any of your configured settings. New features or changes to existing features which may alter the way the way Fabric Sync operates are turned-off by default. The default off approach allows you to control the opt-in of new features and capabilities while keeping current on version.

