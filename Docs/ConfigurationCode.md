# User Configuration by Code

Configuration for the Fabric Sync accelerator by default is driven by a JSON User Configuration file. The User Configuration file can be manually updated or manipulated as required.

Alternatively, Fabric Sync User Configuration can be managed directly by Python Code. The examples below are not exhaustive but are illustrative of code-based configuration management.

<mark><b>NOTE:</b> The following code is can be downloaded as a runable notebook in the example notebook: [Configuration-Code-Example](Examples/Configuration_Code_Example.ipynb).</mark>

### Set-Up
1. Create a Fabric Notebook attached to the OneLake metadata lakehouse
2. PIP Install the Fabric Sync library:
    <code>
    %pip install FabricSync --quiet
    </code>
3. Import the required libraries:
    <code><pre>
    from FabricSync.BQ.Model.Config import *
    from FabricSync.BQ.Enum import *
    from FabricSync.BQ.ModelValidation import UserConfigurationValidation    
    </pre></code>

### Read the User Config File
Read the User Configuration JSON file to the Python object
<code><pre>
config_json_path = "[PATH_TO_CONFIG_FILE]"
config = ConfigDataset.from_json(config_json_path)
</pre></code>

### View Current Config
View the current User Configuration
<code><pre>
print(config.model_dump_json(indent=4, exclude_none=True, exclude_unset=True))
</pre></code>

### Update Misc Config Settings
Use the GitHub documentation, source code or Intellisense to discover the possible settings and update as required.
<code><pre>
config.GCP.API.UseStandardAPI = True
config.GCP.API.UseCDC = True
config.Optimization.UseApproximateRowCounts = True
</pre></code>

### Add Table/View/Materialized View Config
Add a BigQuery BASE_TABLE, VIEW or MATERIALIZED_VIEW to the User Configuration.
<code><pre>
table = ConfigBQTable()

table.ProjectID = "[GCP PROJECT ID]"
table.Dataset = "[GCP DATASET ID]"
table.TableName = "[TABLE, VIEW or MATERIALIZED VIEW]"
table.Enabled = True
table.ObjectType = BigQueryObjectType.VIEW #BigQueryObjectType Enum Value

config.Tables.append(table)
</pre></code>

### Update Table/View/Materialized View Config
If a table already exists in the User Configuration, it can be updated. This example disables table synchronization.
<code><pre>
table_name = "<<<TABLE, VIEW or MATERIALIZED VIEW>>>"
table = next((table for table in config.Tables if table.TableName == table_name), None)

if table:
    table.Enabled = False

    config.Tables = [table if tbl.TableName == table.TableName else tbl for tbl in config.Tables]
</pre></code>

### Delete Table/View/Materialized View Config
Removes a BigQuery object from the User Configuration
<code><pre>
table_name = "[TABLE, VIEW or MATERIALIZED VIEW]"
config.Tables = [tbl for tbl in config.Tables if tbl.TableName != table_name]
</pre></code>

### Validate Config Changes
After making all required changes, the User Configuration can be validated prior to usage. Any validation errors are printed out and can be resolved directly.
<code><pre>
validation_errors = UserConfigurationValidation.validate(config)

if validation_errors:
    print("Invalid User Configuration:")
    print(validation_errors)
else:
    print("User Configuration OK....")
</pre></code>

### Save Config Changes
Save configuration changes back to the current User Configuration file.
<code><pre>
config.to_json(config_json_path)
</pre></code>