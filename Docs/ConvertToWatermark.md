# Convert Load Strategy to Watermark

The Load Strategy for BigQuery objects can be updated freely using the User Configuration file at any point before data is synchronized from BigQuery to OneLake. Once data is synchronized, the configuration is locked to guarantee the consistency of data between BigQuery and Fabric.

If the configuration is locked and the load strategy requires updating, the change must be made manually to ensure data consistency.

The code provided below manually:
- Gets the current max watermark for previously synced data
- Updates the sync_configuration table
- Updates the max watermark in the metastore
- Updates the User Configuration file

<mark><b>NOTE:</b> The following code is can be downloaded as a runable notebook in the example notebook: [Watermark_Upgrade](Examples/Watermark_Upgrade.ipynb).</mark>

### Upgrade to Watermark

1. Create a Fabric Notebook attached to the OneLake metadata lakehouse
2. PIP Install the Fabric Sync library:
    <code>
    %pip install FabricSync --quiet
    </code>
3. Add the code below:
    <code><pre>
    from delta import DeltaTable
    from pyspark.sql.functions import lit, col, max
    from FabricSync.BQ.Model.Config import *
    from FabricSync.BQ.Enum import *

    project_id = "[GCP PROJECT ID]"
    dataset = "[GCP DATASET ID]"
    table_name = "[TABLE, VIEW, MATERIALIZED VIEW NAME]"
    watermark_column = "[WATERMARK COLUMN NAME]"
    config_json_path = "[PATH TO CONFIGURATION FILE]"

    config = ConfigDataset.from_json(config_json_path)

    predicate = f"sync_id='{config.ID}' AND project_id='{project_id}' AND dataset='{dataset}' AND table_name='{table_name}'"
    sync_cfg = spark.table("sync_configuration").where(predicate)

    if sync_cfg.count() > 0:
        print("Updating configuration...")
        c = next(sync_cfg.toLocalIterator(), None)

        #For committed tables, get the max watermark
        if c["sync_state"] == "COMMIT":
            print("Committed table setting watermark...")
            sync_schedule = spark.table("sync_schedule").where(predicate).orderBy(col("completed").desc())
            s = next(sync_schedule.toLocalIterator(), None)

            if s:
                df = spark.table(f"{c['lakehouse']}.{c['lakehouse_table_name']}")
                df = df.agg(max(watermark_column).alias("watermark"))

                w = next(df.toLocalIterator(), None)

                if w:
                    watermark = str(w["watermark"])
                    print(f"Found max watermark: {watermark} ...")  

                    deltaTable = DeltaTable.forName(spark, "sync_schedule")
                    deltaTable.update(
                        condition = f"sync_id='{config.ID}' AND schedule_id='{s['schedule_id']}'",
                        set = { 
                            'max_watermark': lit(watermark)
                        }
                    )
        
        #Update sync_configuration metastore
        print("Updating sync_configuration...")
        deltaTable = DeltaTable.forName(spark, "sync_configuration")
        deltaTable.update(
            condition = predicate,
            set = { 
                'load_strategy': lit(SyncLoadStrategy.WATERMARK) ,
                'load_type': lit(SyncLoadType.APPEND),
                'watermark_column': lit(watermark_column)
            }
        )

        #Update User Configuration File
        print("Updating user configuration...")
        table = next((table for table in config.Tables if table.TableName == table_name), None)

        if table:
            table.LoadStrategy = SyncLoadStrategy.WATERMARK
            table.LoadType = SyncLoadType.APPEND
            table.Keys = [ConfigTableColumn(column=watermark_column)]

            config.Tables = [table if tbl.TableName == table.TableName else tbl for tbl in config.Tables]
            config.to_json(config_json_path)

            print("Finished...")
    else:
        print("Configuration not found...")    
    </pre></code>

4. Update the code above with the required information:
    - <code>project_id</code>: GCP Project ID
    - <code>dataset</code>: GCP Dataset ID
    - <code>table_name</code>: GCP Table/View/Materialized View Name
    - <code>watermark_column</code>: Name of column to use as watermark
    - <code>config_json_path</code>: File API path to User Configuration file
5. Run the Code