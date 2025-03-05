# Update BigQuery Spark Connector

Periodically it may be necessary to update the BigQuery Spark Connector to apply bug fixes or leverage performance improvements made over time. 

While its possible to download the correct BQ Spark connector runtime version from GitHub and deploy it to OneLake, the Fabric Sync accelerator can automatically download the correct connector version for your Spark runtime directly to OneLake.

To get update Fabric Sync to the most current BigQuery Spark connector, use the following steps:

1. Create a Fabric Notebook attached to the OneLake metadata lakehouse
2. PIP Install the Fabric Sync library:
    <code>
    %pip install FabricSync --quiet
    </code>
3. Add the code below:
    <code><pre>
    from FabricSync.BQ.Setup import SetupUtils
    from FabricSync.BQ.FileSystem import OneLakeUtil
    import os

    libraries_path = "Files/Fabric_Sync_Process/libs"

    bq_connector = SetUpUtils.get_bq_spark_connector(f"/lakehouse/default/{libraries_path}")
    workspace_id = spark.conf.get("trident.workspace.id")
    lakehouse_id = spark.conf.get("trident.lakehouse.id")
    uri = OneLakeUtil.get_onelake_uri(workspace_id, lakehouse_id)

    print(os.path.join(uri, libraries_path, bq_connector))
    </pre></code>
4. Run the code
5. Replace the spark.jars path in the <code>%configure</code> command of your Fabric Sync notebook with the ABFSS path outputted by the code

### BigQuery Spark Connector GitHub
Git Hub Project: [https://github.com/GoogleCloudDataproc/spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)

The BigQuery Spark Connector is an Apache licensed open-source project supported and maintained by Google. 