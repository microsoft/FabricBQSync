from pyspark.sql.functions import *
import os
from datetime import datetime
from delta.tables import *
from queue import Queue
from threading import Thread
from pyspark.sql.types import *
from uuid import uuid4

class DeltaStorageInventory:
    def __init__(   self, session:SparkSession, target_lakehouse:str, inventory_date:str = None, \
                    storage_prefix:str = None, parallelism:int = 5):
        self.session = session
        self.target_lakehouse = target_lakehouse
        self.inventory_date = inventory_date
        self.storage_prefix = storage_prefix
        self.parallelism = parallelism

        if self.storage_prefix is None:
            self.storage_prefix = "Files/"

        if self.inventory_date is None:
            self.inventory_date = datetime.today().strftime("%Y/%m/%d")

        dt = datetime.strptime(self.inventory_date, "%Y/%m/%d")

        self.inventory_year = dt.strftime("%Y")
        self.inventory_month = dt.strftime("%m")
        self.inventory_day = dt.strftime("%d")

    def clear_delta_inventory_schema(self):
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.delta_tables")
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.delta_table_files")
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.delta_table_partitions")
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.delta_table_history")
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.delta_table_snapshot")

    def create_temp_tables(self):
        files_schema = StructType([ \
            StructField('data_file', StringType(), True), StructField('operation', StringType(), True), \
            StructField('delta_table', StringType(), True), StructField('inventory_year', StringType(), True), \
            StructField('inventory_month', StringType(), True), StructField('inventory_day', StringType(), True)])

        df = self.session.createDataFrame([], files_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.target_lakehouse}.tmpFiles")

        history_schema = StructType([StructField('version', LongType(), True), StructField('timestamp', TimestampType(), True), \
        StructField('userId', StringType(), True), StructField('userName', StringType(), True), \
        StructField('operation', StringType(), True), StructField('operationParameters', MapType(StringType(), StringType(), True), True), \
        StructField('job', StructType([StructField('jobId', StringType(), True), StructField('jobName', StringType(), True), \
        StructField('runId', StringType(), True), StructField('jobOwnerId', StringType(), True), \
        StructField('triggerType', StringType(), True)]), True), \
        StructField('notebook', StructType([StructField('notebookId', StringType(), True)]), True), \
        StructField('clusterId', StringType(), True), StructField('readVersion', LongType(), True), \
        StructField('isolationLevel', StringType(), True), StructField('isBlindAppend', BooleanType(), True), \
        StructField('operationMetrics', MapType(StringType(), StringType(), True), True), \
        StructField('userMetadata', StringType(), True), StructField('engineInfo', StringType(), True), \
        StructField('delta_table', StringType(), True), StructField('inventory_year', StringType(), True), \
        StructField('inventory_month', StringType(), True), StructField('inventory_day', StringType(), True)])

        df = self.session.createDataFrame([], history_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.target_lakehouse}.tmpHistory")

    def get_delta_tables_from_inventory(self):
        delta_df = self.session.table("_storage_inventory") \
            .where(col("Name").like("%_delta_log%.%")) \
            .orderBy("Name")
        delta_tbls = {}

        for f in delta_df.collect():
            path = f["Name"]
            delta_tbl = os.path.dirname(path).replace("_delta_log", "")

            if delta_tbl not in delta_tbls:
                delta_tbls[delta_tbl] = []
            
            ext = os.path.splitext(path)[1]

            if ext == ".json":
                delta_version = int(os.path.basename(path).replace(ext, ""))
                
                if delta_version not in delta_tbls[delta_tbl]:
                    delta_tbls[delta_tbl].append(delta_version)   
        
        t = self.session.createDataFrame(delta_tbls.items(), ["delta_table", "delta_versions"]) \
            .withColumn("delta_table_id", expr("uuid()")) \
            .withColumn("inventory_year", lit(self.inventory_year)) \
            .withColumn("inventory_month", lit(self.inventory_month)) \
            .withColumn("inventory_day", lit(self.inventory_day))

        return (list(delta_tbls.keys()), t)

    def process_delta_table_logs(self, delta_tables:list[str]):
        workQueue = Queue()

        for tbl in delta_tables:
            workQueue.put(tbl)
        
        self.create_temp_tables()
        self.process_queue(workQueue)

    def process_delta_log(self, tbl:str):
        started = datetime.today()
        print(f"Starting table {tbl} ...")
        ddf = self.session.read.format("json").load(f"{self.storage_prefix}{tbl}/_delta_log/*.json")
        ddf = ddf.withColumn("filename", input_file_name())

        file_analysis = {}

        for r in ddf.orderBy("filename").collect():
            if "add" in r and r["add"]:
                f = r['add']['path']

                if f not in file_analysis:
                    file_analysis[f] = "ADD"
                
            if "remove" in r and r["remove"]:
                f = r['remove']['path']

                if f in file_analysis and file_analysis[f] == "ADD":
                    file_analysis[f] = "REMOVE"
            
        f = self.session.createDataFrame(file_analysis.items(), ["data_file", "operation"]) \
            .withColumn("delta_table", lit(tbl)) \
            .withColumn("inventory_year", lit(self.inventory_year)) \
            .withColumn("inventory_month", lit(self.inventory_month)) \
            .withColumn("inventory_day", lit(self.inventory_day))
        f.write.mode("APPEND").saveAsTable(f"{self.target_lakehouse}.tmpFiles")        

        deltaTbl = DeltaTable.forPath(self.session, f"{self.storage_prefix}{tbl}")
        h = deltaTbl.history() \
            .withColumn("delta_table", lit(tbl)) \
            .withColumn("inventory_year", lit(self.inventory_year)) \
            .withColumn("inventory_month", lit(self.inventory_month)) \
            .withColumn("inventory_day", lit(self.inventory_day))
        h.write.mode("APPEND").saveAsTable(f"{self.target_lakehouse}.tmpHistory")
        completed = datetime.today()
        runtime = completed - started

        print(f"Completed table {tbl} in {(runtime.total_seconds()/60):.4f} mins ...")
        

    def task_runner(self, work_function, workQueue:Queue):
        while not workQueue.empty():
            tbl = workQueue.get()
    
            try:
                work_function(tbl)
            finally:
                workQueue.task_done()

    def process_queue(self, workQueue:Queue):
        for i in range(self.parallelism):
            t=Thread(target=self.task_runner, args=(self.process_delta_log, workQueue))
            t.daemon = True
            t.start() 
                
        workQueue.join()

    def load_storage_inventory(self, inventory_file_path:str, output_type:str):
        if output_type.lower() == "csv":
            df = self.session.read.format(output_type).option("header","true").load(inventory_file_path)
        else:
            df = self.session.read.format(output_type).load(inventory_file_path)

        df.createOrReplaceTempView(f"_storage_inventory")

    def get_delta_file_size_from_inventory(self) -> DataFrame:
        f = self.session.table(f"{self.target_lakehouse}.delta_table_files") \
            .filter(col("inventory_year") == self.inventory_year) \
            .filter(col("inventory_month") == self.inventory_month) \
            .filter(col("inventory_day") == self.inventory_day)

        f = f.withColumn("delta_table_path", concat("delta_table", "data_file")) \
                .withColumn("delta_partition", expr("substring(data_file, 1, len(data_file) - locate('/', reverse(data_file)))"))

        f = f.alias("f")
        i = self.session.table("_storage_inventory").alias("i")

        agg = f.join(i, (f.delta_table_path==i.Name), "left") \
            .select(f["*"], expr("coalesce(i.`content-length`, 0)").alias("content_size"))
        
        return agg

    def get_delta_partitions_source(self) -> DataFrame:
        agg = self.get_delta_file_size_from_inventory()
        
        agg = agg.groupBy("delta_table", \
                "delta_partition", \
                "operation") \
            .agg(count("*").alias("files_count"), \
                sum("content_size").alias("file_size"))

        a = agg.where("operation = 'ADD'").alias("a")
        r = agg.where("operation = 'REMOVE'").alias("r")

        p = a.join(r, (a.delta_table==r.delta_table) & (a.delta_partition==r.delta_partition), "left") \
            .select(a["*"], \
                col("r.files_count").alias("removed_files_count"), \
                col("r.file_size").alias("removed_file_size"))

        p = p.withColumn('removed_files_count', \
                when(col("removed_files_count").isNull(), 0).otherwise(col("removed_files_count"))) \
            .withColumn('removed_file_size', \
                when(col("removed_file_size").isNull(), 0).otherwise(col("removed_file_size"))) \
            .withColumn("total_files_count", col("files_count") + col("removed_files_count")) \
            .withColumn("total_file_size", col("file_size") + col("removed_file_size")) \
            .withColumn("inventory_year", lit(self.inventory_year)) \
            .withColumn("inventory_month", lit(self.inventory_month)) \
            .withColumn("inventory_day", lit(self.inventory_day)) \
            .drop("operation")

        return p

    def get_delta_table_snapshot(self, partitions:DataFrame) -> DataFrame:
        t = partitions.groupBy("delta_table", \
                "inventory_year", \
                "inventory_month", \
                "inventory_day") \
            .agg(sum("files_count").alias("active_files_count"), \
                sum("file_size").alias("active_files_size"), \
                sum("removed_files_count").alias("removed_files_count"), \
                sum("removed_file_size").alias("removed_files_size"), \
                sum("total_files_count").alias("total_files_count"), \
                sum("total_file_size").alias("total_files_size"))

        return t

    def lookup_delta_table_id(self, df:DataFrame) -> DataFrame:
        lkp = self.session.table(f"{self.target_lakehouse}.delta_tables") \
            .select("delta_table_id", "delta_table") \
            .alias("lkp")

        df = df.join(lkp, (df.delta_table==lkp.delta_table)) \
            .select(df["*"], \
                col("lkp.delta_table_id"))
            
        return df

    def save_dataframe(self, df:DataFrame, delta_table:str, merge_criteria:list[str] = []):
        if not "delta_table_id" in df.columns and "delta_table" in df.columns:
            df = self.lookup_delta_table_id(df)

        if self.session.catalog.tableExists(delta_table):
            if "delta_table" not in merge_criteria:
                merge_criteria.append("delta_table_id")

            merge_criteria.append("inventory_year")
            merge_criteria.append("inventory_month")
            merge_criteria.append("inventory_day")

            criteria = [f"s.{t} = t.{t}" for t in merge_criteria]

            deltaTable = DeltaTable.forName(self.session, delta_table)

            deltaTable.alias('t') \
                .merge(df.alias('s'), " AND ".join(criteria)) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            df.write.mode("OVERWRITE").saveAsTable(delta_table)

    def run_from_storage_inventory(self, rule:str, inventory_data_path:str, \
                                    inventory_output_type:str):
        started = datetime.today()

        print(f"Starting Delta Inventory for Rule: {rule} ...")
        inventory_file_path = f"{inventory_data_path}{self.inventory_date}/*/{rule}/*.{inventory_output_type}"
   
        print(f"Getting blob inventory {inventory_file_path} ...")
        self.load_storage_inventory(inventory_file_path, inventory_output_type)
        delta_tables, tables = self.get_delta_tables_from_inventory()

        print(f"Processing delta table logs ...")
        self.process_delta_table_logs(delta_tables)

        print(f"Saving inventory [tables] ...")
        self.save_dataframe(tables, f"{self.target_lakehouse}.delta_tables", ["delta_table"])

        print(f"Saving inventory [files] ...")
        self.save_dataframe(self.session.table(f"{self.target_lakehouse}.tmpFiles"), \
            f"{self.target_lakehouse}.delta_table_files", ["data_file"])
        
        print(f"Saving inventory [history] ...")
        self.save_dataframe(self.session.table(f"{self.target_lakehouse}.tmpHistory"), \
            f"{self.target_lakehouse}.delta_table_history", ["version"])

        partitions = self.get_delta_partitions_source()
        snapshot = self.get_delta_table_snapshot(partitions)
        
        print(f"Saving inventory [partitions] ...")
        self.save_dataframe(partitions, f"{self.target_lakehouse}.delta_table_partitions", ["delta_partition"])
        
        print(f"Saving inventory [snapshot] ...")
        self.save_dataframe(snapshot, f"{self.target_lakehouse}.delta_table_snapshot")

        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.tmpFiles")
        self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.tmpHistory")

        completed = datetime.today()
        runtime = completed - started

        print(f"Finished Delta Inventory for Rule: {rule} in {(runtime.total_seconds()/60):.4f} mins ...")