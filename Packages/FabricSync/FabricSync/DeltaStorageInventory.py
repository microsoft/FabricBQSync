from pyspark.sql.functions import *
import os
import json
from datetime import datetime
from delta.tables import *
from queue import Queue
from threading import Thread
from pyspark.sql.types import *
from uuid import uuid4
from pathlib import Path

class DeltaStorageInventory:
    temp_tables = ["tmpFiles", "tmpHistory"]
    inventory_tables = ["delta_tables","delta_table_files","delta_table_partitions","delta_table_history","delta_table_snapshot"]

    def __init__(   self, session:SparkSession, target_lakehouse:str, inventory_date:str = None, \
                    container:str = None, storage_prefix:str = None, parallelism:int = 5, track_history:bool = False):
        self.sc = session.sparkContext
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.sc._jsc.hadoopConfiguration())

        self.session = session
        self.target_lakehouse = target_lakehouse
        self.inventory_date = inventory_date
        self.storage_prefix = storage_prefix
        self.parallelism = parallelism
        self.track_history = track_history
        self.container = container

        if self.storage_prefix is None:
            self.storage_prefix = "Files/"

        if self.inventory_date is None:
            self.inventory_date = datetime.today().strftime("%Y/%m/%d")

        self.inventory_dt = datetime.strptime(self.inventory_date, "%Y/%m/%d")

        self.inventory_year = self.inventory_dt.strftime("%Y")
        self.inventory_month = self.inventory_dt.strftime("%m")
        self.inventory_day = self.inventory_dt.strftime("%d")

    def is_dbx_runtime(self):
        try:
            dbx = self.session.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
            databricks = True
        except Exception:
            databricks = False
        
        return databricks
    
    def get_files_schema(self, with_table:bool = True):
        base_definition = [StructField('data_file', StringType(), True), \
            StructField('file_info', StructType([StructField('operation', StringType(), True), \
            StructField('file_size', LongType(), True), StructField('row_count', LongType(), True), \
            StructField('delta_version', LongType(), True), \
            StructField('deletionVectorSize', LongType(), True)]), True)]

        if not with_table:
            return StructType(base_definition)

        return StructType(base_definition + [StructField('delta_table', StringType(), True)])
    
    def create_temp_tables(self):
        files_schema = self.get_files_schema()

        df = self.session.createDataFrame([], files_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.target_lakehouse}.tmpFiles")

        base_definition = [StructField('version', LongType(), True), StructField('timestamp', TimestampType(), True), \
                    StructField('userId', StringType(), True), StructField('userName', StringType(), True), \
                    StructField('operation', StringType(), True), StructField('operationParameters', MapType(StringType(), StringType(), True), True), \
                    StructField('notebook', StructType([StructField('notebookId', StringType(), True)]), True), \
                    StructField('clusterId', StringType(), True), StructField('readVersion', LongType(), True), \
                    StructField('isolationLevel', StringType(), True), StructField('isBlindAppend', BooleanType(), True), \
                    StructField('operationMetrics', MapType(StringType(), StringType(), True), True), \
                    StructField('userMetadata', StringType(), True), StructField('engineInfo', StringType(), True), \
                    StructField('delta_table', StringType(), True)]

        job_definition = [StructField('job', StructType([StructField('jobId', StringType(), True), StructField('jobName', StringType(), True), \
                        StructField('runId', StringType(), True), StructField('jobOwnerId', StringType(), True), \
                        StructField('triggerType', StringType(), True)]), True)]

        if not self.is_dbx_runtime():
            job_definition = [StructField('job', StructType([StructField('jobId', StringType(), True), StructField('jobName', StringType(), True), \
                StructField('runId', StringType(), True), StructField('jobOwnerId', StringType(), True), \
                StructField('triggerType', StringType(), True)]), True)]
        else:
            job_definition = [StructField('job', StructType([StructField('jobId', StringType(), True), StructField('jobName', StringType(), True), \
                StructField('jobRunId', StringType(), True), StructField('runId', StringType(), True), \
                StructField('jobOwnerId', StringType(), True), StructField('triggerType', StringType(), True)]), True)]

        history_schema = StructType(base_definition + job_definition)

        df = self.session.createDataFrame([], history_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.target_lakehouse}.tmpHistory")

    def clear_temp_tables(self):
        list(map(lambda x: self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.{x}"), self.temp_tables))

    def clear_delta_inventory_schema(self):
        list(map(lambda x: self.session.sql(f"DROP TABLE IF EXISTS {self.target_lakehouse}.{x}"), self.inventory_tables))

    def path_exists(self, path):
        return self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(path))

    def check_tbl_exists(self, delta_tbl:str) -> bool:
        tbl_path = self.get_clean_tbl_path(delta_tbl)
        return self.path_exists(tbl_path)

    def get_delta_tables_from_inventory(self):
        delta_df = self.session.table("_storage_inventory") \
            .where(col("Name").like("%_delta_log%.%")) \
            .orderBy("Name")
        delta_tbls = {}

        for f in delta_df.collect():
            path = f["Name"]            

            delta_tbl = os.path.dirname(path).replace("_delta_log", "")

            if "compacted" in path or not self.check_tbl_exists(delta_tbl):
                continue
            
            if delta_tbl not in delta_tbls:
                delta_tbls[delta_tbl] = []
            
            ext = Path(path).suffix

            if ext == ".json":
                delta_version = int(Path(path).stem)
                
                if delta_version not in delta_tbls[delta_tbl]:
                    delta_tbls[delta_tbl].append(delta_version)   
        
        t = self.session.createDataFrame(delta_tbls.items(), ["delta_table", "delta_versions"]) \
            .withColumn("delta_table_id", expr("uuid()"))

        return (list(delta_tbls.keys()), t)

    def process_delta_table_logs(self, delta_tables:list[str]):
        workQueue = Queue()

        for tbl in delta_tables:
            workQueue.put(tbl)

        self.create_temp_tables()
        self.process_queue(workQueue)

    def get_deletion_vector_bytes(self, data) -> int:
        deletionVectorBytes = 0

        if "deletionVector" in data:
            deletionVector = data["deletionVector"]

            if deletionVector:
                deletionVectorBytes = int(deletionVector["sizeInBytes"])
        
        return deletionVectorBytes
    
    def get_row_count(self, data) -> int:
        stats = json.loads(data["stats"])
        return stats["numRecords"]

    def get_clean_tbl_path(self, tbl:str) -> str:
        if self.container is not None:
            tbl_path = tbl.replace(f"{self.container}/", "", 1)
        else:
            tbl_path = tbl
        
        return f"{self.storage_prefix}{tbl_path}"

    def process_delta_log(self, tbl:str):
        tbl_path = self.get_clean_tbl_path(tbl)

        if not self.path_exists(tbl_path):
            print(f"SKIPPED - {tbl} path does not exists")
            return

        started = datetime.today()
        print(f"Starting table {tbl} ...")

        ddf = self.session.read.format("json").load(f"{tbl_path}/_delta_log/????????????????????.json")
        ddf = ddf.withColumn("filename", input_file_name())

        file_analysis = {}

        for r in ddf.orderBy("filename").collect():
            file_name = r["filename"]

            if "compacted" in file_name:
                continue

            delta_version = int(Path(file_name).stem)

            if "add" in r and r["add"]:
                f = r["add"]["path"]                
                
                deletionVectorBytes = self.get_deletion_vector_bytes(r["add"])

                if f not in file_analysis:   
                    row_count = self.get_row_count(r["add"])               
                    file_data = ("ADD", r["add"]["size"], row_count, delta_version, deletionVectorBytes)                    
                    file_analysis[f] = file_data
                elif deletionVectorBytes > 0:
                    file_data = list(file_analysis[f])
                    file_data[4] = deletionVectorBytes
                    file_analysis[f] = tuple(file_data)
                
            if "remove" in r and r["remove"]:
                f = r["remove"]["path"]

                deletionVectorBytes = self.get_deletion_vector_bytes(r["remove"])

                if f in file_analysis and file_analysis[f][0] == "ADD":
                    file_data = list(file_analysis[f])
                    file_data[0] = "REMOVE"
                    file_data[3] = delta_version
                    file_data[4] = deletionVectorBytes
                    file_analysis[f] = tuple(file_data)

        files_schema = self.get_files_schema(False)    
        f = self.session.createDataFrame(file_analysis.items(), files_schema) \
            .withColumn("delta_table", lit(tbl)) 
        f.write.mode("APPEND").saveAsTable(f"{self.target_lakehouse}.tmpFiles")        

        deltaTbl = DeltaTable.forPath(self.session, f"{tbl_path}")
        h = deltaTbl.history() \
            .withColumn("delta_table", lit(tbl))
        h.write.mode("APPEND").saveAsTable(f"{self.target_lakehouse}.tmpHistory")

        completed = datetime.today()
        runtime = completed - started

        print(f"Completed table {tbl} in {(runtime.total_seconds()/60):.4f} mins ...")
        
    def task_runner(self, work_function, workQueue:Queue):
        while not workQueue.empty():
            tbl = workQueue.get()
    
            try:
                work_function(tbl)
            except Exception as e:
                print(f"ERROR loading table {tbl}: {e}")
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
            .filter(col("inventory_date") == self.inventory_dt) \
            .select("*", expr("file_info['operation']").alias("operation"), \
                expr("file_info['file_size']").alias("content_size"), \
                expr("file_info['row_count']").alias("row_count"))

        f = f.withColumn("delta_table_path", concat("delta_table", "data_file")) \
                .withColumn("delta_partition", expr("substring(data_file, 1, len(data_file) - locate('/', reverse(data_file)))")) \
                .withColumn("delta_partition", when(col("delta_partition").contains(".parquet"), "<default>") \
                    .otherwise(col("delta_partition")))
        
        return f

    def get_delta_partitions_source(self) -> DataFrame:
        agg = self.get_delta_file_size_from_inventory()
        
        agg = agg.groupBy("delta_table", \
                "delta_partition", \
                "operation") \
            .agg(count("*").alias("files_count"), \
                sum("content_size").alias("file_size"), \
                sum("row_count").alias("row_count"))

        a = agg.where("operation = 'ADD'").alias("a")
        r = agg.where("operation = 'REMOVE'").alias("r")

        p = a.join(r, (a.delta_table==r.delta_table) & (a.delta_partition==r.delta_partition), "left") \
            .select(a["*"], \
                col("r.files_count").alias("removed_files_count"), \
                col("r.file_size").alias("removed_file_size"), \
                col("r.row_count").alias("removed_row_count"))

        p = p.withColumn('removed_files_count', \
                when(col("removed_files_count").isNull(), 0).otherwise(col("removed_files_count"))) \
            .withColumn('removed_file_size', \
                when(col("removed_file_size").isNull(), 0).otherwise(col("removed_file_size"))) \
            .withColumn('removed_row_count', \
                when(col("removed_row_count").isNull(), 0).otherwise(col("removed_row_count"))) \
            .withColumn("total_files_count", col("files_count") + col("removed_files_count")) \
            .withColumn("total_file_size", col("file_size") + col("removed_file_size")) \
            .withColumn("total_row_count", col("row_count") + col("removed_row_count")) \
            .drop("operation")

        return p

    def get_delta_table_snapshot(self, partitions:DataFrame) -> DataFrame:
        t = partitions.groupBy("delta_table") \
            .agg(sum("files_count").alias("active_files_count"), \
                sum("file_size").alias("active_files_size"), \
                sum("row_count").alias("active_row_count"), \
                sum("removed_files_count").alias("removed_files_count"), \
                sum("removed_file_size").alias("removed_files_size"), \
                sum("removed_row_count").alias("removed_row_count"), \
                sum("total_files_count").alias("total_files_count"), \
                sum("total_file_size").alias("total_files_size"), \
                sum("total_row_count").alias("total_row_count"), \
                countDistinct("delta_partition").alias("table_partitions"))

        return t

    def lookup_delta_table_id(self, df:DataFrame) -> DataFrame:
        lkp = self.session.table(f"{self.target_lakehouse}.delta_tables") \
            .select("delta_table_id", "delta_table") \
            .alias("lkp")

        df = df.join(lkp, (df.delta_table==lkp.delta_table)) \
            .select(df["*"], \
                col("lkp.delta_table_id"))
            
        return df

    def save_dataframe(self, df:DataFrame, delta_table:str, merge_criteria:list[str] = [], temporal:bool = True):
        if not "delta_table_id" in df.columns and "delta_table" in df.columns:
            df = self.lookup_delta_table_id(df)

        if temporal and not "inventory_date" in df.columns:
            df = df.withColumn("inventory_date", lit(self.inventory_dt))
        
        if self.session.catalog.tableExists(delta_table):
            if "delta_table" not in merge_criteria:
                merge_criteria.append("delta_table_id")

            if temporal:
                merge_criteria.append("inventory_date")

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
        self.clear_temp_tables()

        if not self.track_history:
            print("Historical data disabled, resetting repository ...")
            self.clear_delta_inventory_schema()
        
        inventory_file_path = f"{inventory_data_path}{self.inventory_date}/*/{rule}/*.{inventory_output_type}"
   
        print(f"Getting blob inventory {inventory_file_path} ...")
        self.load_storage_inventory(inventory_file_path, inventory_output_type)
        delta_tables, tables = self.get_delta_tables_from_inventory()

        print(f"Processing delta table logs ...")
        self.process_delta_table_logs(delta_tables)

        print(f"Saving inventory [tables] ...")
        self.save_dataframe(tables, f"{self.target_lakehouse}.delta_tables", ["delta_table"], False)

        print(f"Saving inventory [files] ...")
        df = self.session.table(f"{self.target_lakehouse}.tmpFiles") \
            .withColumn("inventory_date", lit(self.inventory_dt))
        self.save_dataframe(df, f"{self.target_lakehouse}.delta_table_files", ["data_file"])
        
        print(f"Saving inventory [history] ...")
        df = self.session.table(f"{self.target_lakehouse}.tmpHistory") \
            .withColumn("inventory_date", lit(self.inventory_dt))
        self.save_dataframe(df, f"{self.target_lakehouse}.delta_table_history", ["version"])

        partitions = self.get_delta_partitions_source()
        snapshot = self.get_delta_table_snapshot(partitions)
        
        print(f"Saving inventory [partitions] ...")
        self.save_dataframe(partitions, f"{self.target_lakehouse}.delta_table_partitions", ["delta_partition"])
        
        print(f"Saving inventory [snapshot] ...")
        self.save_dataframe(snapshot, f"{self.target_lakehouse}.delta_table_snapshot")

        self.clear_temp_tables()

        completed = datetime.today()
        runtime = completed - started

        print(f"Finished Delta Inventory for Rule: {rule} in {(runtime.total_seconds()/60):.4f} mins ...")