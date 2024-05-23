from pyspark.sql.functions import *
import os
import sys
import json
from datetime import datetime
from delta.tables import *
from queue import Queue
from threading import Thread
from pyspark.sql.types import *
from uuid import uuid4
from pathlib import Path
from functools import reduce

class DeltaStorageInventory:
    temp_tables = ["tmpFiles", "tmpHistory"]
    inventory_tables = ["delta_tables","delta_table_files","delta_table_partitions","delta_table_history","delta_table_snapshot"]

    def __init__(   self, session:SparkSession, target_lakehouse:str, inventory_date:str = None, \
                    container:str = None, storage_prefix:str = None, parallelism:int = 5, track_history:bool = False,
                    batch_size = 125000000):
        self.session = session
        self.target_lakehouse = target_lakehouse
        self.inventory_date = inventory_date
        self.storage_prefix = storage_prefix
        self.parallelism = parallelism
        self.track_history = track_history
        self.container = container
        self.batch_size = batch_size

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
            StructField('file_info', StructType([StructField('operation', StringType(), False), \
            StructField('file_size', LongType(), True), StructField('row_count', LongType(), True), \
            StructField('delta_version', IntegerType(), True), \
            StructField('deletionVectorSize', LongType(), False)]), False)]

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

    def get_delta_tables_from_inventory(self):
        df = spark.table("_storage_inventory") \
            .where(col("Name").like("%/_delta_log/%.json")) \
            .withColumn("index", expr("len(name) - locate('/', reverse(name))")) \
            .withColumn("delta_table", expr("replace(substring(name, 1, index), '/_delta_log', '')")) \
            .withColumn("file_name", expr("substring(name, index + 2, len(name) - index)")) \
            .withColumn("delta_version", expr("substring(name, index + 2, 20)").cast("int")) \
            .where(~col("file_name").contains("compact")) \
            .select("delta_table", "name", "delta_version") \
            .groupBy("delta_table").agg(collect_list("delta_version").alias("delta_versions"))
        
        df = df.withColumn("delta_table_id", expr("uuid()"))

        tbls = [r["delta_table"] for r in df.collect()]

        return (tbls, df)

    def process_delta_table_logs(self, delta_tables:list[str]):
        workQueue = Queue()

        for tbl in delta_tables:
            workQueue.put(tbl)

        self.create_temp_tables()
        self.process_queue(workQueue)

    def get_clean_tbl_path(self, tbl:str) -> str:
        if self.container is not None:
            tbl_path = tbl.replace(f"{self.container}/", "", 1)
        else:
            tbl_path = tbl
        
        return f"{self.storage_prefix}{tbl_path}"

    def process_delta_log(self, tbl:str):
        tbl_path = self.get_clean_tbl_path(tbl)

        started = datetime.today()
        print(f"Starting table {tbl} ...")

        stats_schema = StructType([StructField('numRecords', LongType(), True)])
        files_schema = StructType( \
            [StructField('dataChange', BooleanType(), True), \
            StructField('deletionVector', StructType([ \
                StructField('sizeInBytes', LongType(), True)]), True), \
            StructField('path', StringType(), True), \
            StructField('stats', StringType(), True), \
            StructField('size', LongType(), True)])

        df = self.session.read.format("json").load(f"{tbl_path}/_delta_log/????????????????????.json")

        if not "add" in df.columns:
            df = df.withColumn("add", struct(*[]))

        if not "remove" in df.columns:
            df = df.withColumn("remove", struct(*[]))

        df = df.withColumn("filename", input_file_name()) \
            .withColumn("_delta_version", expr("substring(filename, len(filename) - locate('/', reverse(filename)) + 2, 20)").cast("int")) \
            .withColumn("added", from_json(to_json(col("add")), files_schema)) \
            .withColumn("removed", from_json(to_json(col("remove")), files_schema)) \
            .withColumn("add_path", col("added.path")) \
            .withColumn("remove_path", col("removed.path")) \
            .withColumn("stats", from_json(col("added.stats"), stats_schema)) \
            .withColumn("_num_records", col("stats.numRecords")) \
            .withColumn("_file_size", col("added.size")) \
            .withColumn("add_dv_sizeInBytes", col("added.deletionVector.sizeInBytes")) \
            .withColumn("remove_dv_sizeInBytes", col("removed.deletionVector.sizeInBytes"))

        adds = df.where(col("add").isNotNull()) \
            .groupBy(col("add_path").alias("path")) \
            .agg(min("_delta_version").alias("dv"), \
                max("_file_size").alias("file_size"), \
                max("_num_records").alias("row_count"), \
                max("add_dv_sizeInBytes").alias("dv_sizeInBytes")) \
            .select("path", "dv", "file_size", "row_count", "dv_sizeInBytes")
            
        removes = df.where(col("remove").isNotNull()) \
            .groupBy(col("remove_path").alias("path")) \
            .agg(max("_delta_version").alias("dv"), \
                max("remove_dv_sizeInBytes").alias("dv_sizeInBytes")) \
            .select("path", "dv", "dv_sizeInBytes")    

        adds = adds.alias("a")
        removes = removes.alias("r")

        f = adds.join(removes, "path", "left") \
            .withColumn("data_file", coalesce("r.path", "a.path")) \
            .withColumn("operation", when(isnull("r.path"), lit("ADD")).otherwise(lit("REMOVE"))) \
            .withColumn("delta_version", coalesce("r.dv", "a.dv")) \
            .withColumn("deletionVectorSize", coalesce("r.dv_sizeInBytes", "a.dv_sizeInBytes", lit(0))) \
            .withColumn("file_info", \
                struct(*[col("operation"), col("file_size"), col("row_count"), col("delta_version"), col("deletionVectorSize")])) \
            .select("data_file", "file_info")

        f = f.withColumn("delta_table", lit(tbl)) 
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

    def process_delta_inventory(self, delta_tables:list[str], tables:DataFrame):
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

    def initialize_delta_inventory(self):
        self.clear_temp_tables()

        if not self.track_history:
            print("Historical data disabled, resetting repository ...")
            self.clear_delta_inventory_schema()
    
    def run_from_storage_inventory(self, rule:str, inventory_data_path:str, \
                                    inventory_output_type:str):
        started = datetime.today()

        print(f"Starting Delta Inventory for Rule: {rule} ...")
        self.initialize_delta_inventory()
        
        inventory_file_path = f"{inventory_data_path}{self.inventory_date}/*/{rule}/*.{inventory_output_type}"
   
        print(f"Getting blob inventory {inventory_file_path} ...")
        self.load_storage_inventory(inventory_file_path, inventory_output_type)
        delta_tables, tables = self.get_delta_tables_from_inventory()

        self.process_delta_inventory(delta_tables, tables)

        completed = datetime.today()
        runtime = completed - started

        print(f"Finished Delta Inventory for Rule: {rule} in {(runtime.total_seconds()/60):.4f} mins ...")