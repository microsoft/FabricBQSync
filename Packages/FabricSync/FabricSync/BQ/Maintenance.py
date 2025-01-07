from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime

from .Core import *
from .Model.Maintenance import *
from .SyncUtils import *

class DeltaInventory(ConfigBase):
    def __init__(self, context:SparkSession, user_config, gcp_credential:str, inventory_func:callable):
        super().__init__(context, user_config, gcp_credential)

        self.lakehouse = self.UserConfig.Fabric.TargetLakehouse.lower()
        self.lakehouse_schema = "" if not self.UserConfig.Fabric.TargetLakehouseSchema else self.UserConfig.Fabric.TargetLakehouseSchema.lower()

        self.inventory_func = inventory_func
        self.storage_prefix = None
        self.lkp = None
        self.track_history = False #UserConfig

        self.inventory_date = datetime.today().strftime("%Y/%m/%d")
        self.inventory_dt = datetime.strptime(self.inventory_date, "%Y/%m/%d")

        self.inventory_year = self.inventory_dt.strftime("%Y")
        self.inventory_month = self.inventory_dt.strftime("%m")
        self.inventory_day = self.inventory_dt.strftime("%d")
    
    def _get_files_schema_(self, with_table:bool = True):        
        base_definition = [StructField('data_file', StringType(), True), 
            StructField('file_info', StructType([StructField('operation', StringType(), False), 
            StructField('file_size', LongType(), True), StructField('row_count', LongType(), True), 
            StructField('delta_version', IntegerType(), True), 
            StructField('deletionVectorSize', LongType(), False)]), False)]

        if not with_table:
            return StructType(base_definition)
        else:
            return StructType(base_definition + [StructField('lakehouse', StringType(), True),
                StructField('lakehouse_schema', StringType(), True), StructField('lakehouse_table', StringType(), True)])
    
    def _create_temp_tables_(self):
        files_schema = self._get_files_schema_()

        df = self.Context.createDataFrame([], files_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpFiles")

        base_definition = [StructField('version', LongType(), True), StructField('timestamp', TimestampType(), True), 
                    StructField('userId', StringType(), True), StructField('userName', StringType(), True), 
                    StructField('operation', StringType(), True), StructField('operationParameters', MapType(StringType(), StringType(), True), True), 
                    StructField('notebook', StructType([StructField('notebookId', StringType(), True)]), True), 
                    StructField('clusterId', StringType(), True), StructField('readVersion', LongType(), True), 
                    StructField('isolationLevel', StringType(), True), StructField('isBlindAppend', BooleanType(), True), 
                    StructField('operationMetrics', MapType(StringType(), StringType(), True), True), 
                    StructField('userMetadata', StringType(), True), StructField('engineInfo', StringType(), True), 
                    StructField('lakehouse', StringType(), True), StructField('lakehouse_schema', StringType(), True), 
                    StructField('lakehouse_table', StringType(), True)]

        job_definition = [StructField('job', StructType([StructField('jobId', StringType(), True), StructField('jobName', StringType(), True), 
            StructField('jobRunId', StringType(), True), StructField('runId', StringType(), True), StructField('jobOwnerId', StringType(), True), 
            StructField('triggerType', StringType(), True)]), True)]

        history_schema = StructType(base_definition + job_definition)

        df = self.Context.createDataFrame([], history_schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpHistory")

    def _process_delta_table_logs_async_(self, delta_tables:list[str], num_threads) -> bool:
        processor = QueueProcessor(num_threads)

        for tbl in delta_tables:
            processor.put((1,tbl))

        processor.process(self._process_delta_log_, self._thread_exception_handler_)

        return (processor.has_exceptions==False)

    def _thread_exception_handler_(self, value):
        self.Logger.sync_status(f"FAILED {value}...")

    def _process_delta_table_logs_(self, delta_tables:list[str]) -> bool:
        self._create_temp_tables_()

        if self.UserConfig.Async.Enabled:
            return self._process_delta_table_logs_async_(delta_tables, self.UserConfig.Async.Parallelism)
        else:
            return self._process_delta_table_logs_async_(delta_tables, 1)

    def _get_clean_tbl_path_(self, tbl:str) -> str:        
        return f"{self.storage_prefix}{tbl}"

    def _process_delta_log_(self, val):
        if isinstance(val, str):
            tbl = val
        else:
            tbl = val[1]

        tbl_path = self._get_clean_tbl_path_(tbl)

        with SyncTimer() as timer:
            self.Logger.sync_status(f"Starting table {tbl} ...")

            stats_schema = StructType([StructField('numRecords', LongType(), True)])
            files_schema = StructType( \
                [StructField('dataChange', BooleanType(), True), \
                StructField('deletionVector', StructType([ \
                    StructField('sizeInBytes', LongType(), True)]), True), \
                StructField('path', StringType(), True), \
                StructField('stats', StringType(), True), \
                StructField('size', LongType(), True)])

            df = self.Context.read.format("json").load(f"{tbl_path}/_delta_log/????????????????????.json")

            if not "add" in df.columns:
                df = df.withColumn("add", struct(*[lit("x_path")]))

            if not "remove" in df.columns:
                df = df.withColumn("remove", struct(*[lit("x_path")]))

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

            f = f.withColumn("lakehouse", lit(self.lakehouse)) \
                .withColumn("lakehouse_schema", lit(self.lakehouse_schema)) \
                .withColumn("lakehouse_table", lit(tbl.lower()))
            f.write.mode("APPEND").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpFiles")

            deltaTbl = DeltaTable.forPath(self.Context, f"{tbl_path}")
            h = deltaTbl.history().withColumn("lakehouse", lit(self.lakehouse)) \
                .withColumn("lakehouse_schema", lit(self.lakehouse_schema)) \
                .withColumn("lakehouse_table", lit(tbl.lower()))
            h.write.mode("APPEND").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpHistory")

        self.Logger.sync_status(f"Completed table {tbl} in {str(timer)} ...")
        
    def _get_delta_file_size_(self) -> DataFrame:
        f = self.Context.table(f"{self.UserConfig.Fabric.MetadataLakehouse}.storage_inventory_table_files") \
            .filter(col("inventory_date") == self.inventory_dt) \
            .select("*", expr("file_info['operation']").alias("operation"), \
                expr("file_info['file_size']").alias("content_size"), \
                expr("file_info['row_count']").alias("row_count"))

        f = f.withColumn("delta_table_path", concat("lakehouse_table", "data_file")) \
                .withColumn("delta_partition", expr("substring(data_file, 1, len(data_file) - locate('/', reverse(data_file)))")) \
                .withColumn("delta_partition", when(col("delta_partition").contains(".parquet"), "<default>") \
                    .otherwise(col("delta_partition")))
        
        return f

    def _get_delta_partitions_source_(self) -> DataFrame:
        agg = self._get_delta_file_size_()
        
        agg = agg.groupBy("lakehouse", "lakehouse_schema", "lakehouse_table", \
                "delta_partition", \
                "operation") \
            .agg(count("*").alias("files_count"), \
                sum("content_size").alias("file_size"), \
                sum("row_count").alias("row_count"))

        a = agg.where("operation = 'ADD'").alias("a")
        r = agg.where("operation = 'REMOVE'").alias("r")

        p = a.join(r, (a["lakehouse"]==r["lakehouse"]) & (a["lakehouse_schema"]==r["lakehouse_schema"]) \
            & (a["lakehouse_table"]==r["lakehouse_table"]) & (a["delta_partition"]==r["delta_partition"]), "left") \
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

    def _get_delta_table_snapshot_(self, partitions:DataFrame) -> DataFrame:
        t = partitions.groupBy("lakehouse", "lakehouse_schema", "lakehouse_table") \
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

    def _get_lookup_table_(self) -> DataFrame:
        if not self.lkp:
            self.lkp = self.Context.table(f"{self.UserConfig.Fabric.MetadataLakehouse}.storage_inventory_tables") \
                .select("inventory_id", "lakehouse", "lakehouse_schema", "lakehouse_table") \
                .alias("lkp")
        
        return self.lkp

    def _lookup_inventory_id_(self, df:DataFrame) -> DataFrame:
        lkp = self._get_lookup_table_()

        df = df.join(lkp, (df["lakehouse"]==lkp["lakehouse"]) & \
            (df["lakehouse_schema"]==lkp["lakehouse_schema"]) & (df["lakehouse_table"]==lkp["lakehouse_table"])) \
                .select(df["*"], col("lkp.inventory_id"))
            
        return df

    def _save_dataframe_(self, df:DataFrame, delta_table:str, merge_criteria:list[str] = [], temporal:bool = True):
        if "lakehouse_table" in df.columns:
            if not "inventory_id" in df.columns:
                df = self._lookup_inventory_id_(df)

        if temporal and not "inventory_date" in df.columns:
            df = df.withColumn("inventory_date", lit(self.inventory_dt))
        
        if self.Context.catalog.tableExists(delta_table):
            merge_criteria = merge_criteria + ["lakehouse", "lakehouse_schema"]

            if "lakehouse_table" not in merge_criteria:
                merge_criteria.append("inventory_id")

            if temporal:
                merge_criteria.append("inventory_date")

            criteria = [f"s.{t} = t.{t}" for t in merge_criteria]

            deltaTable = DeltaTable.forName(self.Context, delta_table)

            deltaTable.alias('t') \
                .merge(df.alias('s'), " AND ".join(criteria)) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            if not temporal:
                df.write.mode("OVERWRITE").saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{delta_table}")
            else:
                if not self.UserConfig.Fabric.TargetLakehouseSchema:
                    df.write.mode("OVERWRITE").partitionBy("lakehouse", "inventory_date") \
                        .saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{delta_table}")
                else:
                    df.write.mode("OVERWRITE").partitionBy("lakehouse", "lakehouse_schema", "inventory_date") \
                        .saveAsTable(f"{self.UserConfig.Fabric.MetadataLakehouse}.{delta_table}")

    def _process_delta_inventory_(self, delta_tables:list[str], tables:DataFrame):
        self.Logger.sync_status(f"Processing delta table logs ...")
        result = self._process_delta_table_logs_(delta_tables)

        if result:
            self.Logger.sync_status(f"Saving inventory [tables] ...")
            self._save_dataframe_(tables, f"storage_inventory_tables", ["lakehouse_table"], False)

            self.Logger.sync_status(f"Saving inventory [files] ...")
            df = self.Context.table(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpFiles") \
                .withColumn("inventory_date", lit(self.inventory_dt))
            self._save_dataframe_(df, f"storage_inventory_table_files", ["data_file"])
            
            self.Logger.sync_status(f"Saving inventory [history] ...")
            df = self.Context.table(f"{self.UserConfig.Fabric.MetadataLakehouse}.tmpHistory") \
                .withColumn("inventory_date", lit(self.inventory_dt))
            self._save_dataframe_(df, f"storage_inventory_table_history", ["version"])

            partitions = self._get_delta_partitions_source_()
            snapshot = self._get_delta_table_snapshot_(partitions)
            
            self.Logger.sync_status(f"Saving inventory [partitions] ...")
            self._save_dataframe_(partitions, f"storage_inventory_table_partitions", ["delta_partition"])
            
            self.Logger.sync_status(f"Saving inventory [snapshot] ...")
            self._save_dataframe_(snapshot, f"storage_inventory_table_snapshot")
        else:
            self.Logger.sync_status(f"Inventory FAILED ...")

        self._clear_temp_tables_()

    def _clear_temp_tables_(self):
        SparkProcessor(self.Context).drop(SyncConstants.get_inventory_temp_tables(), self.UserConfig.Fabric.MetadataLakehouse)

    def _clear_delta_inventory_schema_(self):
        SparkProcessor(self.Context).drop(SyncConstants.get_inventory_tables(), self.UserConfig.Fabric.MetadataLakehouse)

    def _clear_inventory_partition(self):
        tables = ["storage_inventory_table_files", "storage_inventory_table_history", "storage_inventory_table_partitions"]

        cmds = [f"""DELETE FROM {self.UserConfig.Fabric.MetadataLakehouse}.{tbl} 
                    WHERE lakehouse='{self.lakehouse}' 
                    AND lakehouse_schema='{self.lakehouse_schema}' 
                    AND inventory_date='{self.inventory_date}'""" \
            for tbl in tables]

        SparkProcessor(self.Context).process_command_list(cmds)   

    def _initialize_delta_inventory_(self):
        self._clear_temp_tables_()

        if not self.track_history:
            self.Logger.sync_status("Historical data disabled...")
            self._clear_delta_inventory_schema_()
        else:
            self.Logger.sync_status("Resetting inventory date (if exists) ...")
            self._clear_inventory_partition()
    
    def run_inventory(self, **kwargs):
        if not self.inventory_func:
            return
        
        with SyncTimer() as timer:
            self.Logger.sync_status(f"Starting Lakehouse Inventory...")
            self._initialize_delta_inventory_()

            delta_tables, tables = self.inventory_func(kwargs)

            self._process_delta_inventory_(delta_tables, tables)
            self.Context.sql(f"USE {self.UserConfig.Fabric.MetadataLakehouse}")

        self.Logger.sync_status(f"Finished Lakehouse Inventory in {str(timer)}...")

class DeltaOneLakeInventory(DeltaInventory):
    def __init__(self, context:SparkSession, user_config, gcp_credential:str, api_token:str):
        super().__init__(context, user_config, gcp_credential, self._get_onelake_catalog_)
        self.token = api_token

        self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def _load_onelake_tables_(self):
        self.Context.sql(f"USE {self.lakehouse}")

        df = self.Context.sql(f"SHOW TABLES IN {self.lakehouse}")

        df = df.where(f"isTemporary=false and namespace='{self.lakehouse}'") \
            .withColumn("lakehouse", lower(col("namespace"))) \
            .withColumn("lakehouse_schema", lit("")) \
            .withColumn("lakehouse_table", lower(col("tableName"))) \
            .withColumn("inventory_id", expr("uuid()"))  \
            .select("lakehouse", "lakehouse_schema", "lakehouse_table", "inventory_id")   

        onelake_tables = [t["lakehouse_table"] for t in df.collect()]

        return (onelake_tables, df)

    def _get_onelake_catalog_(self, kwargs):
        workspace_id = self.Context.conf.get("trident.workspace.id")
        fabric_api = FabricAPI(workspace_id, self.token)

        lakehouse_id = fabric_api.get_lakehouse_id(self.lakehouse)

        if not lakehouse_id:
            raise SyncConfigurationError("Target Lakehouse not valid.")
        
        self.storage_prefix = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/"
        
        self.Logger.sync_status(f"Loading tables to inventory for {self.lakehouse} ...")
        return self._load_onelake_tables_()

class FabricSyncMaintenance(SyncBase):
    def __init__(self, context:SparkSession, config_path:str, api_token:str):
        super().__init__(context, config_path, clean_session=True)

        self.InventoryManager = DeltaOneLakeInventory(context, self.UserConfig, self.GCPCredential, api_token)

    @Telemetry.Lakehouse_Inventory()
    def run_lakehouse_inventory(self):
        self.InventoryManager.run_inventory()

    def run(self, sync_user_config:bool=False):
        if not self.UserConfig.Maintenance.Enabled:
            self.Logger.sync_status("Fabric Sync Maintenance is not enabled. Please update User Configuration to enable maintenance.")
            return
        
        self.Metastore.create_proxy_views()

        if sync_user_config:
            self.Logger.sync_status(f"Syncing User Config with Metadata metastore...")
            self.Metastore.update_maintenance_config(sync_id=self.UserConfig.ID)

        self.Logger.sync_status(f"Async {self.UserConfig.Maintenance.Strategy} Maintenance started with parallelism of {self.UserConfig.Async.Parallelism}...")

        with SyncTimer() as t:
            if self.UserConfig.Maintenance.Strategy == str(MaintenanceStrategy.SCHEDULED):
                self._run_scheduled_maintenance_()
        
        self.Logger.sync_status(f"{self.UserConfig.Maintenance.Strategy} Maintenance finished in {str(t)}...")
    
    @Telemetry.Delta_Maintenance(maintainence_type="SCHEDULED")
    def _run_scheduled_maintenance_(self):
        df = self.Metastore.get_scheduled_maintenance_schedule(self.UserConfig.ID)

        schedule = [MaintenanceSchedule(**(r.asDict())) for r in df.collect()]
        self._run_maintenance_(schedule)
    
    def _thread_exception_handler_(self, value):
        schedule = value[2]

        schedule.LastStatus = "FAILED"
        self.results.append(schedule)
        self.maintenance_failures(value[1], schedule)

    def _get_failed_maintenance_(self, id) -> MaintenanceSchedule:
        if self.maintenance_failures:
            if id in self.maintenance_failures.keys():
                return self.maintenance_failures[id]
        
        return None

    def _run_maintenance_(self, schedule):
        if schedule:
            self.results = ThreadSafeList()
            self.maintenance_failures = ThreadSafeDict()

            processor = QueueProcessor(num_threads=self.UserConfig.Async.Parallelism)
            [processor.put((s.PartitionIndex, s.Id, s)) for s in schedule]
            processor.process(self._maintenance_job_, self._thread_exception_handler_)

            processed = []

            for s in self.results.unsafe_list:
                if s.FullTableMaintenance and s.PartitionIndex > 1:
                    failed_maint = self._get_failed_maintenance_(s.Id)

                    if failed_maint:
                        s.LastStatus = failed_maint.LastStatus
                        s.LastMaintenance = failed_maint.LastMaintenance
                        s.LastOptimize = failed_maint.LastOptimize
                        s.LastVacuum = failed_maint.LastVacuum

                if not s.PartitionId:
                    s.PartitionId = ""

                s.LastUpdatedDt = datetime.now()
                processed.append(s)

            self.Metastore.update_maintenance_schedule(processed)
    
    def _maintenance_job_(self, value) -> MaintenanceSchedule:
        schedule = value[2]

        maint_type = "Full Table" if schedule.FullTableMaintenance else "Partition"
        
        if (schedule.FullTableMaintenance and schedule.PartitionIndex == 1) or not schedule.FullTableMaintenance:
            with SyncTimer() as t:
                self.Logger.sync_status(f"Starting {maint_type} Maintenance for {value[1]}...")

                try:
                    if schedule.PartitionId:
                        schedule.LakehousePartition = SyncUtil.resolve_fabric_partition_predicate(partition_type=schedule.PartitionType, 
                            partition_column=schedule.PartitionColumn, partition_grain=schedule.PartitionGrain, 
                            partition_id=schedule.PartitionId)

                    schedule = self._optimize_(schedule)
                    schedule = self._vacuum_(schedule)

                    schedule.LastStatus = f"SUCCESS - {maint_type}"
                except Exception as e:
                    self.Logger.sync_status(f"FAILED: {maint_type} Maintenance for {value[1]} failed with error: {str(e)}")
                    raise SyncDataMaintenanceError(msg=str(e)) from e
            
            self.Logger.sync_status(f"{maint_type} Maintenance for {value[1]} finished in {str(t)}...")
        else:
            schedule.LastStatus = f"SUCCESS - {maint_type}"

            if schedule.RunOptimize:
                schedule.LastOptimize = datetime.now()
                schedule.LastMaintenance = datetime.now()
                
            if schedule.RunVacuum:
                schedule.LastVacuum = datetime.now()
                schedule.LastMaintenance = datetime.now()

        self.results.append(schedule)

        return schedule

    def _optimize_(self, schedule:MaintenanceSchedule) -> MaintenanceSchedule:
        if schedule.RunOptimize:
            if not schedule.FullTableMaintenance and schedule.LakehousePartition:
                sql = f"OPTIMIZE {schedule.FabricLakehousePath} WHERE {schedule.LakehousePartition}"
            else:
                sql = f"OPTIMIZE {schedule.FabricLakehousePath}"

            self.Context.sql(sql)
            schedule.LastOptimize = datetime.now()
            schedule.LastMaintenance = datetime.now()

        return schedule

    def _vacuum_(self, schedule:MaintenanceSchedule) -> MaintenanceSchedule:
        if schedule.RunVacuum:
            self.Context.sql(f"VACUUM {schedule.FabricLakehousePath} RETAIN {schedule.RetentionHours} HOURS")
            schedule.LastVacuum = datetime.now()
            schedule.LastMaintenance = datetime.now()

        return schedule