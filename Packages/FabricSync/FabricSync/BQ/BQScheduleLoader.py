class BQScheduleLoader(ConfigBase):
    def __init__(self, config_path, load_proxy_views=True, force_config_reload = False):
        super().__init__(config_path, force_config_reload)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")

        if load_proxy_views:
            super().create_proxy_views()

    def save_schedule_telemetry(self, schedule: SyncSchedule):
        tbl = f"{SyncConstants.SQL_TBL_SYNC_SCHEDULE_TELEMETRY}"

        schema = spark.table(tbl).schema

        rdd = spark.sparkContext.parallelize([Row(
            schedule_id=schedule.ScheduleId,
            project_id=schedule.ProjectId,
            dataset=schedule.Dataset,
            table_name=schedule.TableName,
            partition_id=schedule.PartitionId,
            status="COMPLETE",
            started=schedule.StartTime,
            completed=schedule.EndTime,
            src_row_count=schedule.SourceRows,
            dest_row_count=schedule.DestRows,
            inserted_row_count=schedule.InsertedRows,
            updated_row_count=schedule.UpdatedRows,
            delta_version=schedule.DeltaVersion,
            spark_application_id=schedule.SparkAppId,
            max_watermark=schedule.MaxWatermark,
            summary_load=schedule.SummaryLoadType
        )])

        df = spark.createDataFrame(rdd, schema)
        df.write.mode(SyncConstants.APPEND).saveAsTable(tbl)


    def get_table_delta_version(self, tbl):
        sql = f"DESCRIBE HISTORY {tbl}"
        df = spark.sql(sql) \
            .select(max(col("version")).alias("delta_version"))

        for row in df.collect():
            return row["delta_version"]

    def update_sync_config_state(self, project_id, dataset, table_name):
        sql = f"""
        UPDATE {SyncConstants.SQL_TBL_SYNC_CONFIG} 
        SET sync_state='COMMIT' 
        WHERE
            project_id='{project_id}' AND
            dataset='{dataset}' AND
            table_name='{table_name}'
        """
        spark.sql(sql)

    def get_schedule(self):
        sql = f"""
        WITH last_completed_schedule AS (
            SELECT schedule_id, project_id, dataset, table_name, max_watermark, started AS last_schedule_dt
            FROM (
                SELECT schedule_id, project_id, dataset, table_name, started, max_watermark,
                ROW_NUMBER() OVER(PARTITION BY project_id, dataset, table_name ORDER BY scheduled DESC) AS row_num
                FROM {SyncConstants.SQL_TBL_SYNC_SCHEDULE}
                WHERE status='COMPLETE'
            )
            WHERE row_num = 1
        ),
        tbl_partitions AS (
            SELECT
                sp.table_catalog, sp.table_schema, sp.table_name, sp.partition_id
            FROM bq_information_schema_partitions sp
            JOIN {SyncConstants.SQL_TBL_SYNC_CONFIG} c ON
                sp.table_catalog = c.project_id AND 
                sp.table_schema = c.dataset AND
                sp.table_name = c.table_name
            LEFT JOIN last_completed_schedule s ON 
                sp.table_catalog = s.project_id AND 
                sp.table_schema = s.dataset AND
                sp.table_name = s.table_name
            WHERE ((sp.last_modified_time >= s.last_schedule_dt) OR (s.last_schedule_dt IS NULL))
            AND 
                ((c.load_strategy = 'PARTITION' AND s.last_schedule_dt IS NOT NULL) OR
                    c.load_strategy = 'TIME_INGESTION')
        )

        SELECT c.*, 
            p.partition_id,
            s.schedule_id,
            h.max_watermark,
            h.last_schedule_dt,
            CASE WHEN (h.schedule_id IS NULL) THEN TRUE ELSE FALSE END AS initial_load
        FROM {SyncConstants.SQL_TBL_SYNC_CONFIG} c
        JOIN {SyncConstants.SQL_TBL_SYNC_SCHEDULE} s ON 
            c.project_id = s.project_id AND
            c.dataset = s.dataset AND
            c.table_name = s.table_name
        LEFT JOIN last_completed_schedule h ON
            c.project_id = h.project_id AND
            c.dataset = h.dataset AND
            c.table_name = h.table_name
        LEFT JOIN tbl_partitions p ON
            p.table_catalog = c.project_id AND 
            p.table_schema = c.dataset AND
            p.table_name = c.table_name
        LEFT JOIN {SyncConstants.SQL_TBL_SYNC_SCHEDULE_TELEMETRY} t ON
            s.schedule_id = t.schedule_id AND
            c.project_id = t.project_id AND
            c.dataset = t.dataset AND
            c.table_name = t.table_name AND
            COALESCE(p.partition_id, '0') = COALESCE(t.partition_id, '0') AND
            t.status = 'COMPLETE'
        WHERE s.status = 'SCHEDULED'
            AND c.enabled = TRUE
            AND t.schedule_id IS NULL
            AND c.project_id = '{self.UserConfig.ProjectID}' 
            AND c.dataset = '{self.UserConfig.Dataset}'
        ORDER BY c.priority
        """
        df = spark.sql(sql)
        df.createOrReplaceTempView("LoaderQueue")
        df.cache()

        return df

    def get_max_watermark(self, lakehouse_tbl, watermark_col):
        df = spark.table(lakehouse_tbl) \
            .select(max(col(watermark_col)).alias("watermark"))

        for row in df.collect():
            return row["watermark"]

    def sync_bq_table(self, schedule:SyncSchedule):
        print("{0} {1}...".format(schedule.SummaryLoadType, schedule.TableName))

        if (schedule.LoadStrategy == SyncConstants.PARTITION or \
                schedule.LoadStrategy == SyncConstants.TIME_INGESTION) and schedule.PartitionId is not None:
            print("Load by partition...")
            src = f"{schedule.BQTableName}"

            match schedule.PartitionGrain:
                case "DAY":
                    part_format = "%Y%m%d"
                case "MONTH":
                    part_format = "%Y%m"
                case "YEAR":
                    part_format = "%Y"
                case "HOUR":
                    part_format = "%Y%m%d%H"
                case _:
                    raise Exception("Unsupported Partition Grain in Table Config")

            if schedule.PartitionColumn == "_PARTITIONTIME":                   
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            df_bq = super().read_bq_partition_to_dataframe(src, part_filter)
        else:
            src = schedule.BQTableName     

            if schedule.SourceQuery != "":
                src = schedule.SourceQuery

            df_bq = super().read_bq_to_dataframe(src)

        predicate = None

        if schedule.LoadStrategy == SyncConstants.WATERMARK and not schedule.InitialLoad:
            pk = schedule.PrimaryKey
            max_watermark = schedule.MaxWatermark

            if max_watermark.isdigit():
                predicate = f"{pk} > {max_watermark}"
            else:
                predicate = f"{pk} > '{max_watermark}'"
            
        if predicate is not None:
            df_bq = df_bq.where(predicate)

        df_bq.cache()

        partition = None

        if schedule.IsPartitioned:
            print('Resolving Fabric partitioning...')
            if schedule.PartitionType == SyncConstants.TIME:
                partition_col = schedule.PartitionColumn
                if not schedule.IsTimeIngestionPartitioned:
                    part_format = ""
                    part_col_name = f"__bq_part_{partition_col}"
                    use_proxy_col = False

                    match schedule.PartitionGrain:
                        case "DAY":
                            part_format = "yyyyMMdd"

                            if dict(df_bq.dtypes)[partition_col] == "date":
                                partition = partition_col
                            else:
                                partition = f"{part_col_name}_DAY"
                                use_proxy_col = True
                        case "MONTH":
                            part_format = "yyyyMM"
                            partition = f"{part_col_name}_MONTH"
                            use_proxy_col = True
                        case "YEAR":
                            part_format = "yyyy"
                            partition = f"{part_col_name}_YEAR"
                            use_proxy_col = True
                        case "HOUR":
                            part_format = "yyyyMMddHH"
                            partition = f"{part_col_name}_HOUR"
                            use_proxy_col = True
                        case _:
                            print('Unsupported partition grain...')
                
                    print("{0} partitioning - partitioned by {1} (Requires Proxy Column: {2})".format( \
                        schedule.PartitionGrain, \
                        partition, \
                        use_proxy_col))
                    
                    if use_proxy_col:
                        df_bq = df_bq.withColumn(partition, date_format(col(partition_col), part_format))
                else:
                    part_format = ""
                    partition = f"__bq{partition_col}"

                    match schedule.PartitionGrain:
                        case "DAY":
                            part_format = "%Y%m%d"
                        case "MONTH":
                            part_format = "%Y%m"
                        case "YEAR":
                            part_format = "%Y"
                        case "HOUR":
                            part_format = "%Y%m%d%H"
                        case _:
                            print('Unsupported partition grain...')
                    
                    print("Ingestion time partitioning - partitioned by {0} ({1})".format(partition, schedule.PartitionId))
                    df_bq = df_bq.withColumn(partition, lit(schedule.PartitionId))


        if (schedule.LoadStrategy == SyncConstants.PARTITION or \
                schedule.LoadStrategy == SyncConstants.TIME_INGESTION) and schedule.PartitionId is not None:
            print(f"Writing {schedule.TableName}${schedule.PartitionId} partition...")
            part_filter = f"{partition} = '{schedule.PartitionId}'"

            df_bq.write \
                .mode(SyncConstants.OVERWRITE) \
                .option("replaceWhere", part_filter) \
                .saveAsTable(schedule.LakehouseTableName)
        else:
            if partition is None:
                df_bq.write \
                    .mode(schedule.Mode) \
                    .saveAsTable(schedule.LakehouseTableName)
            else:
                df_bq.write \
                    .partitionBy(partition) \
                    .mode(schedule.Mode) \
                    .saveAsTable(schedule.LakehouseTableName)

        if schedule.LoadStrategy == SyncConstants.WATERMARK:
            schedule.MaxWatermark = self.get_max_watermark(schedule.LakehouseTableName, schedule.PrimaryKey)

        src_cnt = df_bq.count()

        if (schedule.LoadStrategy == SyncConstants.PARTITION or \
                schedule.LoadStrategy == SyncConstants.TIME_INGESTION)  and schedule.PartitionId is not None:
            dest_cnt = src_cnt
        else:
            dest_cnt = spark.table(schedule.LakehouseTableName).count()

        schedule.UpdateRowCounts(src_cnt, dest_cnt, 0, 0)    
        schedule.SparkAppId = spark.sparkContext.applicationId
        schedule.DeltaVersion = self.get_table_delta_version(schedule.LakehouseTableName)
        schedule.EndTime = datetime.now(timezone.utc)

        df_bq.unpersist()

        return schedule

    def process_load_group_telemetry(self, load_grp = None):
        load_grp_filter = ""

        if load_grp is not None:
            load_grp_filter = f"AND r.priority = '{load_grp}'"

        sql = f"""
        WITH schedule_telemetry AS (
                SELECT
                        schedule_id,
                        project_id,
                        dataset,
                        table_name,
                        SUM(CASE WHEN status='COMPLETE' THEN 1 ELSE 0 END) AS completed_activities,
                        SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_activities,
                        MIN(started) as started,
                        MAX(completed) as completed
                FROM bq_sync_schedule_telemetry
                GROUP BY
                schedule_id,
                project_id,
                dataset,
                table_name
        ),
        schedule_watermarks AS (
                SELECT
                        schedule_id,
                        project_id,
                        dataset,
                        table_name,
                        max_watermark,
                        ROW_NUMBER() OVER(PARTITION BY schedule_id,
                                project_id,
                                dataset,
                                table_name ORDER BY completed DESC) AS row_num
                FROM bq_sync_schedule_telemetry
                WHERE max_watermark IS NOT NULL
        ),
        schedule_results AS (
                SELECT
                        s.schedule_id,
                        s.project_id,
                        s.dataset,
                        s.table_name,
                        s.status,
                        CASE WHEN t.failed_activities = 0 THEN 'COMPLETE' ELSE 'FAILED' END AS result_status,
                        t.started,
                        t.completed,
                        t.completed_activities,
                        t.failed_activities,
                        w.max_watermark,
                        s.priority 
                FROM bq_sync_schedule s
                JOIN schedule_telemetry t ON 
                        s.schedule_id = t.schedule_id AND
                        s.project_id = t.project_id AND
                        s.dataset = t.dataset AND
                        s.table_name = t.table_name
                LEFT JOIN schedule_watermarks w ON
                        s.schedule_id = w.schedule_id AND
                        s.project_id = w.project_id AND
                        s.dataset = w.dataset AND
                        s.table_name = w.table_name
        )  

        MERGE INTO bq_sync_schedule s
        USING ( 
                SELECT *
                FROM schedule_results r
                WHERE r.status='SCHEDULED'
                {load_grp_filter}
        ) r
        ON s.schedule_id = r.schedule_id AND
                s.project_id = r.project_id AND
                s.dataset = r.dataset AND
                s.table_name = r.table_name
        WHEN MATCHED THEN
                UPDATE SET
                        s.status = r.result_status,
                        s.started = r.started,
                        s.completed = r.completed,
                        s.completed_activities = r.completed_activities,
                        s.failed_activities = r.failed_activities,
                        s.max_watermark = r.max_watermark

        """
        spark.sql(sql)

    def run_sequential_schedule(self):
        df_schedule = self.get_schedule()

        for row in df_schedule.collect():
            schedule = SyncSchedule(row)

            self.sync_bq_table(schedule)

            self.save_schedule_telemetry(schedule)  

        self.process_load_group_telemetry()
    
    def run_aync_schedule(self):
        dag = ScheduleDAG(timeout=self.UserConfig.Async.NotebookTimeout, \
            concurrency=self.UserConfig.Async.Parallelism)

        schedule = self.get_schedule()

        load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

        grp_dependency = None

        for grp in load_grps:
            checkpoint_dependencies = []
            grp_nm = "GROUP_{0}".format(grp)
            grp_df = schedule.where(f"priority = '{grp}'")

            for tbl in grp_df.collect():
                nm = "{0}.{1}".format(tbl["dataset"], tbl["table_name"])
                dependencies = []

                if tbl["partition_id"] is not None:
                    nm = "{0}${1}".format(nm, tbl["partition_id"])

                if grp_dependency is not None:
                    dependencies.append(grp_dependency)
                
                dag.activities.append( \
                    DAGActivity(nm, "BQ_TBL_PART_LOADER", \
                        self.UserConfig.Async.CellTimeout, \
                        None, None, \
                        dependencies, \
                        schedule_id=tbl["schedule_id"], \
                        project_id=tbl["project_id"], \
                        dataset=tbl["dataset"], \
                        table_name=tbl["table_name"], \
                        partition_id=tbl["partition_id"], \
                        config_json_path=config_json_path))

                checkpoint_dependencies.append(nm)
                print(f"Load Activity: {nm}")

            
            grp_dependency = grp_nm
            print(f"Load Group Checkpoint: {grp_nm}")
            dag.activities.append( \
                DAGActivity(grp_nm, "BQ_LOAD_GROUP_CHECKPOINT", \
                    self.UserConfig.Async.CellTimeout, \
                    None, None, \
                    checkpoint_dependencies, \
                    load_group=grp, \
                    config_json_path=self.ConfigPath))
        
        dag_json = json.dumps(dag, indent=4, cls=ScheduleDAGEncoder)
        #print(dag_json)
        schedule_dag = json.loads(dag_json)

        dag_result = mssparkutils.notebook.runMultiple(schedule_dag, {"displayDAGViaGraphviz":True, "DAGLayout":"spectral", "DAGSize":8})
        return dag_result