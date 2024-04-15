
from pyspark.sql.functions import *
from delta.tables import *
from datetime import datetime, timezone


class BQScheduleLoader(ConfigBase):
    def __init__(self, config_path, gcp_credential):
        super().__init__(config_path, gcp_credential)
        super().create_proxy_views()

    def save_telemetry(self, telemetry: SyncSchedule):
        df = spark.table(SyncConstants.SQL_TBL_SYNC_SCHEDULE) \
            .filter("schedule_id=='{0}'".format(telemetry.ScheduleId)) \
            .withColumn("started", lit(telemetry.StartTime)) \
            .withColumn("src_row_count", lit(telemetry.SourceRows)) \
            .withColumn("dest_row_count", lit(telemetry.DestRows)) \
            .withColumn("dest_inserted_row_count", lit(telemetry.InsertedRows)) \
            .withColumn("dest_updated_row_count", lit(telemetry.UpdatedRows)) \
            .withColumn("delta_version", lit(telemetry.DeltaVersion)) \
            .withColumn("spark_application_id", lit(telemetry.SparkAppId)) \
            .withColumn("max_watermark", lit(telemetry.MaxWatermark)) \
            .withColumn("summary_load", lit(telemetry.SummaryLoadType)) \
            .withColumn("status", lit("COMPLETE")) \
            .withColumn("completed", lit(telemetry.EndTime))

        schedule_df = DeltaTable.forName(spark, SyncConstants.SQL_TBL_SYNC_SCHEDULE).alias("t")

        schedule_df.merge( \
            df.alias('s'), 't.schedule_id = s.schedule_id') \
            .whenMatchedUpdate(set = \
            { \
            "status": "s.status", \
            "started": "s.started", \
            "completed": "s.completed", \
            "src_row_count": "s.src_row_count", \
            "dest_row_count": "s.dest_row_count", \
            "dest_inserted_row_count": "s.dest_inserted_row_count", \
            "dest_updated_row_count": "s.dest_updated_row_count", \
            "delta_version": "s.delta_version", \
            "spark_application_id": "s.spark_application_id", \
            "max_watermark": "s.max_watermark", \
            "summary_load": "s.summary_load" \
            } \
        ).execute()

        if telemetry.LoadStrategy == SyncConstants.PARTITION and not telemetry.InitialLoad:
            self.save_partition_telemtry(telemetry)

    def save_partition_telemtry(self, telemetry: SyncSchedule):
        sql = f"""
        SELECT 
            s.schedule_id, s.project_id, s.dataset, s.table_name, 
            p.partition_id, p.total_rows AS bq_total_rows, 
            p.last_modified_time AS bq_last_modified, p.storage_tier as bq_storage_tier,
            s.started, s.completed
        FROM {SyncConstants.SQL_TBL_SYNC_SCHEDULE_PARTITION} s
        JOIN bq_information_schema_partitions p ON
            s.project_id=p.table_catalog AND
            s.dataset=p.table_schema AND 
            s.table_name=p.table_name
        WHERE s.schedule_id='{telemetry.ScheduleId}'
        """
        df = spark.sql(sql)

        df.write.mode(SyncConstants.APPEND).saveAsTable(SyncConstants.SQL_TBL_SYNC_SCHEDULE_PARTITION)

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

    def get_table_partition_metadata(self, schedule):
        sql = f"""
        WITH last_load AS (
            SELECT project_id, dataset, table_name, MAX(started) AS last_load_update
            FROM {SyncConstants.SQL_TBL_SYNC_SCHEDULE}
            WHERE status='COMPLETE'
            GROUP BY project_id, dataset, table_name
        )

        SELECT
            sp.table_name, sp.partition_id, sp.total_rows, sp.last_modified_time, sp.storage_tier,
            s.last_load_update AS last_part_load
        FROM bq_information_schema_partitions sp
        LEFT JOIN last_load s ON 
            sp.table_catalog = s.project_id AND 
            sp.table_schema = s.dataset AND
            sp.table_name = s.table_name
        WHERE sp.table_catalog = '{schedule.ProjectId}'
        AND sp.table_schema = '{schedule.Dataset}'
        AND sp.table_name = '{schedule.TableName}'
        AND sp.last_modified_time >= s.last_load_update
        """
        return spark.sql(sql)

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
        )

        SELECT c.*, 
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
        WHERE s.status = 'SCHEDULED'
            AND s.priority = 100
            AND c.enabled = TRUE
            AND c.project_id = '{self.UserConfig.ProjectID}' 
            AND c.dataset = '{self.UserConfig.Dataset}'
        """
        df = spark.sql(sql)
        df.cache()

        return df

    def build_bq_partition_query(self, schedule, predicate = []):
        query = f"SELECT * FROM {schedule.BQTableName}"

        if predicate:
            query += " WHERE "
            query += " OR ".join(predicate)
        
        print(query)
        return query

    def get_max_watermark(self, lakehouse_tbl, watermark_col):
        df = spark.table(lakehouse_tbl) \
            .select(max(col(watermark_col)).alias("watermark"))

        for row in df.collect():
            return row["watermark"]

    def sync_bq_table(self, row):
        schedule = SyncSchedule(row)

        print("{0} {1}...".format(schedule.SummaryLoadType, schedule.TableName))

        if schedule.LoadStrategy == SyncConstants.PARTITION and not schedule.InitialLoad:
            print("Load by partition...")
            df_partitions = self.get_table_partition_metadata(schedule)

            predicate = []
            partitions_to_write = []

            for p in df_partitions.collect():
                part_id = p["partition_id"]

                if p["last_modified_time"] > p["last_part_load"]:
                    print("Partition {0} has changes...".format(part_id))

                    partitions_to_write.append(part_id)
                    
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
                                
                    predicate.append(f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{part_id}')")
                else:
                    print("Partition {0} is up to date...".format(part_id))

            src = self.build_bq_partition_query(schedule, predicate)
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

        if schedule.IsPartitioned and not schedule.IsTimeIngestionPartitioned:
            print('Resolving Fabric partitioning...')
            if schedule.PartitionType == SyncConstants.TIME:
                partition_col = schedule.PartitionColumn
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
                    row["partition_grain"], \
                    partition, \
                    use_proxy_col))
                
                if use_proxy_col:
                    df_bq = df_bq.withColumn(partition, date_format(col(partition_col), part_format))

        if schedule.LoadStrategy == SyncConstants.PARTITION and not schedule.InitialLoad:
            if partitions_to_write:
                for part in partitions_to_write:
                    print(f"Writing {schedule.TableName}${part} partition...")
                    part_filter = f"{partition} = '{part}'"
                    
                    pdf = df_bq.where(part_filter)
                    part_cnt = pdf.count()
                    
                    schedule.UpdateRowCounts(0, part_cnt, 0, 0)

                    pdf.write \
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

        if schedule.LoadStrategy != SyncConstants.PARTITION or schedule.InitialLoad:
            dest_cnt = spark.table(schedule.LakehouseTableName).count()
        else:
            dest_cnt = 0

        schedule.UpdateRowCounts(src_cnt, dest_cnt, 0, 0)    
        schedule.SparkAppId = spark.sparkContext.applicationId
        schedule.DeltaVersion = self.get_table_delta_version(schedule.LakehouseTableName)
        schedule.EndTime = datetime.now(timezone.utc)

        df_bq.unpersist()

        self.save_telemetry(schedule)

        if schedule.InitialLoad:
            self.update_sync_config_state(schedule.ProjectId, schedule.Dataset, schedule.TableName)

    def run_schedule(self):
        df_schedule = self.get_schedule()

        for row in df_schedule.collect():
            self.sync_bq_table(row)  