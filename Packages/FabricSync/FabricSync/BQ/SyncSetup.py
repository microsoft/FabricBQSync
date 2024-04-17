class SyncSetup(ConfigBase):
    def __init__(self, config_path):
        if spark.catalog.tableExists("user_config_json"):
            spark.catalog.dropTempView("user_config_json")

        super().__init__(config_path)

    def get_fabric_lakehouse(self, nm):
        lakehouse = None

        try:
            lakehouse = mssparkutils.lakehouse.get(nm)
        except Exception:
            print("Lakehouse not found: {0}".format(nm))

        return lakehouse

    def create_fabric_lakehouse(self, nm):
        lakehouse = self.get_fabric_lakehouse(nm)

        if (lakehouse is None):
            print("Creating Lakehouse {0}...".format(nm))
            mssparkutils.lakehouse.create(nm)

    def setup(self):
        self.create_fabric_lakehouse(self.UserConfig.MetadataLakehouse)
        self.create_fabric_lakehouse(self.UserConfig.TargetLakehouse)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")
        self.create_all_tables()

    def drop_table(self, tbl):
        sql = f"DROP TABLE IF EXISTS {tbl}"
        spark.sql(sql)

    def get_tbl_name(self, tbl):
        return self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

    def create_data_type_map_tbl(self):
        tbl_nm = self.get_tbl_name(SyncConstants.SQL_TBL_DATA_TYPE_MAP)
        self.drop_table(tbl_nm)

        sql = f"""CREATE TABLE IF NOT EXISTS {tbl_nm} (data_type STRING, partition_type STRING, is_watermark STRING)"""
        spark.sql(sql)

        df = spark.read.format("csv").option("header","true").load("Files/data/bq_data_types.csv")
        df.write.mode("OVERWRITE").saveAsTable(tbl_nm)

    def create_sync_config_tbl(self):
        tbl_nm = self.get_tbl_name(SyncConstants.SQL_TBL_SYNC_CONFIG)
        self.drop_table(tbl_nm)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {tbl_nm}
        (
            project_id STRING,
            dataset STRING,
            table_name STRING,
            enabled BOOLEAN,
            lakehouse STRING,
            lakehouse_table_name STRING,
            source_query STRING,
            priority INTEGER,
            load_strategy STRING,
            load_type STRING,
            interval STRING,
            primary_keys ARRAY<STRING>,
            is_partitioned BOOLEAN,
            partition_column STRING,
            partition_type STRING,
            partition_grain STRING,
            watermark_column STRING,
            autodetect BOOLEAN,
            config_override BOOLEAN,
            sync_state STRING,
            created_dt TIMESTAMP,
            last_updated_dt TIMESTAMP
        )
        """
        spark.sql(sql)
    
    def create_sync_schedule_tbl(self):
        tbl_nm = self.get_tbl_name(SyncConstants.SQL_TBL_SYNC_SCHEDULE)
        self.drop_table(tbl_nm)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {tbl_nm} (
            group_schedule_id STRING,
            schedule_id STRING,
            project_id STRING,
            dataset STRING,
            table_name STRING,
            scheduled TIMESTAMP,
            status STRING,
            started TIMESTAMP,
            completed TIMESTAMP,
            completed_activities INT,
            failed_activities INT,
            max_watermark STRING,
            priority INTEGER
        )
        """
        spark.sql(sql)

    def create_sync_schedule_telemetry_tbl(self):
        tbl_nm = self.get_tbl_name(SyncConstants.SQL_TBL_SYNC_SCHEDULE_TELEMETRY)
        self.drop_table(tbl_nm)

        sql = f"""
        CREATE TABLE IF NOT EXISTS {tbl_nm} (
            schedule_id STRING,
            project_id STRING,
            dataset STRING,
            table_name STRING,
            partition_id STRING,
            status STRING,
            started TIMESTAMP,
            completed TIMESTAMP,
            src_row_count BIGINT,
            dest_row_count BIGINT,
            inserted_row_count BIGINT,
            updated_row_count BIGINT,
            delta_version BIGINT,
            spark_application_id STRING,
            max_watermark STRING,
            summary_load STRING
        )
        """
        spark.sql(sql)

    def create_all_tables(self):
        self.create_data_type_map_tbl()
        self.create_sync_config_tbl()
        self.create_sync_schedule_tbl()
        self.create_sync_schedule_telemetry_tbl()