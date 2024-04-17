class ConfigMetadataLoader(ConfigBase):
    def __init__(self, config_path):
        self.JSON_Config_Path = config_path
        
        super().__init__(config_path)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")
    
    def create_autodetect_view(self):
        sql = """
        CREATE OR REPLACE TEMPORARY VIEW bq_table_metadata_autodetect
        AS
        WITH pkeys AS (    
            SELECT
                c.table_catalog, c.table_schema, c.table_name, 
                k.column_name AS pk_col
            FROM bq_information_schema_table_constraints c
            JOIN bq_information_schema_key_column_usage k ON
                k.table_catalog = c.table_catalog AND
                k.table_schema = c.table_schema AND
                k.table_name = c.table_name AND
                k.constraint_name = c.constraint_name
            JOIN bq_information_schema_columns n ON
                n.table_catalog = k.table_catalog AND
                n.table_schema = k.table_schema AND
                n.table_name = k.table_name AND
                n.column_name = k.column_name
            JOIN bq_data_type_map m ON n.data_type = m.data_type
            WHERE c.constraint_type = 'PRIMARY KEY'
            AND m.is_watermark = 'YES'
        ),
        pkeys_cnt AS (
            SELECT 
                table_catalog, table_schema, table_name, 
                COUNT(*) as pk_cnt
            FROM pkeys
            GROUP BY table_catalog, table_schema, table_name
        ),
        watermark_cols AS (
            SELECT 
                k.*
            FROM pkeys k
            JOIN pkeys_cnt c ON 
                k.table_catalog = c.table_catalog AND
                k.table_schema = c.table_schema AND
                k.table_name = c.table_name
            WHERE c.pk_cnt = 1
        ),
        partitions AS (
            SELECT
                table_catalog, table_schema, table_name, 
                count(*) as partition_count,
                avg(len(partition_id)) AS partition_id_len,
                sum(case when partition_id is NULL then 1 else 0 end) as null_partition_count
            FROM bq_information_schema_partitions
            GROUP BY table_catalog, table_schema, table_name
        ), 
        partition_columns AS
        (
            SELECT
                table_catalog, table_schema, table_name,
                column_name, c.data_type,
                m.partition_type AS partitioning_type
            FROM bq_information_schema_columns c
            JOIN bq_data_type_map m ON c.data_type=m.data_type
            WHERE is_partitioning_column = 'YES'
        ),
        partition_cfg AS
        (
            SELECT
                p.*,
                CASE WHEN p.partition_count = 1 AND p.null_partition_count = 1 THEN FALSE ELSE TRUE END AS is_partitioned,
                c.column_name AS partition_col,
                c.data_type AS partition_data_type,
                c.partitioning_type,
                CASE WHEN (c.partitioning_type = 'TIME')
                    THEN 
                        CASE WHEN (partition_id_len = 4) THEN 'YEAR'
                            WHEN (partition_id_len = 6) THEN 'MONTH'
                            WHEN (partition_id_len = 8) THEN 'DAY'
                            WHEN (partition_id_len = 10) THEN 'HOUR'
                            ELSE NULL END
                    ELSE NULL END AS partitioning_strategy
            FROM partitions p
            LEFT JOIN partition_columns c ON 
                p.table_catalog = c.table_catalog AND
                p.table_schema = c.table_schema AND
                p.table_name = c.table_name
        )

        SELECT 
            t.table_catalog, t.table_schema, t.table_name, t.is_insertable_into,
            p.is_partitioned, p.partition_col, p.partition_data_type, p.partitioning_type, p.partitioning_strategy,
            w.pk_col
        FROM bq_information_schema_tables t
        LEFT JOIN watermark_cols w ON 
            t.table_catalog = w.table_catalog AND
            t.table_schema = w.table_schema AND
            t.table_name = w.table_name
        LEFT JOIN partition_cfg p ON
            t.table_catalog = p.table_catalog AND
            t.table_schema = p.table_schema AND
            t.table_name = p.table_name
        """

        spark.sql(sql)

    def sync_bq_information_schema_tables(self):
        bq_table = self.UserConfig.get_bq_table_fullname(SyncConstants.INFORMATION_SCHEMA_TABLES)
        tbl_nm = self.UserConfig.flatten_3part_tablename(SyncConstants.INFORMATION_SCHEMA_TABLES.replace(".", "_"))

        bql = f"""
        SELECT *
        FROM {bq_table}
        WHERE table_type='BASE TABLE'
        AND table_name NOT LIKE '_bqc_%'
        """

        df = self.read_bq_to_dataframe(bql)

        if not self.UserConfig.LoadAllTables:
            filter_list = self.UserConfig.get_table_name_list()
            df = df.filter(col("table_name").isin(filter_list))    

        self.write_lakehouse_table(df, self.UserConfig.MetadataLakehouse, tbl_nm)

    def sync_bq_information_schema_table_dependent(self, dependent_tbl):
        bq_table = self.UserConfig.get_bq_table_fullname(SyncConstants.INFORMATION_SCHEMA_TABLES)
        bq_dependent_tbl = self.UserConfig.get_bq_table_fullname(dependent_tbl)
        tbl_nm = self.UserConfig.flatten_3part_tablename(dependent_tbl.replace(".", "_"))

        bql = f"""
        SELECT c.*
        FROM {bq_dependent_tbl} c
        JOIN {bq_table} t ON 
        t.table_catalog=c.table_catalog AND
        t.table_schema=c.table_schema AND
        t.table_name=c.table_name
        WHERE t.table_type='BASE TABLE'
        AND t.table_name NOT LIKE '_bqc_%'
        """

        df = self.read_bq_to_dataframe(bql)

        if not self.UserConfig.LoadAllTables:
            filter_list = self.UserConfig.get_table_name_list()
            df = df.filter(col("table_name").isin(filter_list)) 

        self.write_lakehouse_table(df, self.UserConfig.MetadataLakehouse, tbl_nm)

    def sync_bq_metadata(self):
        self.sync_bq_information_schema_tables()
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_PARTITIONS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_COLUMNS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE)

    def create_proxy_views(self):
        super().create_proxy_views()

        if not spark.catalog.tableExists("bq_table_metadata_autodetect"):
            self.create_autodetect_view()

    def auto_detect_table_profiles(self):        
        sql = f"""
        WITH default_config AS (
            SELECT autodetect, target_lakehouse FROM user_config_json
        ),
        pk AS (
            SELECT
            a.table_catalog, a.table_schema, a.table_name, array_agg(COALESCE(a.pk_col, u.column)) as pk
            FROM bq_table_metadata_autodetect a
            LEFT JOIN user_config_table_keys u ON
                a.table_catalog = u.project_id AND
                a.table_schema = u.dataset AND
                a.table_name = u.table_name
            GROUP BY a.table_catalog, a.table_schema, a.table_name
        ),
        source AS (
            SELECT
                a.table_catalog as project_id,
                a.table_schema as dataset,
                a.table_name as table_name,
                COALESCE(u.enabled, TRUE) AS enabled,
                COALESCE(u.lakehouse, d.target_lakehouse) AS lakehouse,
                COALESCE(u.lakehouse_target_table, a.table_name) AS lakehouse_table_name,
                COALESCE(u.source_query, '') AS source_query,
                COALESCE(u.load_priority, '100') AS priority,
                CASE WHEN (COALESCE(u.watermark_column, a.pk_col) IS NOT NULL AND
                        COALESCE(u.watermark_column, a.pk_col) <> '') THEN 'WATERMARK' 
                    WHEN (COALESCE(u.partition_enabled, a.is_partitioned) = TRUE) 
                        AND COALESCE(u.partition_column, a.partition_col, '') NOT IN 
                            ('_PARTITIONTIME', '_PARTITIONDATE') THEN 'PARTITION'
                    WHEN (COALESCE(u.partition_enabled, a.is_partitioned) = TRUE) 
                        AND COALESCE(u.partition_column, a.partition_col, '') IN 
                            ('_PARTITIONTIME', '_PARTITIONDATE') THEN 'TIME_INGESTION'
                    ELSE 'FULL' END AS load_strategy,
                CASE WHEN (COALESCE(u.watermark_column, a.pk_col) IS NOT NULL AND
                        COALESCE(u.watermark_column, a.pk_col) <> '') THEN 'APPEND' ELSE
                    'OVERWRITE' END AS load_type,
                COALESCE(u.interval, 'AUTO') AS interval,
                p.pk AS primary_keys,
                COALESCE(u.partition_enabled, a.is_partitioned) AS is_partitioned,
                COALESCE(u.partition_column, a.partition_col, '') AS partition_column,
                COALESCE(u.partition_type, a.partitioning_type, '') AS partition_type,
                COALESCE(u.partition_grain, a.partitioning_strategy, '') AS partition_grain,
                COALESCE(u.watermark_column, a.pk_col, '') AS watermark_column, 
                d.autodetect,
                CASE WHEN u.table_name IS NULL THEN FALSE ELSE TRUE END AS config_override,
                'INIT' AS sync_state,
                CURRENT_TIMESTAMP() as created_dt,
                NULL as last_updated_dt
            FROM bq_table_metadata_autodetect a
            JOIN pk p ON
                a.table_catalog = p.table_catalog AND
                a.table_schema = p.table_schema AND
                a.table_name = p.table_name
            LEFT JOIN user_config_tables u ON 
                a.table_catalog = u.project_id AND
                a.table_schema = u.dataset AND
                a.table_name = u.table_name
            CROSS JOIN default_config d
        )

        MERGE INTO {SyncConstants.SQL_TBL_SYNC_CONFIG} t
        USING source s
        ON t.project_id = s.project_id AND
            t.dataset = s.dataset AND
            t.table_name = s.table_name
        WHEN MATCHED AND t.sync_state <> 'INIT' THEN
            UPDATE SET
                t.enabled = s.enabled,
                t.interval = s.interval,
                t.priority = s.priority,
                t.last_updated_dt = CURRENT_TIMESTAMP()
        WHEN MATCHED AND t.sync_state = 'INIT' THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
