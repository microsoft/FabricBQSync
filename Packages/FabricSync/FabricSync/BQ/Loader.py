from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime, timezone
import json
from json import JSONEncoder
import base64
from pathlib import Path
import os

from FabricSync.BQ.Config import *

class ConfigMetadataLoader(ConfigBase):
    """
    Class handles:
     
    1. Loads the table metadata from the BigQuery information schema tables to 
        the Lakehouse Delta tables
    2. Autodetect table sync configuration based on defined metadata & heuristics
    """
    def __init__(
            self, 
            config_path : str):
        """
        Calls the parent init to load the user config JSON file
        """
        self.JSON_Config_Path = config_path
        
        super().__init__(config_path)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")
    
    def create_autodetect_view(self):
        """
        Creates the autodetect temporary view that uses the BigQuery table metadata
        to determine default sync configuration based on defined heuristics
        """
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
        """
        Reads the INFORMATION_SCHEMA.TABLES from BigQuery for the configuration project_id 
        and dataset returning only BASE TABLEs. Writes the results to the configured 
        Metadata Lakehouse using a unique name based on project_id and dataset to allow 
        for multiple datasets to be tracked independently.
        """
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

    def sync_bq_information_schema_table_dependent(
            self, 
            dependent_tbl : str):
        """
        Reads a child INFORMATION_SCHEMA table from BigQuery for the configuration project_id 
        and dataset. The child table is joined to the TABLES table to filter for BASE TABLEs.
        Writes the results to the configured Fabric Metadata Lakehouse using a unique 
        name based on project_id and dataset to allow for multiple datasets to be tracked independently.
        """
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
        """
        Loads the required INFORMATION_SCHEMA tables from BigQuery:

        1. TABLES
        2. PARTITIONS
        3. COLUMNS
        4. TABLE_CONSTRAINTS
        5. KEY_COLUMN_USAGE
        """
        self.sync_bq_information_schema_tables()
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_PARTITIONS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_COLUMNS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE)

    def create_proxy_views(self):
        """
        Create the proxy views required to handle multiple project_id and datasets in the same lakehouse
        """
        super().create_proxy_views()

        if not spark.catalog.tableExists("bq_table_metadata_autodetect"):
            self.create_autodetect_view()

    def auto_detect_table_profiles(self):
        """
        The autodetect provided the following capabilities:
         
        1. Uses the BigQuery metadata to determine a default config for each table
        2. If a user-defined table configuration is supplied it overrides the default configuration
        3. Write the configuration when the configuration is not locked
            a. The load configuration doesn't support changes without a reload of the data.
            b. The only changes that are support for locked configurations are:
                - Enabling and Disabling the table sync
                - Changing the table load Priority
                - Updating the table load Interval
        """    
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

        spark.sql(sql)

class SyncSetup(ConfigBase):
    """
    Configuration-driven utility to set-up the BQ Sync environment in the Fabric Lakehouse

    1. Creates the Metadata & Target Lakehouse if they do not exists
    2. Drops & Recreates the required metadata and any supporting tables required
    """
    def __init__(
            self, 
            config_path : str):
        """
        Calls the parent init to load the User Config JSON file
        """
        if spark.catalog.tableExists("user_config_json"):
            spark.catalog.dropTempView("user_config_json")

        super().__init__(config_path)

    def get_fabric_lakehouse(
            self, 
            nm : str):
        """
        Returns a Fabric Lakehouse by name or None if it does not exists
        """
        lakehouse = None

        try:
            lakehouse = mssparkutils.lakehouse.get(nm)
        except Exception:
            print("Lakehouse not found: {0}".format(nm))

        return lakehouse

    def create_fabric_lakehouse(
            self, 
            nm : str):
        """
        Creates a Fabric Lakehouse if it does not exists
        """
        lakehouse = self.get_fabric_lakehouse(nm)

        if (lakehouse is None):
            print("Creating Lakehouse {0}...".format(nm))
            mssparkutils.lakehouse.create(nm)

    def setup(self):
        """
        Set-up method to ensure required Lakehouse exists and create required tables
        """
        self.create_fabric_lakehouse(self.UserConfig.MetadataLakehouse)
        self.create_fabric_lakehouse(self.UserConfig.TargetLakehouse)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")
        self.create_all_tables()

    def drop_table(
            self, 
            tbl : str):
        """
        Drops an existing table from the Lakehouse if it exists
        """
        sql = f"DROP TABLE IF EXISTS {tbl}"
        spark.sql(sql)

    def get_tbl_name(
            self, 
            tbl : str) -> str:
        """
        Returns the table name with two-part format for the configuration Metadata Lakehouse
        """
        return self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

    def create_data_type_map_tbl(self):
        """
        Creates the BQ Data Type Mapping table and loads the default data. The csv data must be supplied in the 
        the following path of the configured Metadata Lakehouse:

        Files/data/bq_data_types.csv
        """
        tbl_nm = self.get_tbl_name(SyncConstants.SQL_TBL_DATA_TYPE_MAP)
        self.drop_table(tbl_nm)

        sql = f"""CREATE TABLE IF NOT EXISTS {tbl_nm} (data_type STRING, partition_type STRING, is_watermark STRING)"""
        spark.sql(sql)

        df = spark.read.format("csv").option("header","true").load("Files/data/bq_data_types.csv")
        df.write.mode("OVERWRITE").saveAsTable(tbl_nm)

    def create_sync_config_tbl(self):
        """
        Create the BQ Sync Configuration metadata table
        """
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
        """
        Create the BQ Sync Schedule metadata table
        """
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
        """
        Create the BQ Sync Schedule Telemetry metadata table
        """
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
        """
        Create all required metadata tables
        """
        self.create_data_type_map_tbl()
        self.create_sync_config_tbl()
        self.create_sync_schedule_tbl()
        self.create_sync_schedule_telemetry_tbl()

class Scheduler(ConfigBase):
    """
    Class responsible for calculating the to-be run schedule based on the sync config and 
    the most recent BigQuery table metadata. Schedule is persisted to the Sync Schedule
    Delta table. When tables are scheduled but no updates are detected on the BigQuery side 
    a SKIPPED record is created for tracking purposes.
    """
    def __init__(
            self, 
            config_path : str):
        """
        Calls the parent init to load the user config JSON file
        """
        super().__init__(config_path)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")

    def run(self):
        """
        Process responsible for creating and saving the sync schedule
        """
        sql = f"""
        WITH new_schedule AS ( 
            SELECT UUID() AS group_schedule_id, CURRENT_TIMESTAMP() as scheduled
        ),
        last_bq_tbl_updates AS (
            SELECT table_catalog, table_schema, table_name, max(last_modified_time) as last_bq_tbl_update
            FROM bq_information_schema_partitions
            GROUP BY table_catalog, table_schema, table_name
        ),
        last_load AS (
            SELECT project_id, dataset, table_name, MAX(started) AS last_load_update
            FROM {SyncConstants.SQL_TBL_SYNC_SCHEDULE}
            WHERE status='COMPLETE'
            GROUP BY project_id, dataset, table_name
        ),
        schedule AS (
            SELECT
                n.group_schedule_id,
                UUID() AS schedule_id,
                c.project_id,
                c.dataset,
                c.table_name,
                n.scheduled,
                CASE WHEN ((l.last_load_update IS NULL) OR
                     (b.last_bq_tbl_update >= l.last_load_update))
                    THEN 'SCHEDULED' ELSE 'SKIPPED' END as status,
                NULL as started,
                NULL as completed,   
                NULL as completed_activities,
                NULL as failed_activities,
                NULL as max_watermark,
                c.priority                
            FROM {SyncConstants.SQL_TBL_SYNC_CONFIG} c 
            LEFT JOIN {SyncConstants.SQL_TBL_SYNC_SCHEDULE} s ON 
                c.project_id= s.project_id AND
                c.dataset = s.dataset AND
                c.table_name = s.table_name AND
                s.status = 'SCHEDULED'
            LEFT JOIN last_bq_tbl_updates b ON
                c.project_id= b.table_catalog AND
                c.dataset = b.table_schema AND
                c.table_name = b.table_name
            LEFT JOIN last_load l ON 
                c.project_id= l.project_id AND
                c.dataset = l.dataset AND
                c.table_name = l.table_name
            CROSS JOIN new_schedule n
            WHERE s.schedule_id IS NULL
            AND c.enabled = TRUE
        )

        INSERT INTO {SyncConstants.SQL_TBL_SYNC_SCHEDULE}
        SELECT * FROM schedule s
        WHERE s.project_id = '{self.UserConfig.ProjectID}'
        AND s.dataset = '{self.UserConfig.Dataset}'
        """
        spark.sql(sql)

class BQScheduleLoader(ConfigBase):
    """
    Class repsonsible for processing the sync schedule and handling data movement 
    from BigQuery to Fabric Lakehouse based on each table's configuration
    """
    def __init__(
            self, 
            config_path : str, 
            load_proxy_views : bool =True, 
            force_config_reload : bool = False):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(config_path, force_config_reload)
        spark.sql(f"USE {self.UserConfig.MetadataLakehouse}")

        if load_proxy_views:
            super().create_proxy_views()

    def save_schedule_telemetry(
            self, 
            schedule : SyncSchedule):
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        """
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


    def get_table_delta_version(
            self, 
            tbl : str) -> str:
        """
        Retrieves the delta version from the delta log for the specified table
        """
        sql = f"DESCRIBE HISTORY {tbl}"
        df = spark.sql(sql) \
            .select(max(col("version")).alias("delta_version"))

        for row in df.collect():
            return row["delta_version"]

    def get_schedule(self):
        """
        Gets the schedule activities that need to be run based on the configuration and metadat
        """
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
            s.group_schedule_id,
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

    def get_max_watermark(
            self, 
            lakehouse_tbl : str, 
            watermark_col : str) -> str:
        """
        Get the max value for the supplied table and column
        """
        df = spark.table(lakehouse_tbl) \
            .select(max(col(watermark_col)).alias("watermark"))

        for row in df.collect():
            return row["watermark"]

    def sync_bq_table(
            self, 
            schedule:SyncSchedule):
        """
        Sync the data for a table from BigQuery to the target Fabric Lakehouse based on configuration
        """
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

    def process_load_group_telemetry(
            self, 
            load_grp : str = None):
        """
        When a load group is complete, summarizes the telemetry to close out the schedule
        """
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

    def commit_table_configuration(self):
        """
        After an initial load, locks the table configuration so no changes can occur when reprocessing metadata
        """
        sql = """
        WITH committed AS (
            SELECT project_id, dataset, table_name, MAX(started) as started
            FROM bq_sync_schedule
            WHERE status='COMPLETE'
            GROUP BY project_id, dataset, table_name
        )

        MERGE INTO bq_sync_configuration t
        USING committed c
        ON t.project_id=c.project_id
        AND t.dataset=c.dataset
        AND t.table_name=c.table_name
        WHEN MATCHED AND t.sync_state='INIT' THEN
            UPDATE SET
                t.sync_state='COMMIT'
        """
        spark.sql(sql)


    def run_sequential_schedule(self):
        """
        Run the schedule activities sequentially based on priority order
        """
        df_schedule = self.get_schedule()

        for row in df_schedule.collect():
            schedule = SyncSchedule(row)

            self.sync_bq_table(schedule)

            self.save_schedule_telemetry(schedule)  

        self.process_load_group_telemetry()
        self.commit_table_configuration()
    
    def run_aync_schedule(self):
        """
        Runs the schedule activities in parallel using the runMultiple 
        capabilities of the Fabric Spark Notebook

        - Utilitizes the priority to define load groups to respect priority
        - Parallelism is control from the User Config JSON file
        """
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

        self.commit_table_configuration()