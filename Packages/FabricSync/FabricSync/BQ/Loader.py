from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from delta.tables import *
from datetime import datetime, timezone
from typing import Tuple
from queue import PriorityQueue
from threading import Thread, Lock
import uuid
import traceback

from FabricSync.BQ.Config import *
from FabricSync.DeltaTableUtility import *

class ConfigMetadataLoader(ConfigBase):
    """
    Class handles:
     
    1. Loads the table metadata from the BigQuery information schema tables to 
        the Lakehouse Delta tables
    2. Autodetect table sync configuration based on defined metadata & heuristics
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls the parent init to load the user config JSON file
        """        
        super().__init__(context, user_config, gcp_credential)
    
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
        range_partitions AS 
        (
            SELECT 
                table_catalog, table_schema, table_name,
                SUBSTRING(gen, 16, LEN(gen) - 16) AS partition_range
            FROM (
                SELECT 
                    table_catalog, table_schema, table_name,
                    SUBSTRING(ddl,
                        LOCATE('GENERATE_ARRAY', ddl),
                        LOCATE(')', ddl, LOCATE('GENERATE_ARRAY', ddl)) - LOCATE('GENERATE_ARRAY', ddl) + 1) as gen   
                FROM bq_information_schema_tables 
                WHERE CONTAINS(ddl, 'GENERATE_ARRAY')
            )
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
            w.pk_col, r.partition_range
        FROM bq_information_schema_tables t
        LEFT JOIN watermark_cols w ON 
            t.table_catalog = w.table_catalog AND
            t.table_schema = w.table_schema AND
            t.table_name = w.table_name
        LEFT JOIN partition_cfg p ON
            t.table_catalog = p.table_catalog AND
            t.table_schema = p.table_schema AND
            t.table_name = p.table_name
        LEFT JOIN range_partitions r ON 
            t.table_catalog = r.table_catalog AND
            t.table_schema = r.table_schema AND
            t.table_name = r.table_name
        """

        self.Context.sql(sql)

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
            filter_list = self.UserConfig.get_table_name_list(True)
            df = df.filter(col("table_name").isin(filter_list))    

        self.write_lakehouse_table(df, self.UserConfig.MetadataLakehouse, tbl_nm)

    def sync_bq_information_schema_table_dependent(self, dependent_tbl:str):
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
            filter_list = self.UserConfig.get_table_name_list(True)
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
        5. TABLE_OPTIONS
        """
        self.sync_bq_information_schema_tables()
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_PARTITIONS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_COLUMNS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE)
        self.sync_bq_information_schema_table_dependent(SyncConstants.INFORMATION_SCHEMA_TABLE_OPTIONS)

        self.create_proxy_views()

    def create_infosys_proxy_view(self, trgt:str):
        """
        Creates a covering temporary view over top of the Big Query metadata tables
        """
        clean_nm = trgt.replace(".", "_")
        vw_nm = f"BQ_{clean_nm}"

        tbl = self.UserConfig.flatten_3part_tablename(clean_nm)
        lakehouse_tbl = self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

        sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW {vw_nm}
        AS
        SELECT * FROM {lakehouse_tbl}
        """
        self.Context.sql(sql)

    def create_userconfig_tables_proxy_view(self):
        """
        Explodes the User Config table configuration into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_tables
            AS
            SELECT
                project_id, 
                dataset, 
                tbl.table_name,
                tbl.enabled,tbl.priority,tbl.source_query,
                tbl.load_strategy,tbl.load_type,tbl.interval,
                tbl.watermark.column as watermark_column,
                tbl.partitioned.enabled as partition_enabled,
                tbl.partitioned.type as partition_type,
                tbl.partitioned.column as partition_column,
                tbl.partitioned.partition_grain,
                tbl.partitioned.partition_data_type,
                tbl.partitioned.partition_range,
                tbl.lakehouse_target.lakehouse,
                tbl.lakehouse_target.table_name AS lakehouse_target_table,
                tbl.keys,
                tbl.enforce_partition_expiration AS enforce_partition_expiration,
                tbl.allow_schema_evolution AS allow_schema_evolution,
                tbl.table_maintenance.enabled AS table_maintenance_enabled,
                tbl.table_maintenance.interval AS table_maintenance_interval,
                tbl.table_options
            FROM (
                SELECT 
                    gcp_credentials.project_id as project_id,
                    gcp_credentials.dataset as dataset, 
                    EXPLODE(tables) AS tbl 
                FROM user_config_json)
        """
        self.Context.sql (sql)

    def create_userconfig_tables_cols_proxy_view(self):
        """
        Explodes the User Config table primary keys into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_table_keys
            AS
            SELECT
                project_id, dataset, table_name, pkeys.column
            FROM (
                SELECT
                    project_id, dataset, tbl.table_name, EXPLODE(tbl.keys) AS pkeys
                FROM (SELECT gcp_credentials.project_id as project_id,
                    gcp_credentials.dataset as dataset, 
                    EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        self.Context.sql(sql)

    def create_proxy_views(self):
        """
        Create the user config and covering BQ information schema views
        """
        self.create_userconfig_tables_proxy_view()        
        self.create_userconfig_tables_cols_proxy_view()

        for vw in SyncConstants.get_information_schema_views():
            self.create_infosys_proxy_view(vw)  

        self.create_autodetect_view()      

    def enforce_load_all(self):
        if not self.UserConfig.LoadAllTables:
            sql = "UPDATE bq_sync_configuration SET enabled='FALSE'"
            self.Context.sql(sql)

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
        self.enforce_load_all()
        
        sql = f"""
        WITH default_config AS (
            SELECT COALESCE(autodetect, TRUE) AS autodetect, load_all_tables, target_lakehouse FROM user_config_json
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
                CASE WHEN d.load_all_tables THEN
                    COALESCE(u.enabled, TRUE) ELSE
                    COALESCE(u.enabled, FALSE) END AS enabled,
                COALESCE(u.lakehouse, d.target_lakehouse) AS lakehouse,
                COALESCE(u.lakehouse_target_table, a.table_name) AS lakehouse_table_name,
                COALESCE(u.source_query, '') AS source_query,
                COALESCE(u.priority, '100') AS priority,
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
                COALESCE(u.partition_data_type, a.partition_data_type, '') AS partition_data_type,
                COALESCE(u.partition_range, a.partition_range, '') AS partition_range,
                COALESCE(u.watermark_column, a.pk_col, '') AS watermark_column, 
                d.autodetect,
                COALESCE(u.enforce_partition_expiration, FALSE) AS enforce_partition_expiration,
                COALESCE(u.allow_schema_evolution, FALSE) AS allow_schema_evolution,
                COALESCE(u.table_maintenance_enabled, FALSE) AS table_maintenance_enabled,
                COALESCE(u.table_maintenance_interval, 'AUTO') AS table_maintenance_interval,
                u.table_options,
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
                t.enforce_partition_expiration = s.enforce_partition_expiration,
                t.allow_schema_evolution = s.allow_schema_evolution,
                t.table_maintenance_enabled = s.table_maintenance_enabled,
                t.table_maintenance_interval = s.table_maintenance_interval,
                t.table_options = s.table_options,
                t.last_updated_dt = CURRENT_TIMESTAMP()
        WHEN MATCHED AND t.sync_state = 'INIT' THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """

        self.Context.sql(sql)

class Scheduler(ConfigBase):
    """
    Class responsible for calculating the to-be run schedule based on the sync config and 
    the most recent BigQuery table metadata. Schedule is persisted to the Sync Schedule
    Delta table. When tables are scheduled but no updates are detected on the BigQuery side 
    a SKIPPED record is created for tracking purposes.
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls the parent init to load the user config JSON file
        """
        super().__init__(context, user_config, gcp_credential)

    def get_current_schedule(self, schedule_type:str) -> str:
        sql = f"""
        SELECT group_schedule_id FROM bq_sync_schedule
        WHERE project_id = '{self.UserConfig.ProjectID}'
        AND dataset = '{self.UserConfig.Dataset}'
        AND schedule_type = '{schedule_type}'
        AND status != '{SyncConstants.COMPLETE}'
        """
        df = self.Context.sql(sql)

        schedule = [s["group_schedule_id"] for s in df.collect()]
        schedule_id = None

        if schedule:
            schedule_id = schedule[0]
    
        return schedule_id
    
    def build_schedule(self, schedule_type:str) -> str:
        schedule_id = self.get_current_schedule(schedule_type)

        if not schedule_id:
            schedule_id = self.build_new_schedule(schedule_type)
        
        return schedule_id

    def build_new_schedule(self, schedule_type:str) -> str:
        """
        Process responsible for creating and saving the sync schedule
        """
        group_schedule_id = uuid.uuid4()

        sql = f"""
        WITH new_schedule AS ( 
            SELECT '{group_schedule_id}' AS group_schedule_id, CURRENT_TIMESTAMP() as scheduled
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
                '{schedule_type}' as schedule_type,
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
                s.schedule_type = '{schedule_type}' AND
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
        self.Context.sql(sql)

        return group_schedule_id

class BQScheduleLoader(ConfigBase):
    """
    Class repsonsible for processing the sync schedule and handling data movement 
    from BigQuery to Fabric Lakehouse based on each table's configuration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(context, user_config, gcp_credential)
        self._SyncTableIndex:list[str] = []
        self._TableIndexLock = Lock()

    def appendTableIndex(self, table:str):
        """
        Thread-safe list of sync'd tables
        """
        with self._TableIndexLock:
            if not table in self._SyncTableIndex:
                self._SyncTableIndex.append(table)
    
    def isTabledSynced(self, table:str) -> bool:
        """
        Thread-safe list exists for sync'd tables
        """
        with self._TableIndexLock:
            exists = (table in self._SyncTableIndex)
        
        return exists

    def save_schedule_telemetry(self, schedule:SyncSchedule):
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        """
        tbl = f"{SyncConstants.SQL_TBL_SYNC_SCHEDULE_TELEMETRY}"

        schema = self.Context.table(tbl).schema

        rdd = self.Context.sparkContext.parallelize([Row( \
            schedule_id=schedule.ScheduleId, \
            project_id=schedule.ProjectId, \
            dataset=schedule.Dataset, \
            table_name=schedule.TableName, \
            partition_id=schedule.PartitionId, \
            status=schedule.Status, \
            started=schedule.StartTime, \
            completed=schedule.EndTime, \
            src_row_count=schedule.SourceRows, \
            inserted_row_count=schedule.InsertedRows, \
            updated_row_count=schedule.UpdatedRows, \
            delta_version=schedule.DeltaVersion, \
            spark_application_id=schedule.SparkAppId, \
            max_watermark=schedule.MaxWatermark, \
            summary_load=schedule.SummaryLoadType \
        )])

        df = self.Context.createDataFrame(rdd, schema)
        df.write.mode(SyncConstants.APPEND).saveAsTable(tbl)

    def get_delta_merge_row_counts(self, schedule:SyncSchedule) -> Tuple[int, int, int]:
        """
        Gets the rows affected by merge operation, filters on partition id when table is partitioned
        """
        telemetry = self.Context.sql(f"DESCRIBE HISTORY {schedule.LakehouseTableName}")

        telemetry = telemetry \
            .filter("operation = 'MERGE' AND CAST(timestamp AS DATE) = current_date()") \
            .orderBy("version", ascending=False)

        inserts = 0
        updates = 0
        deletes = 0

        for t in telemetry.collect():
            op_metrics = None

            if schedule.FabricPartitionColumns and schedule.PartitionId:
                if "predicate" in t["operationParameters"] and \
                    schedule.PartitionId in t["operationParameters"]["predicate"]:
                        op_metrics = t["operationMetrics"]
            else:
                op_metrics = t["operationMetrics"]

            if op_metrics:
                inserts = int(op_metrics["numTargetRowsInserted"])
                updates = int(op_metrics["numTargetRowsUpdated"])
                deletes = int(op_metrics["numTargetRowsDeleted"])

                continue

        return (inserts, updates, deletes)
    
    def get_schedule(self, group_schedule_id:str):
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
            WHERE sp.partition_id != '__NULL__'
            AND ((sp.last_modified_time >= s.last_schedule_dt) OR (s.last_schedule_dt IS NULL))
            AND 
                ((c.load_strategy = 'PARTITION' AND s.last_schedule_dt IS NOT NULL) OR
                    (c.load_strategy = 'PARTITION' AND c.partition_type = 'RANGE') OR
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
            AND s.group_schedule_id = '{group_schedule_id}'
        ORDER BY c.priority
        """
        df = self.Context.sql(sql)
        df.createOrReplaceTempView("LoaderQueue")
        df.cache()

        return df

    def get_max_watermark(self, schedule:SyncSchedule, df_bq:DataFrame) -> str:
        """
        Get the max value for the supplied table and column
        """
        df = df_bq.select(max(col(schedule.WatermarkColumn)).alias("watermark"))

        max_watermark = None

        for r in df.collect():
            max_watermark = r["watermark"] 

        return max_watermark

    def get_fabric_partition_proxy_cols(self, partition_grain:str) -> list[str]:
        proxy_cols = ["YEAR", "MONTH", "DAY", "HOUR"]

        match partition_grain:
            case "DAY":
                proxy_cols.remove("HOUR")
            case "MONTH":
                proxy_cols.remove("HOUR")
                proxy_cols.remove("DAY")
            case "YEAR":
                proxy_cols.remove("HOUR")
                proxy_cols.remove("DAY")
                proxy_cols.remove("MONTH")

        return proxy_cols

    def get_bq_partition_id_format(self, partition_grain:str) -> str:
        pattern = None

        match partition_grain:
            case "DAY":
                pattern = "%Y%m%d"
            case "MONTH":
                pattern = "%Y%m"
            case "YEAR":
                pattern = "%Y"
            case "HOUR":
                pattern = "%Y%m%d%H"
        
        return pattern

    def get_derived_date_from_part_id(self, partition_grain:str, partition_id:str) -> datetime:
        dt_format = self.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)

    def create_fabric_partition_proxy_cols(self, df:DataFrame, partition:str, partition_grain:str, proxy_cols:list[str]) -> DataFrame:  
        for c in proxy_cols:
            match c:
                case "HOUR":
                    df = df.withColumn(f"__{partition}_HOUR", \
                        date_format(col(partition), "HH"))
                case "DAY":
                    df = df.withColumn(f"__{partition}_DAY", \
                        date_format(col(partition), "dd"))
                case "MONTH":
                    df = df.withColumn(f"__{partition}_MONTH", \
                        date_format(col(partition), "MM"))
                case "YEAR":
                    df = df.withColumn(f"__{partition}_YEAR", \
                        date_format(col(partition), "yyyy"))
                case _:
                    next
        
        return df

    def get_fabric_partition_cols(self, partition:str, proxy_cols:list[str]):
        return [f"__{partition}_{c}" for c in proxy_cols]

    def get_fabric_partition_predicate(self, partition_dt:datetime, partition:str, proxy_cols:list[str]) -> str:
        partition_predicate = []

        for c in proxy_cols:
            match c:
                case "HOUR":
                    part_id = partition_dt.strftime("%H")
                case "DAY":
                    part_id = partition_dt.strftime("%d")
                case "MONTH":
                    part_id = partition_dt.strftime("%m")
                case "YEAR":
                    part_id = partition_dt.strftime("%Y")
                case _:
                    next
            
            partition_predicate.append(f"__{partition}_{c} = '{part_id}'")

        return " AND ".join(partition_predicate)
    
    def get_bq_range_map(self, tbl_ranges:str) -> DataFrame:
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
        
    def create_fabric_range_partition(self, df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        partition_range = self.get_bq_range_map(schedule.PartitionRange)
        
        df = self.Context.createDataFrame(partition_range, ["range_name", "range_low", "range_high"]) \
            .alias("rng")

        df_bq = df_bq.alias("bq")
        df_bq = df_bq.join(df, (col(f"bq.{schedule.PartitionColumn}") >= col("rng.range_low")) & \
            (col(f"bq.{schedule.PartitionColumn}") < col("rng.range_high"))) \
            .select("bq.*", col("rng.range_name").alias(schedule.FabricPartitionColumns[0]))
        
        return df_bq

    def get_partition_range_predicate(self, schedule:SyncSchedule) -> str:
        partition_range = self.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]

        if not r:
            raise Exception(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"

    def merge_table(self, schedule:SyncSchedule, src:DataFrame) -> SyncSchedule:
        """
        Merge into Lakehouse Table based on User Configuration. Only supports Insert/Update All
        """
        self.Context.conf.set("spark.databricks.delta.merge.repartitionBeforeWrite.enabled", "true")

        constraints = []

        for p in schedule.Keys:
            constraints.append(f"s.{p} = d.{p}")

        if not constraints:
            raise ValueError("One or more keys must be specified for a MERGE operation")
        
        if schedule.FabricPartitionColumns and schedule.PartitionId:
            for p in schedule.FabricPartitionColumns:
                constraints.append(f"d.{p} = '{schedule.PartitionId}'")

        predicate = " AND ".join(constraints)

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        dest = DeltaTable.forName(self.Context, tableOrViewName=schedule.LakehouseTableName)

        dest.alias('d') \
        .merge( \
            src.alias('s'), \
            predicate) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        if (schedule.AllowSchemaEvolution):
            self.Context.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

        results = self.get_delta_merge_row_counts(schedule)

        schedule.UpdateRowCounts(src=0, insert=results[0], update=results[1])
        
        return schedule
        
    def sync_bq_table(self, schedule:SyncSchedule, lock:Lock = None):
        """
        Sync the data for a table from BigQuery to the target Fabric Lakehouse based on configuration

        1. Determines how to retrieve the data from BigQuery
            a. PARTITION & TIME_INGESTION
                - Data is loaded by partition using the partition filter option of the spark connector
            b. FULL & WATERMARK
                - Loaded using the table name or source query and any relevant predicates
        2. Resolve BigQuery to Fabric partition mapping
            a. BigQuery supports TIME and RANGE based partitioning
                - TIME based partitioning support YEAR, MONTH, DAY & HOUR grains
                    - When the grain doesn't exist or a psuedo column is used, a proxy column is added
                        on the Fabric Lakehouse side
                - RANGE partitioning is a backlog feature
        3. Write data to the Fabric Lakehouse
            a. PARTITION write use replaceWhere to overwrite the specific Delta partition
            b. All other writes respect the configure MODE against the write destination
        4. Collect and save telemetry
        """
        print("{0} {1}...".format(schedule.SummaryLoadType, schedule.TableName))

        if not schedule.InitialLoad and self.Context.catalog.tableExists(schedule.LakehouseTableName):
            table_maint = DeltaTableMaintenance(self.Context, schedule.LakehouseTableName)
        else:
            table_maint = None

        if schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None:
            part_format = self.get_bq_partition_id_format(schedule.PartitionGrain)

            if schedule.PartitionDataType == SyncConstants.TIMESTAMP:                  
                part_filter = f"timestamp_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_TIMESTAMP('{part_format}', '{schedule.PartitionId}')"
            else:
                part_filter = f"date_trunc({schedule.PartitionColumn}, {schedule.PartitionGrain}) = PARSE_DATETIME('{part_format}', '{schedule.PartitionId}')"

            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter)
        elif schedule.IsRangePartitioned:
            part_filter = self.get_partition_range_predicate(schedule)
            df_bq = self.read_bq_partition_to_dataframe(schedule.BQTableName, part_filter)
        else:
            src = schedule.BQTableName     

            if schedule.SourceQuery != SyncConstants.EMPTY_STRING:
                src = schedule.SourceQuery

            df_bq = self.read_bq_to_dataframe(src)

        if schedule.LoadStrategy == SyncConstants.WATERMARK and not schedule.InitialLoad:
            predicate = f"{schedule.PrimaryKey} > '{schedule.MaxWatermark}'"
            df_bq.where(predicate)

        df_bq.cache()

        if schedule.IsPartitioned:
            if schedule.PartitionType == SyncConstants.TIME:
                proxy_cols = self.get_fabric_partition_proxy_cols(schedule.PartitionGrain)
                schedule.FabricPartitionColumns = self.get_fabric_partition_cols(schedule.PartitionColumn, proxy_cols)

                if schedule.IsTimeIngestionPartitioned:
                    df_bq = df_bq.withColumn(schedule.PartitionColumn, lit(schedule.PartitionId))               

                df_bq = self.create_fabric_partition_proxy_cols(df_bq, schedule.PartitionColumn, schedule.PartitionGrain, proxy_cols)
            else:
                schedule.FabricPartitionColumns = [f"__{schedule.PartitionColumn}_Range"]
                df_bq = self.create_fabric_range_partition(df_bq, schedule)

        write_config = { **schedule.TableOptions }

        #Schema Evolution
        if not schedule.InitialLoad:
            if schedule.AllowSchemaEvolution and table_maint:
                table_maint.evolve_schema(df_bq)
                write_config["mergeSchema"] = SyncConstants.TRUE

        if not schedule.LoadType == SyncConstants.MERGE or schedule.InitialLoad:
            if (schedule.IsTimePartitionedStrategy and schedule.PartitionId is not None) or schedule.IsRangePartitioned:
                has_lock = False

                if schedule.InitialLoad:
                    has_lock = True
                    lock.acquire()
                else:
                    write_config["partitionOverwriteMode"] = SyncConstants.DYNAMIC
                
                try:
                    df_bq.write \
                        .partitionBy(schedule.FabricPartitionColumns) \
                        .mode(SyncConstants.OVERWRITE) \
                        .options(**write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                finally:
                    self.appendTableIndex(schedule.LakehouseTableName)

                    if has_lock:
                        lock.release()
            else:
                if schedule.FabricPartitionColumns is None:
                    df_bq.write \
                        .mode(schedule.Mode) \
                        .options( **write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                else:
                    df_bq.write \
                        .partitionBy(schedule.FabricPartitionColumns) \
                        .mode(schedule.Mode) \
                        .options( **write_config) \
                        .saveAsTable(schedule.LakehouseTableName)
                
                self.appendTableIndex(schedule.LakehouseTableName)
        else:
            schedule = self.merge_table(schedule, df_bq)
            self.appendTableIndex(schedule.LakehouseTableName)

        if not table_maint:
            table_maint = DeltaTableMaintenance(self.Context, schedule.LakehouseTableName)

        if schedule.LoadStrategy == SyncConstants.WATERMARK:
            schedule.MaxWatermark = self.get_max_watermark(schedule, df_bq)

        src_cnt = df_bq.count()
        schedule.UpdateRowCounts(src_cnt, 0, 0)    
        schedule.SparkAppId = self.Context.sparkContext.applicationId
        schedule.DeltaVersion = table_maint.CurrentTableVersion
        schedule.EndTime = datetime.now(timezone.utc)
        schedule.Status = SyncConstants.COMPLETE
        
        df_bq.unpersist()

        return schedule

    def process_load_group_telemetry(self, group_schedule_id:str):
        """
        When a load group is complete, summarizes the telemetry to close out the schedule
        """
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
                    s.group_schedule_id,
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
                AND r.group_schedule_id = '{group_schedule_id}'
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
        self.Context.sql(sql)

    def commit_table_configuration(self, group_schedule_id:str):
        """
        After an initial load, locks the table configuration so no changes can occur when reprocessing metadata
        """
        sql = f"""
        WITH committed AS (
            SELECT project_id, dataset, table_name, MAX(started) as started
            FROM bq_sync_schedule
            WHERE status='COMPLETE'
            AND group_schedule_id = '{group_schedule_id}'
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
        self.Context.sql(sql)

    def run_sequential_schedule(self, group_schedule_id:str):
        """
        Run the schedule activities sequentially based on priority order
        """
        print(f"Sequential schedule sync starting...")
        df_schedule = self.get_schedule(group_schedule_id)

        for row in df_schedule.collect():
            schedule = SyncSchedule(row)
            self.sync_bq_table(schedule)
            self.save_schedule_telemetry(schedule)  

        self.process_load_group_telemetry(group_schedule_id)
        print(f"Sequential schedule sync complete...")
    
    def schedule_sync(self, schedule:SyncSchedule, lock:Lock) -> SyncSchedule:
        schedule = self.sync_bq_table(schedule, lock)
        self.save_schedule_telemetry(schedule) 

        return schedule

    def task_runner(self, sync_function, workQueue:PriorityQueue, lock:Lock):
        while not workQueue.empty():
            value = workQueue.get()
            schedule = value[2]
            
            try:
                schedule = sync_function(schedule, lock)
            except Exception:
                schedule.Status = "FAILED"
                schedule.SummaryLoadType = traceback.format_exc()
            finally:
                workQueue.task_done()

    def process_queue(self, workQueue:PriorityQueue, task_function, sync_function):
        lock = Lock() 
        for i in range(self.UserConfig.Async.Parallelism):
            t=Thread(target=task_function, args=(sync_function, workQueue, lock))
            t.daemon = True
            t.start() 
            
        workQueue.join()
        
    def run_async_schedule(self, group_schedule_id:str):
        """
        Runs the schedule activities in parallel using python threading

        - Utilitizes the priority to define load groups to respect priority
        - Parallelism is control from the User Config JSON file
        """
        print(f"Async schedule started with parallelism of {self.UserConfig.Async.Parallelism}...")
        workQueue = PriorityQueue()

        schedule = self.get_schedule(group_schedule_id)

        load_grps = [i["priority"] for i in schedule.select("priority").distinct().orderBy("priority").collect()]

        if load_grps:
            for grp in load_grps:
                grp_nm = "LOAD GROUP {0}".format(grp)
                grp_df = schedule.where(f"priority = '{grp}'")

                for tbl in grp_df.collect():
                    s = SyncSchedule(tbl)
                    nm = "{0}.{1}".format(s.Dataset, s.TableName)        

                    if s.PartitionId is not None:
                        nm = "{0}${1}".format(nm, s.PartitionId)        

                    workQueue.put((s.Priority, nm, s))

                if not workQueue.empty():
                    print(f"### Processing {grp_nm}...")
                    self.process_queue(workQueue, self.task_runner, self.schedule_sync)

            self.process_load_group_telemetry(group_schedule_id)
       
        print(f"Async schedule sync complete...")

class BQSync(SyncBase):
    def __init__(self, context:SparkSession, config_path:str):
        super().__init__(context, config_path)

        self.MetadataLoader = ConfigMetadataLoader(context, self.UserConfig, self.GCPCredential)
        self.Scheduler = Scheduler(context, self.UserConfig, self.GCPCredential)
        self.Loader = BQScheduleLoader(context, self.UserConfig, self.GCPCredential)

    def sync_metadata(self):
        self.MetadataLoader.sync_bq_metadata()

        if self.UserConfig.Autodetect:
            self.MetadataLoader.auto_detect_table_profiles()
    
    def build_schedule(self, sync_metadata:bool = True, schedule_type:str = SyncConstants.AUTO) -> str:
        if sync_metadata:
            self.sync_metadata()
        else:
            self.MetadataLoader.create_proxy_views()

        return self.Scheduler.build_schedule(schedule_type)
    
    def run_schedule(self, group_schedule_id:str):
        if self.UserConfig.Async.Enabled:
            self.Loader.run_async_schedule(group_schedule_id)
        else:
            self.Loader.run_sequential_schedule(group_schedule_id)
        
        self.Loader.commit_table_configuration(group_schedule_id)
        self.optimize_metadata_tbls()
    
    def cleanup_session(self):
        temp_views = SyncConstants.get_sync_temp_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS {x}"), temp_views))

    def optimize_metadata_tbls(self):
        tbls = ["bq_sync_configuration", "bq_sync_schedule", "bq_sync_schedule_telemetry"]

        for tbl in tbls:
            table_maint = DeltaTableMaintenance(self.Context, tbl)
            table_maint.optimize_and_vacuum()
    
    def generate_user_config_json_from_metadata(self, path:str):
        bq_tables = self.Context.table(SyncConstants.SQL_TBL_SYNC_CONFIG)
        bq_tables = bq_tables.filter(col("project_id") == self.UserConfig.ProjectID)
        bq_tables = bq_tables.filter(col("dataset") == self.UserConfig.Dataset)

        user_cfg = self.build_user_config_json(bq_tables.collect())

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(user_cfg, f, ensure_ascii=False, indent=4)

    def build_user_config_json(self, tbls:list) -> str:
        tables = list(map(lambda x: self.get_table_config_json(x), tbls))

        return {
            "load_all_tables":self.UserConfig.LoadAllTables,
            "autodetect":self.UserConfig.Autodetect,
            "metadata_lakehouse":self.UserConfig.MetadataLakehouse,
            "target_lakehouse":self.UserConfig.TargetLakehouse,
            
            "gcp_credentials":{
                "project_id":self.UserConfig.GCPCredential.ProjectID,
                "dataset":self.UserConfig.GCPCredential.Dataset,
                "credential":self.UserConfig.GCPCredential.Credential
            },
            
            "async":{
                "enabled":self.UserConfig.Async.Enabled,
                "parallelism":self.UserConfig.Async.Parallelism,
                "cell_timeout":self.UserConfig.Async.CellTimeout,
                "notebook_timeout":self.UserConfig.Async.NotebookTimeout
            },        
            
            "tables": tables
        }

    def get_table_keys_config_json(self, pk:list):
        return [{"column": k} for k in pk]

    def get_table_config_json(self, tbl:Row) -> str:
        keys = self.get_table_keys_config_json(tbl["primary_keys"])

        return {
            "priority":tbl["priority"],
            "table_name":tbl["table_name"],
            "enabled":tbl["enabled"],
            "source_query":tbl["source_query"],
            "enforce_partition_expiration":tbl["enforce_partition_expiration"],
            "allow_schema_evolution":tbl["allow_schema_evolution"],
            "load_strategy":tbl["load_strategy"],
            "load_type":tbl["load_type"],
            "interval":tbl["interval"],
            "table_maintenance":{
                "enabled":tbl["table_maintenance_enabled"],
                "interval":tbl["table_maintenance_interval"]
            },
            "keys":keys,
            "partitioned":{
                "enabled":tbl["is_partitioned"],
                "type":tbl["partition_type"],
                "column":tbl["partition_column"],
                "partition_grain":tbl["partition_grain"],
                "partition_data_type":tbl["partition_data_type"],
                "partition_range":tbl["partition_range"]
            },
            "watermark":{
                "column":tbl["watermark_column"]
            },
            "lakehouse_target":{
                "lakehouse":tbl["lakehouse"],
                "table_name":tbl["lakehouse_table_name"]
            }
        }