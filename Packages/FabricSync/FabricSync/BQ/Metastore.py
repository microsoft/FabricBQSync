from pyspark.sql.session import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce
from typing import List

from .Enum import *
from .Exceptions import *

class FabricMetastore():
    def __init__(self, context:SparkSession):
        self.Context = context

    def build_schedule(self, sync_id:str, schedule_type:ScheduleType):
        sql = f"""
        WITH current_schedule AS (
            SELECT sync_id, project_id, dataset, table_name, schedule_type
            FROM bq_sync_schedule
            WHERE sync_id = '{sync_id}'
            AND schedule_type = '{schedule_type}'
            AND status NOT IN ('COMPLETE', 'SKIPPED', 'EXPIRED')
        ),
        new_schedule AS ( 
            SELECT uuid() AS group_schedule_id, CURRENT_TIMESTAMP() as scheduled
        ),
        last_bq_tbl_updates AS (
            SELECT table_catalog, table_schema, table_name, max(last_modified_time) as last_bq_tbl_update
            FROM bq_information_schema_partitions
            GROUP BY table_catalog, table_schema, table_name
            UNION ALL
            SELECT table_catalog, table_schema, table_name, NULL
            FROM bq_information_schema_views  
            UNION ALL
            SELECT table_catalog, table_schema, table_name, last_refresh_time
            FROM bq_information_schema_materialized_views
        ),
        last_load AS (
            SELECT sync_id, project_id, dataset, table_name, schedule_type, MAX(started) AS last_load_update
            FROM bq_sync_schedule
            WHERE status='COMPLETE'
            GROUP BY sync_id, project_id, dataset, table_name, schedule_type
        ),
        schedule AS (
            SELECT
                n.group_schedule_id,
                UUID() AS schedule_id,
                c.sync_id,
                c.project_id,
                c.dataset,
                c.table_name,
                c.interval AS schedule_type,
                n.scheduled,
                CASE WHEN ((l.last_load_update IS NULL) OR (b.last_bq_tbl_update IS NULL) OR
                        (b.last_bq_tbl_update >= l.last_load_update)) THEN 'SCHEDULED'
                    WHEN b.table_name IS NULL THEN 'EXPIRED' 
                    ELSE 'SKIPPED' END as status,
                NULL as started,
                NULL as completed,   
                NULL as completed_activities,
                NULL as failed_activities,
                NULL as max_watermark,
                c.priority                
            FROM bq_sync_configuration c 
            JOIN last_bq_tbl_updates b ON
                c.project_id= b.table_catalog AND
                c.dataset = b.table_schema AND
                c.table_name = b.table_name
            LEFT JOIN last_load l ON 
                c.sync_id=l.sync_id AND
                c.project_id= l.project_id AND
                c.dataset = l.dataset AND
                c.table_name = l.table_name AND
                c.interval = l.schedule_type
            LEFT ANTI JOIN current_schedule d ON
                c.sync_id=d.sync_id AND
                c.project_id= d.project_id AND
                c.dataset = d.dataset AND
                c.table_name = d.table_name AND
                c.interval = d.schedule_type
            CROSS JOIN new_schedule n
            WHERE c.enabled = TRUE
            --AND d.group_schedule_id IS NULL
            AND c.interval='{schedule_type}'
        )

        INSERT INTO bq_sync_schedule
        SELECT * FROM schedule s
        WHERE s.sync_id = '{sync_id}'
        """

        try:
            self.Context.sql(sql)
        except Exception as e:
            raise SchedulerError(f"{e}") from e
        
    def save_schedule_telemetry(self, rdd):
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        """
        tbl = f"bq_sync_schedule_telemetry"

        schema = self.Context.table(tbl).schema

        df = self.Context.createDataFrame(rdd, schema)

        try:
            df.write.mode("APPEND").saveAsTable(tbl)
        except Exception as e:
            raise SyncLoadError(f"Error Saving Schedule Telemetry: {e}") from e

    def get_schedule(self, sync_id:str, schedule_type:str):
        """
        Gets the schedule activities that need to be run based on the configuration and metadata
        """
        sql = f"""
        WITH last_completed_schedule AS (
            SELECT sync_id, schedule_id, project_id, dataset, table_name, max_watermark, started AS last_schedule_dt
            FROM (
                SELECT sync_id, schedule_id, project_id, dataset, table_name, started, max_watermark,
                ROW_NUMBER() OVER(PARTITION BY sync_id, project_id, dataset, table_name ORDER BY scheduled DESC) AS row_num
                FROM bq_sync_schedule
                WHERE status='COMPLETE'
            )
            WHERE row_num = 1
        ),
        tbl_options AS (
            SELECT table_catalog, table_schema, table_name, CAST(option_value AS boolean) AS option_value
            FROM bq_information_schema_table_options
            WHERE option_name='require_partition_filter'
        ),
        tbl_size AS (
            SELECT
                *,
                ROW_NUMBER() OVER(ORDER BY total_logical_mb DESC) AS size_priority
            FROM (
                SELECT
                    table_catalog, table_schema, table_name,
                    SUM(total_rows) AS total_rows,
                    ROUND(SUM(total_logical_bytes)/ (1024 * 1024),0) AS total_logical_mb
                FROM bq_information_schema_partitions sp
                GROUP BY table_catalog, table_schema, table_name
            )
        ),
        tbl_partitions AS (
            SELECT
                sp.table_catalog, sp.table_schema, sp.table_name, sp.partition_id, o.option_value as require_partition_filter
            FROM bq_information_schema_partitions sp                         
            JOIN bq_sync_configuration c ON sp.table_catalog = c.project_id AND 
                sp.table_schema = c.dataset AND sp.table_name = c.table_name
            LEFT JOIN tbl_options o ON sp.table_catalog = o.table_catalog AND 
                sp.table_schema = o.table_schema AND sp.table_name = o.table_name   
            LEFT JOIN last_completed_schedule s ON sp.table_catalog = s.project_id 
                AND sp.table_schema = s.dataset AND sp.table_name = s.table_name
            WHERE sp.partition_id != '__NULL__'
            AND ((sp.last_modified_time >= s.last_schedule_dt) OR (s.last_schedule_dt IS NULL))
            AND 
                (
                    COALESCE(o.option_value, FALSE) = TRUE OR
                    (c.load_strategy = 'PARTITION' AND s.last_schedule_dt IS NOT NULL) OR
                    (c.load_strategy = 'PARTITION' AND c.partition_type = 'RANGE') OR
                    c.load_strategy = 'TIME_INGESTION'
                )
        )

        SELECT
            c.table_id, c.sync_id, c.load_strategy, c.load_type,
            c.priority, c.project_id, c.dataset, c.table_name,
            c.object_type, 
            CASE WHEN (c.source_query='') THEN NULL ELSE c.source_query END AS source_query, 
            CASE WHEN (c.source_predicate='') THEN NULL ELSE c.source_predicate END AS source_predicate,
            CASE WHEN (c.watermark_column='') THEN NULL ELSE c.watermark_column END AS watermark_column,
            COALESCE(c.is_partitioned, FALSE) AS is_partitioned, 
            CASE WHEN (c.partition_column='') THEN NULL ELSE c.partition_column END AS partition_column, 
            CASE WHEN (c.partition_grain='') THEN NULL ELSE c.partition_grain END AS partition_grain, 
            CASE WHEN (c.partition_range='') THEN NULL ELSE c.partition_range END AS partition_range,
            CASE WHEN (c.partition_type='') THEN NULL ELSE c.partition_type END AS partition_type,  
            CASE WHEN (c.partition_data_type='') THEN NULL ELSE c.partition_data_type END AS partition_data_type, 
            c.lakehouse,
            CASE WHEN (c.lakehouse_schema='') THEN NULL ELSE c.lakehouse_schema END AS lakehouse_schema, 
            c.lakehouse_table_name, 
            CASE WHEN (c.lakehouse_partition='') THEN NULL ELSE c.lakehouse_partition END AS lakehouse_partition, 
            c.use_lakehouse_schema,
            c.enforce_expiration, c.allow_schema_evolution, c.table_maintenance_enabled, c.table_maintenance_interval,
            c.flatten_table, c.flatten_inplace, c.explode_arrays, c.primary_keys,
            p.partition_id,p.require_partition_filter,
            s.group_schedule_id,s.schedule_id,s.status AS sync_status,s.started,s.completed,
            h.max_watermark,h.last_schedule_dt,
            c.column_map,
            CASE WHEN (h.schedule_id IS NULL) THEN TRUE ELSE FALSE END AS initial_load,
            COALESCE(ts.total_rows, 0) AS total_rows, 
            COALESCE(ts.total_logical_mb, 0) AS total_logical_mb, 
            COALESCE(ts.size_priority, 100) AS size_priority
        FROM bq_sync_configuration c
        JOIN bq_sync_schedule s ON c.sync_id = s.sync_id AND c.project_id = s.project_id 
            AND c.dataset = s.dataset AND  c.table_name = s.table_name
        LEFT JOIN last_completed_schedule h ON c.sync_id = h.sync_id AND c.project_id = h.project_id 
            AND c.dataset = h.dataset AND c.table_name = h.table_name
        LEFT JOIN tbl_partitions p ON p.table_catalog = c.project_id 
            AND p.table_schema = c.dataset AND p.table_name = c.table_name
        LEFT JOIN bq_sync_schedule_telemetry t ON s.sync_id = t.sync_id AND s.schedule_id = t.schedule_id 
            AND c.project_id = t.project_id AND c.dataset = t.dataset AND c.table_name = t.table_name AND
            COALESCE(p.partition_id, '0') = COALESCE(t.partition_id, '0') AND t.status = 'COMPLETE'
        LEFT JOIN tbl_size ts ON c.project_id = ts.table_catalog AND c.dataset = ts.table_schema 
            AND c.table_name = ts.table_name
        WHERE s.status IN ('SCHEDULED', 'FAILED') AND c.enabled = TRUE AND t.schedule_id IS NULL
            AND c.sync_id='{sync_id}'
            AND s.schedule_type='{schedule_type}'
        """
        df = self.Context.sql(sql)
        df.createOrReplaceTempView("LoaderQueue")
        df.cache()

        try:
            return df
        except Exception as e:
            raise SchedulerError(f"Failed to retrieve BQ Sync schedule: {e}") from e

    def process_load_group_telemetry(self, sync_id:str, schedule_type:str):
        """
        When a load group is complete, summarizes the telemetry to close out the schedule
        """
        sql = f"""
        WITH schedule_telemetry_last AS (
            SELECT
                sync_id,schedule_id,project_id,dataset,table_name,status,started,completed,max_watermark,
                ROW_NUMBER()OVER(PARTITION BY sync_id,schedule_id,project_id,dataset,table_name 
                    ORDER BY started DESC) AS row_num
            FROM bq_sync_schedule_telemetry
        ),        
        schedule_telemetry AS (
            SELECT
                sync_id,schedule_id,project_id,dataset,table_name,
                SUM(CASE WHEN status='COMPLETE' THEN 1 ELSE 0 END) AS completed_activities,
                SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS failed_activities,
                MIN(started) as started,MAX(completed) as completed
            FROM schedule_telemetry_last
            WHERE row_num = 1
            GROUP BY sync_id,schedule_id,project_id,dataset,table_name
        ),
        schedule_watermarks AS (
            SELECT
                sync_id,schedule_id,project_id,dataset,table_name,max_watermark
            FROM schedule_telemetry_last
            WHERE max_watermark IS NOT NULL
            AND row_num=1
        ),
        schedule_results AS (
            SELECT
                s.sync_id,s.group_schedule_id,s.schedule_id,s.project_id,s.dataset,s.table_name,s.status,
                CASE WHEN t.failed_activities = 0 THEN 'COMPLETE' ELSE 'FAILED' END AS result_status,
                t.started,t.completed,t.completed_activities,t.failed_activities,
                w.max_watermark,s.priority 
            FROM bq_sync_schedule s
            JOIN schedule_telemetry t ON s.sync_id=t.sync_id AND s.schedule_id=t.schedule_id AND
                s.project_id=t.project_id AND s.dataset=t.dataset AND s.table_name=t.table_name
            LEFT JOIN schedule_watermarks w ON s.sync_id=w.sync_id AND s.schedule_id=w.schedule_id 
                AND s.project_id=w.project_id AND s.dataset=w.dataset AND s.table_name=w.table_name
            WHERE s.sync_id='{sync_id}'
            AND s.schedule_type='{schedule_type}' 
            AND s.status IN ('SCHEDULED', 'FAILED')
        ) 

        MERGE INTO bq_sync_schedule s
        USING schedule_results r
        ON s.sync_id=r.sync_id AND s.schedule_id=r.schedule_id AND
            s.project_id=r.project_id AND s.dataset=r.dataset AND s.table_name=r.table_name
        WHEN MATCHED THEN
            UPDATE SET
                s.status = r.result_status,
                s.started = r.started,
                s.completed = r.completed,
                s.completed_activities = r.completed_activities,
                s.failed_activities = r.failed_activities,
                s.max_watermark = r.max_watermark

        """

        try:
            self.Context.sql(sql)
        except Exception as e:
            raise SyncLoadError(f"Error processing Load Group telemetry: {e}") from e

    def commit_table_configuration(self, sync_id:str, schedule_type:str):
        """
        After an initial load, locks the table configuration so no changes can occur when reprocessing metadata
        """
        sql = f"""
        WITH committed AS (
            SELECT sync_id, project_id, dataset, table_name, MAX(started) as started
            FROM bq_sync_schedule
            WHERE status='COMPLETE' AND 
            sync_id='{sync_id}' AND
            schedule_type='{schedule_type}'
            GROUP BY sync_id, project_id, dataset, table_name
        )

        MERGE INTO bq_sync_configuration t
        USING committed c
        ON t.sync_id=c.sync_id AND t.project_id=c.project_id AND t.dataset=c.dataset AND t.table_name=c.table_name
        WHEN MATCHED AND t.sync_state='INIT' THEN
            UPDATE SET
                t.sync_state='COMMIT'
        """
        
        try:
            self.Context.sql(sql)
        except Exception as e:
            raise SchedulerError("Error committing BQ Sync configuration") from e
    
    def create_userconfig_tables_proxy_view(self):
        """
        Explodes the User Config table configuration into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_tables
            AS
            SELECT
                sync_id,
                tbl.project_id as project_id,
                tbl.dataset AS dataset,
                tbl.table_name,
                tbl.object_type AS object_type,
                tbl.enabled AS enabled,
                tbl.priority AS priority,
                tbl.interval AS interval,
                tbl.enforce_expiration AS enforce_expiration,
                tbl.allow_schema_evolution AS allow_schema_evolution,
                tbl.flatten_table AS flatten_table,
                tbl.flatten_inplace AS flatten_inplace,
                tbl.explode_arrays AS explode_arrays,
                tbl.table_maintenance.enabled AS table_maintenance_enabled,
                tbl.table_maintenance.interval AS table_maintenance_interval,
                tbl.source_query,
                tbl.predicate AS source_predicate,
                tbl.load_strategy,
                tbl.load_type,                
                tbl.watermark.column as watermark_column,
                tbl.bq_partition.enabled as partition_enabled,
                tbl.bq_partition.type as partition_type,
                tbl.bq_partition.column as partition_column,
                tbl.bq_partition.partition_grain,
                tbl.bq_partition.partition_data_type,
                tbl.bq_partition.partition_range,
                tbl.lakehouse_target.lakehouse,
                tbl.lakehouse_target.schema AS lakehouse_schema,
                tbl.lakehouse_target.table_name AS lakehouse_target_table,
                tbl.lakehouse_target.partition_by AS lakehouse_partition,
                to_json(tbl.column_map) AS column_map,
                tbl.keys
            FROM (
                SELECT 
                    id AS sync_id,
                    EXPLODE(tables) AS tbl 
                FROM user_config_json)
        """

        try:
            self.Context.sql(sql)
        except Exception as e:
            raise SyncConfigurationError(f"Error creating User Config table view proxy: {e}") from e
    
    def create_userconfig_tables_cols_proxy_view(self):
        """
        Explodes the User Config table primary keys into a temporary view
        """
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_table_keys
            AS
            SELECT
                id AS sync_id, project_id, dataset, table_name, pkeys.column
            FROM (
                SELECT
                    id, tbl.project_id, tbl.dataset, tbl.table_name, EXPLODE_OUTER(tbl.keys) AS pkeys
                FROM (SELECT id, EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        
        try:
            self.Context.sql(sql)
        except Exception as e:
            raise SyncConfigurationError("Error creating User Config columns view proxy") from e
    
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
            t.table_catalog, t.table_schema, t.table_name, 
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

        try:
            self.Context.sql(sql)
        except Exception as e:
            raise AutoDiscoveryError(f"Error creating Autodiscovery proxy view: {e}") from e

    def auto_detect_profiles(self, sync_id:str):        
        sql = f"""
        WITH default_config AS (
            SELECT '{sync_id}' AS sync_id,
            COALESCE(autodiscover.autodetect, TRUE) AS autodetect, 
            autodiscover.materialized_views.load_all AS load_all_materialized_views,
            autodiscover.views.load_all AS load_all_views,
            autodiscover.tables.load_all AS load_all_tables,
            enable_data_expiration,
            fabric.target_lakehouse AS target_lakehouse,
            fabric.target_schema AS target_schema,
            COALESCE(fabric.enable_schemas, FALSE) AS enable_schemas 
            FROM user_config_json
        ),
        bq_objects AS (
            SELECT 
                d.sync_id, d.autodetect, d.load_all_tables AS load_all, d.enable_data_expiration,
                d.target_lakehouse, d.target_schema, d.enable_schemas,
                a.table_catalog, a.table_schema, a.table_name, array_agg(COALESCE(a.pk_col, u.column)) as pk, 'BASE_TABLE' AS object_type
            FROM bq_table_metadata_autodetect a
            LEFT JOIN user_config_table_keys u ON
                a.table_catalog = u.project_id AND
                a.table_schema = u.dataset AND
                a.table_name = u.table_name
            CROSS JOIN default_config d
            GROUP BY d.sync_id, d.autodetect, d.load_all_tables, d.enable_data_expiration,
                d.target_lakehouse, d.target_schema, d.enable_schemas, a.table_catalog, a.table_schema, a.table_name
            UNION ALL
            SELECT
                d.sync_id, d.autodetect, d.load_all_materialized_views, d.enable_data_expiration,
                d.target_lakehouse, d.target_schema, d.enable_schemas,
                v.table_catalog, v.table_schema, v.table_name, NULL, 'MATERIALIZED_VIEW'
            FROM bq_information_schema_materialized_views v
            CROSS JOIN default_config d
            UNION ALL
            SELECT 
                d.sync_id, d.autodetect, d.load_all_views, d.enable_data_expiration,
                d.target_lakehouse, d.target_schema, d.enable_schemas,
                table_catalog, table_schema, table_name , NULL, 'VIEW'
            FROM bq_information_schema_views v
            CROSS JOIN default_config d
        ),
        source AS (
            SELECT
                p.sync_id,
                UUID() AS table_id,
                p.table_catalog as project_id,
                p.table_schema as dataset,
                p.table_name as table_name,
                p.object_type AS object_type,

                CASE WHEN p.load_all THEN
                    COALESCE(u.enabled, TRUE) ELSE
                    COALESCE(u.enabled, FALSE) END AS enabled,
                CASE WHEN (COALESCE(u.lakehouse, '')='') 
                    THEN p.target_lakehouse ELSE u.lakehouse END AS lakehouse,                
                CASE WHEN p.enable_schemas THEN
                    COALESCE(u.lakehouse_schema, COALESCE(p.target_schema, a.table_schema))
                    ELSE NULL END AS lakehouse_schema,
                CASE WHEN (COALESCE(u.lakehouse_target_table,'') ='') 
                    THEN p.table_name ELSE u.lakehouse_target_table END AS lakehouse_table_name,
                COALESCE(u.lakehouse_partition, '') AS lakehouse_partition,
                COALESCE(u.source_query, '') AS source_query,
                COALESCE(u.source_predicate, '') AS source_predicate,
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
                COALESCE(u.load_type, 
                    CASE WHEN (COALESCE(u.watermark_column, a.pk_col) IS NOT NULL AND
                        COALESCE(u.watermark_column, a.pk_col) <> '') THEN 'APPEND' ELSE
                    'OVERWRITE' END) AS load_type,
                COALESCE(u.interval, 'AUTO') AS interval,
                p.pk AS primary_keys,
                COALESCE(u.partition_enabled, a.is_partitioned) AS is_partitioned,
                COALESCE(u.partition_column, a.partition_col, '') AS partition_column,
                COALESCE(u.partition_type, a.partitioning_type, '') AS partition_type,
                COALESCE(u.partition_grain, a.partitioning_strategy, '') AS partition_grain,
                COALESCE(u.partition_data_type, a.partition_data_type, '') AS partition_data_type,
                COALESCE(u.partition_range, a.partition_range, '') AS partition_range,
                COALESCE(u.watermark_column, a.pk_col, '') AS watermark_column, 
                p.autodetect,
                p.enable_schemas AS use_lakehouse_schema,
                CASE WHEN (p.enable_data_expiration) THEN
                    COALESCE(u.enforce_expiration, FALSE) ELSE
                    FALSE END AS enforce_expiration,
                COALESCE(u.allow_schema_evolution, FALSE) AS allow_schema_evolution,
                COALESCE(u.table_maintenance_enabled, FALSE) AS table_maintenance_enabled,
                COALESCE(u.table_maintenance_interval, 'AUTO') AS table_maintenance_interval,
                COALESCE(u.flatten_table, FALSE) AS flatten_table,
                COALESCE(u.flatten_inplace, TRUE) AS flatten_inplace,
                COALESCE(u.explode_arrays, FALSE) AS explode_arrays,
                COALESCE(u.column_map, NULL) AS column_map,
                CASE WHEN u.table_name IS NULL THEN FALSE ELSE TRUE END AS config_override,
                'INIT' AS sync_state,
                CURRENT_TIMESTAMP() as created_dt,
                NULL as last_updated_dt
            FROM bq_objects p
            LEFT JOIN bq_table_metadata_autodetect a ON
                a.table_catalog = p.table_catalog AND
                a.table_schema = p.table_schema AND
                a.table_name = p.table_name AND 
                p.object_type = 'BASE_TABLE'
            LEFT JOIN user_config_tables u ON 
                p.table_catalog = u.project_id AND
                p.table_schema = u.dataset AND
                p.table_name = u.table_name AND
                p.object_type = u.object_type            
        )

        MERGE INTO bq_sync_configuration t
        USING source s
        ON t.sync_id = s.sync_id AND t.project_id = s.project_id AND t.dataset = s.dataset AND t.table_name = s.table_name
        WHEN MATCHED AND t.sync_state <> 'INIT' THEN
            UPDATE SET
                t.enabled = s.enabled,
                t.interval = s.interval,
                t.priority = s.priority,
                t.source_query = s.source_query,
                t.source_predicate = s.source_predicate,
                t.enforce_expiration = s.enforce_expiration,
                t.column_map = s.column_map,
                t.allow_schema_evolution = s.allow_schema_evolution,
                t.table_maintenance_enabled = s.table_maintenance_enabled,
                t.table_maintenance_interval = s.table_maintenance_interval,
                t.last_updated_dt = CURRENT_TIMESTAMP()
        WHEN MATCHED AND t.sync_state = 'INIT' THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.sync_id = '{sync_id}' AND t.enabled = TRUE THEN
            UPDATE SET
                t.enabled = FALSE
        """

        try:
            self.Context.sql(sql)
        except Exception as e:
            raise AutoDiscoveryError(f"Autodiscovery failed: {e}") from e
            
    
    def ensure_schemas(self, workspace:str, sync_id:str):
        df = self.Context.read.table("bq_sync_configuration")
        df = df.filter((col("sync_id")==sync_id) & 
            (col("enabled")==True) & (col("use_lakehouse_schema")==True))
        df = df.select(col("lakehouse"), coalesce(col("lakehouse_schema"), col("dataset")).alias("schema"))

        schemas = [f"{r['lakehouse']}.{r['schema']}" for r in df.collect()]

        try:
            for schema in schemas:
                self.Context.sql(f"CREATE SCHEMA IF NOT EXISTS {workspace}.{schema}")
        except Exception as e:
            raise SyncConfigurationError("Error creating/ensuring Lakehouse schema exists.") from e
    
    def get_sync_temp_views(self) -> List[str]:
        return ["bq_table_metadata_autodetect", \
            "user_config_json", \
            "user_config_table_keys", \
            "user_config_tables"]
            
    def cleanup_session(self):
        temp_views = self.get_sync_temp_views()
        list(map(lambda x: self.Context.sql(f"DROP TABLE IF EXISTS {x}"), temp_views))
    
    def sync_retention_config(self, sync_id:str):
        sql = f"""
        WITH cfg AS (
            SELECT table_catalog, table_schema, table_name, CAST(option_value AS FLOAT) AS expiration_days
            FROM bq_information_schema_table_options
            WHERE option_name='partition_expiration_days'
        ),
        parts AS (
            SELECT
                t.table_catalog, t.table_schema, t.table_name, p.partition_id,
                CAST(ROUND(x.expiration_days * 24, 0) AS INT) AS expiration_hours,
                CASE WHEN (LEN(p.partition_id) = 4) THEN TO_TIMESTAMP(p.partition_id, 'yyyy') + INTERVAL 1 YEAR
                    WHEN (LEN(p.partition_id) = 6) THEN TO_TIMESTAMP(p.partition_id, 'yyyyMM') + INTERVAL 1 MONTH
                    WHEN (LEN(p.partition_id) = 8) THEN TO_TIMESTAMP(p.partition_id, 'yyyyMMdd') + INTERVAL 24 HOURS
                    WHEN (LEN(p.partition_id) = 10) THEN TO_TIMESTAMP(p.partition_id, 'yyyyMMddHH') + INTERVAL 1 HOUR
                ELSE NULL END AS partition_boundary
            FROM bq_information_schema_tables t 
            JOIN bq_information_schema_partitions p ON t.table_catalog=p.table_catalog AND 
                t.table_schema=p.table_schema AND t.table_name=p.table_name
            JOIN cfg x ON t.table_catalog=x.table_catalog 
                AND t.table_schema=x.table_schema AND t.table_name=x.table_name
            WHERE p.partition_id IS NOT NULL
        ),
        src AS (
            SELECT
                table_catalog, table_schema, table_name, partition_id,
                partition_boundary + MAKE_INTERVAL(0, 0, 0, 0, expiration_hours, 0, 0) as expiration    
            FROM parts
            UNION ALL
            SELECT table_catalog, table_schema, table_name, null as partition_id, 
                CAST(REPLACE(REPLACE(option_value, 'TIMESTAMP'), '"') AS TIMESTAMP) as expiration
            FROM bq_information_schema_table_options
            WHERE option_name='expiration_timestamp'
        ),
        expiration AS (
            SELECT c.sync_id, s.table_catalog, s.table_schema, s.table_name, s.partition_id, s.expiration
            FROM src s 
            JOIN bq_sync_configuration c ON s.table_catalog=c.project_id
                AND s.table_schema=c.dataset AND s.table_name=c.table_name
                AND c.enforce_expiration=TRUE
            WHERE c.sync_id='{sync_id}'
        )

        MERGE INTO bq_sync_data_expiration t
        USING expiration s
        ON t.sync_id=s.sync_id AND t.table_catalog=s.table_catalog 
            AND t.table_schema=s.table_schema AND t.table_name=s.table_name
            AND t.partition_id=s.partition_id
        WHEN MATCHED AND t.expiration <> s.expiration THEN
            UPDATE SET
                t.expiration = s.expiration
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """
        
        try:
            self.Context.sql(sql)
        except Exception as e:
            raise DataRetentionError(f"Error applying data expiration policy: {e}") from e
    
    def get_bq_retention_policy(self, sync_id:str) -> DataFrame:
        sql = f"""
            SELECT c.lakehouse,c.lakehouse_schema,c.lakehouse_table_name,c.use_lakehouse_schema,
                c.is_partitioned,c.partition_column,c.partition_type,c.partition_grain,
                e.partition_id
            FROM bq_sync_data_expiration e
            JOIN bq_sync_configuration c ON e.sync_id=c.sync_id AND e.table_catalog=c.project_id
                AND e.table_schema=c.dataset AND e.table_name=c.table_name AND c.enforce_expiration=TRUE
            WHERE e.expiration < current_timestamp()
            AND e.sync_id='{sync_id}'
        """

        try:
            return self.Context.sql(sql)
        except Exception as e:
            raise SyncConfigurationError(f"Failed to retrieve BQ Sync retention policy: {e}") from e
    
    def create_proxy_views(self):
        self.create_userconfig_tables_proxy_view()        
        self.create_userconfig_tables_cols_proxy_view()

        self.create_autodetect_view()