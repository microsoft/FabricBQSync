from pyspark.sql.session import DataFrame # type: ignore
from pyspark.sql.types  import (  # type: ignore
    StructType, StructField, StringType, BooleanType, 
    IntegerType, LongType, TimestampType, ArrayType
)
from typing import List
from delta.tables import DeltaTable  # type: ignore
import functools
import time

from FabricSync.BQ.Enum import SyncScheduleType
from FabricSync.BQ.Model.Maintenance import MaintenanceSchedule
from FabricSync.BQ.Core import ContextAwareBase

class FabricMetastoreSchema():
    data_type_map=StructType([StructField('data_type',StringType(),True),StructField('partition_type',StringType(),True),StructField('is_watermark',StringType(),True)])
    sync_configuration=StructType([StructField('sync_id',StringType(),True),StructField('table_id',StringType(),True),StructField('project_id',StringType(),True),StructField('dataset',StringType(),True),StructField('table_name',StringType(),True),StructField('object_type',StringType(),True),StructField('enabled',BooleanType(),True),StructField('workspace_id',StringType(),True),StructField('workspace_name',StringType(),True),StructField('lakehouse_type',StringType(),True),StructField('lakehouse_id',StringType(),True),StructField('lakehouse',StringType(),True),StructField('lakehouse_schema',StringType(),True),StructField('lakehouse_table_name',StringType(),True),StructField('lakehouse_partition',StringType(),True),StructField('source_query',StringType(),True),StructField('source_predicate',StringType(),True),StructField('priority',IntegerType(),True),StructField('load_strategy',StringType(),True),StructField('load_type',StringType(),True),StructField('interval',StringType(),True),StructField('primary_keys',ArrayType(StringType(),True),True),StructField('is_partitioned',BooleanType(),True),StructField('partition_column',StringType(),True),StructField('partition_type',StringType(),True),StructField('partition_grain',StringType(),True),StructField('partition_data_type',StringType(),True),StructField('partition_range',StringType(),True),StructField('watermark_column',StringType(),True),StructField('autodetect',BooleanType(),True),StructField('use_lakehouse_schema',BooleanType(),True),StructField('enforce_expiration',BooleanType(),True),StructField('allow_schema_evolution',BooleanType(),True),StructField('table_maintenance_enabled',BooleanType(),True),StructField('table_maintenance_interval',StringType(),True),StructField('flatten_table',BooleanType(),True),StructField('flatten_inplace',BooleanType(),True),StructField('explode_arrays',BooleanType(),True),StructField('use_bigquery_export',BooleanType(),True),StructField('use_standard_api',BooleanType(),True),StructField('column_map',StringType(),True),StructField('config_override',BooleanType(),True),StructField('sync_state',StringType(),True),StructField('config_path',StringType(),True),StructField('created_dt',TimestampType(),True),StructField('last_updated_dt',TimestampType(),True)])
    sync_data_expiration=StructType([StructField('sync_id',StringType(),True),StructField('table_catalog',StringType(),True),StructField('table_schema',StringType(),True),StructField('table_name',StringType(),True),StructField('partition_id',StringType(),True),StructField('expiration',TimestampType(),True)])
    sync_maintenance=StructType([StructField('sync_id',StringType(),True),StructField('table_id',StringType(),True),StructField('project_id',StringType(),True),StructField('dataset',StringType(),True),StructField('table_name',StringType(),True),StructField('partition_id',StringType(),True),StructField('lakehouse',StringType(),True),StructField('lakehouse_schema',StringType(),True),StructField('lakehouse_table_name',StringType(),True),StructField('lakehouse_partition',StringType(),True),StructField('row_count',LongType(),True),StructField('table_partition_size',LongType(),True),StructField('last_maintenance_type',StringType(),True),StructField('last_maintenance_interval',StringType(),True),StructField('last_maintenance',TimestampType(),True),StructField('last_optimize',TimestampType(),True),StructField('last_vacuum',TimestampType(),True),StructField('last_maintenance_status',StringType(),True),StructField('created_dt',TimestampType(),True),StructField('last_updated_dt',TimestampType(),True)])
    sync_schedule=StructType([StructField('group_schedule_id',StringType(),True),StructField('schedule_id',StringType(),True),StructField('sync_id',StringType(),True),StructField('table_id',StringType(),True),StructField('project_id',StringType(),True),StructField('dataset',StringType(),True),StructField('table_name',StringType(),True),StructField('schedule_type',StringType(),True),StructField('scheduled',TimestampType(),True),StructField('status',StringType(),True),StructField('started',TimestampType(),True),StructField('completed',TimestampType(),True),StructField('completed_activities',IntegerType(),True),StructField('failed_activities',IntegerType(),True),StructField('max_watermark',StringType(),True),StructField('mirror_file_index',LongType(),True),StructField('priority',IntegerType(),True)])
    sync_schedule_telemetry=StructType([StructField('schedule_id',StringType(),True),StructField('sync_id',StringType(),True),StructField('table_id',StringType(),True),StructField('project_id',StringType(),True),StructField('dataset',StringType(),True),StructField('table_name',StringType(),True),StructField('partition_id',StringType(),True),StructField('status',StringType(),True),StructField('started',TimestampType(),True),StructField('completed',TimestampType(),True),StructField('src_row_count',LongType(),True),StructField('inserted_row_count',LongType(),True),StructField('updated_row_count',LongType(),True),StructField('delta_version',LongType(),True),StructField('spark_application_id',StringType(),True),StructField('max_watermark',StringType(),True),StructField('summary_load',StringType(),True),StructField('source_query',StringType(),True),StructField('source_predicate',StringType(),True),StructField('mirror_file_index',LongType(),True),StructField('bq_api',StringType(),True)])

class Metastore():
    def Retry(func_=None,max_retries:int=3,backoff_factor:int=2):
        """
        Decorator to retry a function in case of ConcurrentModificationException
        This decorator will retry the function up to max_retries times with an exponential backoff
        The backoff factor determines the wait time between retries, which is calculated as backoff_factor ** attempt
        """
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args,**kwargs):
                attempt=0
                last_exception=None

                while attempt < max_retries:
                    try:
                        r=func(*args,**kwargs)
                        break
                    except Exception as e:
                        last_exception=e
                        if "ConcurrentModificationException" in str(e):
                            attempt +=1
                            wait_time=backoff_factor ** attempt
                            time.sleep(wait_time)
                        else:
                            raise e
                else:
                    raise last_exception

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

class FabricMetastore(ContextAwareBase):
    @classmethod
    @Metastore.Retry()
    def build_schedule(cls,schedule_type:SyncScheduleType) -> None:
        """
        Builds the sync schedule for the given schedule type
        This method creates a new schedule for the sync schedule load group
        by querying the sync_configuration table and determining which tables need to be scheduled
        """
        sql=f"""
        WITH last_scheduled_load AS (
            SELECT
                sync_id,schedule_type,scheduled,
                SUM(CASE WHEN (status IN ('FAILED','SCHEDULED')) THEN 1 ELSE 0 END) as open_tasks,
                ROW_NUMBER()OVER(PARTITION BY sync_id,schedule_type ORDER BY scheduled DESC) AS row_num
            FROM sync_schedule s
            GROUP BY sync_id,schedule_type,scheduled
        ),
        last_table_load AS (
            SELECT sync_id,table_id,project_id,dataset,table_name,
                MAX(started) AS last_load_update,
                CASE WHEN (TRY_TO_TIMESTAMP(MAX(max_watermark)) IS NOT NULL) THEN 
                    FLOOR((CAST(NOW() AS LONG) - CAST(TRY_TO_TIMESTAMP(MAX(max_watermark)) AS LONG))/60)
                    ELSE NULL END AS cdc_min_window
            FROM sync_schedule
            WHERE status='COMPLETE'
            GROUP BY sync_id,table_id,project_id,dataset,table_name
        ),
        new_schedule AS ( 
            SELECT uuid() AS group_schedule_id,CURRENT_TIMESTAMP() as scheduled
        ),
        last_bq_tbl_updates AS (
            SELECT sync_id,table_catalog,table_schema,table_name,max(last_modified_time) as last_bq_tbl_update
            FROM information_schema_partitions
            GROUP BY sync_id,table_catalog,table_schema,table_name
            UNION ALL
            SELECT sync_id,table_catalog,table_schema,table_name,last_refresh_time
            FROM information_schema_materialized_views
        ),      
        schedule AS (
            SELECT
                n.group_schedule_id,UUID() AS schedule_id,
                c.sync_id,c.table_id,c.project_id,c.dataset,c.table_name,
                c.interval AS schedule_type,n.scheduled,
                CASE WHEN ((l.last_load_update IS NULL) OR (b.last_bq_tbl_update IS NULL) OR
                        (b.last_bq_tbl_update >=l.last_load_update)) THEN 
                            CASE WHEN c.load_strategy='CDC' AND l.cdc_min_window < 10 THEN 'SKIPPED' 
                                ELSE 'SCHEDULED' END
                    WHEN (b.table_name IS NULL AND c.object_type='BASE_TABLE') THEN 'EXPIRED' 
                    ELSE 'SKIPPED' END as status,
                NULL as started,NULL as completed,NULL as completed_activities,
                NULL as failed_activities,NULL as max_watermark,NULL AS mirror_file_index,
                c.priority                
            FROM sync_configuration c 
            LEFT JOIN last_bq_tbl_updates b ON c.sync_id=b.sync_id AND c.project_id=b.table_catalog 
                AND c.dataset=b.table_schema AND c.table_name=b.table_name
            LEFT JOIN last_table_load l ON c.sync_id=l.sync_id AND c.table_id=l.table_id 
            LEFT JOIN last_scheduled_load d ON c.sync_id=d.sync_id AND c.interval=d.schedule_type AND d.row_num=1
            CROSS JOIN new_schedule n
            WHERE c.enabled=TRUE AND COALESCE(d.open_tasks,0)=0
            AND c.sync_id='{cls.ID}'
            AND c.interval='{schedule_type}'
        )

        INSERT INTO sync_schedule
        SELECT * FROM schedule s
        """

        cls.Context.sql(sql)

    @classmethod
    @Metastore.Retry()
    def save_schedule_telemetry(cls,rdd) -> None:
        """
        Saves the telemetry data for the sync schedule to the sync_schedule_telemetry table
        This method takes an RDD containing the telemetry data and writes it to the sync_schedule_telemetry table
        The telemetry data includes information about the sync schedule, such as the sync_id, table_id, project_id, dataset, table_name,
        partition_id, status, started, completed, src_row_count, inserted_row_count, updated_row_count, delta_version,
        spark_application_id, max_watermark, summary_load, source_query, source_predicate, and mirror_file_index
        """
        df=cls.Context.createDataFrame(rdd,FabricMetastoreSchema.sync_schedule_telemetry)
        df.write.mode("APPEND").saveAsTable("sync_schedule_telemetry")

    @classmethod 
    def get_schedule(cls,schedule_type:SyncScheduleType) -> DataFrame:
        """
        Retrieves the sync schedule for the given schedule type
        This method queries the sync_schedule table to get the schedule for the given schedule type
        and returns a DataFrame containing the schedule information
        This is used to get the schedule for the sync schedule load group
        and to determine the tables that need to be processed for the given schedule type
        """
        sql=f"""
        WITH last_completed_schedule AS (
            SELECT 
                sync_id,schedule_id,table_id,project_id,dataset,table_name,
                mirror_file_index,max_watermark,started AS last_schedule_dt
            FROM (
                SELECT sync_id,schedule_id,table_id,project_id,dataset,table_name,started,max_watermark,mirror_file_index,
                ROW_NUMBER() OVER(PARTITION BY sync_id,table_id,project_id,dataset,table_name ORDER BY scheduled DESC) AS row_num
                FROM sync_schedule
                WHERE status='COMPLETE'
            )
            WHERE row_num=1
        ),
        complex_type_cols AS (
            SELECT
                sync_id,table_catalog, table_schema, table_name,
                CASE 
                    WHEN LOCATE('STRUCT', data_type) > 0 THEN 1 
                    WHEN LOCATE('ARRAY', data_type) > 0 THEN 1 
                    WHEN LOCATE('MAP', data_type) > 0 THEN 1 
                    ELSE 0 END AS is_complex_type
            FROM information_schema_columns
        ),
        complex_types AS (
            SELECT 
                sync_id,table_catalog, table_schema, table_name, 
                CASE 
                    WHEN SUM(is_complex_type) > 0 THEN TRUE
                    ELSE FALSE END AS has_complex_types
            FROM complex_type_cols
            GROUP BY sync_id,table_catalog, table_schema, table_name
        ),
        tbl_options AS (
            SELECT sync_id,table_catalog,table_schema,table_name,CAST(option_value AS boolean) AS option_value
            FROM information_schema_table_options
            WHERE option_name='require_partition_filter'
        ),
        tbl_stats AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY sync_id ORDER BY total_logical_mb DESC) AS size_priority
            FROM (
                SELECT
                    sync_id,table_catalog,table_schema,table_name,
                    MAX(last_modified_time) AS last_modified_time,
                    SUM(total_rows) AS total_rows,
                    ROUND(SUM(total_logical_bytes)/ (1024 * 1024),0) AS total_logical_mb
                FROM information_schema_partitions sp
                GROUP BY sync_id,table_catalog,table_schema,table_name
            )
        ),
        tbl_partitions AS (
            SELECT
                sp.sync_id,c.table_id,sp.table_catalog,sp.table_schema,sp.table_name,sp.partition_id,o.option_value as require_partition_filter
            FROM information_schema_partitions sp                         
            JOIN sync_configuration c ON sp.sync_id=c.sync_id AND sp.table_catalog=c.project_id 
                AND sp.table_schema=c.dataset AND sp.table_name=c.table_name 
            LEFT JOIN tbl_options o ON sp.table_catalog=o.table_catalog AND sp.sync_id=o.sync_id
                AND sp.table_schema=o.table_schema AND sp.table_name=o.table_name   
            LEFT JOIN last_completed_schedule s ON sp.sync_id=s.sync_id AND c.table_id=s.table_id
            WHERE sp.partition_id IS NOT NULL 
            AND sp.partition_id NOT IN ('__NULL__','__UNPARTITIONED__')
            AND ((sp.last_modified_time >=s.last_schedule_dt) OR (s.last_schedule_dt IS NULL))
            AND 
                (
                    COALESCE(o.option_value,FALSE)=TRUE OR
                    (c.load_strategy IN ('PARTITION', 'TIME_INGESTION') AND s.last_schedule_dt IS NOT NULL) OR
                    (c.load_strategy='PARTITION' AND c.partition_type='RANGE') 
                )
        ),
        sorted_columns AS (
            SELECT sync_id,table_catalog,table_schema,table_name,
                    ARRAY_SORT(
                    ARRAY_AGG(st),
                    (left,right) -> case when left.pos < right.pos then -1 when left.pos > right.pos then 1 else 0 end
                    ) AS sorted_struct_array
            FROM (
                SELECT 
                    sync_id,table_catalog,table_schema,table_name,
                    struct(column_name,ordinal_position AS pos) as st
                FROM information_schema_columns
            ) as column_struct
            GROUP BY sync_id,table_catalog,table_schema,table_name
        ),
        tbl_columns AS (
            SELECT sync_id,table_catalog,table_schema,table_name,
                CONCAT_WS(',',TRANSFORM(
                    sorted_struct_array,
                    sorted_struct -> CONCAT('`', sorted_struct.column_name, '`')
                )) AS table_columns
            FROM sorted_columns
        ),
        user_config AS (
            SELECT 
                gcp.api.enable_bigquery_export AS enable_bigquery_export,
                gcp.api.use_standard_api AS enable_standard_api,
                gcp.api.auto_select AS auto_select_api
            FROM user_config_json
        )

        SELECT
            c.table_id,c.sync_id,c.load_strategy,c.load_type,
            c.priority,c.project_id,c.dataset,c.table_name,
            c.object_type,c.source_query,c.source_predicate,c.watermark_column,c.partition_column,
            COALESCE(c.is_partitioned,FALSE) AS is_partitioned,
            c.partition_grain,c.partition_range,c.partition_type,c.partition_data_type,
            c.workspace_id,c.workspace_name,c.lakehouse_type,c.lakehouse_id,c.lakehouse,
            c.lakehouse_schema,c.lakehouse_table_name,c.lakehouse_partition,c.use_lakehouse_schema,
            c.enforce_expiration,c.allow_schema_evolution,c.table_maintenance_enabled,c.table_maintenance_interval,
            c.flatten_table,c.flatten_inplace,
            COALESCE(uc.enable_bigquery_export, FALSE) AS enable_bigquery_export, 
            COALESCE(c.use_bigquery_export, FALSE) AS use_bigquery_export,
            COALESCE(uc.enable_standard_api, FALSE) AS enable_standard_api,
            COALESCE(c.use_standard_api, FALSE) AS use_standard_api,
            COALESCE(uc.auto_select_api, FALSE) AS auto_select_api,
            c.explode_arrays,
            c.primary_keys,
            p.partition_id,p.require_partition_filter,
            s.group_schedule_id,s.schedule_id,s.status AS sync_status,s.started,s.completed,s.mirror_file_index,
            h.max_watermark,h.last_schedule_dt,c.column_map,
            CASE WHEN (h.schedule_id IS NULL) THEN TRUE ELSE FALSE END AS initial_load,
            date_format(ts.last_modified_time,"yyyy-MM-dd'T'HH:mm:ssXXX") AS bq_tbl_last_modified,
            COALESCE(ts.total_rows,0) AS total_rows,
            COALESCE(ts.total_logical_mb,0) AS total_logical_mb,
            COALESCE(ts.size_priority,100) AS size_priority,
            tc.table_columns,
            COALESCE(h.mirror_file_index,1) AS mirror_file_index,
            COALESCE(xt.has_complex_types, FALSE) AS has_complex_types,
            COUNT(*) OVER(PARTITION BY c.sync_id,c.table_id) AS total_table_tasks,
            CONCAT('BQ_SYNC_', UUID()) AS temp_table_id
        FROM sync_configuration c
        JOIN sync_schedule s ON c.sync_id=s.sync_id AND c.table_id=s.table_id
        LEFT JOIN last_completed_schedule h ON c.sync_id=h.sync_id AND c.table_id=h.table_id 
        LEFT JOIN tbl_partitions p ON p.sync_id=c.sync_id AND p.table_catalog=c.project_id 
            AND p.table_schema=c.dataset AND p.table_name=c.table_name
        LEFT JOIN sync_schedule_telemetry t ON s.sync_id=t.sync_id AND s.table_id=t.table_id AND s.schedule_id=t.schedule_id 
            AND COALESCE(p.partition_id,'0')=COALESCE(t.partition_id,'0') AND t.status='COMPLETE'
        LEFT JOIN tbl_stats ts ON c.sync_id=ts.sync_id AND c.project_id=ts.table_catalog 
            AND c.dataset=ts.table_schema AND c.table_name=ts.table_name
        LEFT JOIN tbl_columns tc ON c.sync_id=tc.sync_id AND c.project_id=tc.table_catalog 
            AND c.dataset=tc.table_schema AND c.table_name=tc.table_name
        LEFT JOIN complex_types xt ON c.sync_id=xt.sync_id AND c.project_id=xt.table_catalog 
            AND c.dataset=xt.table_schema AND c.table_name=xt.table_name
        CROSS JOIN user_config uc
        WHERE s.status IN ('SCHEDULED','FAILED') AND c.enabled=TRUE AND t.schedule_id IS NULL
            AND c.sync_id='{cls.ID}'
            AND s.schedule_type='{schedule_type}'
        """
        df=cls.Context.sql(sql)
        df.cache()

        return df

    @classmethod
    @Metastore.Retry()
    def process_load_group_telemetry(cls,schedule_type:SyncScheduleType) -> None:
        """
        Processes the telemetry for the sync schedule load group and updates the sync_schedule table
        This is done by aggregating the telemetry data for the sync schedule and updating the sync_schedule table
        with the aggregated data
        This is used to update the status of the sync schedule and to ensure that the telemetry data is up to date
        """
        sql=f"""
        WITH schedule_telemetry_last AS (
            SELECT
                sync_id,table_id,schedule_id,status,
                MIN(started) OVER(PARTITION BY sync_id,schedule_id,table_id) AS started,
                MAX(completed) OVER(PARTITION BY sync_id,schedule_id,table_id) AS completed,
                MAX(max_watermark) OVER(PARTITION BY sync_id,schedule_id,table_id) AS max_watermark,

                SUM(CASE WHEN status IN ('COMPLETE', 'SKIPPED', 'NO_DATA') THEN 1 ELSE 0 END) 
                    OVER(PARTITION BY sync_id,schedule_id,table_id,started) AS completed_activities,
                SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) 
                    OVER(PARTITION BY sync_id,schedule_id,table_id,started) AS failed_activities,
                MAX(mirror_file_index) OVER(PARTITION BY sync_id,schedule_id,table_id) AS mirror_file_index,
                ROW_NUMBER()OVER(PARTITION BY sync_id,schedule_id,table_id ORDER BY started DESC) AS row_num
            FROM sync_schedule_telemetry
        ),      
        schedule_telemetry AS (
            SELECT
                sync_id,table_id,schedule_id,
                MAX(completed_activities) AS completed_activities,
                MAX(failed_activities) AS failed_activities,
                MIN(started) AS started,
                MAX(completed) AS completed,
                MAX(mirror_file_index) AS mirror_file_index
            FROM schedule_telemetry_last
            WHERE row_num=1
            GROUP BY sync_id,table_id,schedule_id
        ),
        schedule_watermarks AS (
            SELECT
                sync_id,schedule_id,table_id,max_watermark
            FROM schedule_telemetry_last
            WHERE max_watermark IS NOT NULL AND row_num=1
        ),
        schedule_results AS (
            SELECT
                s.sync_id,s.group_schedule_id,s.schedule_id,s.table_id,s.status,
                CASE WHEN t.failed_activities=0 THEN 'COMPLETE' ELSE 'FAILED' END AS result_status,
                t.started,t.completed,t.completed_activities,t.failed_activities,
                w.max_watermark,s.priority,
                COALESCE(t.mirror_file_index,1) AS mirror_file_index
            FROM sync_schedule s
            JOIN schedule_telemetry t ON s.sync_id=t.sync_id AND s.schedule_id=t.schedule_id AND s.table_id=t.table_id
            LEFT JOIN schedule_watermarks w ON s.sync_id=w.sync_id AND s.schedule_id=w.schedule_id AND s.table_id=w.table_id
            WHERE s.sync_id='{cls.ID}'
            AND s.schedule_type='{schedule_type}' 
            AND s.status IN ('SCHEDULED','FAILED')
        ) 

        MERGE INTO sync_schedule s
        USING schedule_results r
        ON s.sync_id=r.sync_id AND s.schedule_id=r.schedule_id AND s.table_id=r.table_id
        WHEN MATCHED THEN
            UPDATE SET
                s.status=r.result_status,
                s.started=r.started,
                s.completed=r.completed,
                s.completed_activities=r.completed_activities,
                s.failed_activities=r.failed_activities,
                s.max_watermark=r.max_watermark,
                s.mirror_file_index=r.mirror_file_index

        """

        cls.Context.sql(sql)

    @classmethod
    @Metastore.Retry()
    def commit_table_configuration(cls,schedule_type:SyncScheduleType) -> None:
        """
        Commits the table configuration for the sync schedule to the sync_configuration table
        This is done by updating the sync_state to 'COMMIT' for all tables that have been successfully processed
        in the sync_schedule table for the given schedule type
        This is used to mark the tables as ready for the next sync cycle
        and to ensure that the configuration is applied to the tables in the metastore
        
        This is typically called after the sync schedule has been processed and the tables have been updated
        and is used to finalize the configuration for the tables in the sync_configuration table
        """ 
        sql=f"""
        WITH committed AS (
            SELECT s.sync_id,s.project_id,s.dataset,s.table_name,MAX(s.started) as started
            FROM sync_schedule s
            JOIN sync_configuration c ON c.sync_id=s.sync_id AND c.project_id=s.project_id 
                AND c.dataset=s.dataset AND c.table_name=s.table_name
            WHERE s.status IN ('COMPLETE', 'SKIPPED', 'NO_DATA') AND s.sync_id='{cls.ID}'
                AND s.schedule_type='{schedule_type}' AND c.sync_state !='COMMIT'
            GROUP BY s.sync_id,s.project_id,s.dataset,s.table_name
        )

        MERGE INTO sync_configuration t
        USING committed c
        ON t.sync_id=c.sync_id AND t.project_id=c.project_id AND t.dataset=c.dataset AND t.table_name=c.table_name
        WHEN MATCHED AND t.sync_state='INIT' THEN
            UPDATE SET
                t.sync_state='COMMIT'
        """
        
        cls.Context.sql(sql)

    @classmethod  
    def create_userconfig_tables_proxy_view(cls) -> None:
        """
        Explodes the User Config table configuration into a temporary view
        """
        sql="""
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
                tbl.use_bigquery_export AS use_bigquery_export,
                tbl.use_standard_api AS use_standard_api,
                tbl.table_maintenance.enabled AS table_maintenance_enabled,
                tbl.table_maintenance.interval AS table_maintenance_interval,
                tbl.source_query,
                tbl.predicate AS source_predicate,
                tbl.load_strategy,
                tbl.load_type,               
                tbl.watermark.column as watermark_column,
                CAST(tbl.bq_partition.enabled AS BOOLEAN) as partition_enabled,
                tbl.bq_partition.type as partition_type,
                tbl.bq_partition.column as partition_column,
                tbl.bq_partition.partition_grain,
                tbl.bq_partition.partition_data_type,
                tbl.bq_partition.partition_range,
                tbl.lakehouse_target.lakehouse_id,
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

        cls.Context.sql(sql)
    
    @classmethod 
    def create_userconfig_tables_cols_proxy_view(cls) -> None:
        """
        Explodes the User Config table keys into a temporary view"""
        sql="""
            CREATE OR REPLACE TEMPORARY VIEW user_config_table_keys
            AS
            SELECT
                id AS sync_id,project_id,dataset,table_name,pkeys.column
            FROM (
                SELECT
                    id,tbl.project_id,tbl.dataset,tbl.table_name,EXPLODE_OUTER(tbl.keys) AS pkeys
                FROM (SELECT id,EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        
        cls.Context.sql(sql)
    
    @classmethod 
    def create_autodetect_view(cls) -> None:
        """
        Creates a temporary view that contains autodetected table metadata
        and user configuration for tables in the metastore
        This view is used to determine the metadata for tables that are not configured in the user config
        but are present in the metastore
        This view is used to populate the sync_configuration table
        and to determine the metadata for tables that are not configured in the user config
        but are present in the metastore
        """
        sql="""
        CREATE OR REPLACE TEMPORARY VIEW table_metadata_autodetect
        AS
        WITH primary_keys AS (    
            SELECT
                c.sync_id,c.table_catalog,c.table_schema,c.table_name,
                ARRAY_AGG(k.column_name) as key_cols
            FROM information_schema_table_constraints c
            JOIN information_schema_key_column_usage k ON k.sync_id=c.sync_id AND
                k.table_catalog=c.table_catalog AND k.table_schema=c.table_schema AND
                k.table_name=c.table_name AND k.constraint_name=c.constraint_name
            JOIN information_schema_columns n ON n.sync_id=k.sync_id AND n.table_catalog=k.table_catalog AND
                n.table_schema=k.table_schema AND n.table_name=k.table_name AND n.column_name=k.column_name
            JOIN data_type_map m ON n.data_type=m.data_type            
            WHERE c.constraint_type='PRIMARY KEY' AND m.is_watermark='YES'     
            GROUP BY c.sync_id,c.table_catalog,c.table_schema,c.table_name       
        ),
        partitions AS (
            SELECT
                sync_id,table_catalog,table_schema,table_name,count(*) as partition_count,
                avg(len(partition_id)) AS partition_id_len,
                sum(case when partition_id is NULL then 1 else 0 end) as null_partition_count
            FROM information_schema_partitions
            WHERE partition_id!='__NULL__'
            GROUP BY sync_id,table_catalog,table_schema,table_name
        ),
        partition_columns AS
        (
            SELECT
                sync_id,table_catalog,table_schema,table_name,column_name,c.data_type,m.partition_type AS partitioning_type
            FROM information_schema_columns c
            JOIN data_type_map m ON c.data_type=m.data_type
            WHERE is_partitioning_column='YES'
        ),
        range_partitions AS 
        (
            SELECT 
                sync_id,table_catalog,table_schema,table_name,
                SUBSTRING(gen,16,LEN(gen) - 16) AS partition_range
            FROM (
                SELECT 
                    sync_id,table_catalog,table_schema,table_name,
                    SUBSTRING(ddl,
                        LOCATE('GENERATE_ARRAY',ddl),
                        LOCATE(')',ddl,LOCATE('GENERATE_ARRAY',ddl)) - LOCATE('GENERATE_ARRAY',ddl) + 1) as gen   
                FROM information_schema_tables 
                WHERE CONTAINS(ddl,'GENERATE_ARRAY')
            )
        ),
        partition_cfg AS
        (
            SELECT
                p.*,
                CASE WHEN p.partition_count=1 AND p.null_partition_count=1 THEN FALSE ELSE TRUE END AS is_partitioned,
                c.column_name AS partition_col,
                c.data_type AS partition_data_type,
                c.partitioning_type,
                CASE WHEN (c.partitioning_type='TIME')
                    THEN 
                        CASE WHEN (partition_id_len=4) THEN 'YEAR'
                            WHEN (partition_id_len=6) THEN 'MONTH'
                            WHEN (partition_id_len=8) THEN 'DAY'
                            WHEN (partition_id_len=10) THEN 'HOUR'
                            ELSE NULL END
                    ELSE NULL END AS partitioning_strategy
            FROM partitions p
            LEFT JOIN partition_columns c ON p.sync_id=c.sync_id AND
                p.table_catalog=c.table_catalog AND p.table_schema=c.table_schema AND p.table_name=c.table_name
        ),
        tbl_options AS (
            SELECT sync_id,table_catalog,table_schema,table_name,CAST(option_value AS boolean) AS option_value
            FROM information_schema_table_options
            WHERE option_name='require_partition_filter'
        )

        SELECT 
            t.sync_id,t.table_catalog,t.table_schema,t.table_name,t.is_change_history_enabled,
            p.is_partitioned,p.partition_col,p.partition_data_type,p.partitioning_type,p.partitioning_strategy,
            r.partition_range,o.option_value as require_partition_filter,
            k.key_cols,
            CASE WHEN (k.key_cols IS NOT NULL AND SIZE(k.key_cols)=1) THEN ELEMENT_AT(k.key_cols,1) ELSE NULL END AS watermark_col
        FROM information_schema_tables t
        LEFT JOIN partition_cfg p ON t.sync_id=p.sync_id AND t.table_catalog=p.table_catalog AND
            t.table_schema=p.table_schema AND t.table_name=p.table_name
        LEFT JOIN range_partitions r ON t.sync_id=r.sync_id AND t.table_catalog=r.table_catalog AND 
            t.table_schema=r.table_schema AND t.table_name=r.table_name
        LEFT JOIN tbl_options o ON t.sync_id=o.sync_id AND t.table_catalog=o.table_catalog AND 
            t.table_schema=o.table_schema AND t.table_name=o.table_name 
        LEFT JOIN primary_keys k ON t.sync_id=k.sync_id AND t.table_catalog=k.table_catalog AND 
            t.table_schema=k.table_schema AND t.table_name=k.table_name 
        """

        cls.Context.sql(sql)

    @classmethod
    @Metastore.Retry()
    def auto_detect_profiles(cls) -> None: 
        """
        Creates the autodetect view and user config tables proxy views
        to allow for autodetection of table metadata and user configuration
        """       
        sql=f"""
        WITH default_config AS (
            SELECT 
                gcp.api.use_cdc AS use_gcp_cdc,
                gcp.api.use_standard_api as enable_standard_api,
                gcp.api.enable_bigquery_export AS enable_bigquery_export,
                COALESCE(autodiscover.autodetect,TRUE) AS autodetect,
                autodiscover.materialized_views.enabled as materialized_views_enabled,
                CASE WHEN autodiscover.materialized_views.enabled THEN
                    autodiscover.materialized_views.load_all ELSE FALSE END AS load_all_materialized_views,
                autodiscover.views.enabled as views_enabled,
                CASE WHEN autodiscover.views.enabled THEN 
                    autodiscover.views.load_all ELSE FALSE END AS load_all_views,
                autodiscover.tables.enabled as tables_enabled,
                CASE WHEN autodiscover.tables.enabled THEN
                    autodiscover.tables.load_all ELSE FALSE END AS load_all_tables,
                enable_data_expiration,
                fabric.workspace_id AS workspace_id,
                fabric.workspace_name AS workspace_name,
                fabric.target_type AS target_lakehouse_type,
                fabric.target_lakehouse_id AS target_lakehouse_id,
                fabric.target_lakehouse AS target_lakehouse,
                fabric.target_schema AS target_schema,
                COALESCE(fabric.enable_schemas,FALSE) AS enable_schemas,
                maintenance.interval AS maintenance_interval,
                maintenance.enabled AS maintenance_enabled
            FROM user_config_json
        ),
        table_defaults AS (
            SELECT 
                table_defaults.project_id AS default_project_id,
                table_defaults.dataset AS default_dataset,
                table_defaults.object_type AS default_object_type,
                table_defaults.priority AS default_priority,
                table_defaults.load_strategy AS default_load_strategy,
                table_defaults.load_type AS default_load_type,
                table_defaults.interval AS default_interval,
                table_defaults.enabled AS default_enabled,
                table_defaults.enforce_expiration AS default_enforce_expiration,
                table_defaults.allow_schema_evolution AS default_allow_schema_evolution,
                table_defaults.flatten_table AS default_flatten_table,
                table_defaults.flatten_inplace AS default_flatten_inplace,
                table_defaults.explode_arrays AS default_explode_arrays,
                table_defaults.use_bigquery_export AS default_use_bigquery_export,
                table_defaults.use_standard_api AS default_use_standard_api,
                table_defaults.table_maintenance.enabled AS default_table_maintenance_enabled,
                table_defaults.table_maintenance.interval AS default_table_maintenance_inteval
            FROM user_config_json
        ),
        user_config_keys AS (
            SELECT
                sync_id,project_id,dataset,table_name,
                ARRAY_AGG(column) as user_key_cols
            FROM user_config_table_keys
            GROUP BY sync_id,project_id,dataset,table_name
        ),
        autodetect AS (
            SELECT
                a.*,
                CASE WHEN size(k.user_key_cols) > 0 THEN k.user_key_cols
                    WHEN size(a.key_cols) > 0 THEN a.key_cols
                    ELSE NULL END AS tbl_key_cols
            FROM table_metadata_autodetect a
            LEFT JOIN user_config_keys k ON k.project_id=a.table_catalog AND k.dataset=a.table_schema
                AND k.table_name=a.table_name
        ),
        bq_metadata AS (
            SELECT
                sync_id,table_catalog,table_schema,table_name,
                'BASE_TABLE' AS object_type,
                tbl_key_cols,watermark_col,require_partition_filter,
                is_change_history_enabled,is_partitioned,partition_col,partition_data_type,
                partitioning_type,partitioning_strategy,partition_range                
            FROM autodetect a
            UNION ALL
            SELECT
                sync_id,table_catalog,table_schema,table_name,'MATERIALIZED_VIEW',
                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
            FROM information_schema_materialized_views
            UNION ALL
            SELECT
                sync_id,table_catalog,table_schema,table_name,'VIEW',
                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
            FROM information_schema_views
        ),
        bq_objects AS (
            SELECT
                m.sync_id,d.autodetect,d.enable_data_expiration,FALSE,
                d.workspace_id,d.workspace_name,d.target_lakehouse_type,d.target_lakehouse_id,
                d.target_lakehouse,d.target_schema,d.enable_schemas,
                d.maintenance_interval,d.maintenance_enabled,
                d.enable_bigquery_export,d.enable_standard_api,
                CASE WHEN object_type='BASE_TABLE' THEN d.load_all_tables
                    WHEN object_type='MATERIALIZED_VIEW' THEN d.load_all_materialized_views
                    WHEN object_type='VIEW' THEN d.load_all_views
                    ELSE FALSE END AS load_all,
                CASE WHEN object_type='BASE_TABLE' THEN d.tables_enabled
                    WHEN object_type='MATERIALIZED_VIEW' THEN d.materialized_views_enabled
                    WHEN object_type='VIEW' THEN d.views_enabled
                    ELSE FALSE END AS type_enabled,
                CASE WHEN object_type='BASE_TABLE' THEN d.use_gcp_cdc ELSE FALSE END AS use_gcp_cdc,
                m.*
            FROM bq_metadata m
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
                CASE WHEN p.load_all THEN COALESCE(CAST(u.enabled AS BOOLEAN),
                        CAST(x.default_enabled AS BOOLEAN), TRUE) 
                    ELSE COALESCE(CAST(u.enabled AS BOOLEAN),
                        CAST(x.default_enabled AS BOOLEAN), FALSE) END AS enabled,               
                p.workspace_id,
                p.workspace_name,
                p.target_lakehouse_type AS lakehouse_type,
                CASE WHEN (u.lakehouse_id IS NULL) THEN p.target_lakehouse_id ELSE u.lakehouse_id END AS lakehouse_id,
                CASE WHEN (u.lakehouse IS NULL) THEN p.target_lakehouse ELSE u.lakehouse END AS lakehouse,               
                CASE WHEN p.enable_schemas THEN
                    COALESCE(u.lakehouse_schema,p.target_schema,p.table_schema)
                    ELSE NULL END AS lakehouse_schema,
                CASE WHEN (COALESCE(u.lakehouse_target_table,NULL) IS NULL) 
                    THEN p.table_name ELSE u.lakehouse_target_table END AS lakehouse_table_name,
                COALESCE(u.lakehouse_partition,NULL) AS lakehouse_partition,
                COALESCE(u.source_query,NULL) AS source_query,
                COALESCE(u.source_predicate,NULL) AS source_predicate,
                COALESCE(u.priority, x.default_priority,'100') AS priority,
                COALESCE(u.load_strategy, x.default_load_strategy, 
                    CASE WHEN (p.use_gcp_cdc AND p.object_type='BASE_TABLE' 
                            AND COALESCE(p.is_change_history_enabled,'NO')='YES' AND NOT COALESCE(p.require_partition_filter,FALSE)
                            AND SIZE(p.tbl_key_cols) > 0
                            AND u.source_query IS NULL
                            AND u.source_predicate IS NULL) THEN 'CDC'
                        WHEN (COALESCE(u.watermark_column,p.watermark_col) IS NOT NULL) THEN 'WATERMARK' 
                        WHEN (COALESCE(u.partition_enabled,p.is_partitioned,FALSE)=TRUE) 
                            AND COALESCE(u.partition_column,p.partition_col,'') NOT IN ('_PARTITIONTIME','_PARTITIONDATE') THEN 'PARTITION'
                        WHEN (COALESCE(u.partition_enabled,p.is_partitioned,FALSE)=TRUE) 
                            AND COALESCE(u.partition_column,p.partition_col,'') IN ('_PARTITIONTIME','_PARTITIONDATE') THEN 'TIME_INGESTION'
                        WHEN (p.use_gcp_cdc AND p.object_type='BASE_TABLE' 
                            AND NOT COALESCE(p.require_partition_filter,FALSE)
                            AND u.source_query IS NULL
                            AND u.source_predicate IS NULL) THEN 'CDC_APPEND'
                        ELSE 'FULL' END) AS load_strategy,
                COALESCE(u.load_type, x.default_load_type, 
                    CASE WHEN (p.use_gcp_cdc AND p.object_type='BASE_TABLE' 
                            AND COALESCE(p.is_change_history_enabled,'NO')='YES' AND NOT COALESCE(p.require_partition_filter,FALSE)
                            AND SIZE(p.tbl_key_cols) > 0) THEN 'MERGE'
                        WHEN (COALESCE(u.watermark_column,p.watermark_col) IS NOT NULL) THEN 'APPEND' 
                        WHEN (p.use_gcp_cdc AND p.object_type='BASE_TABLE' 
                            AND NOT COALESCE(p.require_partition_filter,FALSE)) THEN 'APPEND'
                        ELSE 'OVERWRITE' END) AS load_type,
                COALESCE(u.interval,x.default_interval,'AUTO') AS interval,
                CASE WHEN (p.object_type='BASE_TABLE') THEN p.tbl_key_cols 
                    ELSE k.user_key_cols END AS primary_keys,
                COALESCE(CAST(u.partition_enabled AS BOOLEAN), p.is_partitioned,FALSE) AS is_partitioned,
                COALESCE(u.partition_column,p.partition_col,NULL) AS partition_column,
                COALESCE(u.partition_type,p.partitioning_type,NULL) AS partition_type,
                COALESCE(u.partition_grain,p.partitioning_strategy,NULL) AS partition_grain,
                COALESCE(u.partition_data_type,p.partition_data_type,NULL) AS partition_data_type,
                COALESCE(u.partition_range,p.partition_range,NULL) AS partition_range,
                COALESCE(u.watermark_column,p.watermark_col,NULL) AS watermark_column,
                p.autodetect,
                p.enable_schemas AS use_lakehouse_schema,
                CASE WHEN (p.enable_data_expiration) THEN COALESCE(CAST(u.enforce_expiration AS BOOLEAN),
                    CAST(x.default_enforce_expiration AS BOOLEAN),FALSE) ELSE FALSE END AS enforce_expiration,
                COALESCE(CAST(u.allow_schema_evolution AS BOOLEAN),
                    CAST(x.default_allow_schema_evolution AS BOOLEAN),FALSE) AS allow_schema_evolution,
                COALESCE(CAST(u.table_maintenance_enabled AS BOOLEAN),
                    CAST(x.default_table_maintenance_enabled AS BOOLEAN),p.maintenance_enabled,FALSE) AS table_maintenance_enabled,
                COALESCE(u.table_maintenance_interval,x.default_table_maintenance_inteval,p.maintenance_interval,'AUTO') AS table_maintenance_interval,
                COALESCE(CAST(u.flatten_table AS BOOLEAN),
                    CAST(x.default_flatten_table AS BOOLEAN),FALSE) AS flatten_table,
                COALESCE(CAST(u.flatten_inplace AS BOOLEAN),
                    CAST(x.default_flatten_inplace AS BOOLEAN),TRUE) AS flatten_inplace,
                COALESCE(CAST(u.explode_arrays AS BOOLEAN),
                    CAST(x.default_explode_arrays AS BOOLEAN),FALSE) AS explode_arrays,
                CASE WHEN p.enable_bigquery_export THEN
                    COALESCE(CAST(u.use_bigquery_export AS BOOLEAN), CAST(x.default_use_bigquery_export AS BOOLEAN),FALSE) 
                    ELSE FALSE END AS use_bigquery_export,                
                CASE WHEN p.enable_standard_api THEN
                    COALESCE(CAST(u.use_standard_api AS BOOLEAN), CAST(x.default_use_standard_api AS BOOLEAN), FALSE) 
                    ELSE FALSE END AS use_standard_api,
                COALESCE(u.column_map,NULL) AS column_map,
                CASE WHEN u.table_name IS NULL THEN FALSE ELSE TRUE END AS config_override,
                'INIT' AS sync_state,
                '{cls.UserConfigPath}' AS config_path,
                CURRENT_TIMESTAMP() as created_dt,
                NULL as last_updated_dt
            FROM bq_objects p
            LEFT JOIN user_config_tables u ON 
                p.table_catalog=u.project_id AND p.table_schema=u.dataset AND
                p.table_name=u.table_name AND p.object_type=u.object_type
            LEFT JOIN user_config_keys k ON
                k.project_id=p.table_catalog AND k.dataset=p.table_schema AND k.table_name=p.table_name
            CROSS JOIN table_defaults x
            WHERE CASE WHEN (p.load_all=TRUE) THEN TRUE ELSE
                CASE WHEN (u.table_name IS NULL) THEN FALSE ELSE TRUE END END=TRUE       
                AND p.type_enabled=TRUE  
                AND p.sync_id='{cls.ID}'
        )

        MERGE INTO sync_configuration t
        USING source s
        ON t.sync_id=s.sync_id AND t.project_id=s.project_id AND t.dataset=s.dataset AND t.table_name=s.table_name
            AND t.lakehouse=s.lakehouse AND t.lakehouse_schema=s.lakehouse_schema AND t.lakehouse_table_name=s.lakehouse_table_name
        WHEN MATCHED AND t.sync_state <> 'INIT' THEN
            UPDATE SET
                t.enabled=s.enabled,
                t.interval=s.interval,
                t.priority=s.priority,
                t.workspace_id=s.workspace_id,
                t.workspace_name=s.workspace_name,
                t.lakehouse_type=s.lakehouse_type,
                t.lakehouse_id=s.lakehouse_id,
                t.lakehouse=s.lakehouse,
                t.lakehouse_schema=s.lakehouse_schema,
                t.lakehouse_table_name=s.lakehouse_table_name,
                t.lakehouse_partition=s.lakehouse_partition,
                t.source_query=s.source_query,
                t.source_predicate=s.source_predicate,
                t.enforce_expiration=s.enforce_expiration,
                t.column_map=s.column_map,
                t.allow_schema_evolution=s.allow_schema_evolution,
                t.table_maintenance_enabled=s.table_maintenance_enabled,
                t.table_maintenance_interval=s.table_maintenance_interval,
                t.use_bigquery_export=s.use_bigquery_export,
                t.use_standard_api=s.use_standard_api,
                t.config_path=s.config_path,
                t.last_updated_dt=CURRENT_TIMESTAMP()
        WHEN MATCHED AND t.sync_state='INIT' THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.sync_id='{cls.ID}' AND t.enabled=TRUE THEN
            UPDATE SET
                t.enabled=FALSE
        """

        cls.Context.sql(sql)

    @classmethod         
    def ensure_schemas(cls,workspace_name:str) -> None:
        """
        Ensures that the lakehouse schemas defined in the sync configuration exist in the specified workspace.
        This method retrieves the lakehouse and schema names from the `sync_configuration` table where the `use_lakehouse_schema` is set to TRUE and creates the schemas in the specified workspace if they do not already exist.
        It is typically called after the initial load of metadata to ensure that all necessary schemas are created before any data synchronization occurs.
        The method uses the `sync_id` to filter the configurations and only processes those that are enabled.
        It constructs the schema names in the format `workspace_name.lakehouse.lakehouse_schema` and creates them if they do not exist.
        This is used to ensure that the lakehouse schemas are ready for data synchronization and that the environment is correctly set up for the sync process.
        """

        sql=f"""
            SELECT DISTINCT lakehouse,lakehouse_schema FROM sync_configuration
            WHERE enabled=TRUE AND use_lakehouse_schema=TRUE
            AND sync_id='{cls.ID}'
            """

        df=cls.Context.sql(sql)    
        for schema in [f"`{r['lakehouse']}`.`{r['lakehouse_schema']}`" for r in df.collect()]:
            cls.Context.sql(f"CREATE SCHEMA IF NOT EXISTS `{workspace_name}`.{schema}")
    
    @classmethod
    @Metastore.Retry()
    def sync_retention_config(cls) -> None:
        """
        Synchronizes the data expiration configuration for tables based on the partition expiration settings defined in the BigQuery metadata.
        This method retrieves the partition expiration settings from the `information_schema_table_options` and `information_schema_partitions` tables, calculates the expiration date for each partition, and merges this information into the `sync_data_expiration` table. It ensures that the expiration settings are up-to-date and consistent with the defined retention policies.
        The method also handles the case where a table has a global expiration timestamp set, which applies to all partitions of the table.
        The expiration settings are used to manage data retention and ensure compliance with data governance requirements.
        This method is typically called after the initial load of metadata to ensure that the expiration settings are correctly applied to the tables in the sync configuration.
        It uses a Common Table Expression (CTE) to gather the necessary configuration details and then performs a MERGE operation to update the `sync_data_expiration` table accordingly.
        """
        sql=f"""
        WITH cfg AS (
            SELECT table_catalog,table_schema,table_name,CAST(option_value AS FLOAT) AS expiration_days
            FROM information_schema_table_options
            WHERE option_name='partition_expiration_days'
        ),
        parts AS (
            SELECT
                t.table_catalog,t.table_schema,t.table_name,p.partition_id,
                CAST(ROUND(x.expiration_days * 24,0) AS INT) AS expiration_hours,
                CASE WHEN (LEN(p.partition_id)=4) THEN TO_TIMESTAMP(p.partition_id,'yyyy') + INTERVAL 1 YEAR
                    WHEN (LEN(p.partition_id)=6) THEN TO_TIMESTAMP(p.partition_id,'yyyyMM') + INTERVAL 1 MONTH
                    WHEN (LEN(p.partition_id)=8) THEN TO_TIMESTAMP(p.partition_id,'yyyyMMdd') + INTERVAL 24 HOURS
                    WHEN (LEN(p.partition_id)=10) THEN TO_TIMESTAMP(p.partition_id,'yyyyMMddHH') + INTERVAL 1 HOUR
                ELSE NULL END AS partition_boundary
            FROM information_schema_tables t 
            JOIN information_schema_partitions p ON t.table_catalog=p.table_catalog AND 
                t.table_schema=p.table_schema AND t.table_name=p.table_name
            JOIN cfg x ON t.table_catalog=x.table_catalog 
                AND t.table_schema=x.table_schema AND t.table_name=x.table_name
            WHERE p.partition_id IS NOT NULL
        ),
        src AS (
            SELECT
                table_catalog,table_schema,table_name,partition_id,
                partition_boundary + MAKE_INTERVAL(0,0,0,0,expiration_hours,0,0) as expiration    
            FROM parts
            UNION ALL
            SELECT table_catalog,table_schema,table_name,null as partition_id,
                CAST(REPLACE(REPLACE(option_value,'TIMESTAMP'),'"') AS TIMESTAMP) as expiration
            FROM information_schema_table_options
            WHERE option_name='expiration_timestamp'
        ),
        expiration AS (
            SELECT c.sync_id,s.table_catalog,s.table_schema,s.table_name,s.partition_id,s.expiration
            FROM src s 
            JOIN sync_configuration c ON s.table_catalog=c.project_id
                AND s.table_schema=c.dataset AND s.table_name=c.table_name
                AND c.enforce_expiration=TRUE
            WHERE c.sync_id='{cls.ID}'
        )

        MERGE INTO sync_data_expiration t
        USING expiration s
        ON t.sync_id=s.sync_id AND t.table_catalog=s.table_catalog 
            AND t.table_schema=s.table_schema AND t.table_name=s.table_name
            AND t.partition_id=s.partition_id
        WHEN MATCHED AND t.expiration <> s.expiration THEN
            UPDATE SET
                t.expiration=s.expiration
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """
        
        cls.Context.sql(sql)
    
    @classmethod 
    def get_bq_retention_policy(cls) -> DataFrame:
        """
        Retrieves the BigQuery retention policy for tables that have an expiration set.
        This method queries the `sync_data_expiration` table to find tables that have an expiration date set in the past, indicating that they need to be processed for data retention or deletion.
        It joins with the `sync_configuration` table to get the necessary configuration details for each table, such as lakehouse, schema, and partition information.
        The result includes the lakehouse, schema, table name, whether to use the lakehouse schema, partitioning details, and the expiration date for each table that requires action.
        This is used to manage data retention policies and ensure compliance with data governance requirements.
        """
        sql=f"""
            SELECT c.lakehouse,c.lakehouse_schema,c.lakehouse_table_name,c.use_lakehouse_schema,
                c.is_partitioned,c.partition_column,c.partition_type,c.partition_grain,
                e.partition_id
            FROM sync_data_expiration e
            JOIN sync_configuration c ON e.sync_id=c.sync_id AND e.table_catalog=c.project_id
                AND e.table_schema=c.dataset AND e.table_name=c.table_name AND c.enforce_expiration=TRUE
            WHERE e.expiration < current_timestamp()
            AND e.sync_id='{cls.ID}'
        """

        return cls.Context.sql(sql)
    
    @classmethod
    @Metastore.Retry()
    def update_maintenance_config(cls) -> None:
        """
        Updates the sync_configuration table with the maintenance configuration based on user settings and defaults.
        This method merges the user-defined table maintenance settings with the default settings and the base configuration to ensure that all tables have the correct maintenance settings applied.
        It uses a Common Table Expression (CTE) to gather the necessary configuration details and then performs a MERGE operation to update the sync_configuration table accordingly.
        This is used to ensure that the table maintenance settings are consistent across the sync configuration and that any changes in user settings are reflected in the sync configuration.
        """
        sql=f"""
            WITH tbl_config AS (
                SELECT 
                    project_id,dataset,table_name,
                    table_maintenance_enabled AS tbl_config_maintenance_enabled,
                    CASE WHEN (table_maintenance_interval='AUTO') THEN NULL ELSE table_maintenance_interval END AS tbl_config_maintenance_interval
                FROM user_config_tables
            ),
            base_config AS (
                SELECT
                    CASE WHEN (maintenance.interval='AUTO') 
                        THEN NULL ELSE maintenance.interval END AS maintenance_interval,
                    table_defaults.table_maintenance.enabled AS default_enabled,
                    CASE WHEN (table_defaults.table_maintenance.interval='AUTO') 
                        THEN NULL ELSE table_defaults.table_maintenance.interval END AS default_interval
                FROM user_config_json
            ),
            sync_config AS (
                SELECT
                    c.sync_id,c.project_id,c.dataset,c.table_name,
                    COALESCE(
                        COALESCE(u.tbl_config_maintenance_enabled,b.default_enabled),
                            c.table_maintenance_enabled) AS table_maintenance_enabled,
                    COALESCE(
                        COALESCE(COALESCE(u.tbl_config_maintenance_interval,b.default_interval),
                            b.maintenance_interval),c.table_maintenance_interval) AS table_maintenance_interval
                FROM sync_configuration c
                LEFT JOIN tbl_config u ON c.project_id=u.project_id AND
                    c.dataset=u.dataset AND c.table_name=u.table_name
                CROSS JOIN base_config b
                WHERE c.sync_id='{cls.ID}'
            )

            MERGE INTO sync_configuration t
            USING sync_config s
            ON t.sync_id=s.sync_id AND t.project_id=s.project_id AND t.dataset=s.dataset AND t.table_name=s.table_name
            WHEN MATCHED AND (t.table_maintenance_enabled <> s.table_maintenance_enabled OR 
                t.table_maintenance_interval <> s.table_maintenance_interval) THEN
                UPDATE SET
                    t.table_maintenance_enabled=s.table_maintenance_enabled,
                    t.table_maintenance_interval=s.table_maintenance_interval,
                    t.last_updated_dt=CURRENT_TIMESTAMP()
        """

        cls.Context.sql(sql)
    
    @classmethod 
    def create_maintenance_views(cls) -> None:
        """
        Creates a temporary view for maintenance scheduling based on the sync configuration and table partitions.
        This view aggregates information about table partitions, their last maintenance dates, and the next scheduled maintenance dates.
        The view is used to determine which tables require maintenance actions such as optimization and vacuuming.
        """
        sql=f"""
        CREATE OR REPLACE TEMPORARY VIEW maintenance_snap
            AS
            WITH 
                base_config AS (
                    SELECT
                        maintenance.enabled AS maintenance_enabled,maintenance.strategy AS maintenance_strategy,
                        maintenance.track_history,maintenance.retention_hours,
                        maintenance.thresholds.rows_changed,maintenance.thresholds.table_size_growth,
                        maintenance.thresholds.file_fragmentation,maintenance.thresholds.out_of_scope_size
                    FROM user_config_json
                ),
                sync_config AS (
                    SELECT
                        LOWER(c.sync_id) AS sync_id,LOWER(c.table_id) AS table_id,
                        LOWER(c.project_id) AS project_id,LOWER(c.dataset) AS dataset,
                        LOWER(c.table_name) AS table_name,
                        c.table_maintenance_interval AS last_maintenance_interval,c.table_maintenance_enabled,
                        CASE WHEN (c.table_maintenance_interval='DAY') THEN 1
                            WHEN (c.table_maintenance_interval='WEEK') THEN 7
                            WHEN (c.table_maintenance_interval='MONTH') THEN 30
                            WHEN (c.table_maintenance_interval='QUARTER') THEN 90
                            WHEN (c.table_maintenance_interval='YEAR') THEN 365
                            ELSE 0 END AS maintenance_interval_days,
                        LOWER(c.lakehouse) AS lakehouse,LOWER(c.lakehouse_schema) AS lakehouse_schema,
                        LOWER(c.lakehouse_table_name) AS lakehouse_table_name,
                        c.partition_column,c.partition_type,c.partition_grain,
                        b.*
                    FROM sync_configuration c
                    CROSS JOIN base_config b
                    WHERE c.sync_state='COMMIT'                    
                ),
                tbl_partitions AS (
                    SELECT
                        LOWER(t.table_catalog) AS table_catalog,LOWER(t.table_schema) AS table_schema,
                        LOWER(t.table_name) AS table_name,p.last_modified_time,
                        COALESCE(p.partition_id,'') AS partition_id,p.total_rows,p.total_logical_bytes,
                        m.last_maintenance,m.last_optimize,m.last_vacuum,
                        m.row_count AS last_row_count,
                        m.table_partition_size AS last_table_partition_size,
                        SUM(
                            CASE 
                                WHEN (m.last_maintenance IS NULL) THEN 0 
                                WHEN (m.last_maintenance < p.last_modified_time) THEN 0
                                ELSE 1 END
                            ) OVER (PARTITION BY t.table_catalog,t.table_schema,t.table_name) AS partition_maintenance,
                        COUNT(*) OVER (PARTITION BY t.table_catalog,t.table_schema,t.table_name) AS partition_count
                    FROM information_schema_tables t
                    JOIN information_schema_partitions p ON t.table_catalog=p.table_catalog 
                        AND t.table_schema=p.table_schema AND t.table_name=p.table_name
                    LEFT JOIN sync_maintenance m ON t.table_catalog=m.project_id 
                        AND t.table_schema=m.dataset AND t.table_name=m.table_name
                        AND COALESCE(p.partition_id,'')=m.partition_id
                        AND m.sync_id='{cls.ID}'
                    WHERE COALESCE(p.partition_id,'') !='__NULL__'
                )

                SELECT
                    m.*,p.partition_id,p.last_modified_time,
                    p.total_rows AS row_count,p.total_logical_bytes AS table_partition_size,
                    p.last_maintenance,p.last_optimize,p.last_vacuum,
                    p.last_row_count,p.last_table_partition_size,
                    ROW_NUMBER() OVER(PARTITION BY p.table_catalog,p.table_schema,p.table_name 
                        ORDER BY p.partition_id) AS partition_index,
                    CASE WHEN ((p.partition_maintenance/p.partition_count) <=0.5f) THEN TRUE
                        ELSE FALSE END AS full_table_maintenance,
                    CASE WHEN last_maintenance_interval='AUTO' THEN NULL  
                        WHEN p.last_maintenance IS NULL THEN CURRENT_DATE()
                        ELSE DATE_ADD(p.last_maintenance,m.maintenance_interval_days) END AS next_scheduled_maintenance,
                    CASE WHEN (p.last_maintenance IS NULL OR m.maintenance_strategy='INTELLIGENT' 
                        OR last_maintenance_interval='AUTO') THEN CURRENT_DATE()
                        ELSE DATE_ADD(p.last_maintenance,m.maintenance_interval_days) END AS next_maintenance
                FROM tbl_partitions p
                JOIN sync_config m ON p.table_catalog=m.project_id AND p.table_schema=m.dataset AND p.table_name=m.table_name
                WHERE m.table_maintenance_enabled=TRUE
                    AND m.maintenance_enabled=TRUE
                    AND m.sync_id='{cls.ID}'
        """

        cls.Context.sql(sql)

    @classmethod 
    def get_scheduled_maintenance_schedule(cls) -> DataFrame:
        """
        Retrieves the scheduled maintenance schedule for tables in the lakehouse.
        This method constructs a SQL query that identifies tables that require maintenance based on their last modified time and last maintenance date.
        Returns: DataFrame containing the maintenance schedule with details such as run optimization and vacuum flags, next maintenance date, and partition information.
        """
        sql=f"""
            WITH scheduled AS (
                SELECT 
                    *,
                    CURRENT_TIMESTAMP() AS created_dt,
                    CASE WHEN (last_modified_time IS NULL OR last_maintenance IS NULL) THEN TRUE
                        WHEN  (last_modified_time >=last_maintenance) THEN TRUE ELSE
                            FALSE END AS run_optimize,
                    CASE WHEN (last_modified_time IS NULL OR last_maintenance IS NULL) THEN TRUE
                        WHEN  (last_modified_time >=last_maintenance) THEN TRUE ELSE
                            FALSE END AS run_vacuum,
                    COUNT(*) OVER(PARTITION BY sync_id,lakehouse,lakehouse_schema,lakehouse_table_name) as table_parts
                FROM maintenance_snap
            )

            SELECT 
                *
            FROM scheduled
            WHERE sync_id='{cls.ID}' 
            AND maintenance_strategy='SCHEDULED' 
            AND (
                next_maintenance <=CURRENT_DATE() OR 
                (full_table_maintenance=TRUE AND table_parts > 1)
            )   
        """

        return cls.Context.sql(sql)

    @classmethod 
    def get_inventory_based_maintenance_schedule(cls) -> DataFrame:
        """
        Retrieves the maintenance schedule based on the inventory of table partitions and files.
        This method constructs a SQL query that aggregates partition and file information to determine
        the maintenance requirements for each table partition in the lakehouse.
        Returns: DataFrame containing the maintenance schedule with details such as rows changed ratio,
        """
        sql=f"""
            WITH lh_partitions AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,delta_partition,
                        POSEXPLODE(SPLIT(delta_partition,'/'))
                    FROM storage_inventory_table_partitions
                    WHERE sync_id='{cls.ID}' AND delta_partition !='<default>'
                ),
                lh_partition_parts AS (
                    SELECT
                        *,
                        if(INSTR(col,'=') > 0,
                            SUBSTRING(col,INSTR(col,'=')+1,LEN(col) - INSTR(col,'=')),
                            NULL) as part
                    FROM lh_partitions
                ),
                lh_partition_struct AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,delta_partition,
                        STRUCT(pos,part) as pt
                    FROM lh_partition_parts
                    WHERE part IS NOT NULL
                ),
                lh_sorted_struct AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,delta_partition,
                        ARRAY_SORT(
                            ARRAY_AGG(pt),
                            (left,right) -> case left.pos < right.pos -1 when left.pos > right.pos then 1 else 0 end
                        ) AS sorted_struct_array
                    FROM lh_partition_struct
                    GROUP BY lakehouse,lakehouse_schema,lakehouse_table,delta_partition
                ),
                lh_partition_map AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,delta_partition,
                        CONCAT_WS('',TRANSFORM(
                            sorted_struct_array,
                            sorted_struct -> sorted_struct.part
                        )) AS partition_id
                    FROM lh_sorted_struct
                ),
                inventory_files AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,
                        substring(data_file,1,len(data_file) - locate('/',reverse(data_file))) as delta_partition,
                        file_info['file_size'] as file_size 
                    FROM  storage_inventory_table_files
                    WHERE sync_id='{cls.ID}' AND file_info['operation']='ADD'
                ),
                table_files AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,
                        CASE WHEN (CONTAINS(delta_partition,'.parquet')) THEN '<default>' ELSE delta_partition END AS delta_partition,
                        CASE WHEN ((file_size/(1000*1000*1000)) < 1) THEN 1 ELSE 0 END as partial_file
                    FROM inventory_files
                ),
                partition_files AS (
                    SELECT
                        lakehouse,lakehouse_schema,lakehouse_table,delta_partition,
                        SUM(partial_file) AS partial_files,
                        COUNT(*) AS table_partition_file_count
                    FROM table_files
                    GROUP BY lakehouse,lakehouse_schema,lakehouse_table,delta_partition
                ),
                partition_inventory AS (
                    SELECT p.*,COALESCE(m.partition_id,p.delta_partition) AS partition_id,
                    f.partial_files,f.table_partition_file_count
                    FROM storage_inventory_table_partitions p 
                    JOIN partition_files f ON p.lakehouse=f.lakehouse AND p.lakehouse_schema=f.lakehouse_schema
                        AND p.lakehouse_table=f.lakehouse_table AND p.delta_partition=f.delta_partition
                    LEFT JOIN lh_partition_map m ON p.lakehouse=m.lakehouse AND p.lakehouse_schema=m.lakehouse_schema
                        AND p.lakehouse_table=m.lakehouse_table AND p.delta_partition=m.delta_partition
                    WHERE p.sync_id='{cls.ID}' 
                ),
                inventory_maintenance_snap AS (
                    SELECT
                        COALESCE(m.last_row_count/p.row_count,1) AS rows_changed_ratio,
                        COALESCE(p.removed_file_size/p.file_size,0) AS out_of_scope_size_ratio,
                        COALESCE(m.last_table_partition_size/p.file_size,1) AS table_size_growth_ratio,

                        CASE WHEN (p.table_partition_file_count <=1) THEN 0
                            ELSE p.partial_files/p.table_partition_file_count END AS file_fragmentation_ratio,

                        m.sync_id,m.project_id,m.dataset,m.table_id,m.table_name,m.partition_id,m.lakehouse,m.lakehouse_schema,
                        m.lakehouse_table_name,p.row_count,p.file_size AS table_partition_size,
                        m.track_history,m.retention_hours,m.rows_changed,m.table_size_growth,m.file_fragmentation,m.out_of_scope_size,
                        m.last_maintenance_interval,m.maintenance_strategy,m.next_scheduled_maintenance,
                        m.next_maintenance,m.last_optimize,m.last_vacuum,m.full_table_maintenance,
                        m.partition_index,m.partition_type,m.partition_grain,m.partition_column,
                        CURRENT_TIMESTAMP AS created_dt
                    FROM maintenance_snap m
                    JOIN partition_inventory p ON p.sync_id=m.sync_id AND p.lakehouse=m.lakehouse
                        AND p.lakehouse_schema=COALESCE(m.lakehouse_schema,'')
                        AND p.lakehouse_table=m.lakehouse_table_name
                        AND p.partition_id=CASE WHEN (m.partition_id='') THEN '<default>' ELSE m.partition_id END
                ),
                inventory_maintenance AS(
                    SELECT
                        *,
                        CASE WHEN (next_scheduled_maintenance <=CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_scheduled_maint,
                        CASE WHEN ((rows_changed_ratio > rows_changed) OR 
                            (table_size_growth_ratio > table_size_growth) OR 
                            (file_fragmentation_ratio > file_fragmentation)) THEN TRUE
                            ELSE FALSE END AS run_optimize,
                        CASE WHEN (out_of_scope_size_ratio > out_of_scope_size) THEN TRUE ELSE FALSE END as run_vacuum
                    FROM inventory_maintenance_snap            
                )

            SELECT
                rows_changed_ratio,out_of_scope_size_ratio,table_size_growth_ratio,file_fragmentation_ratio,
                sync_id,project_id,dataset,table_id,table_name,partition_id,lakehouse,lakehouse_schema,
                lakehouse_table_name,row_count,table_partition_size,maintenance_strategy AS last_maintenance_type,
                track_history,retention_hours,rows_changed,table_size_growth,file_fragmentation,out_of_scope_size,
                last_maintenance_interval,maintenance_strategy,next_scheduled_maintenance,
                next_maintenance,last_optimize,last_vacuum,full_table_maintenance,
                partition_index,partition_type,partition_grain,partition_column,created_dt,
                CASE WHEN (is_scheduled_maint=TRUE) THEN TRUE ELSE run_optimize END AS run_optimize,
                CASE WHEN (is_scheduled_maint=TRUE) THEN TRUE ELSE run_vacuum END AS run_vacuum
            FROM inventory_maintenance
            WHERE sync_id='{cls.ID}' AND maintenance_strategy='INTELLIGENT'
            AND next_maintenance <=CURRENT_DATE()
            AND ((run_optimize=TRUE OR run_vacuum=TRUE) OR (is_scheduled_maint=TRUE))
        """

        return cls.Context.sql(sql)

    @classmethod
    @Metastore.Retry()
    def update_maintenance_schedule(cls,schedules:List[MaintenanceSchedule]) -> None:
        """
        Update the maintenance schedule in the metastore based on the provided schedules.
        This method will merge the provided schedules into the `sync_maintenance` table, ensuring that the maintenance
        information is up-to-date and reflects the current state of the system.
        Parameters: List of MaintenanceSchedule objects containing the maintenance details to be updated.
        Returns: None
        """
        #keys=['table_maintenance_interval','strategy','next_maintenance','run_optimize','run_vacuum']
        data=[]

        if schedules:
            for s in schedules: data.append(s.model_dump())

            maint_tbl=DeltaTable.forName(cls.Context,"sync_maintenance")
            df=cls.Context.createDataFrame(data=data,schema=FabricMetastoreSchema.sync_maintenance)

            maint_tbl.alias('m') \
                .merge(
                    source=df.alias('u'),
                    condition="m.sync_id=u.sync_id AND m.table_id=u.table_id AND m.partition_id=u.partition_id"
                ) \
                .whenNotMatchedInsertAll() \
                .whenMatchedUpdate(set =
                {
                    "row_count": "u.row_count",
                    "table_partition_size": "u.table_partition_size",
                    "last_maintenance_type": "u.last_maintenance_type",
                    "last_maintenance_interval": "u.last_maintenance_interval",
                    "last_maintenance": "u.last_maintenance",
                    "last_optimize": "u.last_optimize",
                    "last_vacuum": "u.last_vacuum",
                    "last_updated_dt": "u.last_updated_dt"
                }
                ).execute()

    @classmethod 
    def create_proxy_views(cls):
        """
        Create proxy views for user configuration tables and columns, and autodetect view.
        This method is called during the initialization of the metastore to ensure that the views are created
        and available for use in the current session.
        """
        cls.create_userconfig_tables_proxy_view()        
        cls.create_userconfig_tables_cols_proxy_view()
        cls.create_autodetect_view() 