class Scheduler(ConfigBase):
    def __init__(self, config_path, gcp_credential):
        super().__init__(config_path, gcp_credential)

    def run(self):
        sql = f"""
        WITH new_schedule AS ( 
            SELECT CURRENT_TIMESTAMP() as scheduled
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
                NULL as src_row_count,
                NULL as dest_row_count,
                NULL as dest_inserted_row_count,
                NULL as dest_updated_row_count,
                NULL as delta_version,
                NULL as spark_application_id,
                NULL as max_watermark,
                NULL as summary_load,
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