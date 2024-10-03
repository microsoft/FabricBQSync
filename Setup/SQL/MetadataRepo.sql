DROP TABLE IF EXISTS bq_sync_data_expiration;
CREATE TABLE IF NOT EXISTS bq_sync_data_expiration (
    sync_id STRING, 
    table_catalog STRING, 
    table_schema STRING, 
    table_name STRING, 
    partition_id STRING, 
    expiration TIMESTAMP
);

DROP TABLE IF EXISTS bq_data_type_map;
CREATE TABLE IF NOT EXISTS bq_data_type_map
(
    data_type STRING, 
    partition_type STRING, 
    is_watermark STRING
);

DROP TABLE IF EXISTS bq_sync_configuration;
CREATE TABLE IF NOT EXISTS bq_sync_configuration
(
    sync_id STRING,
    project_id STRING,
    dataset STRING,
    table_name STRING,
    object_type STRING,
    enabled BOOLEAN,
    lakehouse STRING,
    lakehouse_schema STRING,
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
    partition_data_type STRING,
    partition_range STRING,
    watermark_column STRING,
    autodetect BOOLEAN,
    use_lakehouse_schema BOOLEAN,
    enforce_expiration BOOLEAN,
    allow_schema_evolution BOOLEAN,
    table_maintenance_enabled BOOLEAN,
    table_maintenance_interval STRING,
    flatten_table BOOLEAN,
    flatten_inplace BOOLEAN,
    explode_arrays BOOLEAN,
    table_options ARRAY<STRUCT<key:STRING,value:STRING>>,
    config_override BOOLEAN,
    sync_state STRING,
    created_dt TIMESTAMP,
    last_updated_dt TIMESTAMP
);

DROP TABLE IF EXISTS bq_sync_schedule;
CREATE TABLE IF NOT EXISTS bq_sync_schedule 
(
    group_schedule_id STRING,
    schedule_id STRING,
    sync_id STRING,
    project_id STRING,
    dataset STRING,
    table_name STRING,
    schedule_type STRING,
    scheduled TIMESTAMP,
    status STRING,
    started TIMESTAMP,
    completed TIMESTAMP,
    completed_activities INT,
    failed_activities INT,
    max_watermark STRING,
    priority INTEGER
);

DROP TABLE IF EXISTS bq_sync_schedule_telemetry;
CREATE TABLE IF NOT EXISTS bq_sync_schedule_telemetry 
(
    schedule_id STRING,
    sync_id STRING,
    project_id STRING,
    dataset STRING,
    table_name STRING,
    partition_id STRING,
    status STRING,
    started TIMESTAMP,
    completed TIMESTAMP,
    src_row_count BIGINT,
    inserted_row_count BIGINT,
    updated_row_count BIGINT,
    delta_version BIGINT,
    spark_application_id STRING,
    max_watermark STRING,
    summary_load STRING
);