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
    partition_data_type STRING,
    partition_range STRING,
    watermark_column STRING,
    autodetect BOOLEAN,
    enforce_partition_expiration BOOLEAN,
    allow_schema_evoluton BOOLEAN,
    table_maintenance_enabled BOOLEAN,
    table_maintenance_interval STRING,
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