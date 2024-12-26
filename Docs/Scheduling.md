# Scheduling
Longer term the accelerator will support robust scheduling capabilities that allow object-level schedule definition and integration with common enterprise job queues and schedulers like Fabric Pipelines, Azure Data Factory, Apache Airflow and others. 

Currently, AUTO scheduling is the default for all objects. AUTO scheduling is evaluated on every active (enabled) object.

When the scheduler runs every active object at the time of scheduling is recorded to the schedule as either <code>SCHEDULED</code> or <code>SKIPPED</code>. Skipped records are recorded for auditing and troubleshooting purposes.

During schedule building, BigQuery objects are handled differently based on their type.

- Base Tables: The scheduler looks at the BigQuery metadata to determine if there have been any changes to either the table or its partition since the last successful sync. If the table or partition has not changed or it is using long-term (archived) BigQuery storage the scheduled run for the table/partition is skipped.

- Views: Since views are virtual, there is no useful BigQuery metadata within the context of scheduling. Views are included on every schedule run.

- Materialized Views: The schedule treats materialized views like tables using the BigQuery <code>last_refresh_time</code> to determine if there have been any changes to materialized view since the last successful sync.

Please note that the scheduler does not allow duplicate schedules. Incomplete schedules or schedules that have <code>FAILED</code> will block new schedules from being created until the schedule completes or is cancelled.
