# Logging

Fabric Sync provides logging built on top of the native Python logging capabilities, yet separate from the built-in Spark log sinks provided as part of the Fabric Spark runtime.

The accelerator collects logs in JSON-format in your configured Metadata Lakehouse with a daily-rollover by default. You can modify the log location using the <code>logging</code>/<code>log_path</code> setting in the User Configuration file.

### Telemetry
The Fabric Sync accelerator collects two types of telemetry:

1. Data Telemetry - context, specific telemetry that may contain sensitive-data like project id, dataset id, table names, source and destination row counts, watermarks, etc. 
    - Data Telemetry is only collected and used within your Fabric workspace. This telemetry is never shared with Microsoft or others abd is collected within your configured Metadata Lakehouse.
2. Usage Telemetry - Anonymous Fabric Sync feature usage signals.
    - Usage events are sent to an Azure endpoint and are aggregated for reporting and analytics on Fabric Sync usage. No customer data is collected or sent for this telemetry.
    - The following telemetry signals are used:
        - Install
        - Upgrade
        - Metadata Sync
        - Autodiscover
        - Scheduler
        - Sync/Load
        - Table Maintenance
        - Data Expiration