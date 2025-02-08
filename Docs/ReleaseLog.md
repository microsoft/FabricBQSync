# Fabric Sync Release Log

### Version 2.1.0
    - New Load Strategies:
        - CDC Append (BigQuery Preview)
        - CDC (BigQuery Preview)
    - Lakehouse Schema Support (Fabric Preview)
    - Support for Mirrored Databases as destination through Fabric Open Mirroring (Fabric Preview)
    - Support for multiple loaders using shared metadata lakehouse
    - Inline/Automatic Version Updates for v2.0 and greater
    - Updates to default behaviors:
        - GCP API use_cdc enabled by default
	    - GCP API use_standard_api enabled by default
	    - Optimization use_approximate_row_counts enabled by default
    - Installer Updates:
        - Updates for Open Mirroring
        - Support for Fabric Spark Environments
        - Improved Notebook configuration/mapping

### Version 2.0.8
    - Intelligent Maintenance

### Version 2.0.5
    - Data Maintenance (see docs)
        - Scheduled maintenance support
        - Lakehouse Delta Inventory

### Version 2.0.0
    - Performance improvements
    - Rruntime Optimization Options
        - Approximate Row Counts
        - Disabled Data Frame Caching
    - Configuration updates - Reorganized/regroup for simplicity and clarity
    - Unified logging framework & telemetry
    - Table pattern-match filters for discovery
    - Source predicate without source_query
    - OneLake partitioning - override BQ partitioning schema
    - Column-mapping - rename and data-type conversion
    - BQ Spark Connector optimization parameters
    - Improved reporting/exception handling in Installer
    - Automated version upgrade utility
    - Migration to UV and .toml/Build Modernization