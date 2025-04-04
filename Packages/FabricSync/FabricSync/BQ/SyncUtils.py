from pyspark.sql import (
    SparkSession, DataFrame, Observation, Row
)
from pyspark.sql.functions import (
    to_date, to_timestamp, date_format, explode, try_to_number, col, lit,
    count, max
)
from packaging import version as pv
from datetime import (
    datetime, date
)
from typing import (
    List, Tuple, Any
)
import warnings

from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Model.Config import (
    ConfigDataset, ConfigObjectFilter, MappedColumn
)
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.SessionManager import Session
from FabricSync.BQ.Enum import (
    ObjectFilterType, SupportedTypeConversion, BQPartitionType, CalendarInterval,
    SyncLoadType, SyncLoadStrategy
)
from FabricSync.BQ.Model.Schedule import SyncSchedule
from FabricSync.BQ.Warnings import SyncUnsupportedConfigurationWarning
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.Exceptions import SyncConfigurationError

class SyncUtil(ContextAwareBase):
    @classmethod
    def configure_context(cls, user_config:ConfigDataset, gcp_credential:str=None, 
                                    config_path:str=None, token:str=None) -> None:
        """
        Initialize a Spark session and configure Spark settings based on the provided config.
        This function retrieves the current SparkSession (or creates one if it does not exist)
        and updates various configuration settings such as Delta Lake properties, partition
        overwrite mode, and custom application/logging metadata.
        Parameters:
            config (ConfigDataset): Contains application, logging, and telemetry settings.
            user_config_path (str): The path to the user configuration file.
            fabric_api_token (str): The Fabric API token.
        Returns:
            None
        """
        Session.set_spark_conf("spark.sql.parquet.vorder.enabled", "false")
        Session.set_spark_conf("spark.databricks.delta.optimizeWrite.enabled", "false")
        Session.set_spark_conf("spark.databricks.delta.collect.stats", "false")

        Session.set_spark_conf("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        Session.set_spark_conf("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        Session.set_spark_conf("spark.databricks.delta.properties.defaults.minWriterVersion", "7")
        Session.set_spark_conf("spark.databricks.delta.properties.defaults.minReaderVersion", "3")
        Session.set_spark_conf("spark.sql.sources.partitionOverwriteMode", "dynamic")

        if gcp_credential:
            Session.GCPCredentials = gcp_credential

        Session.ApplicationID = user_config.ApplicationID
        Session.ID = user_config.ID
        Session.LogPath = user_config.Logging.LogPath
        Session.LogLevel = user_config.Logging.LogLevel
        Session.Telemetry = user_config.Logging.Telemetry


        Session.TelemetryEndpoint = f"{user_config.Logging.TelemetryEndPoint}.azurewebsites.net"

        Session.WorkspaceID = user_config.Fabric.WorkspaceID

        if user_config.Version:
            Session.Version = pv.parse(user_config.Version)

        Session.MetadataLakehouse = user_config.Fabric.MetadataLakehouse
        Session.MetadataLakehouseID = user_config.Fabric.MetadataLakehouseID
        Session.TargetLakehouse = user_config.Fabric.TargetLakehouse
        Session.TargetLakehouseID = user_config.Fabric.TargetLakehouseID
        Session.SchemaEnabled = user_config.Fabric.EnableSchemas

        if config_path:
            Session.UserConfigPath = config_path

        if token:
            Session.FabricAPIToken = token

    @classmethod
    def ensure_sync_views(cls):
        """
        Ensures that the Fabric Sync views are created in the metadata lakehouse.
        This function checks the SyncViewState flag in the session and creates the views if necessary.
        Returns:
            None
        """
        if not Session.SyncViewState:
            FabricMetastore.create_proxy_views()
            Session.SyncViewState = True

    @classmethod
    def build_filter_predicate(cls, filter:ConfigObjectFilter) -> str:
        """
        Build a SQL predicate string based on the specified object filter.
        :param filter: A configuration object filter that includes:
            - pattern: The table name pattern to match.
            - type: The filter type, indicating whether to INCLUDE or EXCLUDE tables
              matching the pattern.
        :return: A string containing the appropriate SQL predicate (e.g.
            "table_name LIKE ..." or "table_name NOT LIKE ..."), or None if the filter
            is not provided or incomplete.
        """

        if filter:
            if filter.type and filter.pattern:
                if filter.type == ObjectFilterType.INCLUDE.value:
                    return f"table_name LIKE '{filter.pattern}'"
                else:
                    return f"table_name NOT LIKE '{filter.pattern}'"
        
        return None

    @classmethod
    def map_column(cls, map:MappedColumn, df:DataFrame) -> DataFrame:
        """
        Maps or renames the source column in the given Spark DataFrame based on the provided mapping configuration.
        This function can perform type conversions, including string-to-date, string-to-timestamp, 
        string-to-decimal, date-to-string, and timestamp-to-string conversions. If the requested 
        conversion is unsupported, a warning message is displayed. It also supports optional removal 
        of the original source column or renaming it to the specified destination column.
        Args:
            map (MappedColumn):
                An object containing source and destination column details, type conversion flags, 
                format strings, and drop or rename indicators.
            df (pyspark.sql.DataFrame):
                A Spark DataFrame on which the column mapping or renaming will be applied.
        Returns:
            pyspark.sql.DataFrame:
                A new DataFrame with the column mapped or renamed according to the defined rules.
        """

        if map.IsTypeConversion:
            type_map = f"{map.Source.Type}_TO_{map.Destination.Type}"

            supported_conversion = False

            try:
                type_conversion = SupportedTypeConversion[type_map]
                supported_conversion = True
            except KeyError:
                warnings.warn(f"WARNING: Skipped Unsupported Type Conversion ({map.Source.Name}): {map.Source.Type} to {map.Destination.Type}", 
                    SyncUnsupportedConfigurationWarning)
                supported_conversion = False

            if supported_conversion:
                match type_conversion:
                    case SupportedTypeConversion.STRING_TO_DATE:
                        df = df.withColumn(map.Destination.Name, to_date(col(map.Source.Name), map.Format))
                    case SupportedTypeConversion.STRING_TO_TIMESTAMP:
                        df = df.withColumn(map.Destination.Name, to_timestamp(col(map.Source.Name), map.Format))
                    case SupportedTypeConversion.STRING_TO_DECIMAL:
                        df = df.withColumn(map.Destination.Name, try_to_number(col(map.Source.Name), lit(map.Format)))
                    case SupportedTypeConversion.DATE_TO_STRING | SupportedTypeConversion.TIMESTAMP_TO_STRING:
                        df = df.withColumn(map.Destination.Name, date_format(col(map.Source.Name), map.Format))
                    case _:
                        df = df.withColumn(map.Destination.Name, col(map.Source.Name).cast(map.Destination.Type.lower()))
                
                if map.DropSource:
                    df = df.drop(map.Source.Name)
        elif map.IsRename:
            df = df.withColumnRenamed(map.Source.Name, map.Destination.Name)
        elif map.DropSource:
            df = df.drop(map.Source.Name)

        return df

    @classmethod
    def format_watermark(cls, max_watermark) -> str:
        """
        Formats a watermark value as a string.
        If the input is a date object, it is returned in "YYYY-MM-DD" format.
        If the input is a datetime object, it is returned in "YYYY-MM-DD HH:MM:SS±ZZZZ" format.
        Otherwise, the input is converted to a string.
        Args:
            max_watermark: The value to format, which can be a date, datetime, or any object convertible to string.
        Returns:
            str: The formatted string representation of the watermark.
        """
        if not max_watermark:
            return None

        if type(max_watermark) is date:
            return max_watermark.strftime("%Y-%m-%d")
        elif type(max_watermark) is datetime:
            return max_watermark.strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            return str(max_watermark)

    @classmethod
    def save_dataframe(cls, table_name:str, df:DataFrame, mode:str, format:str="delta", path=None, partition_by:list=None, options:dict=None) -> None:
        """
        Saves a Spark DataFrame to a specified path or as a table.
        Parameters:
            table_name (str): Name of the table to save the DataFrame under.
            df (DataFrame): The Spark DataFrame to be saved.
            mode (str): Save mode (e.g., 'append', 'overwrite').
            format (str, optional): Format in which to save the DataFrame (default is 'delta').
            path (str, optional): Filesystem path to save the DataFrame. If not provided, saves as a table.
            partition_by (list, optional): Columns to partition by.
            options (dict, optional): Additional write options to pass to the writer.
        Returns:
            None
        """
        
        writer = df.write.mode(mode).format(format)

        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if options:
            writer = writer.options(**options)
        
        if path:
            writer.save(path)
        else:
            writer.saveAsTable(table_name)

    @classmethod
    def get_lakehouse_partitions(cls, schedule:SyncSchedule) -> List[str]:
        """
        Returns the list of partition columns for a Lakehouse dataset based on the provided SyncSchedule object.
        This function first checks if a LakehousePartition is defined in the schedule. If not, it falls back to the FabricPartitionColumns.
        Args:
            schedule (SyncSchedule): The SyncSchedule object containing the partition details.
        Returns:
            List[str]: A list of partition columns for the Lakehouse dataset.
        """
        if schedule.LakehousePartition:
            return schedule.LakehousePartition.split(",")
        else:
            return schedule.FabricPartitionColumns

    @classmethod
    def transform(cls, schedule:SyncSchedule, df:DataFrame) -> Tuple[SyncSchedule, DataFrame]:
        """
        Applies transformations to a Spark DataFrame based on the provided SyncSchedule object.
        This function performs column mapping, flattening of nested structs, and exploding of array columns.
        Args:
            schedule (SyncSchedule): The SyncSchedule object containing the transformation details.
            df (DataFrame): The Spark DataFrame to transform.
        Returns:
            Tuple[SyncSchedule, DataFrame]: A tuple containing the updated SyncSchedule object and the transformed DataFrame.
        """
        df = SyncUtil.map_columns(schedule, df)

        return (schedule, df)
    
    @classmethod
    def map_columns(cls, schedule:SyncSchedule, df:DataFrame) -> DataFrame:
        """
        Maps or renames columns in a Spark DataFrame based on the provided SyncSchedule object.
        This function iterates over the MappedColumns in the schedule and applies the column mappings to the DataFrame.
        Args:
            schedule (SyncSchedule): The SyncSchedule object containing the column mapping details.
            df (DataFrame): The Spark DataFrame to which the column mappings will be applied.
        Returns:
            DataFrame: A new Spark DataFrame with the column mappings applied.
        """
        maps = schedule.get_column_map()

        if maps:
            for m in maps:
                df = SyncUtil.map_column(m, df)
        
        return df 
    
    @classmethod
    def flatten(cls, schedule:SyncSchedule, df:DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Flattens a DataFrame based on the provided SyncSchedule.
        This method flattens a DataFrame based on the provided SyncSchedule, optionally exploding
        arrays and returning the flattened DataFrame and the original DataFrame if necessary.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the flatten settings.
            df (DataFrame): The DataFrame to flatten.
        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing the flattened DataFrame and the original
            DataFrame if the original DataFrame was flattened; otherwise, the original DataFrame is None.
        """
        if schedule.Load_Type == SyncLoadType.MERGE and schedule.ExplodeArrays:
            raise SyncConfigurationError("Invalid load configuration: Merge is not supported when Explode Arrays is enabed")
                
        if schedule.FlattenInPlace:
            df = SyncUtil.flatten_df(schedule.ExplodeArrays, df)
            flattened = None
        else:
            flattened = SyncUtil.flatten_df(schedule.ExplodeArrays, df)
        
        return (df, flattened)
    
    @classmethod
    def flatten_structs(cls, nested_df:DataFrame) -> DataFrame:
        """
        Flattens all nested struct columns of a Spark DataFrame, producing a single-level
        DataFrame with each nested field converted to a new column.
        Parameters:
            nested_df (DataFrame): The Spark DataFrame that contains potentially nested
                struct columns.
        Returns:
            DataFrame: A new Spark DataFrame with nested struct fields flattened into
            top-level columns.
        Example:
            Suppose you have a DataFrame with struct columns:
                root
                 |-- person: struct (nullable = true)
                 |    |-- name: string (nullable = true)
                 |    |-- address: struct (nullable = true)
                 |    |    |-- city: string (nullable = true)
                 |    |    |-- zip: string (nullable = true)
            After calling flatten_structs, the resulting DataFrame will have columns:
            [ person_name, person_address_city, person_address_zip ]
        """

        stack = [((), nested_df)]
        columns = []

        while len(stack) > 0:        
            parents, df = stack.pop()

            flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) \
                    for c in df.dtypes if c[1][:6] != "struct"]
                
            nested_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]
            
            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(nested_col + ".*")
                stack.append((parents + (nested_col,), projected_df))
            
        return nested_df.select(columns)

    @classmethod
    def flatten_df(cls, explode_arrays:bool, df:DataFrame) -> DataFrame:
        """
        Flattens nested fields in a Spark DataFrame and optionally explodes array columns.
        Args:
            explode_arrays (bool): Whether to explode array columns before flattening.
            df (DataFrame): The Spark DataFrame to flatten.
        Returns:
            DataFrame: A flattened DataFrame with nested fields and arrays resolved.
        """
        
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]

        if len(array_cols) > 0 and explode_arrays:
            while len(array_cols) > 0:        
                for array_col in array_cols:            
                    cols_to_select = [x for x in df.columns if x != array_col ]            
                    df = df.withColumn(array_col, explode(col(array_col)))
                    
                df = SyncUtil.flatten_structs(df)
                
                array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        else:
            df = SyncUtil.flatten_structs(df)

        return df

    @classmethod
    def resolve_fabric_partition_predicate(cls, partition_type:BQPartitionType, partition_column:str, partition_grain:CalendarInterval, partition_id:str, alias:str = None) -> str:
        """
        Generates and returns the partition predicate used to filter data in a Fabric dataset 
        table based on the provided partition type and partition identifier.
        This method supports both time-based and integer range-style partitions:
        • For time-based partitions, the predicate is constructed using a derived date 
            from the partition identifier and the partition proxies that map to the 
            appropriate date interval.
        • For integer range-style partitions, the predicate is composed directly from the 
            partition identifier.
        :param partition_type: The type of partition (e.g., time-based or range-based).
        :param partition_column: The column used to partition the dataset.
        :param partition_grain: The calendar interval granularity for time-based partitions.
        :param partition_id: The partition value to filter on, representing either a date 
                                                 or an integer-based range identifier.
        :param alias: Optional table alias to prepend before the predicate.
        :return: The partition predicate expression as a string, ready for use in SQL queries.
        """

        if partition_type == BQPartitionType.TIME:
            partition_dt=SyncUtil.get_derived_date_from_part_id(partition_grain, partition_id)
            proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(partition_grain)
            predicate = SyncUtil.get_fabric_partition_predicate(partition_dt, partition_column, proxy_cols) 
        else:
            predicate = f"__{partition_column}_Range='{partition_id}'"
        
        if alias:
            predicate = f"{alias}.{predicate}"

        return predicate

    @classmethod
    def get_fabric_partition_proxy_cols(cls, partition_grain:CalendarInterval) -> List[CalendarInterval]:
        """
        Generates a list of proxy columns for partitioning based on the specified partition grain.
        This function examines the given partition_grain and removes unsupported
        CalendarInterval values from the list of all intervals. For instance, if the
        partition_grain is DAY, then HOUR is removed from the resulting list. The same
        logic applies to other interval grains.
        Args:
            partition_grain (CalendarInterval):
                The partition grain that determines which CalendarIntervals should remain.
        Returns:
            List[CalendarInterval]:
                A list of remaining CalendarInterval values that are valid for the given
                partition grain.
        """

        proxy_cols = list(CalendarInterval)

        match partition_grain:
            case CalendarInterval.DAY:
                proxy_cols.remove(CalendarInterval.HOUR)
            case CalendarInterval.MONTH:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
            case CalendarInterval.YEAR:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
                proxy_cols.remove(CalendarInterval.MONTH)

        return [x for x in proxy_cols]
    
    @classmethod
    def get_bq_partition_id_format(cls, partition_grain:CalendarInterval) -> str:
        """
        Return the BigQuery partition ID format string based on the specified partition grain.
        Parameters:
            partition_grain (CalendarInterval): The interval for partitioning (e.g., DAY, MONTH, YEAR, HOUR).
        Returns:
            str: A date/time format string corresponding to the given partition grain.
        """

        pattern = None

        match partition_grain:
            case CalendarInterval.DAY:
                pattern = "%Y%m%d"
            case CalendarInterval.MONTH:
                pattern = "%Y%m"
            case CalendarInterval.YEAR:
                pattern = "%Y"
            case CalendarInterval.HOUR:
                pattern = "%Y%m%d%H"
        
        return pattern

    @classmethod
    def get_derived_date_from_part_id(cls, partition_grain:CalendarInterval, partition_id:str) -> datetime:
        """
        Generates a datetime object from a given partition identifier string based on the specified partition grain.
        Args:
            partition_grain (CalendarInterval): An interval defining how the partition date is formatted.
            partition_id (str): A string identifier representing the partition date.
        Returns:
            datetime: A datetime object derived from the partition identifier.
        """

        dt_format = SyncUtil.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)
    
    @classmethod
    def create_fabric_partition_proxy_cols(cls, df:DataFrame, partition:str, proxy_cols:list[CalendarInterval]) -> DataFrame:  
        """
        Creates new columns in the given Spark DataFrame based on the specified partition column
        and the provided calendar intervals.
        Parameters:
            cls (Type): The class object reference.
            df (DataFrame): A Spark DataFrame to which the partition proxy columns will be added.
            partition (str): The name of the existing column used to generate partition-based columns.
            proxy_cols (list[CalendarInterval]): A list of calendar intervals (e.g., HOUR, DAY, MONTH, YEAR)
                indicating which types of partition proxy columns to create.
        Returns:
            DataFrame: A Spark DataFrame that includes newly added partition proxy columns.
        """

        for c in proxy_cols:
            match c:
                case CalendarInterval.HOUR:
                    df = df.withColumn(f"__{partition}_HOUR", \
                        date_format(col(partition), "HH"))
                case CalendarInterval.DAY:
                    df = df.withColumn(f"__{partition}_DAY", \
                        date_format(col(partition), "dd"))
                case CalendarInterval.MONTH:
                    df = df.withColumn(f"__{partition}_MONTH", \
                        date_format(col(partition), "MM"))
                case CalendarInterval.YEAR:
                    df = df.withColumn(f"__{partition}_YEAR", \
                        date_format(col(partition), "yyyy"))
        
        return df

    @classmethod
    def get_fabric_partition_cols(cls, partition:str, proxy_cols:list[CalendarInterval]) -> List[str]:
        """
        Generates a list of partition proxy column names based on the specified partition column and calendar intervals.
        Parameters:
            partition (str): The name of the partition column.
            proxy_cols (list[CalendarInterval]): A list of calendar intervals (e.g., HOUR, DAY, MONTH, YEAR)
                indicating which types of partition proxy columns to create.
        Returns:
            List[str]: A list of partition proxy column names based on the partition column and intervals.
        """
        return [f"__{partition}_{c.value}" for c in proxy_cols]

    @classmethod
    def get_fabric_partition_predicate(cls, partition_dt:datetime, partition:str, proxy_cols:list[CalendarInterval], alias:str = None) -> str:
        """
        Generates a partition predicate string based on the specified partition datetime and calendar intervals.
        Parameters:
            partition_dt (datetime): The datetime object representing the partition date.
            partition (str): The name of the partition column.
            proxy_cols (list[CalendarInterval]): A list of calendar intervals (e.g., HOUR, DAY, MONTH, YEAR)
                indicating which types of partition proxy columns to create.
            alias (str, optional): An optional table alias to prepend to the predicate.
        Returns:
            str: The partition predicate expression as a string, ready for use in SQL queries.
        """
        partition_predicate = []

        for c in proxy_cols:
            match c:
                case CalendarInterval.HOUR:
                    part_id = partition_dt.strftime("%H")
                case CalendarInterval.DAY:
                    part_id = partition_dt.strftime("%d")
                case CalendarInterval.MONTH:
                    part_id = partition_dt.strftime("%m")
                case CalendarInterval.YEAR:
                    part_id = partition_dt.strftime("%Y")

            if alias:
                partition_predicate.append(f"{alias}.__{partition}_{c} = '{part_id}'")
            else:
                partition_predicate.append(f"__{partition}_{c} = '{part_id}'")

        return " AND ".join(partition_predicate)
    
    @classmethod
    def get_bq_range_map(cls, tbl_ranges:str) -> DataFrame:
        """
        Generates a list of range partitions based on the specified range map string.
        Parameters:
            tbl_ranges (str): A comma-separated string representing the range partition map.
                The string should be formatted as "start-end,step", where:
                - start: The starting value of the range.
                - end: The ending value of the range.
                - step: The increment value for each partition range.
                Returns:
                DataFrame: A DataFrame containing the range partition map with columns for the range name,
                range low, and range high values.
        """
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
    
    @classmethod
    def create_fabric_range_partition(cls, context:SparkSession, df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        """
        Creates a range partition column in the given Spark DataFrame based on the specified range map.
        Parameters:
            context (SparkSession): The Spark session object.
            df_bq (DataFrame): The Spark DataFrame to which the range partition column will be added.
            schedule (SyncSchedule): The SyncSchedule object containing the partition range details.
            Returns:
            DataFrame: A new Spark DataFrame with the range partition column added.
        """
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        
        df = context.createDataFrame(partition_range, ["range_name", "range_low", "range_high"]) \
            .alias("rng")

        df_bq = df_bq.alias("bq")
        df_bq = df_bq.join(df, (col(f"bq.{schedule.PartitionColumn}") >= col("rng.range_low")) & \
            (col(f"bq.{schedule.PartitionColumn}") < col("rng.range_high"))) \
            .select("bq.*", col("rng.range_name").alias(schedule.FabricPartitionColumns[0]))
        
        return df_bq

    @classmethod
    def get_partition_range_predicate(cls, schedule:SyncSchedule) -> str:
        """
        Generates a partition predicate string based on the specified SyncSchedule object.
        Parameters:
            schedule (SyncSchedule): The SyncSchedule object containing the partition details.
            Returns:
            str: The partition predicate expression as a string, ready for use in SQL queries.
        """
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]
        if not r:
            raise SyncConfigurationError(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"
    
    @classmethod
    def get_source_metrics(cls, schedule:SyncSchedule, df_bq:DataFrame, observation:Observation = None) -> Tuple[int, Any]:
        """
        Retrieves the source row count and watermark from the provided DataFrame and Observation.
        This method calculates the row count from the DataFrame and retrieves the watermark from the
        Observation object if available. If no Observation is provided, the row count is calculated
        directly from the DataFrame, and the watermark is set to the BQ table's last modified timestamp.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the source metrics.
            df_bq (DataFrame): The DataFrame containing the source data.
            observation (Observation): An optional Observation object for monitoring row counts and watermarks.
        Returns:
            Tuple[int, Any]: A tuple containing the source row count and watermark value.
        """
        row_count = 0
        watermark = None

        if observation:
            observations = observation.get
            row_count = observations["row_count"]

            if "watermark" in observations:
                watermark = SyncUtil.format_watermark(observations["watermark"])
        else:
            if schedule.Load_Strategy == SyncLoadStrategy.WATERMARK:
                df = df_bq.select(max(col(schedule.WatermarkColumn)).alias("watermark"), count("*").alias("row_count"))

                row = df.first()
                row_count = row["row_count"]
                watermark = SyncUtil.format_watermark(row["watermark"])
            else:
                df = df_bq.select(count("*").alias("row_count"))

                row = df.first()
                row_count = row["row_count"]
    
        return (row_count, watermark)
    
    @classmethod
    def save_schedule_telemetry(cls, schedule:SyncSchedule) -> None:
        """
        Write status and telemetry from sync schedule to Sync Schedule Telemetry Delta table
        Args:
            schedule (SyncSchedule): The synchronization schedule containing the telemetry details.
        Returns:
            None
        """
        rdd = cls.Context.sparkContext.parallelize([Row( 
            schedule_id=schedule.ScheduleId, 
            sync_id=schedule.SyncId,
            table_id=schedule.TableId,
            project_id=schedule.ProjectId, 
            dataset=schedule.Dataset, 
            table_name=schedule.TableName, 
            partition_id=schedule.PartitionId, 
            status=schedule.Status.value, 
            started=schedule.StartTime, 
            completed=schedule.EndTime, 
            src_row_count=schedule.SourceRows, 
            inserted_row_count=schedule.InsertedRows, 
            updated_row_count=schedule.UpdatedRows, 
            delta_version=schedule.DeltaVersion, 
            spark_application_id=schedule.SparkAppId, 
            max_watermark=schedule.MaxWatermark, 
            summary_load=schedule.SummaryLoadType, 
            source_query=schedule.SourceQuery, 
            source_predicate=schedule.SourcePredicate,
            mirror_file_index=schedule.MirrorFileIndex 
        )])

        FabricMetastore.save_schedule_telemetry(rdd)