from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime, date
import warnings

from .Model.Config import *
from .Core import *
from .Enum import *
from .Model.Schedule import SyncSchedule
from .Constants import SyncConstants
from .DeltaTableUtility import *
from .Warnings import SyncUnsupportedConfigurationWarning

class SyncUtil():
    @staticmethod
    def build_filter_predicate(filter:ConfigObjectFilter):
        if filter:
            if filter.type and filter.pattern:
                if filter.type == ObjectFilterType.INCLUDE.value:
                    return f"table_name LIKE '{filter.pattern}'"
                else:
                    return f"table_name NOT LIKE '{filter.pattern}'"
        
        return None

    @staticmethod
    def map_column(map:MappedColumn, df:DataFrame) -> DataFrame:
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

        return df

    @staticmethod
    def format_watermark(max_watermark):
        if type(max_watermark) is date:
            return max_watermark.strftime("%Y-%m-%d")
        elif type(max_watermark) is datetime:
            return max_watermark.strftime("%Y-%m-%d %H:%M:%S%z")
        else:
            return str(max_watermark)

    @staticmethod
    def save_dataframe(table_name:str, df:DataFrame, mode:str, partition_by:list=None, options:dict=None):
        writer = df.write.mode(mode)

        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if options:
            writer = writer.options(**options)
        
        writer.saveAsTable(table_name)

    @staticmethod
    def optimize_bq_sync_metastore(context):
        SparkProcessor(context).optimize_vacuum(SyncConstants.get_metadata_tables())

    @staticmethod
    def flatten_structs(nested_df:DataFrame) -> DataFrame:
        """
        Recurses through Dataframe and flattens columns of with datatype struct
        using '_' notation
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

    @staticmethod
    def flatten_df(explode_arrays:bool, df:DataFrame) -> DataFrame:
        """
        Recurses through Dataframe and flattens complex types
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

    @staticmethod
    def resolve_fabric_partition_predicate(partition_type:str, partition_column:str, partition_grain:str, partition_id:str):
        if PartitionType[partition_type] == PartitionType.TIME:
            partition_dt=SyncUtil.get_derived_date_from_part_id(partition_grain, partition_id)
            proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(partition_grain)
            predicate = SyncUtil.get_fabric_partition_predicate(partition_dt, partition_column, proxy_cols) 
        else:
            predicate = f"__{partition_column}_Range='{partition_id}'"
        
        return predicate

    @staticmethod
    def get_fabric_partition_proxy_cols(partition_grain:str) -> list[str]:
        proxy_cols = list(CalendarInterval)

        match CalendarInterval[partition_grain]:
            case CalendarInterval.DAY:
                proxy_cols.remove(CalendarInterval.HOUR)
            case CalendarInterval.MONTH:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
            case CalendarInterval.YEAR:
                proxy_cols.remove(CalendarInterval.HOUR)
                proxy_cols.remove(CalendarInterval.DAY)
                proxy_cols.remove(CalendarInterval.MONTH)

        return [x.value for x in proxy_cols]
    
    @staticmethod
    def get_bq_partition_id_format(partition_grain:str) -> str:
        pattern = None

        match CalendarInterval[partition_grain]:
            case CalendarInterval.DAY:
                pattern = "%Y%m%d"
            case CalendarInterval.MONTH:
                pattern = "%Y%m"
            case CalendarInterval.YEAR:
                pattern = "%Y"
            case CalendarInterval.HOUR:
                pattern = "%Y%m%d%H"
        
        return pattern

    @staticmethod
    def get_derived_date_from_part_id(partition_grain:str, partition_id:str) -> datetime:
        dt_format = SyncUtil.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)
    
    @staticmethod
    def create_fabric_partition_proxy_cols(df:DataFrame, partition:str, proxy_cols:list[str]) -> DataFrame:  
        for c in proxy_cols:
            match CalendarInterval[c]:
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

    @staticmethod
    def get_fabric_partition_cols(partition:str, proxy_cols:list[str]):
        return [f"__{partition}_{c}" for c in proxy_cols]

    @staticmethod
    def get_fabric_partition_predicate(partition_dt:datetime, partition:str, proxy_cols:list[str]) -> str:
        partition_predicate = []

        for c in proxy_cols:
            match CalendarInterval[c]:
                case CalendarInterval.HOUR:
                    part_id = partition_dt.strftime("%H")
                case CalendarInterval.DAY:
                    part_id = partition_dt.strftime("%d")
                case CalendarInterval.MONTH:
                    part_id = partition_dt.strftime("%m")
                case CalendarInterval.YEAR:
                    part_id = partition_dt.strftime("%Y")

            partition_predicate.append(f"__{partition}_{c} = '{part_id}'")

        return " AND ".join(partition_predicate)
    
    @staticmethod
    def get_bq_range_map(tbl_ranges:str) -> DataFrame:
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
    
    @staticmethod
    def create_fabric_range_partition(context:SparkSession, df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        
        df = context.createDataFrame(partition_range, ["range_name", "range_low", "range_high"]) \
            .alias("rng")

        df_bq = df_bq.alias("bq")
        df_bq = df_bq.join(df, (col(f"bq.{schedule.PartitionColumn}") >= col("rng.range_low")) & \
            (col(f"bq.{schedule.PartitionColumn}") < col("rng.range_high"))) \
            .select("bq.*", col("rng.range_name").alias(schedule.FabricPartitionColumns[0]))
        
        return df_bq

    @staticmethod
    def get_partition_range_predicate(schedule:SyncSchedule) -> str:
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]
        if not r:
            raise SyncConfigurationError(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"