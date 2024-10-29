from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime

from ..Config import *
from ..Core import *
from ..Admin.DeltaTableUtility import *
from ..Enum import *
from .Model.Schedule import SyncSchedule

class BQDataRetention(ConfigBase):
    """
    Class responsible for BQ Table and Partition Expiration
    """
    def __init__(self, context:SparkSession, user_config, gcp_credential:str):
        """
        Calls parent init to load User Config from JSON file
        """
        super().__init__(context, user_config, gcp_credential)
    
    def execute(self): 
        """
            Executes the data expiration and retention policy enforcement and updates.
            This method performs the following actions:
            1. Enforces the data expiration and retention policy by calling the 
               `_enforce_retention_policy` method.
            2. Updates the data expiration and retention policy by synchronizing 
               the retention configuration with the Metastore using the user's configuration ID.
            Prints messages to indicate the progress of each action.
        """

        print ("Enforcing Data Expiration/Retention Policy...")
        self._enforce_retention_policy()
        print ("Updating Data Expiration/Retention Policy...")
        self.Metastore.sync_retention_config(self.UserConfig.ID)

    def _enforce_retention_policy(self):
        """
        Enforces the retention policy for BigQuery tables based on the configuration.
        This method retrieves the retention policy configuration from the Metastore
        and applies it to the relevant tables. If a partition ID is specified, it
        will drop the partition; otherwise, it will drop the entire table.
        The table name is constructed based on whether the lakehouse schema is used.
        The method prints out the action being taken (expiring partition or table)
        for logging purposes.
        Parameters:
        None
        Returns:
        None
        """

        df = self.Metastore.get_bq_retention_policy(self.UserConfig.ID)

        for d in df.collect():
            if d["use_lakehouse_schema"]:
                table_name = f"{d['lakehouse']}.{d['lakehouse_schema']}.{d['lakehouse_table_name']}"
            else:
                table_name = f"{d['lakehouse']}.{d['lakehouse_table_name']}"

            table_maint = DeltaTableMaintenance(self.Context, table_name)

            if d["partition_id"]:
                print(f"Expiring Partition: {table_name}${d['partition_id']}")
                predicate = SyncUtil.resolve_fabric_partition_predicate(d["partition_type"], d["partition_column"], 
                    d["partition_grain"], d["partition_id"])
                table_maint.drop_partition(predicate)
            else:
                print(f"Expiring Table: {table_name}")
                table_maint.drop_table()  

class SyncUtil():
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
        """
            Resolves the partition predicate for a given fabric partition.
            Args:
                partition_type (str): The type of the partition (e.g., 'TIME').
                partition_column (str): The column name of the partition.
                partition_grain (str): The grain of the partition (e.g., 'DAY', 'MONTH').
                partition_id (str): The identifier of the partition.
            Returns:
                str: The resolved partition predicate.
        """

        if PartitionType[partition_type] == PartitionType.TIME:
            partition_dt=SyncUtil.get_derived_date_from_part_id(partition_grain, partition_id)
            proxy_cols = SyncUtil.get_fabric_partition_proxy_cols(partition_grain)

            predicate = SyncUtil.get_fabric_partition_predicate(partition_dt, partition_column, proxy_cols) 
        else:
            predicate = f"__{partition_column}_Range='{partition_id}'"
        
        return predicate

    @staticmethod
    def get_fabric_partition_proxy_cols(partition_grain:str) -> list[str]:
        """
        Returns a list of proxy columns based on the specified partition grain.
            Args:
                partition_grain (str): The partition grain, which should be one of the values in CalendarInterval.
            Returns:
                list[str]: A list of proxy column names corresponding to the partition grain.
        """
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
        """
            Returns the BigQuery partition ID format string based on the given partition grain.
            Args:
                partition_grain (str): The partition grain, which can be 'DAY', 'MONTH', 'YEAR', or 'HOUR'.
            Returns:
                str: The corresponding date format string for the partition grain.
        """
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
        """
        Converts a BigQuery partition ID to a datetime object based on the specified partition grain.
        Args:
            partition_grain (str): The grain of the partition (e.g., 'DAY', 'MONTH', 'YEAR').
            partition_id (str): The partition ID to be converted.
        Returns:
            datetime: The derived datetime object from the partition ID.
        """

        dt_format = SyncUtil.get_bq_partition_id_format(partition_grain)
        return datetime.strptime(partition_id, dt_format)
    
    @staticmethod
    def create_fabric_partition_proxy_cols(df:DataFrame, partition:str, proxy_cols:list[str]) -> DataFrame:  
        """
        Adds proxy columns to the DataFrame based on the specified partition and proxy columns.
        Parameters:
        df (DataFrame): The input DataFrame.
        partition (str): The partition column name.
        proxy_cols (list[str]): A list of proxy column types to be added. 
                                Valid values are 'HOUR', 'DAY', 'MONTH', 'YEAR'.
        Returns:
        DataFrame: The DataFrame with the added proxy columns.
        """
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
        """
            Generates a list of partitioned column names for a given partition and list of proxy columns.

            Args:
                partition (str): The partition name to be prefixed.
                proxy_cols (list[str]): A list of proxy column names.

            Returns:
                list[str]: A list of partitioned column names in the format "__{partition}_{column}".
        """
        return [f"__{partition}_{c}" for c in proxy_cols]

    @staticmethod
    def get_fabric_partition_predicate(partition_dt:datetime, partition:str, proxy_cols:list[str]) -> str:
        """
            Generates a partition predicate string for a given datetime, partition, and list of proxy columns.
            Args:
                partition_dt (datetime): The datetime object representing the partition date and time.
                partition (str): The partition name.
                proxy_cols (list[str]): A list of proxy column names.
            Returns:
                str: A string representing the partition predicate, formatted as "__{partition}_{proxy_col} = '{part_id}'" 
                     for each proxy column, joined by " AND ".
        """

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
        """
            Parses a string of comma-separated integers representing a range and step size,
            and returns a DataFrame containing partition ranges.
            Args:
                tbl_ranges (str): A string of comma-separated integers in the format "start,end,step".
            Returns:
                DataFrame: A DataFrame containing partition ranges with columns for the range string,
                           start value, and end value.
        """
        bq_range = [int(r.strip()) for r in tbl_ranges.split(",")]
        partition_range = [(f"{r}-{r + bq_range[2]}", r, r + bq_range[2]) for r in range(bq_range[0], bq_range[1], bq_range[2])]
        return partition_range
    
    @staticmethod
    def create_fabric_range_partition(context:SparkSession, df_bq:DataFrame, schedule:SyncSchedule) -> DataFrame:
        """
        Creates a range partition for a given DataFrame based on the provided schedule.
        Args:
            context (SparkSession): The Spark session to use for creating DataFrames.
            df_bq (DataFrame): The input DataFrame from BigQuery.
            schedule (SyncSchedule): The synchronization schedule containing partitioning details.
        Returns:
            DataFrame: A DataFrame with the range partition applied, including a new column for the partition range name.
        """
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
        """
        Generates a BigQuery partition range predicate string based on the provided schedule.
        Args:
            schedule (SyncSchedule): The synchronization schedule containing partition details.
        Returns:
            str: A string representing the partition range predicate for BigQuery.
        Raises:
            Exception: If the partition ID from the schedule does not match any range in the partition range map.
        """
        partition_range = SyncUtil.get_bq_range_map(schedule.PartitionRange)
        r = [x for x in partition_range if str(x[1]) == schedule.PartitionId]

        if not r:
            raise Exception(f"Unable to match range partition id {schedule.PartitionId} to range map.")

        return f"{schedule.PartitionColumn} >= {r[0][1]} AND {schedule.PartitionColumn} < {r[0][2]}"