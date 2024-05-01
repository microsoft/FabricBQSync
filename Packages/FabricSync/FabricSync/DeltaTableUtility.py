from pyspark.sql import *
from pyspark.sql.functions import * 
from delta.tables import *
from typing import Tuple

class DeltaTableMaintenance:
    """
    Maintenance class that wraps  simple table metadata calls and 
    all the heavy lifting for maintenance operations

    1. Deep merge schema support:
        a. Detect schema changes that require a drop or data type update:
            - Drop column in destination
            - Update column data type in destination
        b. Drop table using file system shortcut
        c. Optimize
        d. Vacuum

    """
    __detail:Row = None

    def __init__(self, context:SparkSession, spark_utils, table_nm:str):
        """
        Init function, creates instance of DeltaTable class
        """
        self.__context__ = context
        self.__spark_utils__ = spark_utils
        self.TableName = table_nm
        self.DeltaTable = DeltaTable.forName(context, table_nm)

    @property
    def Context(self) -> SparkSession:
        return self.__context__
    
    @property
    def SparkUtils(self):
        return self.__spark_utils__
    
    @property
    def CurrentTableVersion(self) -> int:
        """
        Retrieves the max delta version for the table from the table history
        """
        history = self.get_table_history() \
            .select(max(col("version")).alias("delta_version"))

        return [r[0] for r in history.collect()][0]

    @property
    def Detail(self) -> DataFrame:
        """
        Delta table metadata detail row
        """
        if not self.__detail:
            self.__detail = self.DeltaTable.detail().collect()[0]
        
        return self.__detail


    @property
    def OneLakeLocation(self) -> str:
        """
        OneLake file location for the table
        """
        return self.Detail["location"]

    def get_table_history(self, only_current_day:bool = False) -> DataFrame:
        """
        Gets the table history with optional filter for current day
        """
        history = self.DeltaTable.history()

        if only_current_day:
            history = history.filter("CAST(timestamp AS DATE) = current_date()")

        return history

    def SchemaDiff(self, src: DataFrame, dest: DataFrame):
        """
        Detects schema difference between the delta table and a source dataframe
        """
        srcSchema = {x[0]:x[1] for x in src.dtypes}
        destSchema = {x[0]:x[1] for x in dest.dtypes}
            
        srcNotPresent = set(dest.columns) - set(src.columns)
        destNotPresent = set(src.columns) - set(dest.columns)
        
        diffSchema = {k:v for k,v in srcSchema.items() if k not in destNotPresent}
        
        typesChanged = {}
        for column_name in diffSchema:
            if diffSchema[column_name] != destSchema[column_name]:
                typesChanged[column_name] = srcSchema[column_name]
        
        
        return destNotPresent, srcNotPresent, typesChanged
    
    def evolve_schema(self, src:DataFrame):
        """
        If schema differences are detected that between the source and delta table:

        1. Drops required columns from the destination
        2. Updates column data types in the destination

        Raises an error if a partitioning column is altered
        """
        dest = self.DeltaTable.toDF()

        new_columns, removed_columns, updated_columns  = self.SchemaDiff(src, dest)

        if removed_columns or updated_columns:
            partition_columns = self.Detail["partitionColumns"]
            tbl_properties = self.Detail["properties"]

            print("Resolving schema differences that require table overwrite...")
            for c in removed_columns:
                if not c in partition_columns:
                    print(f"Removing Column {c}...")
                    dest = dest.drop(c)
                else:
                    raise Exception("Schema Evolution Violation: Partition Column cannot be dropped")

            for c in updated_columns:
                print(f"Changing Column {c} type to {updated_columns[c]}...")
                dest = dest.withColumn(c, col(c).cast(updated_columns[c]))
            
            write_options = {"overwriteSchema":"true"}
            write_options = {**write_options, **tbl_properties}

            dest.write.options(**write_options) \
                .partitionBy(partition_columns) \
                .mode("overwrite") \
                .saveAsTable("test_table")
    
    def drop_partition(self, partition_filter:str):
        """
        Drops a partition of data from the delta table
        """
        self.DeltaTable.delete(partition_filter)

    def drop_table(self):
        """
        Optimized table drop that directly deletes the table from storage
        """
        self.SparkUtils.fs.rm(self.OneLakeLocation, recurse=True)
    
    def optimize_and_vacuum(self, partition_filter:str = None):
        """
        Combined optimize and vacuum
        """
        self.optimize(partition_filter)
        self.vacuum()
    
    def optimize(self, partition_filter:str = None):
        """
        Table optimize to compact small files
        """
        if partition_filter:
            self.DeltaTable.optimize().where(partition_filter).executeCompaction()
        else:
            self.DeltaTable.optimize().executeCompaction()

    def vacuum(self):
        """
        Sync vacuum with rentention forced to zero
        """
        self.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.DeltaTable.vacuum(0)
        self.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
