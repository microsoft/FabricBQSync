from delta.tables import *
from pyspark.sql import SparkSession, DataFrame
import warnings

from ..Model.Config import *
from ..Core import *
from ..Logging import *
from ..Exceptions import SuncLoadError
from ..Warnings import SyncTableRequiresOptimizeWarning

class DeltaTableMaintenance:
    __detail:Row = None

    def __init__(self, context:SparkSession, table_nm:str):
        self.Context = context
        self.TableName = table_nm
        self.DeltaTable = DeltaTable.forName(context, table_nm)
        self.Logger = SyncLogger(context).get_logger()
    
    @property
    def CurrentTableVersion(self) -> int:
        history = self.get_table_history() \
            .select(max(col("version")).alias("delta_version"))

        return [r[0] for r in history.collect()][0]

    @property
    def Detail(self) -> DataFrame:
        if not self.__detail:
            self.__detail = self.DeltaTable.detail().collect()[0]
        
        return self.__detail


    @property
    def OneLakeLocation(self) -> str:
        return self.Detail["location"]

    @property
    def DefaultTableProperties(self) -> dict:
        return {
            'delta.enableTypeWidening':'true',
            'delta.columnMapping.mode':'name',
            'delta.minReaderVersion':'3',
            'delta.minWriterVersion':'7'
        }        

    def set_default_table_properties(self):
        properties = [f"'{k}'='{v}'" for k, v in self.DefaultTableProperties]
        sql = f"ALTER TABLE {self.TableName} SET TBLPROPERTIES ({','.join(properties)});"
        print(sql)
        self.Context.sql(sql)

    def get_table_history(self, only_current_day:bool = False) -> DataFrame:
        history = self.DeltaTable.history()

        if only_current_day:
            history = history.filter("CAST(timestamp AS DATE) = current_date()")

        return history

    def schemaDiff(self, src: DataFrame, dest: DataFrame):
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
    
    def get_widening_type_map(self):
        map = {}

        map["byte"] = "short,int".split(",")
        map["short"] = "int".split(",")

        ## - Reserved for Delta 4.0 support
        #map["byte"] = "short,int,long,decimal,double".split(",")
        #map["short"] = "int,long,decimal,double".split(",")
        #map["int"] = "long,decimal,double".split(",")
        #map["long"] = "decimal".split(",")
        #map["float"] = "double".split(",")
        #map["decimal"] = "decimal".split(",")
        #map["date"] = "timestampNTZ".split(",")

        return map

    def can_widen_type(self, src, dest):
        type_map = self.get_widening_type_map()

        try:
            m = type_map[src]
            return dest in m
        except KeyError:
            return False

    def evolve_schema(self, src:DataFrame):
        manual_evolution = False
        dest = self.DeltaTable.toDF()
        destSchema = {x[0]:x[1] for x in dest.dtypes}

        new_columns, removed_columns, updated_columns  = self.schemaDiff(src, dest)

        if removed_columns or updated_columns:
            partition_columns = self.Detail["partitionColumns"]
            tbl_properties = self.Detail["properties"]

            if removed_columns:
                self.Logger.Sync_Status("Resolving schema differences that require table overwrite...")
                rpc = list(set(removed_columns) & set(partition_columns))

                if rpc:
                    raise SyncLoadError("Schema Evolution Violation: Partition Column cannot be dropped")                    

            if updated_columns:
                for c in updated_columns:
                    if not self.can_widen_type(destSchema[c], updated_columns[c]):
                        self.Logger.Sync_Status(f"Manually changing column {c} type to {updated_columns[c]}...")
                        manual_evolution = True
                        dest = dest.withColumn(c, col(c).cast(updated_columns[c]))
                
            if manual_evolution:
                write_options = {"overwriteSchema":"true"}
                write_options = {**write_options, **tbl_properties}

                self.Logger.Sync_Status(f"Manual schema evoluation required. Rewriting {self.TableName} with converted data types...")
                try:
                    dest.write.options(**write_options).partitionBy(partition_columns).mode("overwrite").saveAsTable(self.TableName)
                    warnings.warn(f"{self.TableName} schema evolved manually. Table maintenance is recommended.", 
                        SyncTableRequiresOptimizeWarning)
                except Exception as e:
                    raise SyncLoadError("Failed to manually evolve table schema") from e
    
    def drop_partition(self, partition_filter:str):
        self.DeltaTable.delete(partition_filter)

    def drop_table(self):
        self.Context.sql(f"DROP TABLE IF EXISTS {self.TableName}")
    
    def optimize_and_vacuum(self, partition_filter:str = None):
        self.optimize(partition_filter)
        self.vacuum()
    
    def optimize(self, partition_filter:str = None):
        if partition_filter:
            self.DeltaTable.optimize().where(partition_filter).executeCompaction()
        else:
            self.DeltaTable.optimize().executeCompaction()

    def vacuum(self):
        self.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.DeltaTable.vacuum(0)
        self.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")