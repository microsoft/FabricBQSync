from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime, timezone

from ...Enum import *

class SyncSchedule:
    """
    Scheduled configuration object that also is used to track and store telemetry from load process
    """
    EndTime:datetime = None
    SourceRows:int = 0
    InsertedRows:int = 0
    UpdatedRows:int = 0
    DeltaVersion:str = None
    SparkAppId:str = None
    MaxWatermark:str = None
    Status = SyncStatus.SCHEDULED
    FabricPartitionColumns:list[str] = None

    def __init__(self, row:Row):
        """
        Scheduled load Configuration load from Data Row
        """
        self.Row = row
        self.StartTime = datetime.now(timezone.utc)
        self.SyncId = row["sync_id"]
        self.GroupScheduleId = row["group_schedule_id"]
        self.ScheduleId = row["schedule_id"]
        self.LoadStrategy = self.assign_enum_val(LoadStrategy, row["load_strategy"])
        self.LoadType = self.assign_enum_val(LoadType, row["load_type"])
        self.InitialLoad = row["initial_load"]
        self.LastScheduleLoadDate = row["last_schedule_dt"]
        self.Priority = row["priority"]
        self.ProjectId = row["project_id"]
        self.Dataset = row["dataset"]
        self.TableName = row["table_name"]
        self.ObjectType = self.assign_enum_val(BigQueryObjectType, row["object_type"])
        self.SourceQuery = row["source_query"]
        self.MaxWatermark = row["max_watermark"]
        self.WatermarkColumn = row["watermark_column"]
        self.IsPartitioned = row["is_partitioned"]
        self.PartitionColumn = row["partition_column"]
        self.PartitionType = self.assign_enum_val(PartitionType, row["partition_type"])
        self.PartitionGrain = row["partition_grain"]
        self.PartitionId = row["partition_id"]     
        self.PartitionDataType = self.assign_enum_val(BQDataType, row["partition_data_type"])  
        self.PartitionRange = row["partition_range"]
        self.RequirePartitionFilter = row["require_partition_filter"]
        self.Lakehouse = row["lakehouse"]
        self.LakehouseSchema = row["lakehouse_schema"]
        self.DestinationTableName = row["lakehouse_table_name"]
        self.UseLakehouseSchema = row["use_lakehouse_schema"]
        self.EnforcePartitionExpiration = row["enforce_expiration"]
        self.AllowSchemaEvolution = row["allow_schema_evolution"]
        self.EnableTableMaintenance = row["table_maintenance_enabled"]
        self.TableMaintenanceInterval = row["table_maintenance_interval"]
        self.FlattenTable = row["flatten_table"]
        self.FlattenInPlace = row["flatten_inplace"]
        self.ExplodeArrays = row["explode_arrays"]
        self.SummaryLoadType = None
    
    @property
    def TableOptions(self) -> dict[str, str]:
        """
        Returns the configured table options
        """
        opts = {}

        if self.Row["table_options"]:
            for r in self.Row["table_options"]:
                opts[r["key"]] = r["value"]
                
        return opts

    @property
    def DefaultSummaryLoadType(self) -> str:
        """
        Summarized the load strategy based on context
        """
        if self.InitialLoad and not self.IsTimeIngestionPartitioned and \
            not self.IsRangePartitioned and not (self.IsPartitioned and self.RequirePartitionFilter):
            return "INITIAL FULL_OVERWRITE"
        else:
            return f"{self.LoadStrategy}_{self.LoadType}"
    
    @property
    def Mode(self) -> str:
        """
        Returns the write mode based on context
        """
        if self.InitialLoad:
            return str(LoadType.OVERWRITE)
        else:
            return str(self.LoadType)
    
    @property
    def Keys(self) -> list[str]:
        """
        Returns list of keys
        """        
        if self.Row["primary_keys"]:
            return [k for k in self.Row["primary_keys"]]
        else:
            return None
        
    @property
    def PrimaryKey(self) -> str:
        """
        Returns the first instance of primary key. Only used for tables with a single primary key
        """        
        if self.Row["primary_keys"]:
            return self.Row["primary_keys"][0]
        else:
            return None

    @property
    def LakehouseTableName(self) -> str:
        """
        Returns the two-part Lakehouse table name
        """
        if self.UseLakehouseSchema:
            table_nm = f"{self.Lakehouse}.{self.LakehouseSchema}.{self.DestinationTableName}"
        else:
            table_nm = f"{self.Lakehouse}.{self.DestinationTableName}"

        return table_nm

    @property
    def BQTableName(self) -> str:
        """
        Returns the three-part BigQuery table name
        """
        return f"{self.ProjectId}.{self.Dataset}.{self.TableName}"


    @property
    def IsTimeIngestionPartitioned(self) -> bool:
        """
        Bool indicator for time ingestion tables
        """
        return self.LoadStrategy == LoadStrategy.TIME_INGESTION

    @property
    def IsRangePartitioned(self) -> bool:
        """
        Bol indicator for range partitioned tables
        """
        return self.LoadStrategy == LoadStrategy.PARTITION and self.PartitionType == PartitionType.RANGE
    
    @property
    def IsTimePartitionedStrategy(self) -> bool:
         """
         Bool indicator for the two time partitioned strategies
         """
         return ((self.LoadStrategy == LoadStrategy.PARTITION and 
            self.PartitionType == PartitionType.TIME) or self.LoadStrategy == LoadStrategy.TIME_INGESTION)
    
    @property
    def IsPartitionedSyncLoad(self) -> bool:
        """
        Determines if the sync load is partitioned.
        This method checks if the sync load is partitioned based on the following conditions:
        - The `IsPartitioned` attribute is True.
        - If the `IsTimePartitionedStrategy` attribute is True and `PartitionId` is set, or if the `IsRangePartitioned` attribute is True.
        Returns:
            bool: True if the sync load is partitioned, False otherwise.
        """

        if self.IsPartitioned:
            if (self.IsTimePartitionedStrategy and self.PartitionId) or self.IsRangePartitioned:
                return True
        return False
    
    def assign_enum_val(self, enum_class, value):
        """
        Assigns a value to an enum class.
        Args:
            enum_class (Enum): The enumeration class to which the value should be assigned.
            value (Any): The value to be assigned to the enum class.
        Returns:
            Enum or None: The corresponding enum member if the value is valid, otherwise None.
        """

        try:
            return enum_class(value)
        except ValueError:
            return None

    def UpdateRowCounts(self, src:int, insert:int = 0, update:int = 0):
        """
        Updates the telemetry row counts based on table configuration
        """
        self.SourceRows += src

        if not self.LoadType == LoadType.MERGE:
            self.InsertedRows += src            
            self.UpdatedRows = 0
        else:
            self.InsertedRows += insert
            self.UpdatedRows += update