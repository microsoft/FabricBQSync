class SyncSchedule:
    EndTime = None
    SourceRows = 0
    DestRows = 0
    InsertedRows = 0
    UpdatedRows = 0
    DeltaVersion = None
    SparkAppId = None
    MaxWatermark = None
    Status = None

    def __init__(self, row):
        self.Row = row
        self.StartTime = datetime.now(timezone.utc)
        self.ScheduleId = row["schedule_id"]
        self.LoadStrategy = row["load_strategy"]
        self.LoadType = row["load_type"]
        self.InitialLoad = row["initial_load"]
        self.ProjectId = row["project_id"]
        self.Dataset = row["dataset"]
        self.TableName = row["table_name"]
        self.SourceQuery = row["source_query"]
        self.MaxWatermark = row["max_watermark"]
        self.IsPartitioned = row["is_partitioned"]
        self.PartitionColumn = row["partition_column"]
        self.PartitionType = row["partition_type"]
        self.PartitionGrain = row["partition_grain"]
        self.WatermarkColumn = row["watermark_column"]
        self.LastScheduleLoadDate = row["last_schedule_dt"]
        self.Lakehouse = row["lakehouse"]
        self.DestinationTableName = row["lakehouse_table_name"]
        self.PartitionId = row["partition_id"]
    
    @property
    def SummaryLoadType(self):
        if self.InitialLoad:
            return SyncConstants.INITIAL_FULL_OVERWRITE
        else:
            return "{0}_{1}".format(self.LoadStrategy, self.LoadType)
    
    @property
    def Mode(self):
        if self.InitialLoad:
            return SyncConstants.OVERWRITE
        else:
            return self.LoadType
    
    @property
    def PrimaryKey(self):
        if self.Row["primary_keys"]:
            return self.Row["primary_keys"][0]
        else:
            return None
    
    @property
    def LakehouseTableName(self):
        return "{0}.{1}".format(self.Lakehouse, self.DestinationTableName)
        
    @property
    def BQTableName(self):
        return "{0}.{1}.{2}".format(self.ProjectId, self.Dataset, self.TableName)

    @property
    def IsTimeIngestionPartitioned(self):
        is_time = False

        if self.PartitionColumn == "_PARTITIONTIME" or self.PartitionColumn == "_PARTITIONDATE":
            is_time = True;

        return is_time;


    def UpdateRowCounts(self, src, dest, insert, update):
        self.SourceRows += src
        self.DestRows += dest

        match self.LoadStrategy:
            case SyncConstants.WATERMARK:
                self.InsertedRows += src     
            case SyncConstants.PARTITION:
                self.InsertedRows += dest  
            case _:
                self.InsertedRows += dest

        self.UpdatedRows = 0