from dataclasses import dataclass
from pydantic import Field
import json

from typing import (
    List, Optional, Any
)
from pyspark.sql.types import ( # type: ignore
    StructField, StructType, StringType
)
from pyspark.sql import ( # type: ignore
    DataFrame, SparkSession
)
from dataclasses import (
    dataclass, field
)

from FabricSync.BQ.Model.Config import SyncBaseModel
from FabricSync.BQ.Enum import (
    PredicateType, SchemaView, BigQueryAPI, FileSystemType
)

@dataclass
class HDFSFile:
    """
    HDFSFile is a dataclass that represents a file in HDFS (Hadoop Distributed File System).
    It includes attributes such as the file name, path, modification time, whether it is a directory,
    and the file system type.
    Attributes:
        name (str): The name of the file.
        path (str): The path to the file in HDFS.
        mod_time (int): The last modification time of the file in milliseconds since epoch.
        is_dir (bool): A boolean indicating whether the file is a directory.
        fs_type (FileSystemType): The type of file system, e.g., HDFS or local.
    """
    name: str
    path: str
    mod_time: int
    is_dir: bool
    fs_type: FileSystemType
    
class BQQueryModelPredicate(SyncBaseModel):
    """
    BQQueryModelPredicate is a Pydantic model that represents a predicate for a BigQuery query.
    It includes fields for the type of predicate (AND/OR) and the predicate string itself.
    Attributes:
        Type (PredicateType): The type of the predicate, either AND or OR.
        Predicate (str): The predicate string to be applied in the query.
    """
    Type:PredicateType = Field(alias="type", default=PredicateType.AND)
    Predicate:str = Field(alias="predicate", default=None)

class BQQueryModel(SyncBaseModel):
    """
    BQQueryModel is a Pydantic model that represents a BigQuery query configuration.
    It includes fields for the schedule ID, project ID, task ID, dataset, table name,
    query, partition filter, predicates, and API type.
    Attributes:
        ScheduleId (str): The ID of the schedule associated with the query.
        ProjectId (str): The ID of the BigQuery project.
        TaskId (str): The ID of the task associated with the query.
        Location (str): The location of the BigQuery resources.
        Dataset (str): The name of the dataset in BigQuery.
        TableName (str): The name of the table in BigQuery.
        Query (Optional[str]): The SQL query to be executed. Defaults to None.
        PartitionFilter (str): The filter to apply to partitions in the query.
        Predicate (Optional[List[BQQueryModelPredicate]]): A list of predicates to apply to the query. Defaults to None.
        API (BigQueryAPI): The API type to use for the query. Defaults to BigQueryAPI.STORAGE.
        Cached (bool): Indicates whether the query results should be cached. Defaults to True.
        Metadata (bool): Indicates whether to include metadata in the query results. Defaults to False.
        UseForceBQJobConfig (bool): Indicates whether to use a forced BigQuery job configuration. Defaults to False.
    Methods:
        add_predicate(predicate:str, type:PredicateType=PredicateType.AND) -> None:
            Adds a predicate to the BQQueryModel.
    """
    ScheduleId:str = Field(alias="ScheduleId", default=None)
    ProjectId:str = Field(alias="ProjectId", default=None)
    TaskId:str = Field(alias="TaskId", default=None)
    Dataset:str = Field(alias="Dataset", default=None)
    TableName:str = Field(alias="TableName", default=None)
    TempTableId:str = Field(alias="TempTableId", default=None)
    Query:Optional[str] = Field(alias="Query", default=None)
    PartitionFilter:str = Field(alias="PartitionFilter", default=None)
    Predicate:Optional[List[BQQueryModelPredicate]] = Field(alias="Predicate", default=None)
    API:BigQueryAPI = Field(alias="API", default=BigQueryAPI.STORAGE)
    Cached:bool = Field(alias="Cached", default=True)
    Metadata:bool = Field(alias="Metadata", default=False)
    UseForceBQJobConfig:bool = Field(alias="UseForceBQJobConfig", default=False)

    def add_predicate(self, predicate:str, type:PredicateType=PredicateType.AND) -> None:
        """
        Adds a predicate to the BQQueryModel.
        Args:
            predicate (str): The predicate string to be added.
            type (PredicateType, optional): The type of the predicate (AND/OR). Defaults to PredicateType.AND.
        """
        d = {"predicate": predicate, "type":type.value}
        p = BQQueryModelPredicate(**d)

        if self.Predicate:
            self.Predicate.append(p)
        else:
            self.Predicate = [p]      

class InformationSchemaModel(SyncBaseModel):
    """
    InformationSchemaModel is a Pydantic model that represents a schema view in BigQuery.
    It includes fields for the view name, table name, columns, and schema.
    It provides methods to define the model from a SchemaView, generate SQL queries, and retrieve DataFrame schemas.
    Attributes:
        View (str): The name of the schema view.
        TableName (str): The name of the table associated with the view.
        Columns (str): A comma-separated string of column names in the view.
        Schema (str): A JSON string representing the schema of the view.
    """
    TableName:Optional[str] = Field(alias="table_name", default=None)
    View:Optional[str] = Field(alias="view", default=None)
    Columns:Optional[str] = Field(alias="columns", default=None)
    Schema:Optional[str] = Field(alias="schema", default=None)

    @staticmethod
    def define(view:SchemaView, columns:str, schema:str) -> "InformationSchemaModel":
        """
        Defines an InformationSchemaModel from a SchemaView, columns, and schema.
        Args:
            view (SchemaView): The SchemaView object containing the view name.
            columns (str): A comma-separated string of column names in the view.
            schema (str): A JSON string representing the schema of the view.
        Returns:
            InformationSchemaModel: An instance of InformationSchemaModel initialized with the provided parameters.
        """
        d = {
            "view": view.value,
            "table_name": view.name.lower(),
            "columns": columns,
            "schema": schema
        }
        return InformationSchemaModel(**d)
    
    def get_base_sql(self, sync_id:str, path:str, alias:str = None) -> str:
        """
        Generates a base SQL query string for the schema view.
        Args:
            sync_id (str): The synchronization ID to be included in the query.
            location (str): The BigQuery location.
            project (str): The BigQuery project ID.
            dataset (str): The BigQuery dataset name.
            alias (str, optional): An alias for the schema view. Defaults to None.
        Returns:
            str: A SQL query string that selects the sync_id and specified columns from the schema view.
        """
        if alias:
            cols = [f"{alias}.{c}" for c in self.Columns.split(",")]

            return f"SELECT '{sync_id}' AS sync_id, {','.join(cols)} FROM `{path}` AS {alias} "
        else:
            return f"SELECT '{sync_id}' AS sync_id, {self.Columns} FROM `{path}` "

    def get_df_schema(self) -> StructType:
        """
        Generates a DataFrame schema based on the model's schema.
        Returns:
            StructType: A StructType object representing the DataFrame schema, including a sync_id field.
        """
        if self.Schema:
            sync_id_col = [StructField("sync_id", StringType(), True)] 
            table_schema = StructType.fromJson(json.loads(self.Schema))
            return StructType(sync_id_col + table_schema.fields)
        else:
            return None

    def get_empty_df(self, context:SparkSession) -> DataFrame:
        """
        Creates an empty DataFrame with the schema defined in the model.
        Args:
            context (SparkSession): The Spark session to create the DataFrame in.
        Returns:
            DataFrame: An empty DataFrame with the schema defined in the model.
        """
        return context.createDataFrame(data = context.sparkContext.emptyRDD(), schema = self.get_df_schema())

@dataclass(order=True)
class CommandSet:
    """
    CommandSet is a dataclass that holds a set of commands with an associated priority.
    It is used to manage and execute commands in a prioritized manner.
    Attributes:
        Priority (int): The priority of the command set. Lower values indicate higher priority.
        Commands (Any): The commands to be executed, can be a list or a single command string.
    """
    Priority:int
    Commands:Any=field(compare=False)

    def __init__(self, cmd=None) -> None:
        """
        Initializes a CommandSet instance with a default priority and commands.
        Args:
            cmd (Any, optional): The commands to be included in the CommandSet. 
                                 Can be a list of commands or a single command string.
                                 Defaults to None.
        """
        self.Priority=1
        if isinstance(cmd, list):
            self.Commands = cmd
        elif isinstance(cmd, str):
            self.Commands = []
            self.Commands.append(cmd)
        else:
            self.Commands = []