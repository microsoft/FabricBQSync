from dataclasses import dataclass
from pydantic import Field
import json

from typing import (
    List, Optional, Any
)
from pyspark.sql.types import (
    StructField, StructType, StringType
)
from pyspark.sql import (
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
    name: str
    path: str
    mod_time: int
    is_dir: bool
    fs_type: FileSystemType
    
class BQQueryModelPredicate(SyncBaseModel):
    Type:PredicateType = Field(alias="type", default=PredicateType.AND)
    Predicate:str = Field(alias="predicate", default=None)

class BQQueryModel(SyncBaseModel):
    ProjectId:str = Field(alias="ProjectId", default=None)
    Dataset:str = Field(alias="Dataset", default=None)
    TableName:str = Field(alias="TableName", default=None)
    Query:Optional[str] = Field(alias="Query", default=None)
    PartitionFilter:str = Field(alias="PartitionFilter", default=None)
    Predicate:Optional[List[BQQueryModelPredicate]] = Field(alias="Predicate", default=None)
    API:BigQueryAPI = Field(alias="API", default=BigQueryAPI.STORAGE)

    Cached:bool = Field(alias="Cached", default=True)

    def add_predicate(self, predicate:str, type:PredicateType=PredicateType.AND) -> None:
        d = {"predicate": predicate, "type":type.value}
        p = BQQueryModelPredicate(**d)

        if self.Predicate:
            self.Predicate.append(p)
        else:
            self.Predicate = [p]      

class InformationSchemaModel(SyncBaseModel):
    TableName:Optional[str] = Field(alias="table_name", default=None)
    View:Optional[str] = Field(alias="view", default=None)
    Columns:Optional[str] = Field(alias="columns", default=None)
    Schema:Optional[str] = Field(alias="schema", default=None)

    @staticmethod
    def define(view:SchemaView, columns:str, schema:str) -> "InformationSchemaModel":
        d = {
            "view": view.value,
            "table_name": view.name.lower(),
            "columns": columns,
            "schema": schema
        }
        return InformationSchemaModel(**d)
    
    def get_base_sql(self, sync_id:str, project:str, dataset:str, alias:str = None) -> str:
        if alias:
            cols = [f"{alias}.{c}" for c in self.Columns.split(",")]

            return f"SELECT '{sync_id}' AS sync_id, {','.join(cols)} FROM {project}.{dataset}.{self.View} AS {alias}"
        else:
            return f"SELECT '{sync_id}' AS sync_id, {self.Columns} FROM {project}.{dataset}.{self.View}"

    def get_df_schema(self) -> StructType:
        if self.Schema:
            sync_id_col = [StructField("sync_id", StringType(), True)] 
            table_schema = StructType.fromJson(json.loads(self.Schema))
            return StructType(sync_id_col + table_schema.fields)
        else:
            return None

    def get_empty_df(self, context:SparkSession) -> DataFrame:
        return context.createDataFrame(data = context.sparkContext.emptyRDD(), schema = self.get_df_schema())

@dataclass(order=True)
class CommandSet:
    Priority:int
    Commands:Any=field(compare=False)

    def __init__(self, cmd=None) -> None:
        self.Priority=1
        if isinstance(cmd, list):
            self.Commands = cmd
        elif isinstance(cmd, str):
            self.Commands = []
            self.Commands.append(cmd)
        else:
            self.Commands = []