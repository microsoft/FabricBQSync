from pydantic import Field
from typing import List, Optional
from pyspark.sql.types import *
from pyspark.sql import DataFrame, SparkSession
import json

from .Config import *
class BQQueryModelPredicate(SyncBaseModel):
    Type:str = Field(alias="type", default=str(PredicateType.AND))
    Predicate:str = Field(alias="predicate", default=None)

class BQQueryModel(SyncBaseModel):
    ProjectId:str = Field(alias="ProjectId", default=None)
    Dataset:str = Field(alias="Dataset", default=None)
    TableName:str = Field(alias="TableName", default=None)
    Query:Optional[str] = Field(alias="Query", default=None)
    PartitionFilter:str = Field(alias="PartitionFilter", default=None)
    Predicate:Optional[List[BQQueryModelPredicate]] = Field(alias="Predicate", default=None)
    API:str = Field(alias="API", default=str(BigQueryAPI.STORAGE))

    def add_predicate(self, predicate:str, type:PredicateType=PredicateType.AND):
        d = {"predicate": predicate, "type":str(type)}
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
    def define(view:SchemaView, columns:str, schema:str):
        d = {
            "view": str(view),
            "table_name": f"BQ_{view}".replace(".", "_"),
            "columns": columns,
            "schema": schema
        }
        return InformationSchemaModel(**d)
    
    def get_base_sql(self, project:str, dataset:str, alias:str = None):
        if alias:
            cols = [f"{alias}.{c}" for c in self.Columns.split(",")]

            return f"SELECT {','.join(cols)} FROM {project}.{dataset}.{str(self.View)} AS {alias}"
        else:
            return f"SELECT {self.Columns} FROM {project}.{dataset}.{str(self.View)}"

    def get_df_schema(self):
        if self.Schema:
            return StructType.fromJson(json.loads(self.Schema))
        else:
            return None

    def get_empty_df(self, context:SparkSession) -> DataFrame:
        return context.createDataFrame(data = context.sparkContext.emptyRDD(), schema = self.get_df_schema())