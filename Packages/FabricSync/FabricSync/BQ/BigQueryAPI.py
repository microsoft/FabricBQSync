from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import json
import base64 as b64
import uuid

from google.cloud import bigquery
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Model import ConfigDataset, BQQueryModel
from FabricSync.BQ.Validation import SqlValidator

class BigQueryClient(ContextAwareBase):
    def __init__(self, config:ConfigDataset) -> None:
        """
        Initializes a new instance of the BigQueryClient class.
        """
        self.UserConfig = config

    def __get_bq_reader_config(self, query:BQQueryModel) -> dict:
        """
        Returns the configuration dictionary for the BigQuery Spark Connector.
        Parameters:
            partition_filter (str, optional): Filter for tables that have mandatory partition filters or when reading table partitions.
        Returns:
            dict: Configuration dictionary containing Spark Reader options for the BigQuery Spark Connector, including:
                - credentials: GCP service account credentials.
                - viewsEnabled: Set to "true" to enable reading queries, views, or information schema.
                - materializationProject (optional): Billing project ID where views will be materialized to temporary tables for storage API.
                - materializationDataset (optional): Dataset where views will be materialized to temporary tables for storage API.
                - parentProject (optional): Billing project ID for API transaction costs, defaults to service account project ID if not specified.
                - filter (optional): Filter for tables that have mandatory partition filters or when reading table partitions.
        """
        cfg = {
            "project": query.ProjectId,
            "dataset": query.Dataset,
            "credentials" : self.GCPCredential,
            "viewsEnabled" : "true",
            "traceJobId" : f"FABRIC_SYNC_{self.ID}_{uuid.uuid4()}",
            "bigQueryJobLabel.msjobtype": "fabricsync",
            "bigQueryJobLabel.msjobgroup": Util.remove_special_characters(self.ID.lower())
        }
    
        if self.UserConfig.GCP.API.MaterializationProjectID:
            cfg["materializationProject"] = self.UserConfig.GCP.API.MaterializationProjectID
        else:
            cfg["materializationProject"] = query.ProjectId
        
        if self.UserConfig.GCP.API.MaterializationDataset:
            cfg["materializationDataset"] = self.UserConfig.GCP.API.MaterializationDataset
        else:
            cfg["materializationDataset"] = query.Dataset

        if self.UserConfig.GCP.API.BillingProjectID:
            cfg["parentProject"] = self.UserConfig.GCP.API.BillingProjectID
        else:
            cfg["parentProject"] = query.ProjectId

        if query.PartitionFilter:
            cfg["filter"] = query.PartitionFilter
        
        return cfg

    def read_from_storage_api(self, query:BQQueryModel) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame.
        Parameters:
            query (BQQueryModel): The query model.
        Returns:
            DataFrame: The DataFrame.
        """
        cfg = self.__get_bq_reader_config(query)
        q = query.TableName if not query.Query else query.Query

        if query.Predicate:
            q = self.__build_bq_query(query)

        return self.Context.read.format("bigquery").options(**cfg).load(q)

    def read_from_standard_api(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the standard BigQuery API.
        Parameters:
            query (BQQueryModel): The query model.
            schema (StructType, optional): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        sql_query = self.__build_bq_query(query)

        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential)

        return bq_client.read_to_dataframe(sql_query, schema)

    def read_from_exported_bucket(self, query:BQQueryModel) -> DataFrame:
        gcs_path = self.__get_gcs_storage_path(query)
        self.__export_bq_table(query, gcs_path)

        return self.Context.read.format("parquet").load(gcs_path)

    def __export_bq_table(self, query:BQQueryModel, gcs_path:str) -> str:
        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential)

        export_query = f"""
        EXPORT DATA OPTIONS(
        uri='{gcs_path}/*.snappy.parquet',
        format='PARQUET',
        compression='SNAPPY',
        overwrite=true) AS
        {self.__build_bq_query(query)}
        """

        query_job = bq_client.run_query(export_query)
        job_result = query_job.result()

    def __get_gcs_storage_path(self, query:BQQueryModel) -> str:
        uri = self.UserConfig.GCP.Storage.BucketUri
        prefix = self.UserConfig.GCP.Storage.PrefixPath

        if prefix:
            return f"{uri}/{prefix}/fabric_sync/{query.ProjectId}/{query.Dataset}/{query.TableName}"
        else:
            return f"{uri}/fabric_sync/{query.ProjectId}/{query.Dataset}/{query.TableName}"

    def __build_bq_query(self, query:BQQueryModel) -> str:
        """
        Builds a valid BigQuery SQL statement based on the provided BQQueryModel.
        If the provided query is invalid, a default "SELECT * FROM <TableName>" 
        statement is used.  Partition filters and user-defined predicates are 
        appended as needed. 
        Args:
            query (BQQueryModel): Object containing query, table name, and 
                partition/predicate details.
        Returns:
            str: A valid SQL SELECT statement reflecting any filters or predicates.
        """
        
        sql = query.Query if query.Query else query.TableName

        if not SqlValidator.is_valid(sql):
            sql = f"SELECT * FROM {query.TableName}"

        if query.PartitionFilter:
            query.add_predicate(query.PartitionFilter)

        if query.Predicate:
            p = [f"{p.Type} {p.Predicate}" for p in query.Predicate]
            predicates = " ".join(p)

            if not SqlValidator.has_predicate(sql):  
                idx = predicates.find(" ")          
                sql = f"{sql} WHERE {predicates[idx+1:]}"
            else:
                sql = f"{sql} {predicates}"

        return sql

class BigQueryStandardClient(ContextAwareBase):
    def __init__(self, project_id:str, credentials:str) -> None:
        """
        Initializes a new instance of the BigQueryStandardClient class.
        Args:
            project_id (str): The project ID.
            credentials (str): The credentials.
        """
        key = json.loads(b64.b64decode(credentials))
        bq_credentials = gcpCredentials.from_service_account_info(key)

        self.project_id = project_id
        self.client = bigquery.Client(project=project_id, credentials=bq_credentials)

        self.job_config = bigquery.QueryJobConfig(
            labels={
                'msjobtype': 'fabricsync',
                'msjobgroup': Util.remove_special_characters(self.ID.lower())
            }
        )

    def run_query(self, sql:str):
        query = self.client.query(sql, 
            job_id=f"FABRIC_SYNC_{self.ID}_{uuid.uuid4()}", 
            job_retry=None,
            job_config=self.job_config)
            
        return query

    def read_to_dataframe(self, sql:str, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given SQL query into a DataFrame.
        Args:
            sql (str): The SQL query.
            schema (StructType): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        bq = self.run_query(sql).to_dataframe()

        if not bq.empty:
            return self.Context.createDataFrame(bq, schema=schema)
        else:
            return None