from pyspark.sql import DataFrame # type: ignore
from pyspark.sql.types import StructType # type: ignore

import json
import base64 as b64
import uuid

from threading import Lock


from google.cloud import bigquery
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Model.Core import BQQueryModel
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Validation import SqlValidator
from FabricSync.BQ.GoogleStorageAPI import BucketStorageClient
from FabricSync.BQ.Threading import QueueProcessor
from FabricSync.BQ.Logging import SyncLogger

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

        #print(q)
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
        """
        Reads the data from the given BigQuery query into a DataFrame using the exported bucket.
        Parameters:
            query (BQQueryModel): The query model.
        Returns:
            DataFrame: The DataFrame.
        """
        bucket_client = BucketStorageClient(self.UserConfig, self.GCPCredential)
        gcs_path = bucket_client.get_storage_path(query.ScheduleId, query.TaskId)
        
        self.__export_bq_table(query, gcs_path)

        return self.Context.read.format("parquet").load(gcs_path)

    def read_from_standard_export(self, query:BQQueryModel) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the standard BigQuery API export.
        Parameters:
            query (BQQueryModel): The query model.
        Returns:
            DataFrame: The DataFrame.
        """
        sql_query = self.__build_bq_query(query)
        bq_client = BigQueryStandardExport(query.ProjectId, self.GCPCredential)

        return bq_client.export(sql_query, 
            num_threads=self.UserConfig.Async.Parallelism, 
            num_partitions=self.UserConfig.Optimization.StandardAPIExport.ResultPartitions, 
            page_size=self.UserConfig.Optimization.StandardAPIExport.PageSize)

    def __export_bq_table(self, query:BQQueryModel, gcs_path:str) -> str:
        """
        Exports the BigQuery table to a Google Cloud Storage bucket in Parquet format.
        Parameters:
            query (BQQueryModel): The query model containing the table name and other details.
            gcs_path (str): The Google Cloud Storage path where the exported data will be stored.
        Returns:
            str: The Google Cloud Storage path where the exported data is stored."""
        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential)

        export_query = f"""
        EXPORT DATA OPTIONS(
        uri='{gcs_path}/*.snappy.parquet',
        format='PARQUET',
        compression='SNAPPY',
        overwrite=true) AS
        {self.__build_bq_query(query)}
        """
        
        #print(export_query)

        query_job = bq_client.run_query(export_query)
        query_job.result()

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
            sql = f"SELECT * FROM `{query.TableName}`"

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
        self.credentials = credentials
        self.project_id = project_id
        self.__client:bigquery.Client = None

    @property
    def _client(self) -> bigquery.Client:
        """
        Returns the BigQuery client instance.
        If the client is not initialized, it creates a new instance using the provided credentials.
        Returns:
            bigquery.Client: The BigQuery client instance.
        """
        if not self.__client:
            key = json.loads(b64.b64decode(self.credentials))
            gcp_credentials = gcpCredentials.from_service_account_info(key)        
            self.__client = bigquery.Client(project=self.project_id, credentials=gcp_credentials)

        return self.__client

    @property
    def _job_config(self) -> bigquery.QueryJobConfig:
        """
        Returns the job configuration for the BigQuery query.
        The configuration includes options for allowing large results and setting labels for the job.
        Returns:
            bigquery.QueryJobConfig: The job configuration for the BigQuery query.
        """
        return bigquery.QueryJobConfig(
            allow_large_results=True,
            labels={
                'msjobtype': 'fabricsync',
                'msjobgroup': Util.remove_special_characters(self.ID.lower())
            }
        )

    def run_query(self, sql:str):
        """
        Runs the given SQL query using the BigQuery client.
        Args:
            sql (str): The SQL query to run.
        Returns:
            bigquery.QueryJob: The query job object containing the results of the query.
        """
        query = self._client.query(sql, 
            job_id=f"FABRIC_SYNC_{self.ID}_{uuid.uuid4()}", 
            job_retry=None,
            job_config=self._job_config)
            
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

class BigQueryStandardExport(BigQueryStandardClient):
    def __init__(self, project_id:str, credentials:str) -> None:
        """
        Initializes a new instance of the BigQueryStandardExport class.
        Args:
            project_id (str): The project ID.
            credentials (str): The credentials.
        """
        super().__init__(project_id, credentials)

        self.__query_job = None
        self.__data_df:DataFrame = None
        self.__export_row_count = 0
        self.__lock = Lock()
        self.__log = SyncLogger.getLogger()

    def __process_page(self, page:int):
        """
        Processes a single page of results from the BigQuery query.
        Args:
            page (int): The page number to process.
        """
        self.__log.sync_status(f"STANDARD API EXPORT - PROCESSING PAGE - {page}...", verbose=False)

        df = self.__query_job.result(
            page_size=self.page_size,
            start_index=(page - 1) * self.page_size,
            max_results=self.page_size
            ).to_dataframe()
        
        if not df.empty:
            row_count = len(df)
            sdf = self.Context.createDataFrame(df)

            with self.__lock:
                self.__export_row_count += row_count
                if self.__data_df:
                    self.__data_df = self.__data_df.union(sdf)
                else:
                    self.__data_df = sdf
    
    def __thread_exception_handler(self, value) -> None:
        """
        Handles exceptions raised by the thread processor.
        Args:
            value (Exception): The exception raised by the thread.
        """
        self.__log.error(f"STANDARD API EXPORT - THREAD ERROR - {value}...", verbose=False)
    
    def export(self, sql:str, num_threads:int = 10, num_partitions:int = 1, page_size:int = 100000) -> DataFrame:
        """
        Exports the data from the given SQL query using the standard BigQuery API.
        Args:
            sql (str): The SQL query.
            num_threads (int, optional): The number of threads to use for processing. Defaults to 10.
            num_partitions (int, optional): The number of partitions for the result DataFrame. Defaults to 1.
            page_size (int, optional): The size of each page to process. Defaults to 100000.
        Returns:
            DataFrame: The DataFrame containing the exported data.
        """
        self.page_size = page_size
        self.__query_job = self.run_query(sql)
        results = self.__query_job.result()

        total_rows = results.total_rows

        rows = 0
        page = 1
        processor = QueueProcessor(num_threads=num_threads)

        while(rows <= total_rows):
            rows = page * self.page_size
            processor.put(page)

            #print(f"{page}-{(page - 1) * self.page_size}-{rows}-{total_rows}")
            page += 1 
        
        if not processor.empty():
            processor.process(self.__process_page, self.__thread_exception_handler)

            if not processor.has_exceptions:
                self.__log.sync_status(f"STANDARD API EXPORT - SUCCESS - EXPORTED {self.__export_row_count} ROWS...", verbose=False)
                return self.__data_df.repartition(num_partitions)
            else:
                self.__log.sync_status(f"STANDARD API EXPORT - FAILED...", verbose=False)
                return None