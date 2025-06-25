from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, DecimalType
)
import json
import base64 as b64
import uuid

from threading import Lock

from google.cloud import bigquery
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Model.Core import BQQueryModel
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Validation import SqlValidator
from FabricSync.BQ.GoogleStorageAPI import BucketStorageClient
from FabricSync.BQ.Threading import QueueProcessor

class BigQueryClient(ContextAwareBase):
    """
    A class to interact with Google BigQuery.
    This class provides methods to read data from BigQuery using different APIs,
    such as the Storage API, Standard API, and Exported Bucket API.
    It also provides methods to submit queries, read data into DataFrames, and manage temporary tables.
    Attributes:
        UserConfig (ConfigDataset): The user configuration settings loaded from a JSON file.
    Methods:
        __init__(config:ConfigDataset) -> None:
            Initializes a new instance of the BigQueryClient class.
        __get_bq_reader_config(query:BQQueryModel) -> dict:
            Returns the configuration for reading from BigQuery.
        read_from_storage_api(query:BQQueryModel) -> DataFrame:
            Reads the data from the given BigQuery query into a DataFrame using the Storage API.
        read_from_standard_api(query:BQQueryModel, schema:StructType = None) -> DataFrame:
            Reads the data from the given BigQuery query into a DataFrame using the Standard API.
        read_from_exported_bucket(query:BQQueryModel) -> DataFrame:
            Reads the data from the given BigQuery query into a DataFrame using the Exported Bucket API.
        read_from_standard_export(query:BQQueryModel) -> DataFrame:
            Reads the data from the given BigQuery query into a DataFrame using the Standard API export.
        drop_temp_table(project_id:str, temp_table:str) -> None:
            Drops a temporary table in BigQuery.
        __export_bq_table(query:BQQueryModel, gcs_path:str) -> str:
            Exports the BigQuery table to a Google Cloud Storage bucket in Parquet format.
    Raises:
        SyncConfigurationError: If the GCP configuration is not set in the user configuration.
        BQConnectorError: If there is an error reading data from BigQuery.
    """
    def __init__(self, config:ConfigDataset) -> None:   
        """
        Initializes a new instance of the BigQueryClient class.
        Args:
            config (ConfigDataset): The user configuration settings loaded from a JSON file.
        Raises:
            SyncConfigurationError: If the GCP configuration is not set in the user configuration.  
        """
        self.UserConfig = config

    def __get_bq_reader_config(self, query:BQQueryModel) -> dict:
        """
        Returns the configuration for reading from BigQuery.
        Parameters:
            query (BQQueryModel): The query model.
        Returns:
            dict: The configuration dictionary including project, dataset, credentials, and other options.
        """
        cfg = {
            "project": query.ProjectId,
            "dataset": query.Dataset,
            "credentials" : self.GCPCredential,
            "viewsEnabled" : "true",
            "traceJobId" : f"FABRIC_SYNC_JOB_{uuid.uuid4()}".lower(),
            "parentProject" : query.ProjectId,
            "materializationProject" : query.ProjectId,
            "materializationDataset" : query.Dataset,
            "bigQueryJobLabel.msjobtype": "fabricsync",
            "bigQueryJobLabel.msjobgroup": self.SafeID,
            "bigQueryJobLabel.msclienttype": "spark",
            "bigQueryJobLabel.msjobclient": f"FABRIC_SYNC_CLIENT_{uuid.uuid4()}".lower()
        }

        p, l, d = self.UserConfig.GCP.resolve_materialization_path(query.ProjectId, query.Dataset)

        cfg["materializationProject"] = p if p else cfg["materializationProject"]        
        cfg["materializationDataset"] = d if d else cfg["materializationDataset"]

        if self.UserConfig.GCP.API.BillingProjectID:
            cfg["parentProject"] = self.UserConfig.GCP.API.BillingProjectID

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
        self.Logger.debug(f"BQ STORAGE API...")
        cfg = self.__get_bq_reader_config(query)

        if (self.UserConfig.GCP.API.ForceBQJobConfig or query.UseForceBQJobConfig) and not query.Metadata:
            self.Logger.debug(f"BQ STORAGE API - FORCE JOB CONFIG...")
            dataset_id, table_id = self.__submit_bq_query_job(cfg, query)

            if "filter" in cfg:
                del cfg["filter"]

            cfg["dataset"] = dataset_id

            q = table_id
        else:
            q = query.TableName if not query.Query else query.Query

            if query.Predicate:
                q = self.__build_bq_query(query)

        df = self.Context.read.format("bigquery").options(**cfg).load(q)

        if not df.isEmpty():
            return df

        return None

    def __submit_bq_query_job(self, cfg:dict, query:BQQueryModel):
        """
        Submits a BigQuery query job using the provided query model and client ID.
        Parameters:
            client_id (str): The client ID.
            query (BQQueryModel): The query model containing the project ID, dataset, and other details.
        Returns:
            str: The table ID of the destination table where the query results are stored.
        """
        self.Logger.debug(f"BQ SUBMIT QUERY JOB...")
        sql = self.__build_bq_query(query)

        location = self.UserConfig.GCP.get_dataset_location(query.ProjectId, cfg["materializationDataset"])
        self.Logger.debug(f"BQ SUBMIT QUERY JOB - {query.ProjectId}, {cfg['materializationDataset']} - LOCATION - {location}...")
        
        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential, location)

        destination_table = self.UserConfig.GCP.format_table_path(cfg["materializationProject"],
                                                                  cfg["materializationDataset"], 
                                                                  query.TempTableId)
        self.Logger.debug(f"BQ SUBMIT QUERY JOB - TEMP TABLE - {destination_table}...")

        job_config = bigquery.QueryJobConfig(
            allow_large_results=True,
            destination=destination_table,
            labels={
                'msjobtype': 'fabricsync',
                'msjobgroup': self.SafeID,
                'msclienttype': 'standard',
                'msjobclient': cfg["bigQueryJobLabel.msjobclient"]
            }
        )

        query_job = bq_client.run_query(sql, job_cfg=job_config)
        query_job.result()

        self.Logger.debug(f"BQ SUBMIT QUERY JOB - TEMP TABLE - {query_job.destination.dataset_id}.{query_job.destination.table_id}...")

        return (query_job.destination.dataset_id, query_job.destination.table_id)

    def read_from_standard_api(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the standard BigQuery API.
        Parameters:
            query (BQQueryModel): The query model.
            schema (StructType, optional): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        self.Logger.debug(f"BQ STANDARD API...")
        sql_query = self.__build_bq_query(query)

        location = self.UserConfig.GCP.get_dataset_location(query.ProjectId, query.Dataset)
        self.Logger.debug(f"BQ STANDARD API - {query.ProjectId}, {query.Dataset} - LOCATION - {location}...")

        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential, location)

        df = bq_client.read_to_dataframe(sql_query, schema)

        if df and not query.Metadata and query.TableName:
            df = bq_client.convert_bq_data_types(df, bq_client.get_bq_schema(query.TableName))

        return df

    def read_from_exported_bucket(self, query:BQQueryModel) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the exported bucket.
        Parameters:
            query (BQQueryModel): The query model.\
        Returns:
            DataFrame: The DataFrame.
        """
        self.Logger.debug(f"BQ EXPORTED BUCKET...")
        bucket_client = BucketStorageClient(self.UserConfig, self.GCPCredential)
        gcs_path = bucket_client.get_storage_path(query.ScheduleId, query.TaskId)
        
        self.__export_bq_table(query, gcs_path)

        df = self.Context.read.format("parquet").load(gcs_path)

        if not df.isEmpty():
            return df

        return None

    def read_from_standard_export(self, query:BQQueryModel) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the standard BigQuery API export.
        Parameters:
            query (BQQueryModel): The query model.
        Returns:
            DataFrame: The DataFrame.
        """
        self.Logger.debug(f"BQ STANDARD API EXPORT...")
        sql_query = self.__build_bq_query(query)
        
        location = self.UserConfig.GCP.get_dataset_location(query.ProjectId, query.Dataset)
        bq_client = BigQueryStandardExport(query.ProjectId, self.GCPCredential, location)

        return bq_client.export(sql_query, 
            num_threads=self.UserConfig.Async.Parallelism, 
            num_partitions=self.UserConfig.Optimization.StandardAPIExport.ResultPartitions, 
            page_size=self.UserConfig.Optimization.StandardAPIExport.PageSize)

    def drop_temp_tables(self, datasets:list[tuple[str,str]]) -> None:
        """
        Drops a temporary tables from a write dataset in BigQuery.
        Parameters:
            datasets (list[tuple[str,str]]): A list of tuples containing project ID and dataset name.
        """

        for project_id, dataset in datasets:
            if project_id and dataset:
                project,location,ds = self.UserConfig.GCP.resolve_materialization_path(project_id, dataset)

                project = project_id if not project else project
                ds = dataset if not ds else ds

                bq_client = BigQueryStandardClient(project_id, self.GCPCredential, location)

                query = f"""
                DECLARE tbl STRING;

                BEGIN
                FOR record IN (
                    SELECT  
                    CONCAT(table_catalog, ".", table_schema, ".", table_name) AS tbl
                    FROM `{project}.{ds}.INFORMATION_SCHEMA.TABLES`
                    WHERE table_name LIKE 'BQ_SYNC_%'
                ) DO

                    SET tbl = record.tbl;
                    EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s`;", tbl);
                END FOR;
                END;
                """

                try:
                    self.Logger.debug(f"BQ DROPPING TEMP TABLES - {project}.{ds} ...")

                    query_job = bq_client.run_query(query)
                    query_job.result()
                except Exception as e:
                    self.Logger.warning(f"WARNING - UNABLE TO DROP BQ TEMP TABLES - {project}.{ds} - This is most likely due to a permissions issue ...")

    def __export_bq_table(self, query:BQQueryModel, gcs_path:str) -> str:
        """
        Exports the BigQuery table to a Google Cloud Storage bucket in Parquet format.
        Parameters:
            query (BQQueryModel): The query model containing the table name and other details.
            gcs_path (str): The Google Cloud Storage path where the exported data will be stored.
        Returns:
            str: The Google Cloud Storage path where the exported data is stored."""
        location = self.UserConfig.GCP.get_dataset_location(query.ProjectId, query.Dataset)
        bq_client = BigQueryStandardClient(query.ProjectId, self.GCPCredential, location)

        export_query = f"""
        EXPORT DATA OPTIONS(
        uri='{gcs_path}/*.snappy.parquet',
        format='PARQUET',
        compression='SNAPPY',
        overwrite=true) AS
        {self.__build_bq_query(query)}
        """
        
        self.Logger.debug(f"BQ EXPORT QUERY -> {export_query}")

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
            if query.Query:
                self.Logger.debug(f"Invalid BQ Query: {query.Query}")
            
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

        self.Logger.debug(f"BQ QUERY -> {sql}")

        return sql

class BigQueryStandardClient(ContextAwareBase):
    """
    A class to interact with Google BigQuery using the standard API.
    This class provides methods to run queries, read data into DataFrames, and manage job configurations.
    Attributes:
        project_id (str): The project ID for the BigQuery client.
        credentials (str): The credentials for the BigQuery client.
        _client_id (str): A unique client ID for the BigQuery client.
        __client (bigquery.Client): The BigQuery client instance.
    Methods:
        __init__(project_id:str, credentials:str, location:str=None) -> None:
            Initializes a new instance of the BigQueryStandardClient class.
        generate_job_id() -> str:
            Generates a unique job ID for the BigQuery job.
        _client() -> bigquery.Client:
            Returns the BigQuery client instance.
        _job_config() -> bigquery.QueryJobConfig:
            Returns the job configuration for the BigQuery query.
        run_query(sql:str, job_id:str = None, job_cfg:bigquery.QueryJobConfig = None):
            Runs the given SQL query using the BigQuery client.
        read_to_dataframe(sql:str, schema:StructType = None) -> DataFrame:
            Reads the data from the given SQL query into a DataFrame.
    """
    def __init__(self, project_id:str, credentials:str, location:str=None) -> None:
        """
        Initializes a new instance of the BigQueryStandardClient class.
        Args:
            project_id (str): The project ID.
            credentials (str): The credentials.
            location (str, optional): The location for the BigQuery client. Defaults to None.
        Raises:
            SyncConfigurationError: If the GCP configuration is not set in the user configuration.
        This class provides methods to run queries, read data into DataFrames, and manage job configurations.
        """
        self.credentials = credentials
        self.project_id = project_id
        self.location = location
        self.__numeric_type = DecimalType(38,9)
        self.__big_numeric_type = DecimalType(76,38)

        self.__client:bigquery.Client = None

        self._client_id = f"FABRIC_SYNC_CLIENT_{uuid.uuid4()}"

    def generate_job_id(self) -> str:
        """
        Generates a unique job ID for the BigQuery job.
        Returns:
            str: A unique job ID in the format "FABRIC_SYNC_JOB_<uuid>".
        """
        return f"FABRIC_SYNC_JOB_{uuid.uuid4()}".lower()

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
            self.__client = bigquery.Client(
                project=self.project_id, 
                credentials=gcp_credentials,
                location=self.location)

        return self.__client

    @property
    def _job_config(self) -> bigquery.QueryJobConfig:
        """
        Returns the job configuration for the BigQuery query.
        The configuration includes options for allowing large results and setting labels for the job.
        Returns:
            bigquery.QueryJobConfig: The job configuration for the BigQuery query.
        """
        job_config = bigquery.QueryJobConfig(
            allow_large_results=True,
            labels={
                'msjobtype': 'fabricsync',
                'msjobgroup': self.SafeID,
                'msclienttype': 'standard',
                'msjobclient': self._client_id.lower()
            }
        )

        return job_config

    def get_bq_schema(self, table:str) -> dict[str, str]:
        """
        Retrieves the schema of a BigQuery table.
        Args:
            table (str): The fully qualified table name in the format "project.dataset.table".
        Returns:
            dict[str, str]: A dictionary mapping field names to their BigQuery data types.
        """
        tbl = self._client.get_table(table)
        return {f.name: f.field_type for f in tbl.schema}

    def convert_bq_data_types(self, df:DataFrame, table_schema:dict[str, str]) -> DataFrame:
        """
        Converts specific BigQuery data types in a DataFrame to their corresponding Spark data types.
        Args:
            df (DataFrame): The DataFrame containing the data to convert.
            table_schema (dict[str, str]): A dictionary mapping field names to their BigQuery data types.
        Returns:
            DataFrame: The DataFrame with the converted data types.
        """
        self.Logger.debug(f"STANDARD API - CONVERT BQ DATA TYPES ...")

        for f in df.schema.fields:
            if f.name in table_schema:
                bq_type = table_schema[f.name]

                if bq_type == "NUMERIC":
                    df = df.withColumn(f.name, df[f.name].cast(self.__numeric_type))     
                elif bq_type == "BIGNUMERIC":
                    df = df.withColumn(f.name, df[f.name].cast(self.__big_numeric_type))
        
        return df

    def run_query(self, sql:str, job_id:str = None, job_cfg:bigquery.QueryJobConfig = None):
        """
        Runs the given SQL query using the BigQuery client.
        Args:
            sql (str): The SQL query to run.
            job_id (str, optional): The job ID for the query. If not provided, a new job ID is generated.
            job_cfg (bigquery.QueryJobConfig, optional): The job configuration for the query. If not provided, the default job configuration is used.
        Returns:
            bigquery.QueryJob: The query job object containing the results of the query.
        """
        if not job_id:
            job_id = self.generate_job_id()

        if not job_cfg:
            job_cfg = self._job_config
        
        self._job_client_id = f"FABRIC_SYNC_{self.SafeID}_{uuid.uuid4()}"
        
        self.Logger.debug(f"STANDARD API - QUERY - CLIENT ({self._client_id}) - JOB ({job_id}) - {sql}")

        query = self._client.query(sql, 
            job_id=job_id.lower(), 
            job_retry=None,
            job_config=job_cfg)
            
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
        job_id = self.generate_job_id()

        self.Logger.debug(f"STANDARD API - TO_DATAFRAME - CLIENT ({self._client_id}) - JOB ({job_id})")
        bq = self.run_query(sql, job_id=job_id).to_dataframe()

        if len(bq) > 0:
            return self.Context.createDataFrame(bq, schema=schema)
        else:
            return None

class BigQueryStandardExport(BigQueryStandardClient):
    def __init__(self, project_id:str, table:str, credentials:str, location:str=None) -> None:
        """
        Initializes a new instance of the BigQueryStandardExport class.
        Args:
            project_id (str): The project ID.
            credentials (str): The credentials.
            location (str, optional): The location for the BigQuery client. Defaults to None.
        Methods:
            __process_page(page:int) -> None:
                Processes a single page of results from the BigQuery query.
            __thread_exception_handler(value:Exception) -> None:
                Handles exceptions raised by the thread processor.
            export(sql:str, num_threads:int = 10, num_partitions:int = 1, page_size:int = 100000) -> DataFrame:
                Exports the data from the given SQL query using the standard BigQuery API.
        """
        super().__init__(project_id, credentials, location)

        self.__bq_schema = self.get_bq_schema(table)
        self.__query_job = None
        self.__data_df:DataFrame = None
        self.__export_row_count = 0
        self.__lock = Lock()
        self.__job_id = self.generate_job_id()

    def __process_page(self, page:int):
        """
        Processes a single page of results from the BigQuery query.
        Args:
            page (int): The page number to process.
        """
        self.Logger.debug(f"STANDARD API EXPORT - PROCESSING PAGE - CLIENT ({self._client_id}) - JOB ({self.__job_id}) - {page}...")

        df = self.__query_job.result(
            page_size=self.page_size,
            start_index=(page - 1) * self.page_size,
            max_results=self.page_size
            ).to_dataframe()
        
        if len(df) > 0:
            row_count = len(df)
            sdf = self.Context.createDataFrame(df)
            sdf = self.convert_bq_data_types(sdf, self.__bq_schema)

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
        self.Logger.error(f"STANDARD API EXPORT - THREAD ERROR - CLIENT ({self._client_id}) - JOB ({self.__job_id}) - {value}...")
    
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
        self.__query_job = self.run_query(sql, job_id=self.__job_id)
        results = self.__query_job.result()

        total_rows = results.total_rows

        rows = 0
        page = 1
        processor = QueueProcessor(num_threads=num_threads)

        while(rows <= total_rows):
            rows = page * self.page_size
            processor.put(page)
            page += 1 
        
        if not processor.empty():
            processor.process(self.__process_page, self.__thread_exception_handler)

            if not processor.has_exceptions:
                self.Logger.debug(f"STANDARD API EXPORT - SUCCESS - CLIENT ({self._client_id}) - JOB ({self.__job_id}) - EXPORTED {self.__export_row_count} ROWS...")

                if len(self.__data_df) > 0:
                    return self.__data_df.repartition(num_partitions)

                return None
            else:
                self.Logger.debug(f"STANDARD API EXPORT - CLIENT ({self._client_id}) - JOB ({self.__job_id}) - FAILED...")
                return None