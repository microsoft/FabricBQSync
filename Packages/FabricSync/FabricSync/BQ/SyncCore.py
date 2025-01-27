from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from logging import Logger

from FabricSync.BQ.Validation import SqlValidator
from FabricSync.BQ.Utils import Util, SyncUtil
from FabricSync.BQ.Metastore import FabricMetastore
from FabricSync.BQ.Enum import BigQueryAPI

from FabricSync.BQ.Core import (
    ContextAwareBase
) 
from FabricSync.BQ.Auth import (
    TokenProvider, Credentials, GCPAuth
)
from FabricSync.BQ.BigQueryAPI import BigQueryClient
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Model.Core import BQQueryModel
from FabricSync.BQ.Logging import SyncLogger
from FabricSync.BQ.Exceptions import (
    SyncConfigurationError, BQConnectorError
)
  
        
class ConfigBase(ContextAwareBase):
    def __init__(self, user_config:ConfigDataset, token_provider:TokenProvider = None) -> None:
        """
        Initializes a new instance of the ConfigBase class.
        Args:
            user_config (ConfigDataset): The user configuration.
            token_provider (TokenProvider): The token provider.
        """
        self.UserConfig = user_config
        self.TokenProvider = token_provider

    @property
    def GCPCredential(self) -> str:
        """
        Gets the GCP credential.
        """
        return self.Context.conf.get("credentials")

    @property
    def Logger(self):
        """
        Gets the logger.
        Returns:
            Logger: The logger.
        """
        if self._logger is None:
            self._logger = SyncLogger().get_logger()
        
        return self._logger

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
            "bigQueryJobLabel" : self.UserConfig.ID
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
        
    def read_bq_to_dataframe(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame.
        Parameters:
            query (BQQueryModel): The query model.
            schema (StructType, optional): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        try:
            if query.API == str(BigQueryAPI.STORAGE):
                df = self.__read_bq_storage_to_dataframe(query)
            else:
                df = self.__read_bq_standard_to_dataframe(query, schema)

            if query.Cached:
                df.cache()
        except Exception as e:
            raise BQConnectorError(msg=f"Read to dataframe failed: {e}", query=query) from e
        
        return df

    def __read_bq_storage_to_dataframe(self, query:BQQueryModel) -> DataFrame:
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

        df = self.Context.read.format("bigquery").options(**cfg).load(q)

        return df

    def __read_bq_standard_to_dataframe(self, query:BQQueryModel, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given BigQuery query into a DataFrame using the standard BigQuery API.
        Parameters:
            query (BQQueryModel): The query model.
            schema (StructType, optional): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        sql_query = self.__build_bq_query(query)

        bq_client = BigQueryClient(query.ProjectId, self.GCPCredential)
        df = bq_client.read_to_dataframe(sql_query, schema)

        return df

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

class SyncBase(ContextAwareBase):
    def __init__(self, config_path:str, credentials:Credentials) -> None:
        """
        Initializes a new instance of the SyncBase class.
        Args:
            config_path (str): The path to the JSON user config.
            credentials (Credentials): The credentials.
        """
        if config_path is None:
            raise SyncConfigurationError("Missing Path to JSON User Config")

        self.TokenProvider = TokenProvider(credentials)
        self.ConfigPath = config_path

        self.load_user_config()

        self.Context.sql(f"USE {self.UserConfig.Fabric.get_metadata_lakehouse()}")
        FabricMetastore.create_proxy_views()

    @property
    def Logger(self):
        """
        Gets the logger.
        Returns:
            Logger: The logger.
        """
        if self._logger is None:
            self._logger = SyncLogger().get_logger()
        
        return self._logger

    def load_user_config(self):
        """
        Loads the user's configuration from a specified JSON file and sets up the session context.
        This method performs the following steps:
        1. Reads the configuration details from a JSON file specified by self.ConfigPath.
        2. Loads the necessary GCP credentials.
        3. Configures the session environment using the loaded credentials.
        Raises:
            FileNotFoundError: If the specified JSON configuration file cannot be located.
            ValueError: If the JSON file is invalid or missing necessary configuration.
        Notes:
            - The loaded configuration is stored in self.UserConfig.
            - This method relies on helper functions for credential loading and session configuration.
        """
        self.UserConfig = self.__load_user_config_from_json(self.ConfigPath)            
        gcp_credential = self.__load_gcp_credential()
        self.__configure_session_context(gcp_credential)

    def __configure_session_context(self, gcp_credential:str) -> None:
        """
        Configures the Spark session context with the user configuration settings
        and the base-64 encoded GCP credentials.
        Args:
            gcp_credential (str): The GCP credential.
        """
        SyncUtil.initialize_spark_session(self.UserConfig)
        self.Context.conf.set("credentials", gcp_credential)

    def __load_user_config_from_json(self, config_path:str) -> ConfigDataset:
        """
        Load user configuration from a specified JSON file and create a ConfigDataset instance.
        Reads the JSON configuration file, converts it into a ConfigDataset object, and applies
        default table settings. The resulting ConfigDataset object is then used to create a
        temporary view in Spark for later use.
        Args:
            config_path (str): The path to the JSON user configuration file.
        Returns:
            ConfigDataset: The user configuration settings
        """
        
        config = ConfigDataset.read_json_config(config_path)
        cfg = ConfigDataset(**config)            
        cfg.apply_table_defaults(config)

        config_df = self.Context.read.json(self.Context.sparkContext.parallelize([cfg.model_dump_json()]))
        config_df.createOrReplaceTempView("user_config_json")
        config_df.cache()

        return cfg

    def __load_gcp_credential(self) -> str:
        """
        Loads the GCP credential from the user configuration settings.
        1. If the credential is base-64 encoded, it is returned as is.
        2. If the credential is a file path, it is read and encoded.
        3. If the credential is missing or invalid, an exception is raised.
        Returns:
            str: The GCP credential.
        Raises:
            SyncConfigurationError: If the GCP credential is missing or invalid.
        """
        credential = None
        if Util.is_base64(self.UserConfig.GCP.GCPCredential.Credential):
            credential = self.UserConfig.GCP.GCPCredential.Credential
        else:
            try:
                credential = GCPAuth.get_encoded_credentials_from_path(self.UserConfig.GCP.GCPCredential.CredentialPath)
            except Exception as e:
                raise SyncConfigurationError(f"Failed to parse GCP credential file: {e}") from e
        
        if not credential:
            raise SyncConfigurationError(f"GCP credential is missing or is in an invalid format.")
        
        return credential