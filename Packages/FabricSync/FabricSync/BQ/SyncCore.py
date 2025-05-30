from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from FabricSync.BQ.Enum import BigQueryAPI
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Auth import (
    TokenProvider, Credentials, GCPAuth
)
from FabricSync.BQ.SyncUtils import SyncUtil
from FabricSync.BQ.BigQueryAPI import BigQueryClient
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Model.Core import BQQueryModel
from FabricSync.BQ.Exceptions import (
    SyncConfigurationError, BQConnectorError, SyncKeyVaultError
)
 
class ConfigBase(ContextAwareBase):
    def __init__(self) -> None:
        """
        Initializes a new instance of the ConfigBase class.
        """
        self.UserConfig = ConfigDataset.from_json(self.UserConfigPath)
        
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
            bq_client = BigQueryClient(self.UserConfig)

            match query.API:
                case BigQueryAPI.BUCKET:
                    df = bq_client.read_from_exported_bucket(query)
                case BigQueryAPI.STANDARD:
                    df = bq_client.read_from_standard_api(query, schema)
                case _:
                    df = bq_client.read_from_storage_api(query)

            if query.Cached:
                df.cache()
        except Exception as e:
            raise BQConnectorError(msg=f"Read to dataframe failed: {e}", query=query) from e
        
        return df

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
        self.init_sync_session(config_path)
        

    def init_sync_session(self, config_path):
        """
        Initializes the session context for the synchronization process.
        Args:
            config_path (str): The path to the JSON user configuration file.
        """
        self.load_user_config(config_path)
        self.Context.sql(f"USE {self.UserConfig.Fabric.get_metadata_lakehouse()}")

    def load_user_config(self, config_path:str) -> None:
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
        self.UserConfig = self.__load_user_config_from_json(config_path)

        if not self.UserConfig.GCP.GCPCredential.CredentialSecretKey:
            gcp_credential = GCPAuth.load_gcp_credential(self.UserConfig)
        else:
            gcp_credential = self.__load_credentials_from_key_vault(
                self.UserConfig.GCP.GCPCredential.CredentialSecretKeyVault,
                self.UserConfig.GCP.GCPCredential.CredentialSecretKey)

        SyncUtil.configure_context(self.UserConfig, gcp_credential, config_path, 
                                    self.TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))

    def __load_credentials_from_key_vault(self, key_vault:str, key:str) -> str:
        try:
            return self.TokenProvider.get_secret(key_vault, key)
        except Exception as e:
            raise SyncKeyVaultError(e)

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
        config = ConfigDataset.from_json(config_path)

        config_df = self.Context.read.json(self.Context.sparkContext.parallelize([config.model_dump_json()]))
        config_df.createOrReplaceTempView("user_config_json")
        config_df.cache()

        return config