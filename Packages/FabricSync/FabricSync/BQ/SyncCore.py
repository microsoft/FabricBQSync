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
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Threading import SparkProcessor
 
class ConfigBase(ContextAwareBase):
    """
    Base class for configuration operations, providing methods to read BigQuery data into a DataFrame.
    This class handles the initialization of user configurations and provides a method to read data from BigQuery.
    Attributes:
        UserConfig (ConfigDataset): The user configuration settings loaded from a JSON file.
    Methods:
        __init__() -> None:
            Initializes a new instance of the ConfigBase class and loads user configuration.
        read_bq_to_dataframe(query: BQQueryModel, schema: StructType = None) -> DataFrame:
            Reads the data from the given BigQuery query into a DataFrame.
    Raises:
        BQConnectorError: If there is an error reading data from BigQuery.
    """
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

            if df and query.Cached:
                df.cache()
        except Exception as e:
            raise BQConnectorError(msg=f"Read to dataframe failed: {e}", query=query) from e
        
        return df

class SyncBase(ContextAwareBase):
    """
    Base class for synchronization operations, providing methods to initialize and manage the sync session.
    This class handles the loading of user configurations, setting up the session context, and managing credentials.
    Attributes:
        UserConfig (ConfigDataset): The user configuration settings loaded from a JSON file.
        TokenProvider (TokenProvider): The token provider for managing authentication tokens.
    Methods:
        __init__(config_path: str, credentials: Credentials) -> None:
            Initializes a new instance of the SyncBase class with the specified configuration path and credentials.
        init_sync_session(config_path: str) -> None:
            Initializes the session context for the synchronization process using the provided configuration path.
        load_user_config(config_path: str) -> None:
            Loads the user's configuration from a specified JSON file and sets up the session context.
        __load_user_config_from_json(config_path: str) -> ConfigDataset:
            Loads user configuration from a specified JSON file and creates a ConfigDataset instance.
        __load_credentials_from_key_vault(key_vault: str, key: str) -> str:
            Loads credentials from the specified key vault using the provided key.
    Raises:
        SyncConfigurationError: If the configuration path is missing or invalid.
        SyncKeyVaultError: If there is an error retrieving credentials from the key vault.
    """
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
        self.drop_system_views()
        self.load_user_config(config_path)
        self.Context.sql(f"USE {self.UserConfig.Fabric.get_metadata_lakehouse()}")

    def drop_system_views(self):
        """
        Drops all temporary views created during the synchronization process.
        This method iterates through the list of temporary views defined in SyncConstants and executes a drop
        command for each view. It ensures that all temporary views are removed from the Spark session to maintain a clean state.
        Raises:
            SyncConfigurationError: If there is an error in the configuration or if the views cannot be dropped.
        Notes:
            - This method is typically called at the end of the synchronization process to clean up temporary resources
            - It uses the SparkProcessor to execute the drop commands in a distributed manner.
        """
        cmds = [f"DROP VIEW IF EXISTS {view};" for view in SyncConstants.get_sync_temp_views()]
        SparkProcessor.process_command_list(cmds)
            
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
        """
        Loads credentials from the specified key vault using the provided key.
        Args:
            key_vault (str): The name of the key vault.
            key (str): The key to retrieve the credentials.
        Returns:
            str: The credentials retrieved from the key vault.
        Raises:
            SyncKeyVaultError: If there is an error retrieving the credentials from the key vault.
        """
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