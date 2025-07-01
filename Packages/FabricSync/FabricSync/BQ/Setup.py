from packaging import version as pv
from github import * # type: ignore
from requests.exceptions import HTTPError
from io import BytesIO
import json
import os
import shutil
import uuid
import random
import time

from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Utils import (
    Util, SyncTimer
)
from FabricSync.BQ.APIClient import FabricAPI
from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.FileSystem import OneLakeFileSystem
from FabricSync.BQ.Enum import FabricDestinationType
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.SessionManager import Session
from FabricSync.BQ.Exceptions import (
    SyncConfigurationError
)
from FabricSync.BQ.Auth import (
    GCPAuth, TokenProvider
)
from FabricSync.BQ.SyncUtils import SyncUtil

class SetUpUtils(ContextAwareBase):
    """
    A utility class for setting up the BigQuery Spark connector.
    This class provides methods to download the latest BigQuery Spark connector jar file.
    It is designed to be used in the context of a Fabric Sync installation.
    Attributes:
        Context: The context of the Fabric Sync installation.
    Methods:
        get_bq_spark_connector(path: str) -> str:
            Downloads the latest BigQuery Spark connector jar file and saves it to the specified path.
    """
    @classmethod
    def get_bq_spark_connector(cls, path:str) -> str:
        """
        Downloads the latest BigQuery Spark connector jar file.
        Args:
            path: The path to save the jar file.
        Returns:
            The name of the jar file.
        """
        g = Github() # type: ignore
        repo = g.get_repo("GoogleCloudDataproc/spark-bigquery-connector")
        latest_release = cls.__get_latest_bq_spark_connector(repo.get_releases())

        sv = pv.parse(cls.Context.version)
        jar_name = f"spark-{sv.major}.{sv.minor}-bigquery-{latest_release.title}.jar"

        jars = [j for j in latest_release.get_assets() if j.name == jar_name]

        if jars:
            jar = jars[0]

        url = jar.browser_download_url
        Util.download_file(url, f"{path}/{jar_name}")

        return jar_name

    @classmethod
    def __get_latest_bq_spark_connector(cls, releases):
        """
        Gets the latest BigQuery Spark connector release.
        Args:
            releases: The releases.
        Returns:
            The latest release.
        """
        lr = None
        lv = None
        
        for r in releases:
            if r.title:
                if lr:
                    v = pv.parse(r.title)

                    if v > lv:
                        lr = r
                        lv = v
                else:
                    lr = r
                    lv = pv.parse(r.title)

        return lr

class Installer(ContextAwareBase):
    """
    A class to install the Fabric Sync for BigQuery.
    This class provides methods to initialize the installer, create the metastore,
    build a new configuration, download notebooks, validate lakehouse schemas,
    create or update the environment, and install the Fabric Sync.
    Attributes:
        GIT_URL: The URL of the Git repository containing the Fabric Sync files.
    Methods:
        __init__(credential_provider):
            Initializes a new instance of the Installer class.
        _initialize_installer(data):
            Initializes the installer with the provided data.
        _create_metastore():
            Creates the metastore for the Fabric Sync.
        _build_new_config():
            Builds a new configuration for the Fabric Sync.
        _download_notebooks():
            Downloads the notebooks from the Git repository and customizes them.
        _validate_lakehouse_schemas(lakehouse_id, lakehouse_name):
            Validates the lakehouse schemas based on the user configuration.
        _get_environment_yaml():
            Gets the environment YAML for the Fabric Sync.
        _create_or_update_environment(name):
            Creates or updates the Fabric Spark environment with the specified name.
        _get_folder_id(name):
            Gets the folder ID for the specified name in the Fabric workspace.
        _get_or_create_workspace_folders():
            Gets or creates the workspace folders for the Fabric Sync.
        _create_data_pipeline(notebook, friendly_notebook_name):
            Creates a data pipeline for the specified notebook in the Fabric workspace.
        __install(data):
            Installs the Fabric Sync for BigQuery with the provided data.
        install(data):
            Installs the Fabric Sync with the provided data.
    """
    GIT_URL = "https://raw.githubusercontent.com/microsoft/FabricBQSync/main"
    FABRIC_SYNC_FOLDER_NAME = "Fabric Sync"
    FABRIC_SYNC_FOLDER_NOTEBOOKS = "Notebooks"
    FABRIC_SYNC_FOLDER_DATA_PIPELINES = "Data Pipelines"
    FABRIC_SYNC_FOLDER_ENVIRONMENTS = "Environments"
    FABRIC_SYNC_FOLDER_LAKEHOUSES = "Lakehouses"
    FABRIC_SYNC_FOLDER_DATABASES = "Databases"

    FABRIC_SYNC_FOLDERS = [FABRIC_SYNC_FOLDER_NOTEBOOKS, FABRIC_SYNC_FOLDER_DATA_PIPELINES, FABRIC_SYNC_FOLDER_ENVIRONMENTS, 
        FABRIC_SYNC_FOLDER_LAKEHOUSES, FABRIC_SYNC_FOLDER_DATABASES]

    def __init__(self, credential_provider): 
        """
        Initializes a new instance of the Installer class.
        Args:
            credential_provider: The credential provider.
        """
        self.__TokenProvider = TokenProvider(credential_provider)
        self.__onelake = None

    def __initialize_installer(self, data):
        """
        Initializes the installer.
        This method sets up the necessary paths, configurations, and API clients for the Fabric Sync installation.
        It creates the required directories, initializes the user configuration, and sets up the Fabric API client
        for the specified workspace.
        It also ensures that the necessary parameters are set in the data dictionary.
        Raises:
            SyncConfigurationError: If the installation configuration is invalid.
        Args:
            data: The data.
        """ 
        self.__data = data 

        self.__default_path = "/lakehouse/default"
        self.__working_path = "Files/Fabric_Sync_Process"

        self.__config_path = f"{self.__working_path}/config"
        self.__libs_path = f"{self.__working_path}/libs"
        self.__logs_path = f"{self.__working_path}/logs"
        self.__temp_path = f"{self.__default_path}/{self.__working_path}/tmp"

        os.makedirs(os.path.dirname(self.LogPath), exist_ok=True) 
        os.makedirs(self.__temp_path, exist_ok=True) 

        self.__data["version"] = str(Session.CurrentVersion)

        if "reinitialize_existing_metadata" not in self.__data:
            self.__data["reinitialize_existing_metadata"] = True

        if "enable_schemas" not in self.__data:
            self.__data["enable_schemas"] = True
        
        if "create_spark_environment" not in self.__data:
            self.__data["create_spark_environment"] = False
        
        if "target_type" not in self.__data:
            self.__data["target_type"] = FabricDestinationType.LAKEHOUSE

        self.__user_config = self.__build_new_config()
        SyncUtil.configure_context(user_config=self.__user_config, 
                                    token=self.__TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))  

        self.__fabric_api = FabricAPI(self.WorkspaceID, 
                                    self.__TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))

    @property
    def UserConfig(self):
        """
        Gets the user configuration.
        Returns:
            The user configuration.
        """
        return self.__user_config

    @property
    def Data(self):
        """
        Gets the data for the installer.
        Returns:
            The data for the installer.
        """
        return self.__data
    
    @property
    def Onelake(self):
        """
        Gets the OneLake file system.
        Returns:
            The OneLake file system.
        """
        return self.__onelake
        
    def __create_metastore(self) -> None:
        """
        Creates the metastore for the Fabric Sync.
        This method creates the necessary tables in the metastore for the Fabric Sync.
        Returns:
            None
        """
        table_path = self.__onelake._get_onelake_path("Tables")
        table_path += "/dbo" if self.__user_config.Fabric.EnableSchemas else ""

        LakehouseCatalog.create_metastore_tables(table_path)

        url = f"{Installer.GIT_URL}/Setup/v{self.__data['asset_version']}/Data/bq_data_types.csv"
        data = Util.download_file_to_string(url)

        dt = self.Context.sparkContext.parallelize(data.splitlines())        
        df = self.Context.read.csv(dt, header=True)
        df.write.mode("OVERWRITE").format("delta").save(f"{table_path}/data_type_map")
    
    def __build_new_config(self) -> ConfigDataset:
        """
        Builds a new configuration for the Fabric Sync.
        This method creates a minimal configuration dataset with the necessary parameters for the Fabric Sync.
        Returns:
            ConfigDataset: A configuration dataset with the minimal required parameters.
        """
        minimal_config_data = {
            "correlation_id": str(uuid.uuid4()),
            "id": self.__data["loader_name"],
            "version": self.__data["version"],
            "autodiscover": {
                "autodetect": True,
                "tables": {
                    "enabled": True,
                    "load_all": True
                },
                "views": {
                    "enabled": True,
                    "load_all": True
                }
            },
            "optimization": {
                    "use_approximate_row_counts": True
            },
            "fabric": {
                "workspace_id": self.WorkspaceID,
                "enable_schemas": self.__data["enable_schemas"],
                "metadata_lakehouse": self.__data["metadata_lakehouse"],
                "target_lakehouse": self.__data["target_lakehouse"],
                "target_type": self.__data["target_type"]
            },
            "gcp": {
                "gcp_credentials":{
                    "credential": "NOT_SET"
                },
                "api":{
                    "materialization_default": {
                        "project_id": self.__data["gcp_project_id"],
                        "dataset": {
                            "dataset_id": self.__data["gcp_dataset_id"],
                        }
                    },
                    "billing_project_id": self.__data["gcp_project_id"],
                    "use_standard_api": True
                },
                "projects": [
                {
                    "project_id": self.__data["gcp_project_id"],
                    "datasets": [
                        {
                            "dataset_id": self.__data["gcp_dataset_id"]
                        }
                    ]
                }
                ]
            },
            "logging": {
                "log_path": f"{self.__default_path}/{self.__logs_path}/fabric_sync.log"
            }
        }

        return ConfigDataset(**minimal_config_data)

    def __download_notebooks(self):
        """
        Downloads the notebooks from the Git repository, customizes them, and installs them into the Fabric workspace.
        This method retrieves the notebooks from the specified Git URL, replaces placeholders with actual values,
        and uploads them to the Fabric workspace.
        It also creates a random suffix for the notebook names to avoid conflicts.
        Raises:
            SyncConfigurationError: If there is an error with the notebook download or customization.
        Returns:
            None
        """
        randomizer = f"0000{random.randint(1, 1000)}"
        suffix = randomizer[-4:]

        notebooks_folder_id = self.__get_folder_id(self.FABRIC_SYNC_FOLDER_NOTEBOOKS)
        
        url_suffix = "-Environment" if self.__data["create_spark_environment"] else ""

        git_notebooks = [
                    {"id": "Fabric Sync Notebook", "name": f"Fabric-Sync-Notebook-v{self.__data['version']}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.__data['asset_version']}/Fabric-Sync{url_suffix}.ipynb",
                        "lakehouse_only": False},
                    {"id": "Fabric Sync Maintenance", "name": f"Fabric-Sync-Maintenance-v{self.__data['version']}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.__data['asset_version']}/Fabric-Sync-Maintenance{url_suffix}.ipynb",
                        "lakehouse_only": True}
                    ]
        
        for notebook in git_notebooks:
            try:
                if not notebook["lakehouse_only"] or self.__user_config.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                    nb_data = Util.download_file_to_string(notebook["url"])

                    nb_data = nb_data.replace("<<<PATH_SPARK_BQ_JAR>>>", 
                        self.__onelake._get_onelake_path(f"{self.__libs_path}/{self.__data['spark_jar']}"))
                    nb_data = nb_data.replace("<<<PATH_TO_USER_CONFIG>>>", f"{self.__default_path}/{self.__data['user_config_file_path']}")
                    nb_data = nb_data.replace("<<<VERSION>>>", self.__user_config.Version)

                    nb = json.loads(nb_data)

                    nb["metadata"]["dependencies"]["lakehouse"] = {
                            "default_lakehouse": self.__user_config.Fabric.MetadataLakehouseID,
                            "default_lakehouse_name": self.__user_config.Fabric.MetadataLakehouse,
                            "default_lakehouse_workspace_id": self.__user_config.Fabric.WorkspaceID,
                            "known_lakehouses" : [
                                {
                                    "id": self.__user_config.Fabric.TargetLakehouseID
                                }
                            ]
                        }

                    if self.__data["create_spark_environment"]:
                        nb["metadata"]["dependencies"]["environment"] = {
                                "environmentId": self.__data["environment_id"],
                                "workspaceId": self.__user_config.Fabric.WorkspaceID
                            }
                        
                    notebook_id = self.__fabric_api.Notebook.get_by_name(notebook["name"])

                    if notebook_id:
                        notebook["name"] = f"{notebook['name']}-{suffix}"

                    self.__fabric_api.Notebook.create(
                        name = notebook["name"], 
                        data = json.dumps(nb),
                        folder = notebooks_folder_id,
                        update_hook = lambda x: self.Logger.sync_status(f"{notebook['id']} {x}"))
                    
                    #Create the notebook associated data pipeline
                    self.__create_data_pipeline(notebook = notebook["name"], friendly_notebook_name = notebook["id"])
            except Exception as e:
                self.Logger.sync_status(f"Failed to deploy Fabric Sync notebook to workspace: {notebook['name']}: {e}")

    def __validate_lakehouse_schemas(self, lakehouse_id:str, lakehouse_name:str) -> None:
        """
        Validates the lakehouse schemas. 
        Args:
            lakehouse_id: The lakehouse ID.
            lakehouse_name: The lakehouse name.
        Raises:
            SyncConfigurationError: If the lakehouse schemas are invalid.
        Returns:
            None
        """
        has_schema = self.__fabric_api.Lakehouse.has_schema(lakehouse_id)

        if self.__user_config.Fabric.EnableSchemas:
            if not has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS NOT enabled for schemas.")
        else:
            if has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS schema-enabled.")

    def __get_environment_yaml(self) -> BytesIO:
        """
        Gets the environment YAML.
        Returns:
            BytesIO: The environment YAML.
        """
        environments_yaml = f"dependencies:\r\n- pip:\r\n  - fabricsync"
        return BytesIO(environments_yaml.encode('utf-8'))

    def __create_or_update_environment(self, name:str) -> str:
        """
        Creates or updates the environment.
        Args:
            name: The name.
        Raises:
            TimeoutError: If the environment times out.
            HTTPError: If the environment creation fails.
        Returns:
            str: The environment ID.
        """
        try:
            environments_folder_id = self.__get_folder_id(self.FABRIC_SYNC_FOLDER_ENVIRONMENTS)

            self.Logger.sync_status(f"Creating (or Updating) Fabric Spark Environment: {name}...")
            environment_id = self.__fabric_api.Environment.get_or_create(name=name, folder=environments_folder_id)
            self.__data["environment_id"] = environment_id

            self.Logger.sync_status(f"Fabric Spark Environment created: {name}...")

            publish_state = self.__fabric_api.Environment.get_publish_state(environment_id)

            if (publish_state == "running"):
                self.Logger.sync_status(f"{name} is currently publishing. Waiting for publish to complete...")
                self.__fabric_api.Environment.wait_for_publish(environment_id)

            self.__fabric_api.Environment.stage_library(environment_id, "environment.yml", self.__get_environment_yaml())

            spark_jar = Util.read_file_to_buffer(f"{self.__temp_path}/{self.__data['spark_jar']}")
            self.__fabric_api.Environment.stage_library(environment_id, "spark-bigquery.jar", spark_jar)
            
            publish_state = self.__fabric_api.Environment.get_publish_state(environment_id)

            self.Logger.sync_status(f"{name} created/updated and is publishing...")
            self.__fabric_api.Environment.publish(environment_id)
        except TimeoutError as api_timeout:
            self.Logger.sync_status(f"Environment Timeout Error. Please make ensure environment publishing is complete before retry.")
        except HTTPError as http_err:
            self.Logger.sync_status(f"Environment Create/Epdate Error: {http_err}")

    def __get_folder_id(self, name:str) -> str:
        """
        Gets the folder ID for the specified name.
        This method checks if the folder map is initialized and retrieves the folder ID for the specified name
        Args:
            name: The name of the folder.
        Returns:
            str: The folder ID if found, otherwise None.
        """
        if self.__folder_map:
            folder_name = f"{self.FABRIC_SYNC_FOLDER_NAME} {name}"

            if folder_name in self.__folder_map:
                return self.__folder_map[folder_name]
        
        return None

    def __get_or_create_workspace_folders(self) -> dict[str, str]:
        """
        Gets or creates the workspace folders for the Fabric Sync.
        This method creates the main Fabric Sync folder and its subfolders in the Fabric workspace.
        It returns a dictionary mapping folder names to their IDs.
        Returns:
            dict[str, str]: A dictionary mapping folder names to their IDs.
        """
        folder_map = {}

        self.Logger.sync_status(f"Creating {self.FABRIC_SYNC_FOLDER_NAME} workspace folder...")
        folder_map[self.FABRIC_SYNC_FOLDER_NAME] = self.__fabric_api.Folder.get_or_create(self.FABRIC_SYNC_FOLDER_NAME)

        for f in self.FABRIC_SYNC_FOLDERS:
            if self.__user_config.Fabric.TargetType == FabricDestinationType.LAKEHOUSE and \
                f == self.FABRIC_SYNC_FOLDER_DATABASES:
                continue
            
            if not self.__data["create_spark_environment"] and f == self.FABRIC_SYNC_FOLDER_ENVIRONMENTS:
                continue

            name = f"{self.FABRIC_SYNC_FOLDER_NAME} {f}"
            self.Logger.sync_status(f"Creating {name} workspace folder...")
            folder_map[name] = self.__fabric_api.Folder.get_or_create(name=name, folder=folder_map[self.FABRIC_SYNC_FOLDER_NAME])
        
        return folder_map

    def __create_data_pipeline(self, notebook:str, friendly_notebook_name:str) -> None:
        """
        Creates a data pipeline for the specified notebook.
        This method retrieves the notebook ID and creates a data pipeline with the specified friendly name.
        It raises an exception if the notebook ID is not found.
        Args:
            notebook: The notebook name.
            friendly_notebook_name: A friendly name for the notebook.
        Raises:
            SyncConfigurationError: If the notebook ID is not found.
        Returns:
            None
        """
        notebook_id = self.__fabric_api.Notebook.get_id(notebook)

        if notebook_id:
            pipeline_name = f"{notebook}_Data_Pipeline"
            self.Logger.sync_status(f"Creating data pipeline: {pipeline_name}...")
            data_pipelines_folder_id = self.__get_folder_id(self.FABRIC_SYNC_FOLDER_DATA_PIPELINES)

            payload = {
                    "name": pipeline_name,
                    "properties": {
                        "activities": [
                            {
                                "name": f"{friendly_notebook_name}",
                                "type": "TridentNotebook",
                                "dependsOn": [],
                                "policy": {
                                    "timeout": "0.12:00:00",
                                    "retry": 0,
                                    "retryIntervalInSeconds": 30,
                                    "secureOutput": False,
                                    "secureInput": False
                                },
                                "typeProperties": {
                                    "notebookId": notebook_id,
                                    "workspaceId": self.WorkspaceID,
                                    "parameters": {
                                        "_inlineInstallationEnabled": {
                                            "value": "True",
                                            "type": "bool"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            
            self.__fabric_api.DataPipeline.create(pipeline_name, data=payload, folder=data_pipelines_folder_id)
        else:
            self.Logger.warning(f"Skipping data pipeline creation for {notebook} - Notebook not found...")

    @Telemetry.Install
    def install(self, data) -> None:
        """
        Installs the Fabric Sync for BigQuery.
        This method initializes the installer, creates the necessary lakehouses or databases,
        downloads the required libraries, and sets up the notebooks in the Fabric workspace.
        It also handles the creation of the Fabric Spark environment if specified.
        It raises exceptions if the installation configuration is invalid or if there are errors with the HTTP request.
        Args:
            data: The data for the installation.
        Raises:
            SyncConfigurationError: If the installation configuration is invalid.
            HTTPError: If there is an error with the HTTP request.
        Returns:
            None
        """
        try:
            with SyncTimer() as t:
                self.__initialize_installer(data)

                data["asset_version"] = f"{Session.CurrentVersion.major}.0.0"

                self.Logger.sync_status("Starting BQ Sync Installer...")

                if not os.path.isfile(self.__data["gcp_credential_path"]):
                    raise SyncConfigurationError("""GCP Credentials not found.
                    Please make sure your credentials have been uploaded to the environment and that the gcp_credential_path parameter is correct.""")

                #Workspace Folders
                self.__folder_map = self.__get_or_create_workspace_folders()

                #Create Lakehouses
                lakehouses_folder_id = self.__get_folder_id(self.FABRIC_SYNC_FOLDER_LAKEHOUSES)

                self.__user_config.Fabric.WorkspaceName = self.__fabric_api.Workspace.get_name(self.WorkspaceID)

                self.Logger.sync_status("Creating metadata lakehouse (if not exists)...")
                self.__user_config.Fabric.MetadataLakehouseID = self.__fabric_api.Lakehouse.get_or_create(
                    name = self.__user_config.Fabric.MetadataLakehouse, folder=lakehouses_folder_id,
                    enable_schemas = self.__user_config.Fabric.EnableSchemas)
                    
                self.__validate_lakehouse_schemas(self.__user_config.Fabric.MetadataLakehouseID, self.__user_config.Fabric.MetadataLakehouse)

                if self.__user_config.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                    self.Logger.sync_status("Creating mirrored lakehouse (if not exists)...")
                    self.__user_config.Fabric.TargetLakehouseID = self.__fabric_api.Lakehouse.get_or_create(
                        name = self.__user_config.Fabric.TargetLakehouse, folder=lakehouses_folder_id, 
                        enable_schemas = self.__user_config.Fabric.EnableSchemas)
                    
                    self.__validate_lakehouse_schemas(self.__user_config.Fabric.TargetLakehouseID, self.__user_config.Fabric.TargetLakehouse)
                else:
                    databases_folder_id = self.__get_folder_id(self.FABRIC_SYNC_FOLDER_DATABASES)

                    self.Logger.sync_status("Creating mirrored database (if not exists)...")
                    self.__user_config.Fabric.TargetLakehouseID = self.__fabric_api.OpenMirroredDatabase.get_or_create(
                        name = self.__user_config.Fabric.TargetLakehouse, folder=databases_folder_id)

                    while True:
                        status = self.__fabric_api.OpenMirroredDatabase.get_mirroring_status(self.__user_config.Fabric.TargetLakehouseID)

                        match status.lower():
                            case "starting" | "running":
                                self.Logger.sync_status(f"Finished {self.__user_config.Fabric.TargetLakehouse} mirrored database...")
                                move_exit = True
                            case "initialized" | "stopped":
                                self.Logger.sync_status(f"Starting {self.__user_config.Fabric.TargetLakehouse} mirrored database ({status})...")
                                self.__fabric_api.OpenMirroredDatabase.start_mirroring(self.__user_config.Fabric.TargetLakehouseID)
                                move_exit = False                        
                            case _:
                                self.Logger.sync_status(f"Waiting for {self.__user_config.Fabric.TargetLakehouse} mirrored database ({status})...")
                                move_exit = False
                        
                        if move_exit:
                            break
                        
                        time.sleep(5)
                
                if self.__user_config.Fabric.EnableSchemas:
                    self.__user_config.Fabric.MetadataSchema = "dbo"
                    self.__user_config.Fabric.TargetLakehouseSchema = self.__data["gcp_dataset_id"]

                self.__onelake = OneLakeFileSystem(self.__user_config.Fabric.WorkspaceID, 
                                                 self.__user_config.Fabric.MetadataLakehouseID)
                
                #Create metadata tables and required metadata
                if self.__data["reinitialize_existing_metadata"]:
                    self.Logger.sync_status("Initializing required metadata tables...")
                    self.__create_metastore()

                #Download the appropriate jar for the current spark runtime
                self.Logger.sync_status("Downloading BigQuery Spark connector libraries..")
                self.__data["spark_jar"] = SetUpUtils.get_bq_spark_connector(self.__temp_path)
                self.__onelake.copyFromLocalFile(
                    f"{self.__temp_path}/{self.__data['spark_jar']}", f"{self.__libs_path}/{self.__data['spark_jar']}")

                #Encode for embedding in config file
                self.Logger.sync_status("Generating initial configuration file...")
                encoded_credential = GCPAuth.get_encoded_credentials_from_path(self.__data["gcp_credential_path"])
                self.__user_config.GCP.GCPCredential.Credential = encoded_credential        

                config_path = f"{self.__config_path}/{self.__user_config.ID.replace(' ', '_')}_v{self.__user_config.Version}.json"
                
                self.__onelake.write(config_path, self.__user_config.model_dump_json(exclude_none=True, exclude_unset=True, indent=4))
                self.__data["user_config_file_path"] = config_path
            
                if self.__data["create_spark_environment"]:
                    #Create a Fabric Spark Environment
                    self.Logger.sync_status("Creating/Updating Spark Environment...")
                    self.__create_or_update_environment(self.__data["spark_environment_name"])
                
                #Get sync notebooks from Git, customize and install into the workspace
                self.Logger.sync_status("Copying Fabric Sync artifacts to Fabric workspace...")
                self.__download_notebooks()

            self.Logger.sync_status(f"Fabric Sync Installer finished in {str(t)}!")

            try:
                shutil.rmtree(self.__temp_path)
                self.Logger.debug(f"Installer Temp Directory '{self.__temp_path}' has been deleted.")
            except FileNotFoundError:
                self.Logger.debug(f"Installer Temp Directory '{self.__temp_path}' does not exist.")
        except SyncConfigurationError as e:
            self.Logger.sync_status(f"INSTALL CONFIGURATION FAILURE: {e}")
        except HTTPError as re:
            self.Logger.sync_status(f"INSTALL FAILURE: {re}")
        except Exception as ex:
            self.Logger.sync_status(f"INSTALL FAILURE (unhandled exception): {ex}")