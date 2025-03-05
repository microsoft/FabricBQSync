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
    @classmethod
    def get_bq_spark_connector(cls, path:str) -> str:
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
    GIT_URL = "https://raw.githubusercontent.com/microsoft/FabricBQSync/main"

    def __init__(self, credential_provider): 
        """
        Initializes a new instance of the Installer class.
        Args:
            credential_provider: The credential provider.
        """
        self._TokenProvider = TokenProvider(credential_provider)
        self._onelake = None

    def _initialize_installer(self, data):
        """
        Initializes the installer.
        Args:
            data: The data.
        """ 
        self._data = data 

        self._default_path = "/lakehouse/default"
        self._working_path = "Files/Fabric_Sync_Process"

        self._config_path = f"{self._working_path}/config"
        self._libs_path = f"{self._working_path}/libs"
        self._logs_path = f"{self._working_path}/logs"
        self._temp_path = f"{self._default_path}/{self._working_path}/tmp"

        os.makedirs(os.path.dirname(self.LogPath), exist_ok=True) 
        os.makedirs(self._temp_path, exist_ok=True) 

        self._data["version"] = str(Session.CurrentVersion)

        if "reinitialize_existing_metadata" not in self._data:
            self._data["reinitialize_existing_metadata"] = True

        if "enable_schemas" not in self._data:
            self._data["enable_schemas"] = False
        
        if "create_spark_environment" not in self._data:
            self._data["create_spark_environment"] = False
        
        if "target_type" not in self._data:
            self._data["target_type"] = FabricDestinationType.LAKEHOUSE

        self._user_config = self._build_new_config()
        SyncUtil.configure_context(user_config=self._user_config, 
                                    token=self._TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))  

        self._fabric_api = FabricAPI(self.WorkspaceID, 
                                    self._TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))

    def _create_metastore(self) -> None:
        """
        Creates the metastore.
        Returns:
            None
        """
        table_path = self._onelake._get_onelake_path("Tables")
        table_path += "/dbo" if self._user_config.Fabric.EnableSchemas else ""

        LakehouseCatalog.create_metastore_tables(table_path)

        url = f"{Installer.GIT_URL}/Setup/v{self._data['asset_version']}/Data/bq_data_types.csv"
        data = Util.download_file_to_string(url)

        dt = self.Context.sparkContext.parallelize(data.splitlines())        
        df = self.Context.read.csv(dt, header=True)
        df.write.mode("OVERWRITE").format("delta").save(f"{table_path}/data_type_map")
    
    def _build_new_config(self):
        """
        Builds a new configuration.
        """
        minimal_config_data = {
            "correlation_id": str(uuid.uuid4()),
            "id": self._data["loader_name"],
            "version": self._data["version"],
            "autodiscover": {
                "autodetect": True,
                "tables": {
                    "enabled": True,
                    "load_all": True
                }
            },
            "optimization": {
                    "use_approximate_row_counts": True
            },
            "fabric": {
                "workspace_id": self.WorkspaceID,
                "enable_schemas": self._data["enable_schemas"],
                "metadata_lakehouse": self._data["metadata_lakehouse"],
                "target_lakehouse": self._data["target_lakehouse"],
                "target_type": self._data["target_type"]
            },
            "gcp": {
                "gcp_credentials":{
                    "credential": "NOT_SET"
                },
                "api":{
                    "materialization_project_id": self._data["gcp_project_id"],
                    "materialization_dataset": self._data["gcp_dataset_id"],
                    "billing_project_id": self._data["gcp_project_id"],
                    "use_standard_api": False
                },
                "projects": [
                {
                    "project_id": self._data["gcp_project_id"],
                    "datasets": [
                    {
                        "dataset": self._data["gcp_dataset_id"]
                    }
                    ]
                }
                ]
            },
            "logging": {
                "log_path": f"{self._default_path}/{self._logs_path}/fabric_sync.log"
            }
        }

        return ConfigDataset(**minimal_config_data)

    def _download_notebooks(self):
        """
        Downloads the notebooks.
        """
        randomizer = f"0000{random.randint(1, 1000)}"
        suffix = randomizer[-4:]

        url_suffix = "-Environment" if self._data["create_spark_environment"] else ""

        git_notebooks = [
                    {"name": f"Fabric-Sync-Notebook-v{self._data['version']}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self._data['asset_version']}/Fabric-Sync{url_suffix}.ipynb",
                        "lakehouse_only": False},
                    {"name": f"Fabric-Sync-Maintenance-v{self._data['version']}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self._data['asset_version']}/Fabric-Sync-Maintenance{url_suffix}.ipynb",
                        "lakehouse_only": True}
                    ]
        
        for notebook in git_notebooks:
            try:
                if not notebook["lakehouse_only"] or self._user_config.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                    nb_data = Util.download_file_to_string(notebook["url"])

                    nb_data = nb_data.replace("<<<PATH_SPARK_BQ_JAR>>>", 
                        self._onelake._get_onelake_path(f"{self._libs_path}/{self._data['spark_jar']}"))
                    nb_data = nb_data.replace("<<<PATH_TO_USER_CONFIG>>>", f"{self._default_path}/{self._data['user_config_file_path']}")
                    nb_data = nb_data.replace("<<<VERSION>>>", self._user_config.Version)

                    nb = json.loads(nb_data)

                    nb["metadata"]["dependencies"]["lakehouse"] = {
                            "default_lakehouse": self._user_config.Fabric.MetadataLakehouseID,
                            "default_lakehouse_name": self._user_config.Fabric.MetadataLakehouse,
                            "default_lakehouse_workspace_id": self._user_config.Fabric.WorkspaceID,
                            "known_lakehouses" : [
                                {
                                    "id": self._user_config.Fabric.TargetLakehouseID
                                }
                            ]
                        }

                    if self._data["create_spark_environment"]:
                        nb["metadata"]["dependencies"]["environment"] = {
                                "environmentId": self._data["environment_id"],
                                "workspaceId": self._user_config.Fabric.WorkspaceID
                            }
                        
                    notebook_id = self._fabric_api.Notebook.get_by_name(notebook["name"])

                    if notebook_id:
                        notebook["name"] = f"{notebook['name']}-{suffix}"

                    self._fabric_api.Notebook.create(notebook["name"], json.dumps(nb),
                        lambda x: self.Logger.sync_status(f"{notebook['name']} {x}"))
            except Exception as e:
                self.Logger.sync_status(f"Failed to deploy Fabric Sync notebook to workspace: {notebook['name']}: {e}")

    def _validate_lakehouse_schemas(self, lakehouse_id:str, lakehouse_name:str) -> None:
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
        has_schema = self._fabric_api.Lakehouse.has_schema(lakehouse_id)

        if self._user_config.Fabric.EnableSchemas:
            if not has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS NOT enabled for schemas.")
        else:
            if has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS schema-enabled.")

    def _get_environment_yaml(self) -> BytesIO:
        """
        Gets the environment YAML.
        Returns:
            BytesIO: The environment YAML.
        """
        environments_yaml = f"dependencies:\r\n- pip:\r\n  - fabricsync"
        return BytesIO(environments_yaml.encode('utf-8'))


    def _create_or_update_environment(self, name:str) -> str:
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
            self.Logger.sync_status(f"Creating (or Updating) Fabric Spark Environment: {name}...")
            environment_id, created = self._fabric_api.Environment.get_or_create(name)
            self._data["environment_id"] = environment_id

            if created:
                self.Logger.sync_status(f"New Fabric Spark Environment created: {name}...", verbose=True)

            publish_state = self._fabric_api.Environment.get_publish_state(environment_id)

            if (publish_state == "running"):
                self.Logger.sync_status(f"{name} is currently publishing. Waiting for publish to complete...")
                self._fabric_api.Environment.wait_for_publish(environment_id)

            self._fabric_api.Environment.stage_library(environment_id, "environment.yml", self._get_environment_yaml())

            spark_jar = Util.read_file_to_buffer(f"{self._temp_path}/{self._data['spark_jar']}")
            self._fabric_api.Environment.stage_library(environment_id, "spark-bigquery.jar", spark_jar)
            
            publish_state = self._fabric_api.Environment.get_publish_state(environment_id)

            self.Logger.sync_status(f"{name} created/updated and is publishing...")
            self._fabric_api.Environment.publish(environment_id)
        except TimeoutError as api_timeout:
            self.Logger.sync_status(f"Environment Timeout Error. Please make ensure environment publishing is complete before retry.")
        except HTTPError as http_err:
            self.Logger.sync_status(f"Environment Create/Epdate Error: {http_err}")


    @Telemetry.Install
    def install(self, data) -> None:
        """
        Installs the Fabric Sync.
        Args:
            data: The data.
        Raises:
            SyncConfigurationError: If the configuration is invalid.
            HTTPError: If the installation fails.
        Returns:
            None
        """
        try:
            with SyncTimer() as t:
                self._initialize_installer(data)

                data["asset_version"] = f"{Session.CurrentVersion.major}.0.0"

                self.Logger.sync_status("Starting BQ Sync Installer...")

                if not os.path.isfile(self._data["gcp_credential_path"]):
                    raise SyncConfigurationError("""GCP Credentials not found.
                    Please make sure your credentials have been uploaded to the environment and that the gcp_credential_path parameter is correct.""")

                #Create Lakehouses            
                self._user_config.Fabric.WorkspaceName = self._fabric_api.Workspace.get_name(self.WorkspaceID)

                self.Logger.sync_status("Creating metadata lakehouse (if not exists)...")
                self._user_config.Fabric.MetadataLakehouseID, is_metdata_lakehouse_new = self._fabric_api.Lakehouse.get_or_create(
                    self._user_config.Fabric.MetadataLakehouse, self._user_config.Fabric.EnableSchemas)
                    
                if not is_metdata_lakehouse_new:
                    self._validate_lakehouse_schemas(self._user_config.Fabric.MetadataLakehouseID, 
                                                    self._user_config.Fabric.MetadataLakehouse)

                if self._user_config.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                    self.Logger.sync_status("Creating mirrored lakehouse (if not exists)...")
                    self._user_config.Fabric.TargetLakehouseID, is_new = self._fabric_api.Lakehouse.get_or_create(
                        self._user_config.Fabric.TargetLakehouse, self._user_config.Fabric.EnableSchemas)
                    
                    if not is_new:
                        self._validate_lakehouse_schemas(self._user_config.Fabric.TargetLakehouseID, 
                                                        self._user_config.Fabric.TargetLakehouse)
                else:
                    self.Logger.sync_status("Creating mirrored database (if not exists)...")
                    self._user_config.Fabric.TargetLakehouseID, is_new = self._fabric_api.OpenMirroredDatabase.get_or_create(
                        self._user_config.Fabric.TargetLakehouse)

                    while True:
                        status = self._fabric_api.OpenMirroredDatabase.get_mirroring_status(self._user_config.Fabric.TargetLakehouseID)

                        match status.lower():
                            case "starting" | "running":
                                self.Logger.sync_status(f"Finished {self._user_config.Fabric.TargetLakehouse} mirrored database...")
                                move_exit = True
                            case "initialized" | "stopped":
                                self.Logger.sync_status(f"Starting {self._user_config.Fabric.TargetLakehouse} mirrored database ({status})...")
                                self._fabric_api.OpenMirroredDatabase.start_mirroring(self._user_config.Fabric.TargetLakehouseID)
                                move_exit = False                        
                            case _:
                                self.Logger.sync_status(f"Waiting for {self._user_config.Fabric.TargetLakehouse} mirrored database ({status})...")
                                move_exit = False
                        
                        if move_exit:
                            break
                        
                        time.sleep(5)
                
                if self._user_config.Fabric.EnableSchemas:
                    self._user_config.Fabric.MetadataSchema = "dbo"
                    self._user_config.Fabric.TargetLakehouseSchema = self._data["gcp_dataset_id"]

                self._onelake = OneLakeFileSystem(self._user_config.Fabric.WorkspaceID, 
                                                 self._user_config.Fabric.MetadataLakehouseID)
                
                #Create metadata tables and required metadata
                if is_metdata_lakehouse_new or self._data["reinitialize_existing_metadata"]:
                    self.Logger.sync_status("Initializing required metadata tables...")
                    self._create_metastore()

                #Download the appropriate jar for the current spark runtime
                self.Logger.sync_status("Downloading BigQuery Spark connector libraries..")
                self._data["spark_jar"] = SetUpUtils.get_bq_spark_connector(self._temp_path)
                self._onelake.copyFromLocalFile(
                    f"{self._temp_path}/{self._data['spark_jar']}", f"{self._libs_path}/{self._data['spark_jar']}")

                #Encode for embedding in config file
                self.Logger.sync_status("Generating initial configuration file...")
                encoded_credential = GCPAuth.get_encoded_credentials_from_path(self._data["gcp_credential_path"])
                self._user_config.GCP.GCPCredential.Credential = encoded_credential        

                config_path = f"{self._config_path}/{self._user_config.ID.replace(' ', '_')}_v{self._user_config.Version}.json"
                
                self._onelake.write(config_path, self._user_config.model_dump_json(exclude_none=True, exclude_unset=True, indent=4))
                self._data["user_config_file_path"] = config_path
            
                if self._data["create_spark_environment"]:
                    #Create a Fabric Spark Environment
                    self.Logger.sync_status("Creating/Updating Spark Environment...")
                    self._create_or_update_environment(self._data["spark_environment_name"])
                
                #Get sync notebooks from Git, customize and install into the workspace
                self.Logger.sync_status("Copying Fabric Sync artifacts to Fabric workspace...")
                self._download_notebooks()

            self.Logger.sync_status(f"Fabric Sync Installer finished in {str(t)}!")

            try:
                shutil.rmtree(self._temp_path)
                self.Logger.debug(f"Installer Temp Directory '{self._temp_path}' has been deleted.")
            except FileNotFoundError:
                self.Logger.debug(f"Installer Temp Directory '{self._temp_path}' does not exist.")
        except SyncConfigurationError as e:
            self.Logger.sync_status(f"INSTALL CONFIGURATION FAILURE: {e}")
        except HTTPError as re:
            self.Logger.sync_status(f"INSTALL FAILURE: {re}")