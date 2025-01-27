from packaging import version as pv
from pyspark.sql import SparkSession

from github import * # type: ignore

import os
import pandas as pd # type: ignore
import uuid
import random
import time
from requests.exceptions import HTTPError

from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Utils import Util
from FabricSync.BQ.APIClient import FabricAPI
from FabricSync.BQ.Logging import (
    SyncLogger, Telemetry
)

from FabricSync.BQ.Logging import Telemetry
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.SyncUtils import (
    SyncUtil, SyncTimer
)
from FabricSync.BQ.Utils import Util
from FabricSync.Meta import Version
from FabricSync.BQ.Enum import FabricDestinationType

from FabricSync.BQ.Core import (
    TokenProvider, ContextAwareBase
)
from FabricSync.BQ.Exceptions import (
    SyncInstallError, SyncConfigurationError
)
from FabricSync.BQ.Auth import GCPAuth

class SetupUtils():    
    @staticmethod
    def get_bq_spark_connector(spark_version, jar_path:str) -> str:
        """
        Downloads the BigQuery Spark connector jar for the given Spark version.
        Args:
            spark_version (str): The Spark version.
            jar_path (str): The path to save the jar.
        Returns:
            str: The jar name.
        """
        g = Github() # type: ignore
        repo = g.get_repo("GoogleCloudDataproc/spark-bigquery-connector")
        latest_release = SetupUtils.get_latest_bq_spark_connector(repo.get_releases())

        sv = pv.parse(spark_version)
        jar_name = f"spark-{sv.major}.{sv.minor}-bigquery-{latest_release.title}.jar"

        jars = [j for j in latest_release.get_assets() if j.name == jar_name]

        if jars:
            jar = jars[0]

        url = jar.browser_download_url
        lakehouse_path = f"{jar_path}/{jar.name}"

        if not os.path.isfile(lakehouse_path):
            Util.download_file(url, lakehouse_path)
        
        return jar_name

    @staticmethod
    def get_latest_bq_spark_connector(releases):
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
        self.TokenProvider = TokenProvider(credential_provider)
        self.correlation_id = str(uuid.uuid4())

        self.working_path = "Files/BQ_Sync_Process"
        self.base_path = f"/lakehouse/default/{self.working_path}"
        self.local_path = f"{self.base_path}/sql"
        self.config_path = f"{self.base_path}/config"
        self.libs_path = f"{self.base_path}/libs"
        self.logs_path = f"{self.base_path}/logs"
        self.notebooks_path = f"{self.base_path}/notebooks"

        self.__ensure_paths()

    def __ensure_paths(self):
        """
        Ensures the required paths exist.
        """
        installer_pathes = [self.base_path, self.local_path, self.config_path, 
            self.libs_path, self.logs_path, self.notebooks_path]

        Util.ensure_paths(installer_pathes)

    def __get_sql_source_from_git(self):
        """
        Gets the SQL source from Git.
        """
        git_content = [
            {"name": "bq_data_types.csv", "url": f"{Installer.GIT_URL}/Setup/v{self.data['asset_version']}/Data/bq_data_types.csv"}
            ]

        for c in git_content:
            try:
                Util.download_file(c["url"], f"{self.local_path}/{c['name']}")
            except Exception as e:
                raise SyncInstallError(f"Unabled to download from git: {c['name']}")

    def __create_sql_metadata_from_source(self):
        """
        Creates the SQL metadata from the source.
        """      
        LakehouseCatalog.create_metastore_tables()        
        self.__load_sql_metadata()
    
    def __load_sql_metadata(self):
        """
        Loads the SQL metadata.
        """
        self.__get_sql_source_from_git()

        df_pandas = pd.read_csv(f"{self.local_path}/bq_data_types.csv")
        df = self.Context.createDataFrame(df_pandas)
        df.write.mode("OVERWRITE").saveAsTable(
            self.UserConfig.Fabric.get_metadata_table_path("data_type_map"))
    
    def __build_new_config(self):
        """
        Builds a new configuration.
        """
        minimal_config_data = {
            "correlation_id": self.correlation_id,
            "id": self.data["loader_name"],
            "version": self.data["version"],
            "autodiscover": {
                "autodetect": True,
                "tables": {
                    "enabled": True,
                    "load_all": True
                }
            },
            "fabric": {
                "workspace_id": self.data["workspace_id"],
                "enable_schemas": self.data["enable_schemas"],
                "metadata_lakehouse": self.data["metadata_lakehouse"],
                "target_lakehouse": self.data["target_lakehouse"],
                "target_type": self.data["target_type"]
            },
            "gcp": {
                "gcp_credentials":{
                    "credential": "NOT_SET"
                },
                "projects": [
                {
                    "project_id": self.data["gcp_project_id"],
                    "datasets": [
                    {
                        "dataset": self.data["gcp_dataset_id"]
                    }
                    ]
                }
                ]
            },
            "logging": {
                "log_path": f"{self.logs_path}/fabric_sync.log"
            }
        }

        return ConfigDataset(**minimal_config_data)

    def __download_bq_connector(self):
        """
        Downloads the BigQuery connector.
        """
        self.data["spark_jar"] = SetupUtils.get_bq_spark_connector(self.Context.version, self.libs_path)
        self.data["spark_jar_path"] = f"abfss://{self.UserConfig.Fabric.WorkspaceID}@onelake.dfs.fabric.microsoft.com/" + \
            f"{self.UserConfig.Fabric.MetadataLakehouseID}/{self.working_path}/libs/{self.data['spark_jar']}"

    def __download_notebooks(self):
        """
        Downloads the notebooks.
        """
        randomizer = f"0000{random.randint(1, 1000)}"
        suffix = randomizer[-4:]

        git_notebooks = [
                    {"name": f"BQ-Sync-Notebook-v{self.data['version']}-{suffix}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.data['asset_version']}/BQ-Sync.ipynb",
                        "file_name": "BQ-Sync.ipynb"},
                    {"name": f"BQ-Sync-Maintenance-v{self.data['version']}-{suffix}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.data['asset_version']}/BQ-Sync-Maintenance.ipynb",
                        "file_name": "BQ-Sync-Maintenance.ipynb"}
                    ]
        
        for notebook in git_notebooks:
            try:
                nb_data = Util.download_encoded_to_string(notebook["url"])

                nb_data = nb_data.replace("<<<FABRIC_WORKSPACE_ID>>>", self.UserConfig.Fabric.WorkspaceID)
                nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_ID>>>", self.UserConfig.Fabric.MetadataLakehouseID)
                nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_NAME>>>", self.UserConfig.Fabric.MetadataLakehouse)
                nb_data = nb_data.replace("<<<PATH_SPARK_BQ_JAR>>>", self.data["spark_jar_path"])
                nb_data = nb_data.replace("<<<PATH_TO_USER_CONFIG>>>", self.data["user_config_file_path"])
                nb_data = nb_data.replace("<<<VERSION>>>", self.UserConfig.Version)

                self.fabric_api.upload_fabric_notebook(notebook["name"], nb_data)
                self.Logger.sync_status(f"{notebook['name']} copied to workspace, it may take a moment to appear...")
            except Exception as e:
                raise SyncInstallError(f"Failed to deploy BQ Sync notebook to workspace: {notebook['name']}: {e}")

    def __initialize_installer(self, data):
        """
        Initializes the installer.
        Args:
            data: The data.
        """    
        self.data = data 

        self.data["workspace_id"] = self.Context.conf.get("trident.workspace.id")
        self.data["version"] = Version.CurrentVersion

        self.UserConfig = self.__build_new_config()
        SyncUtil.initialize_spark_session(self.UserConfig)       

        self.Logger = SyncLogger().get_logger()
        self.fabric_api = FabricAPI(self.data["workspace_id"], self.TokenProvider.get_token(TokenProvider.FABRIC_TOKEN_SCOPE))

    def __validate_lakehouse_schemas(self, lakehouse_id:str, lakehouse_name:str):
        has_schema = self.fabric_api.Lakehouse.has_schema(lakehouse_id)

        if self.UserConfig.Fabric.EnableSchemas:
            if not has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS NOT enabled for schemas.")
        else:
            if has_schema:
                raise SyncConfigurationError(f"Invalid Configuration: {lakehouse_name} lakehouse already exists and IS schema-enabled.")

    @Telemetry.Install
    def install(self, data):
        """
        Installs the Fabric Sync Accelerator.
        Args:
            data: The data.
        """
        with SyncTimer() as t:
            self.__initialize_installer(data)

            av = pv.parse(data["version"])
            data["asset_version"] = f"{av.major}.0.0"

            self.Logger.sync_status("Starting BQ Sync Installer...")

            if not os.path.isfile(self.data["gcp_credential_path"]):
                raise SyncInstallError("""GCP Credentials not found. 
                Please make sure your credentials have been uploaded to the environment and that the gcp_credential_path parameter is correct.""")

            #Create Lakehouses            
            self.UserConfig.Fabric.WorkspaceName = self.fabric_api.Workspace.get_name()

            self.Logger.sync_status("Creating metadata lakehouse (if not exists)...")
            self.UserConfig.Fabric.MetadataLakehouseID, is_new = self.fabric_api.Lakehouse.get_or_create(
                self.UserConfig.Fabric.MetadataLakehouse, self.UserConfig.Fabric.EnableSchemas)
                
            if not is_new:
                self.__validate_lakehouse_schemas(self.UserConfig.Fabric.MetadataLakehouseID, 
                                                  self.UserConfig.Fabric.MetadataLakehouse)

            if self.UserConfig.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
                self.Logger.sync_status("Creating mirrored lakehouse (if not exists)...")
                self.UserConfig.Fabric.TargetLakehouseID, is_new = self.fabric_api.Lakehouse.get_or_create(
                    self.UserConfig.Fabric.TargetLakehouse, self.UserConfig.Fabric.EnableSchemas)
                
                if not is_new:
                    self.__validate_lakehouse_schemas(self.UserConfig.Fabric.TargetLakehouseID, 
                                                      self.UserConfig.Fabric.TargetLakehouse)
            else:
                self.Logger.sync_status("Creating mirrored database (if not exists)...")
                self.UserConfig.Fabric.TargetLakehouseID = self.fabric_api.OpenMirroredDatabase.get_or_create(
                    self.UserConfig.Fabric.TargetLakehouse)

                while True:
                    status = self.fabric_api.OpenMirroredDatabase.get_mirroring_status(
                        self.UserConfig.Fabric.TargetLakehouseID)

                    match status.lower():
                        case "starting" | "running":
                            self.Logger.sync_status(f"Finished {self.UserConfig.Fabric.TargetLakehouse} mirrored database...")
                            move_exit = True
                        case "initialized" | "stopped":
                            self.Logger.sync_status(f"Starting {self.UserConfig.Fabric.TargetLakehouse} mirrored database ({status})...")
                            self.fabric_api.start_mirroring(self.UserConfig.Fabric.TargetLakehouseID)
                            move_exit = False                        
                        case _:
                            self.Logger.sync_status(f"Waiting for {self.UserConfig.Fabric.TargetLakehouse} mirrored database ({status})...")
                            move_exit = False
                    
                    if move_exit:
                        break
                    
                    time.sleep(5)
            
            #Create metadata tables and required metadata
            self.Logger.sync_status("Creating required metadata objects...")
            self.__create_sql_metadata_from_source()

            #Download the appropriate jar for the current spark runtime
            self.Logger.sync_status("Downloading BigQuery Spark connector libraries..")
            self.__download_bq_connector()

            #Encode for embedding in config file
            self.Logger.sync_status("Generating initial configuration file...")
            encoded_credential = GCPAuth.get_encoded_gcp_credentials_from_path(self.data["gcp_credential_path"])
            self.UserConfig.GCP.GCPCredential.Credential = encoded_credential        

            config_path = f"{self.config_path}/{self.UserConfig.ID.replace(' ', '_')}_v{self.UserConfig.Version}.json"
            
            #SetupUtils.generate_base_config(path, config_data)
            self.UserConfig.to_json(config_path)
            self.data["user_config_file_path"] = config_path 

            ##Get sync notebooks from Git, customize and install into the workspace
            self.Logger.sync_status("Copying BQ Sync artifacts to Fabric workspace...")
            self.__download_notebooks()
        
        self.Logger.sync_status(f"BQ Sync Installer finished in {str(t)}!")

class SparkEnvironmentUtil(ContextAwareBase):
    def __init__(self, api_token:str) -> None:
        """
        Initializes a new instance of the SparkEnvironmentUtil class.
        Args:
            api_token (str): The API token.
        """
        self.environment_id = None
        self.base_path = "/lakehouse/default/Files/BQ_Sync_Process"
        self.libraries_path = "/lakehouse/default/Files/BQ_Sync_Process/libs"

        self.fabric_api = FabricAPI(self.Context.conf.get("trident.workspace.id"), api_token)

        self.__ensure_paths()

    def __ensure_paths(self) -> None:
        """
        Ensures the required paths exist.
        """
        paths = [self.base_path, self.libraries_path]
        Util.ensure_paths(paths)

    def __create_environment_yaml(self) -> None:
        """
        Creates the environment yaml.
        """
        version = Version.CurrentVersion
        environments_yaml = """dependencies:\r\n- pip:\r\n  - fabricsync==<<<VERSION>>>"""
        yaml_path = f"{self.libraries_path}/environment.yml"

        with open(yaml_path, 'w') as file:
            file.write(environments_yaml.replace("<<<VERSION>>>", version))
        
        return yaml_path
    
    def __download_bq_connector(self) -> None:
        """
        Downloads the BigQuery connector.
        """
        connector = SetupUtils.get_bq_spark_connector(spark_version=self.Context.version, 
            jar_path=self.libraries_path)
        return f"{self.libraries_path}/{connector}"

    def __wait_for_publish(self, environment_id:str) -> None:
        """
        Waits for the environment to publish.
        Args:
            environment_id (str): The environment ID.
        """
        try:
            self.Logger.sync_status("Waiting for Fabric Spark Environment publish to complete (5 min timeout)...", verbose=True)
            self.fabric_api.Environment.wait_for_publish(environment_id)
            self.Logger.sync_status("Fabric Spark Environment publish completed!")
        except TimeoutError:
            self.Logger.sync_status("Timed out waiting for Fabric Spark Environment publish to complete...")

    def create_or_update_environment(self, name) -> str:
        """
        Creates or updates the Fabric Spark environment.
        Args:
            name (str): The name.
        """
        try:
            self.Logger.sync_status(f"Creating (or Updating) Fabric Spark Environment: {name}...")
            environment_id, created = self.fabric_api.Environment.get_or_create(name)

            if created:
                self.Logger.sync_status(f"New Fabric Spark Environment created: {name}...", verbose=True)

            environment_yml_path = self.__create_environment_yaml()
            bq_connector_path = self.__download_bq_connector()

            files = [environment_yml_path,bq_connector_path]
            self.fabric_api.Environment.stage_libraries(environment_id, files)

            publish_state = self.fabric_api.Environment.get_publish_state(environment_id)

            if (publish_state != "running"):
                self.Logger.sync_status(f"{name} created/updated and is publishing...")
                self.fabric_api.Environment.publish(environment_id)
                self.__wait_for_publish(environment_id)                
            else:
                self.Logger.sync_status(f"{name} updated but requires MANUAL publishing...")

        except HTTPError as http_err:
            self.Logger.sync_status(f"Fabric API Error: {http_err}")
        except Exception as err:
            self.Logger.sync_status(f"An error occurred: {err}")