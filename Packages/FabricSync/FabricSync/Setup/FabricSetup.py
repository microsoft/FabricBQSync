from packaging import version as pv
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from github import *

import requests
import urllib.request
import os
import json
import shutil
import pandas as pd
from pathlib import Path
import uuid
import random

from ..BQ.Metastore import *
from ..BQ.Logging import *
from ..BQ.Model.Config import *
from ..BQ.Core import *
from ..BQ.SyncUtils import *
from ..BQ.Utils import *
from ..BQ.Exceptions import *
from ..BQ.Constants import SyncConstants
from ..Meta import Version

class SetupUtils():
    @staticmethod
    def read_file_to_string(path:str) -> str:
        contents = ""

        with open(path, 'r') as f:
            contents = f.readlines()  

        return contents

    @staticmethod
    def download_file(url:str, path:str):
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

    @staticmethod
    def download_encoded_to_string(url:str) -> str:
        contents = ""

        for data in urllib.request.urlopen(url):
            contents += data.decode('utf-8')
        
        return contents
    
    @staticmethod
    def ensure_paths(paths):
        for p in paths:
            if not Path(p).is_dir():
                os.mkdir(p)

    @staticmethod
    def get_bq_spark_connector(spark_version, jar_path:str) -> str:
        g = Github()
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
            SetupUtils.download_file(url, lakehouse_path)
        
        return jar_name

    @staticmethod
    def get_latest_bq_spark_connector(releases):
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
    
    @staticmethod
    def initialize_logger(context:SparkSession, config:ConfigDataset):    
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.application_id", config.ApplicationID)
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.name", config.ID)
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_path", config.Logging.LogPath)
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_level", config.Logging.LogLevel)
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.log_telemetry", config.Logging.Telemetry)
        context.conf.set(f"{SyncConstants.SPARK_CONF_PREFIX}.telemetry_endpoint", 
            f"{config.Logging.TelemetryEndPoint}.azurewebsites.net")

        return SyncLogger(context).get_logger()

    @staticmethod
    def generate_base_config(config_path:str, data:str):
        json_obj = json.dumps(data, indent=4)

        with open(config_path, "w") as outfile:
            outfile.write(json_obj)
    
    @staticmethod
    def download_notebooks(workspace_id:str, token:str, git_notebooks, data=None, notebooks_path=None, use_local=False):
        fabric_api = FabricAPI(workspace_id=workspace_id, api_token=token)

        for notebook in git_notebooks:
            try:
                if not use_local:
                    nb_data = SetupUtils.download_encoded_to_string(notebook["url"])
                else:
                    nb_local = SetupUtils.read_file_to_string(f"{notebooks_path}/{notebook['file_name']}")
                    nb_data = "\r\n".join(nb_local)

                if data:
                    nb_data = nb_data.replace("<<<FABRIC_WORKSPACE_ID>>>", data["workspace_id"])
                    nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_ID>>>", data["metadata_lakehouse_id"])
                    nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_NAME>>>", data["metadata_lakehouse"])
                    nb_data = nb_data.replace("<<<PATH_SPARK_BQ_JAR>>>", data["spark_jar_path"])
                    nb_data = nb_data.replace("<<<PATH_TO_USER_CONFIG>>>", data["user_config_file_path"])
                    nb_data = nb_data.replace("<<<VERSION>>>", data["version"])

                fabric_api.upload_fabric_notebook(notebook["name"], nb_data)
            except Exception as e:
                print(e)
                error_msg = f"Failed to deploy BQ Sync notebook to workspace: {notebook['name']}"
                raise SyncInstallError(msg=error_msg) from e

class MetaStoreDataUpdates():
    def process(context:SparkSession, version):
        sv = pv.parse(version)

        match sv.major:
            case 2:
                MetaStoreDataUpdates.process_v2(context)
            case _:
                pass

    
    def process_v2(context:SparkSession):
        deltaTable = DeltaTable.forName(context, "bq_sync_configuration")

        deltaTable.update(
            condition = "table_id IS NULL",
            set = { 'table_id': lit(str(uuid.uuid4())) }
        )

class Installer():
    GIT_URL = "https://raw.githubusercontent.com/microsoft/FabricBQSync/main"

    def __init__(self, context:SparkSession, api_token:str): 
        self.data = {}
        self.Context = context
        self.token = api_token
        self.correlation_id = str(uuid.uuid4())

        self.working_path = "Files/BQ_Sync_Process"
        self.base_path = f"/lakehouse/default/{self.working_path}"
        self.local_path = f"{self.base_path}/sql"
        self.config_path = f"{self.base_path}/config"
        self.libs_path = f"{self.base_path}/libs"
        self.logs_path = f"{self.base_path}/logs"
        self.notebooks_path = f"{self.base_path}/notebooks"

        self._ensure_paths()

    @property
    def cleanup_artifacts(self):
        if "cleanup_artifacts" in self.data:
            return self.data["cleanup_artifacts"]
        else:
            return True

    @property
    def use_local_artifacts(self):
        if "use_local_artifacts" in self.data:
            return self.data["use_local_artifacts"]
        else:
            return False

    def _ensure_paths(self):
        installer_pathes = [self.base_path, self.local_path, self.config_path, 
            self.libs_path, self.logs_path, self.notebooks_path]

        for p in installer_pathes:
            if not Path(p).is_dir():
                os.mkdir(p)

    def _get_sql_source_from_git(self, local_path:str):
        git_content = [
            {"name": "bq_data_types.csv", "url": f"{Installer.GIT_URL}/Setup/v{self.data['asset_version']}/Data/bq_data_types.csv"}
            ]

        for c in git_content:
            try:
                SetupUtils.download_file(c["url"], f"{local_path}/{c['name']}")
            except Exception as e:
                error_msg = f"Unabled to download from git: {c['name']}"
                raise SyncInstallError(msg=error_msg) from e

    def _create_sql_metadata_from_source(self, metadata_lakehouse, local_path:str):
        if not self.use_local_artifacts:
            self._get_sql_source_from_git(local_path)

        self.Context.sql(f"USE {metadata_lakehouse}")
        
        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)
            self._create_metastore_table_from_schema(tbl, schema)
        
        self._load_sql_metadata(local_path)

        if self.cleanup_artifacts:
            shutil.rmtree(local_path)
    
    def _load_sql_metadata(self, local_path:str):
        df_pandas = pd.read_csv(f"{local_path}/bq_data_types.csv")
        df = self.Context.createDataFrame(df_pandas)
        df.write.mode("OVERWRITE").saveAsTable("bq_data_type_map")
    
    def _build_new_config(self):
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
                "metadata_lakehouse": self.data["metadata_lakehouse"],
                "target_lakehouse": self.data["target_lakehouse"]
            },
            "gcp": {
                "projects": [
                {
                    "project_id": self.data["gcp_project_id"],
                    "datasets": [
                    {
                        "dataset": self.data["gcp_dataset_id"]
                    }
                    ]
                }
                ],
                "gcp_credentials": { 
                    "credential": self.data["encoded_credential"]
                }
            },
            "logging": {
                "log_level": "SYNC_STATUS",
                "log_path": f"{self.logs_path}/fabric_sync.log"
            }
        }

        return minimal_config_data

    def _download_bq_connector(self):
        self.data["spark_jar"] = SetupUtils.get_bq_spark_connector(self.Context.version, self.libs_path)
        self.data["spark_jar_path"] = f"abfss://{self.data['workspace_id']}@onelake.dfs.fabric.microsoft.com/" + \
            f"{self.data['metadata_lakehouse_id']}/{self.working_path}/libs/{self.data['spark_jar']}"

    def _generate_config(self, path, config_data):
        SetupUtils.generate_base_config(path, config_data)
        self.data["user_config_file_path"] = path
        
    def _parse_user_config(self, data):
        config_file = data["config_file"]
        config = Path(config_file).read_text()
        config_data = json.loads(config)

        c = {
            "correlation_id": Util.get_config_value(config_data, "correlation_id", None),
            "id": Util.get_config_value(config_data, "id", "BY SYNC LOADER"),
            "version": data["version"],
            "autodiscover": {
                "autodetect": Util.get_config_value(config_data, "autodetect", True),
                "tables": {
                    "enabled": True,
                    "load_all": Util.get_config_value(config_data, "load_all_tables", True)
                },
                "views": {
                    "enabled": Util.get_config_value(config_data, "enable_views", False),
                    "load_all": Util.get_config_value(config_data, "load_all_views", False)
                },
                "materialized_views": {
                    "enabled": Util.get_config_value(config_data, "enable_materialized_views", False),
                    "load_all": Util.get_config_value(config_data, "load_all_materialized_views", False)
                }
            },
            "fabric": {
                "workspace_id": data["workspace_id"],
                "metadata_lakehouse": Util.get_config_value(config_data, "fabric.metadata_lakehouse", raise_error=True),
                "target_lakehouse": Util.get_config_value(config_data, "fabric.target_lakehouse", raise_error=True)
            },
            "gcp": {
                "api": {
                    "use_standard_api": Util.get_config_value(config_data, "use_standard_api", False),
                    "materialization_project_id": None,
                    "materialization_dataset": None,
                    "billing_project_id": None
                },
                "projects": [],
                "gcp_credentials": { 
                    "credential": Util.get_config_value(config_data, "gcp_credentials.credential", raise_error=True)
                }
            },
            "logging": {
                "log_level": "SYNC_STATUS",
                "log_path": f"{self.logs_path}/fabric_sync.log",
                "telemetry_endpoint": "prdbqsyncinsights"
            },
            "async": {
                "enabled": Util.get_config_value(config_data, "async.enabled", True),
                "parallelism": Util.get_config_value(config_data, "async.parallelism", 10),
                "notebook_timeout": Util.get_config_value(config_data, "async.notebook_timeout", 3600),
                "cell_timeout": Util.get_config_value(config_data, "async.cell_timeout", 1800)
            },
            "tables": []
        }

        p_id = None
        d_id = None

        projects = Util.get_config_value(config_data, "gcp_credentials.projects", raise_error=True)

        for project in projects:
            datasets = Util.get_config_value(project, "datasets", raise_error=True)

            ds = []

            for dataset in datasets:
                if not d_id:
                    d_id = Util.get_config_value(dataset, "dataset", raise_error=True)

                d =  {"dataset": Util.get_config_value(dataset, "dataset", raise_error=True)}
                ds.append(d)

            p = {
                "project_id": Util.get_config_value(project, "project_id", raise_error=True),
                "datasets": ds
            }

            if not p_id:
                p_id = Util.get_config_value(project, "project_id", raise_error=True)

            c["gcp"]["projects"].append(p)

        c["gcp"]["api"]["materialization_project_id"] = Util.get_config_value(config_data, "gcp_credentials.materialization_project_id", p_id)
        c["gcp"]["api"]["materialization_dataset"] = Util.get_config_value(config_data, "gcp_credentials.materialization_dataset", d_id)
        c["gcp"]["api"]["billing_project_id"] = Util.get_config_value(config_data, "gcp_credentials.billing_project_id", p_id)

        table_defaults = Util.get_config_value(config_data, "table_defaults", None)
        tables = Util.get_config_value(config_data, "tables", None)

        if table_defaults:
            if "table_options" in table_defaults:
                del table_defaults["table_options"]
                
            c["table_defaults"] = table_defaults
            
        if tables:
            for table in tables:
                if table["table_name"] != "__BLANK__TEMPLATE__":
                    if "partitioned" in table:
                        table['bq_partition'] = table.pop('partitioned')
                    
                    if "table_options" in table:
                        del table["table_options"]
                        
                    c["tables"].append(table)
        
        if len(c["tables"]) == 0:
            del c["tables"]

        return c        

    def _download_sync_notebook(self):
        random_int = random.randint(1, 1000)
        randomizer = f"0000{random_int}"
        git_notebooks = [
                    {"name": f"BQ-Sync-Notebook-v{self.data['version']}-{randomizer[-4:]}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.data['asset_version']}/BQ-Sync.ipynb",
                        "file_name": "BQ-Sync.ipynb"},
                    {"name": f"BQ-Sync-Maintenance-v{self.data['version']}-{randomizer[-4:]}", 
                        "url": f"{Installer.GIT_URL}/Notebooks/v{self.data['asset_version']}/BQ-Sync-Maintenance.ipynb",
                        "file_name": "BQ-Sync-Maintenance.ipynb"}
                    ]
        
        try:
            SetupUtils.download_notebooks(self.data["workspace_id"], self.token, git_notebooks, self.data, self.notebooks_path, self.use_local_artifacts)
            self.Logger.sync_status(f"Notebook successfully copied to workspace, it may take a moment to appear...")
        except SyncInstallError as e:
            raise e
        
        if self.cleanup_artifacts:
            shutil.rmtree(self.notebooks_path)

    def _initialize_installer(self, data, initialize_existing:bool = False):    
        self.data = data 

        if not initialize_existing:
            self.data["workspace_id"] = self.Context.conf.get("trident.workspace.id")
            self.data["version"] = Version.CurrentVersion

        cfg = ConfigDataset()
        
        cfg.ApplicationID = self.correlation_id
        cfg.ID = self.data["loader_name"]
        cfg.Logging.LogPath = f"{self.logs_path}/fabric_sync.log"
        
        self.Context.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        self.Context.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        self.Context.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "7")
        self.Context.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "3")
        self.Context.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        self.Logger = SetupUtils.initialize_logger(self.Context, cfg)

    def _initialize_existing(self, data):
        data["workspace_id"] = self.Context.conf.get("trident.workspace.id")
        data["version"] = Version.CurrentVersion

        config_data = self._parse_user_config(data)

        current_id = Util.get_config_value(config_data, "correlation_id", None)

        if current_id:
            self.correlation_id = current_id
        else:
            config_data["correlation_id"] = self.correlation_id

        data["loader_name"] = Util.get_config_value(config_data, "id", raise_error=True)
        data["metadata_lakehouse"] = Util.get_config_value(config_data, "fabric.metadata_lakehouse", raise_error=True)
        data["target_lakehouse"] = Util.get_config_value(config_data, "fabric.target_lakehouse", raise_error=True)

        return (config_data, data)

    def _create_schema_field_sql(self, table_name, field, schema):
        after_col = None

        for f in schema:
            if f.name == field.name:
                break
            else:
                after_col = f.name

        if after_col:
            return f"ALTER TABLE {table_name} ADD COLUMN {field.name} {field.dataType.simpleString()} AFTER {after_col};"
        else:
            return f"ALTER TABLE {table_name} ADD COLUMN {field.name} {field.dataType.simpleString()}"

    def _create_metastore_table_from_schema(self, table_name, schema):
        df = self.Context.createDataFrame(
            data=self.Context.sparkContext.emptyRDD(),schema=schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{table_name}")

    def _sync_schema(self, table_name, target_schema):
        cmds = []

        df = self.Context.table(table_name)
        source_schema = df.schema

        target_diff = set(target_schema) - set(source_schema)
        source_diff = set(source_schema) - set(target_schema)

        if target_diff or source_diff:
            cmds.append(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');")
        
        if source_diff:
            for f in source_diff:
                cmds.append(f"ALTER TABLE {table_name} DROP COLUMN {f.name};")
        
        if target_diff:
            for f in target_diff:
                cmds.append(self._create_schema_field_sql(table_name, f, target_schema))
        
        return cmds

    def _upgrade_metastore(self, metadata_lakehouse, local_path:str):
        cmds = [f"USE {metadata_lakehouse}"]

        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)

            if self.Context.catalog.tableExists(tbl):
                cmds = cmds + self._sync_schema(tbl, schema)
            else:
                self._create_metastore_table_from_schema(tbl, schema)

        if cmds:
            [self.Context.sql(c) for c in cmds]
        
        if self.cleanup_artifacts:
            shutil.rmtree(local_path)

    @Telemetry.Delta_Maintenance(maintainence_type="SYNC_METADATA")
    def _optimize_metastore(self):
        SyncUtil.optimize_bq_sync_metastore(self.Context)

    def upgrade(self, data):
        try:
            self._run_upgrade(data)
        except Exception as e:
            self.Logger.sync_status(f"BQ Sync Installed Failed!")
            self.Logger.error(e)

    @Telemetry.Upgrade
    def _run_upgrade(self, data):
        with SyncTimer() as t:
            config_data, data = self._initialize_existing(data)
            self._initialize_installer(data, initialize_existing=True)

            av = pv.parse(data["version"])
            self.data["asset_version"] = f"{av.major}.0.0"

            self.Logger.sync_status(f"Starting BQ Sync Upgrade to v{self.data['version']}...")

            self.Logger.sync_status("Get lakehouse metadata...")
            fabric_api = FabricAPI(workspace_id=self.data["workspace_id"], api_token=self.token)
            self.data["workspace_name"] = fabric_api.get_workspace_name()
            self.data["metadata_lakehouse_id"] = fabric_api.get_or_create_lakehouse(self.data["metadata_lakehouse"])
            self.data["target_lakehouse_id"] = fabric_api.get_or_create_lakehouse(self.data["target_lakehouse"])

            config_data["fabric"]["workspace_id"] = self.data["workspace_id"]

            #Encode for embedding in config file
            self.Logger.sync_status("Updating configuration file...")
            current_cfg_file = Path(self.data["config_file"]).stem
            cfg = f"{self.config_path}/{current_cfg_file}_v{self.data['version']}.json"
            self._generate_config(cfg, config_data)

            if not self.data["config_file_only"]:
                #Create metadata tables and required metadata
                self.Logger.sync_status("Updating metadata objects...")
                self._upgrade_metastore(self.data["metadata_lakehouse"], self.local_path)

                MetaStoreDataUpdates.process(self.Context, data["version"])

                self.Logger.sync_status("Optimizing BQ Sync Metastore...")
                self._optimize_metastore()

                #Download the appropriate jar for the current spark runtime
                self.Logger.sync_status("Updating BigQuery Spark connector libraries..")
                self._download_bq_connector()

                #Get sync notebooks from Git, customize and install into the workspace
                self.Logger.sync_status("Copying updated BQ Sync artifacts to Fabric workspace...")
                self._download_sync_notebook()
        
        self.Logger.sync_status(f"BQ Sync Upgrade finished in {str(t)}!")

    def install(self, data):
        try:
            self._run_install(data)
        except Exception as e:
            self.Logger.sync_status(f"BQ Sync Installed Failed!")
            self.Logger.error(e)

    @Telemetry.Install
    def _run_install(self, data):
        with SyncTimer() as t:
            self._initialize_installer(data)

            av = pv.parse(data["version"])
            data["asset_version"] = f"{av.major}.0.0"

            self.Logger.sync_status("Starting BQ Sync Installer...")

            if not os.path.isfile(self.data["gcp_credential_path"]):
                self.Logger.error("GCP Credentials not found")
                raise Exception("""GCP Credentials not found. 
                Please make sure your credentials have been uploaded to the environment and that the gcp_credential_path parameter is correct.""")

            #Create Lakehouses
            self.Logger.sync_status("Creating metadata and mirror lakehouses (if not exists)...")
            fabric_api = FabricAPI(workspace_id=self.data["workspace_id"], api_token=self.token)
            self.data["workspace_name"] = fabric_api.get_workspace_name()
            self.data["metadata_lakehouse_id"] = fabric_api.get_or_create_lakehouse(self.data["metadata_lakehouse"])
            self.data["target_lakehouse_id"] = fabric_api.get_or_create_lakehouse(self.data["target_lakehouse"])

            #Create metadata tables and required metadata
            self.Logger.sync_status("Creating required metadata objects...")
            self._create_sql_metadata_from_source(self.data["metadata_lakehouse"], self.local_path)

            #Download the appropriate jar for the current spark runtime
            self.Logger.sync_status("Downloading BigQuery Spark connector libraries..")
            self._download_bq_connector()

            #Encode for embedding in config file
            self.Logger.sync_status("Generating initial configuration file...")
            credential_data = SetupUtils.read_file_to_string(self.data["gcp_credential_path"])
            credential_data = [l.strip() for l in credential_data]
            credential_data = ''.join(credential_data)

            encoded_credential = Util.encode_base64(credential_data)
            self.data["encoded_credential"] = encoded_credential

            config_data = self._build_new_config()

            loader_cfg = self.data["loader_name"].replace(" ", "_")
            cfg = f"{self.config_path}/{loader_cfg}_v{self.data['version']}.json"
            self._generate_config(cfg, config_data)

            #Get sync notebooks from Git, customize and install into the workspace
            self.Logger.sync_status("Copying BQ Sync artifacts to Fabric workspace...")

            self._download_sync_notebook()
        
        self.Logger.sync_status(f"BQ Sync Installer finished in {str(t)}!")