from packaging import version
from pyspark.sql import SparkSession
#from notebookutils import mssparkutils
from github import *

import requests
import urllib.request
import os
import json
import shutil
import pandas as pd
from pathlib import Path
from ..Admin.FabricAPI import FabricAPIUtil

class SetupUtil():
    def __init__(self, session:SparkSession):
        self.session = session

    def read_file_to_string(self, path:str) -> str:
        contents = ""

        with open(path, 'r') as f:
            contents = f.readlines()  

        return contents

    def download_file(self, url:str, path:str):
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

    def download_encoded_to_string(self, url:str) -> str:
        contents = ""

        for data in urllib.request.urlopen(url):
            contents += data.decode('utf-8')
        
        return contents
        
    def get_bq_spark_connector(self, jar_path:str) -> str:
        g = Github()
        repo = g.get_repo("GoogleCloudDataproc/spark-bigquery-connector")
        latest_release = self.get_latest_bq_spark_connector(repo.get_releases())

        sv = version.parse(self.session.version)
        jar_name = f"spark-{sv.major}.{sv.minor}-bigquery-{latest_release.title}.jar"

        jars = [j for j in latest_release.get_assets() if j.name == jar_name]

        if jars:
            jar = jars[0]

        url = jar.browser_download_url
        lakehouse_path = f"{jar_path}/{jar.name}"

        if not os.path.isfile(lakehouse_path):
            self.download_file(url, lakehouse_path)
        
        return jar_name

    def get_latest_bq_spark_connector(self, releases):
        lr = None
        lv = None
        
        for r in releases:
            if r.title:
                if lr:
                    v = version.parse(r.title)

                    if v > lv:
                        lr = r
                        lv = v
                else:
                    lr = r
                    lv = version.parse(r.title)

        return lr

    def get_sql_source_from_git(self, local_path:str):
        git_content = [
            {"name": "bq_data_types.csv", "url": "https://raw.githubusercontent.com/microsoft/FabricBQSync/main/Setup/Data/bq_data_types.csv"},
            {"name": "MetadataRepo.sql", "url":"https://raw.githubusercontent.com/microsoft/FabricBQSync/main/Setup/SQL/MetadataRepo.sql"}
            ]

        for c in git_content:
            self.download_file(c["url"], f"{local_path}/{c['name']}")

    def create_sql_metadata_from_source(self, metadata_lakehouse, local_path:str):
        self.get_sql_source_from_git(local_path)

        sql_contents = self.read_file_to_string(f"{local_path}/MetadataRepo.sql")

        self.session.sql(f"USE {metadata_lakehouse}")
        query = ""

        for line in sql_contents:
            query += line

            if ';' in line:
                self.session.sql(query)
                query = ""
        
        self.load_sql_metadata(local_path)

        shutil.rmtree(local_path)
    
    def load_sql_metadata(self, local_path:str):
        df_pandas = pd.read_csv(f"{local_path}/bq_data_types.csv")
        df = self.session.createDataFrame(df_pandas)
        df.write.mode("OVERWRITE").saveAsTable("bq_data_type_map")

    def generate_base_config(self, config_path:str, data:str):
        json_obj = json.dumps(data, indent=4)

        with open(config_path, "w") as outfile:
            outfile.write(json_obj)
    
    def run_installer(self, data: str):
        print("Starting BQ Sync Installer...")

        fabric_api = FabricAPIUtil()
        working_path = "Files/BQ_Sync_Process"
        #base_path = "/lakehouse/default/Files/sync_setup"
        base_path = f"/lakehouse/default/{working_path}"
        local_path = f"{base_path}/sql"
        config_path = f"{base_path}/config"
        libs_path = f"{base_path}/libs"
        

        if not os.path.isfile(data["gcp_credential_path"]):
            raise Exception("""GCP Credentials not found. 
            Please make sure your credentials have been uploaded to the environment and that the gcp_credential_path parameter is correct.""")

        installer_pathes = [base_path, local_path, config_path, libs_path]

        for p in installer_pathes:
            if not Path(p).is_dir():
                os.mkdir(p)

        #Create Lakehouses
        workspace_id = fabric_api.get_workspace_id()
        workspace_name = fabric_api.get_workspace_name()
        metadata_lakehouse_id = fabric_api.get_or_create_lakehouse(workspace_id, data["metadata_lakehouse"])
        target_lakehouse_id = fabric_api.get_or_create_lakehouse(workspace_id, data["target_lakehouse"])

        #Create metadata tables and required metadata
        self.create_sql_metadata_from_source(data["metadata_lakehouse"], local_path)

        #Download the appropriate jar for the current spark runtime
        spark_jar = self.get_bq_spark_connector(libs_path)
        spark_jar_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{metadata_lakehouse_id}/{working_path}/libs/{spark_jar}"

        #Download the sync python package from Git
        wheel_name = f"FabricSync-1.0.0-py3-none-any.whl"
        wheel_url = f"https://github.com/microsoft/FabricBQSync/raw/main/Packages/FabricSync/dist/{wheel_name}"

        self.download_file(wheel_url, f"{libs_path}/{wheel_name}")

        wheel_path = f"/lakehouse/default/{working_path}/libs/{wheel_name}"

        #Encode for embedding in config file
        credential_data = self.read_file_to_string(data["gcp_credential_path"])
        credential_data = [l.strip() for l in credential_data]
        credential_data = ''.join(credential_data)

        encoded_credential = fabric_api.encode_base64(credential_data)

        #Create default config file
        config_data = {
            "id":data["loader_name"],
            "load_all_tables":True,
            "autodetect":True,
            "fabric":{
                "workspace_id":workspace_name,
                "metadata_lakehouse":data["metadata_lakehouse"],
                "target_lakehouse":data["target_lakehouse"]
            },
            "gcp_credentials":{
                "projects": [
                    {
                        "project_id":data["gcp_project_id"],
                        "datasets": [
                            {"dataset":data["gcp_dataset_id"]}
                        ]
                    }
                ],
                "materialization_project_id":data["gcp_project_id"],
                "materialization_dataset":data["gcp_dataset_id"],
                "billing_project_id":data["gcp_project_id"],
                "credential":encoded_credential
            },        
            "async":{
                "enabled":True,
                "parallelism":5,
                "cell_timeout":0,
                "notebook_timeout":0
            },
            "table_defaults":{
                "priority": 100,
                "project_id":data["gcp_project_id"],
		        "dataset":data["gcp_dataset_id"], 
                "object_type":"BASE_TABLE",
                "enabled": True
            },
            "tables": [{
                "table_name": "__BLANK__TEMPLATE__",
                "enabled": False
            }]
	    }

        cfg = f"{config_path}/fabric_bq_config.json"
        self.generate_base_config(cfg, config_data)
        user_config_file_path = f"/lakehouse/default/{working_path}/config/{Path(cfg).name}"

        ##Copy configs and libs to working path
        #mssparkutils.fs.cp(f"{base_path.replace('/lakehouse/default/', '')}/", \
        #    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{metadata_lakehouse_id}/{working_path}",
        #    recurse=True)
        
        ##Clean up installer files
        #shutil.rmtree(base_path)

        #Get sync notebooks from Git, customize and install into the workspace
        git_notebooks = [ \
            {"name": "BQ-Sync-Notebook", "url": "https://raw.githubusercontent.com/microsoft/FabricBQSync/main/Notebooks/BQ-Sync.ipynb"}]

        for notebook in git_notebooks:
            nb_data = self.download_encoded_to_string(notebook["url"])

            nb_data = nb_data.replace("<<<FABRIC_WORKSPACE_ID>>>", workspace_id)
            nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_ID>>>", metadata_lakehouse_id)
            nb_data = nb_data.replace("<<<METADATA_LAKEHOUSE_NAME>>>", data["metadata_lakehouse"])
            nb_data = nb_data.replace("<<<PATH_SPARK_BQ_JAR>>>", spark_jar_path)
            nb_data = nb_data.replace("<<<PATH_TO_BQ_SYNC_PACKAGE>>>", wheel_path)
            nb_data = nb_data.replace("<<<PATH_TO_USER_CONFIG>>>", user_config_file_path)

            fabric_api.upload_fabric_notebook(workspace_id, notebook["name"], nb_data)
        
        print("BQ Sync Installer Finished!")