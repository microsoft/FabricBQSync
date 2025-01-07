from requests.exceptions import HTTPError

import time
import threading
from packaging import version as pv

from pyspark.sql import SparkSession
from ..Meta import Version
from ..BQ.Utils import FabricAPI
from ..Setup import SetupUtils

class FabricEnvironmentUtil():
    def __init__(self, context:SparkSession, api_token:str):
        self.Context = context

        self.environment_id = None
        self.base_path = "/lakehouse/default/Files/BQ_Sync_Process"
        self.libraries_path = "/lakehouse/default/Files/BQ_Sync_Process/libs"

        workspace_id=self.Context.conf.get("trident.workspace.id")
        self.fabric_api = FabricAPI(workspace_id=workspace_id, api_token=api_token)

        self._ensure_paths()

    def _ensure_paths(self):
        paths = [self.base_path, self.libraries_path]
        SetupUtils.ensure_paths(paths)

    def _create_environment_yaml(self):
        version = Version.CurrentVersion
        environments_yaml = """dependencies:\r\n- pip:\r\n  - fabricsync==<<<VERSION>>>"""
        yaml_path = f"{self.libraries_path}/environment.yml"

        with open(yaml_path, 'w') as file:
            file.write(environments_yaml.replace("<<<VERSION>>>", version))
        
        return yaml_path
    
    def _download_bq_connector(self):
        connector = SetupUtils.get_bq_spark_connector(spark_version=self.Context.version, 
            jar_path=self.libraries_path)
        return f"{self.libraries_path}/{connector}"

    def _check_publishing_status(self):
        if not self.environment_id:
            return None

        return self.fabric_api.get_spark_environment_publish_state(self.environment_id)

    def _run_with_timeout(self, func, timeout):
        def wrapper():
            func()
        
        thread = threading.Thread(target=wrapper)
        thread.start()
        thread.join(timeout*60)
        
        if thread.is_alive():
            raise TimeoutError()

    def _poll_publish_status(self):
        state = self._check_publishing_status()

        while (state == "running"):
            print("Environment still publishing...")
            time.sleep(15)
            state = self._check_publishing_status()

    def wait_for_publish(self, timeout_minutes:int = 5):
        try:
            self._run_with_timeout(self._poll_publish_status, timeout_minutes)
            print("Environment publish completed!")
        except TimeoutError:
            print("Timed out waiting for environment publish to complete...")

    def create_or_update_environment(self, name):
        try:
            print(f"Create (or Update) Environment: {name}...")
            self.environment_id = self.fabric_api.get_or_create_spark_environment(name)

            environment_yml_path = self._create_environment_yaml()
            bq_connector_path = self._download_bq_connector()

            files = [environment_yml_path,bq_connector_path]
            self.fabric_api.stage_spark_environment_libraries(self.environment_id, files)

            publish_state = self.fabric_api.get_spark_environment_publish_state(self.environment_id)

            if (publish_state != "running"):
                result = self.fabric_api.publish_spark_environment(self.environment_id)
                print(f"{name} created/updated and is publishing...")
            else:
                print(f"{name} updated but requires MANUAL publishing...")

        except HTTPError as http_err:
            print(f"Fabric API Error: {http_err}")
        except Exception as err:
            print(f"An error occurred: {err}")