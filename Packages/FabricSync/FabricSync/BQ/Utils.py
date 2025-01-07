import requests
import base64 as b64
from os import path

class Util():
    @staticmethod
    def encode_base64(val:str) -> str:
        b = b64.b64encode(bytes(val, 'utf-8'))
        return b.decode('utf-8')

    @staticmethod
    def get_config_value(json_data, json_path, default=None, raise_error=False):
        paths = json_path.split('.')
        level = json_data

        for p in paths:
            if p in level:
                level = level[p]
                val = level
            else:
                val = None
                break
        
        if not val and raise_error:
            raise ValueError(f"Missing Key: {json_path}")
        elif not val:
            return default
        
        return val

    @staticmethod
    def assign_enum_val(enum_class, value):
        try:
            return enum_class(value)
        except ValueError:
            return None
    
    @staticmethod
    def assign_enum_name(enum_class, name):
        try:
            return enum_class[name]
        except ValueError:
            return None

class RestAPIProxy:
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.headers = headers

    def get(self, endpoint, params=None, headers=None):
        if not headers:
            headers = self.headers

        response = requests.get(f"{self.base_url}/{endpoint}", params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None, files=None, headers=None):
        if not headers:
            headers = self.headers

        response = requests.post(f"{self.base_url}/{endpoint}", data=data, json=json, files=files, headers=headers)
        return self._handle_response(response)

    def put(self, endpoint, data=None, json=None, headers=None):
        if not headers:
            headers = self.headers

        response = requests.put(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint, headers=None):
        if not headers:
            headers = self.headers
            
        response = requests.delete(f"{self.base_url}/{endpoint}", headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response):
        if response.status_code in (200, 201, 202):
            return response
        else:
            response.raise_for_status()

class FabricAPI():
    def __init__(self, workspace_id:str, api_token:str):
        self.workspace_id = workspace_id
        self.token = api_token

        headers = {'Content-Type':'application/json','Authorization': f'Bearer {self.token}'}
        self.rest_api_proxy = RestAPIProxy(base_url="https://api.fabric.microsoft.com/v1/", headers=headers)
    
    def create_spark_environment(self, environment_name):
        uri = f"workspaces/{self.workspace_id}/environments"
        data = {
            "displayName": environment_name
        }

        result = self.rest_api_proxy.post(endpoint=uri, json=data).json()
        return result["id"]
    
    def create_lakehouse(self, lakehouse_name):
        uri = f"workspaces/{self.workspace_id}/lakehouses"
        data = {
            "displayName": lakehouse_name
        }

        result = self.rest_api_proxy.post(endpoint=uri, json=data).json()
        return result["id"]

    def get_workspace_name(self) -> str:
        uri = f"workspaces/{self.workspace_id}/"
        response = self.rest_api_proxy.get(endpoint=uri).json()

        return response["displayName"]

    def get_or_create_lakehouse(self, lakehouse_name:str) -> str:
        lakehouse_id = self.get_lakehouse_id(lakehouse_name)

        if not lakehouse_id:
            lakehouse_id = self.create_lakehouse(lakehouse_name)

        return lakehouse_id

    def get_or_create_spark_environment(self, environment_name:str) -> str:
        environment_id = self.get_spark_environment_by_name(environment_name)

        if not environment_id:
            environment_id = self.create_spark_environment(environment_name)
        
        return environment_id

    def get_lakehouse_id(self, lakehouse_name: str) -> str:
        uri = f"workspaces/{self.workspace_id}/lakehouses"
        response = self.rest_api_proxy.get(endpoint=uri).json()

        o = [x["id"] for x in response["value"] if x["displayName"].lower() == lakehouse_name.lower()]

        if o:
            return o[0]

        return None

    def get_spark_environment_by_name(self, environment_name):
        uri = f"workspaces/{self.workspace_id}/environments"
        result = self.rest_api_proxy.get(endpoint=uri).json()

        if result and "value" in result:
            environment = [e["id"] for e in result["value"] if e["displayName"] == environment_name]

            if environment:
                return environment[0]

        return None

    def get_spark_environment_publish_state(self, environment_id):
        environment = self.get_spark_environment(environment_id)

        if environment:
            return environment["properties"]["publishDetails"]["state"]
        
        return None
    
    def get_spark_environment(self, environment_id):
        uri = f"workspaces/{self.workspace_id}/environments/{environment_id}/"
        return self.rest_api_proxy.get(endpoint=uri).json()

    def publish_spark_environment(self, environment_id):
        uri = f"workspaces/{self.workspace_id}/environments/{environment_id}/staging/publish"
        return self.rest_api_proxy.post(endpoint=uri).json()
    
    def stage_file_to_spark_environment(self, environment_id, file_path):
        headers = {'Authorization': f"Bearer {self.token}"}
        uri = f'workspaces/{self.workspace_id}/environments/{environment_id}/staging/libraries'

        with open(file_path,'rb') as file:        
            files = {'file':(path.basename(file_path),file)}
            self.rest_api_proxy.post(endpoint=uri, files=files, headers=headers)

    def stage_spark_environment_libraries(self, environment_id, files):
        for file in files:
            self.stage_file_to_spark_environment(environment_id, file)

    def upload_fabric_notebook(self, name:str, data:str):
        encode_data = Util.encode_base64(data)
        uri = f"workspaces/{self.workspace_id}/notebooks"

        notebook_payload = {
            "displayName": name,
            "description": f"Fabric BQ Sync: {name}",
            "definition": {
            "format": "ipynb",
            "parts": [
                {
                "path": "notebook-content.py",
                "payload": encode_data,
                "payloadType": "InlineBase64"
                }
            ]
            }
        }

        r = self.rest_api_proxy.post(endpoint=uri, json=notebook_payload)