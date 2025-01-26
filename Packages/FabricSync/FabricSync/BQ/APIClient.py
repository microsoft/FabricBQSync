import requests
from requests.models import Response
import json
import os
from threading import Thread
import time

from typing import (
    Dict, Tuple, Any
)

from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Enum import FabricItemType

class RestAPIProxy:
    def __init__(self, base_url, headers=None) -> None:
        """
        Rest API Proxy
        Args:
            base_url (str): Base URL
            headers (Dict): Headers
        """
        self.base_url = base_url
        self.headers = headers

    def get(self, endpoint, params=None, headers=None) -> Response:
        """
        Gets a resource from the API
        Args:
            endpoint (str): API endpoint
            params (Dict): Parameters
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        response = requests.get(f"{self.base_url}/{endpoint}", params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None, files=None, headers=None) -> Response:
        """
        Posts a resource to the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            files (Dict): Files
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        response = requests.post(f"{self.base_url}/{endpoint}", data=data, json=json, files=files, headers=headers)
        return self._handle_response(response)

    def put(self, endpoint, data=None, json=None, headers=None) -> Response:
        """
        Puts a resource in the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        response = requests.put(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)
    
    def patch(self, endpoint, data=None, json=None, headers=None) -> Response:
        """
        Patches a resource in the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        response = requests.patch(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint, headers=None) -> Response:
        """
        Deletes a resource from the API
        Args:
            endpoint (str): API endpoint
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers
            
        response = requests.delete(f"{self.base_url}/{endpoint}", headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response) -> Response:
        """
        Handles the response from the API
        Args:
            response (Response): Response from the API
        Returns:
            Response: Response from the API
        """
        if response.status_code in (200, 201, 202):
            return response
        else:
            response.raise_for_status()

class BaseFabricItem:
    def __init__(self, type:FabricItemType, workspace_id:str, api_token:str):
        """
        Base Fabric Item API Client
        Args:
            type (FabricItemType): Fabric item type
            workspace_id (str): Workspace ID
            api_token (str): API Token
        """
        self.workspace_id = workspace_id
        self.token = api_token
        self.type = type

        headers = {'Content-Type':'application/json','Authorization': f'Bearer {self.token}'}
        self.rest_api_proxy = RestAPIProxy(base_url="https://api.fabric.microsoft.com/v1/", headers=headers)
    
    @property
    def uri(self) -> str:
        """
        Returns the URI of the Fabric item
        Returns:
            str: URI of the Fabric item
        """
        if self.type == FabricItemType.WORKSPACE:
            return f"{self.type}"
        else:
            return f"workspaces/{self.workspace_id}/{self.type}"
    
    def create(self, name:str, data:Dict=None) -> str:
        """
        Creates a Fabric item
        Args:
            name (str): Name of the Fabric item
            data (Dict): Data to create the Fabric item
        Returns:
            str: Fabric item ID
        """
        if not data:
            data = {"displayName": name}
        response = self.rest_api_proxy.post(endpoint=self.uri, json=data).json()
        
        return response["id"]

    def get(self, id:str) -> str:
        """
        Gets a Fabric item by ID
        Args:
            id (str): ID of the Fabric item
        Returns:
            str: Fabric item
        """
        return self.rest_api_proxy.get(endpoint=f"{self.uri}/{id}").json()
    
    def get_name(self, id:str) -> str:
        """
        Gets the name of a Fabric item by ID
        Args:
            id (str): ID of the Fabric item
        Returns:
            str: Name of the Fabric item
        """
        response = self.get(id)
        return response["displayName"]

    def get_id(self, name:str) -> str:
        """
        Gets the ID of a Fabric item by name
        Args:
            name (str): Name of the Fabric item
        Returns:
            str: ID of the Fabric item
        """
        response = self.list()
        return [x["id"] for x in response["value"] if x["displayName"].lower() == name.lower()][0]
    
    def list(self) -> str:
        """
        Lists all Fabric items
        Returns:
            str: Fabric items
        """ 
        return self.rest_api_proxy.get(endpoint=self.uri).json()
    
    def _do_fabric_item_command(self, id:str, path:str) -> str:
        """
        Executes a command on a Fabric item
        Args:
            id (str): ID of the Fabric item
            path (str): Path to the command
        Returns:
            str: Response from the Fabric API
        """
        return self.rest_api_proxy.post(endpoint=f"{self.uri}/{id}/{path}").json()

    def get_by_name(self, name:str) -> str:
        """
        Gets a Fabric item by name
        Args:
            name (str): Name of the Fabric item
        Returns:
            str: Fabric item
        """
        response = self.list()
        return [x for x in response["value"] if x["displayName"].lower() == name.lower()][0]
    
    def get_or_create(self, name:str, data:Dict = None) -> Tuple[str, bool]:
        """
        Gets or creates a Fabric item
        Args:
            name (str): Name of the Fabric item
            data (Dict): Data to create the Fabric item
        Returns:
            Tuple[str, bool]: Fabric item ID and flag indicating if the item is new
        """
        id = self.get_id(name)
        is_new = False

        if not id:
            id = self.create(name, data)
            is_new = True
        
        return (id, is_new)
    
    def __run_with_timeout(self, api_function:callable, id:str, result:Any, while_match:bool, timeout:int, poll_seconds:int): 
        """
        Runs the Fabric API function with a timeout
        Args:
            api_function (callable): API function to call
            id (str): ID of the Fabric item
            result (Any): Expected result
            while_match (bool): Flag to indicate if the status should match the expected result
            timeout (int): Timeout in minutes
            poll_seconds (int): Polling interval in seconds
        """       
        thread = Thread(target=self.__poll_status, args=(api_function, id, result, while_match, poll_seconds))
        thread.start()
        thread.join(timeout*60)
        
        if thread.is_alive():
            raise TimeoutError()

    def __poll_status(self, api_function:callable, id:str, result:Any, while_match:bool, poll_seconds:int):
        """
        Polls the Fabric API for the status of the Fabric item
        Args:
            api_function (callable): API function to call
            id (str): ID of the Fabric item
            result (Any): Expected result
            while_match (bool): Flag to indicate if the status should match the expected result
            poll_seconds (int): Polling interval in seconds
        """
        while (True):
            state = api_function(id)
            print(f"POLLING FABRIC API: {self.type} for {result} status State: {state}...")

            if state == result and not while_match:
                break
            elif state != result and while_match:
                break

            time.sleep(poll_seconds)

    def _wait_status(self, api_function:callable, id:str, result:Any, 
                     while_match:bool=True, timeout_minutes:int = 1, poll_seconds:int = 1):
        """
        Waits for the status of the Fabric API to match the expected result
        Args:
            api_function (callable): API function to call
            id (str): ID of the Fabric item
            result (Any): Expected result
            while_match (bool): Flag to indicate if the status should match the expected result
            timeout_minutes (int): Timeout in minutes
            poll_seconds (int): Polling interval in seconds
        """
        try:
            self.__run_with_timeout(api_function, id, result, while_match, timeout_minutes, poll_seconds)
            print(f"POLLING FABRIC API: {self.type} {result} complete")
        except TimeoutError:
            print("Timed out waiting for environment publish to complete...")

class FabricWorkspace(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Workspace API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricWorkspace: Fabric Workspace API Client
        """
        super().__init__(FabricItemType.WORKSPACE, workspace_id, api_token)

class FabricLakehouse(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Lakehouse API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricLakehouse: Fabric Lakehouse API Client
        """
        super().__init__(FabricItemType.LAKEHOUSE, workspace_id, api_token)

    def create(self, name:str, enable_schemas:bool=False) -> str:
        """
        Creates a lakehouse
        Args:
            name (str): Name of the lakehouse
            enable_schemas (bool): Enable schemas
        Returns:
                str: Lakehouse ID
        """
        data = {
            "displayName": name,
            "description": f"Fabric Sync Lakehouse: {name}",
            "creationPayload": {
                "enableSchemas": enable_schemas
            }
        }

        return super().create(name, data)

    def has_schema(self, id:str):
        """
        Checks if the lakehouse has a default schema
        Args:
            id (str): Lakehouse ID
        Returns:
                bool: True if the lakehouse has a default schema, False otherwise
        """
        result = self.get(id)
        return "defaultSchema" in result["properties"]

class FabricEnvironment(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Environment API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricEnvironment: Fabric Environment API Client
        """
        super().__init__(FabricItemType.ENVIRONMENT, workspace_id, api_token)

    def publish(self, id:str) -> str:
        """
        Publishes the environment
        Args:
            id (str): Environment ID
        Returns:
            str: Response from the Fabric API
        """
        return self._do_fabric_item_command(id, "staging/publish")
    
    def stage_file(self, id:str, file_path:str) -> str:
        """
        Stages a file to the environment
        Args:
            id (str): Environment ID
            file_path (str): File path
        Returns:
            str: Response from the Fabric API
        """
        headers = {'Authorization': f"Bearer {self.token}"}
        uri = f'workspaces/{self.workspace_id}/environments/{id}/staging/libraries'

        with open(file_path,'rb') as file:        
            files = {'file':(os.path.basename(file_path),file)}
            response = self.rest_api_proxy.post(endpoint=uri, files=files, headers=headers)
    
        return response

    def stage_libraries(self, id:str, files) -> None:
        """
        Stages files to the environment
        Args:
            id (str): Environment ID
            files (list): List of file paths
        """
        for file in files:
            self.stage_file(id, file)
    
    def get_publish_state(self, id) -> str:
        """
        Returns the publish state of the environment
        Args:
            id (str): Environment ID
        Returns:
                str: Publish state of the environment
        """
        environment = self.get(id)

        if environment:
            return environment["properties"]["publishDetails"]["state"]
        
        return None
    
    def wait_for_publish(self, id) -> None:
        """
        Waits for the environment to be published
        Args:
            id (str): Environment ID
        """
        self._wait_status(self.get_publish_state, id, "running", 
                          while_match=True, timeout_minutes=5, poll_seconds=15)

class FabricOpenMirroredDatabase(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Open Mirrored Database API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricOpenMirroredDatabase: Fabric Open Mirrored Database API Client
        """
        super().__init__(FabricItemType.MIRRORED_DATABASE, workspace_id, api_token)

    def create(self, name:str) -> str:
        """
        Creates a mirrored database
        Args:
            name (str): Name of the mirrored database
        Returns:
            str: Mirrored Database ID
        """
        mirror_db_definition = {
                "properties": {
                    "source": {
                        "type": "GenericMirror",
                        "typeProperties": {}
                    },
                    "target": {
                        "type": "MountedRelationalDatabase",
                        "typeProperties": {
                            "format": "Delta"
                        }
                    }
                }
            }
        
        encode_data = Util.encode_base64(json.dumps(mirror_db_definition))
        payload = {
            "displayName": name,
            "description": f"Fabric Sync - {name}",
            "definition": {
                "parts": [
                    {
                        "path": "mirroring.json",
                        "payload": encode_data,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }

        return super().create(name, payload)

    def start_mirroring(self, id:str) -> str:
        """
        Starts the mirroring operation
        Args:
            id (str): Mirrored Database ID
        Returns:
            str: Response from the Fabric API
        """
        return self._do_fabric_item_command(id, "startMirroring")
    
    def stop_mirroring(self, id:str) -> str:
        """
        Stops the mirroring operation
        Args:
            id (str): Mirrored Database ID
        Returns:
            str: Response from the Fabric API
        """
        return self._do_fabric_item_command(id, "stopMirroring")
    
    def get_mirroring_status(self, id:str) -> str:
        """
        Returns the status of the mirroring operation
        Args:
            id (str): Mirrored Database ID
        Returns:
            str: Status of the mirroring operation
        """
        response = self._do_fabric_item_command(id, "getMirroringStatus")
        return response["status"]
    
    def wait_for_mirroring(self, id:str, status:str) -> None:
        """
        Waits for the mirroring operation to complete
        Args:
            id (str): Mirrored Database ID
        """
        self._wait_status(self.get_mirroring_status, id, status, 
                          while_match=True, timeout_minutes=1, poll_seconds=5)

class FabricNotebook(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Notebook API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        """
        super().__init__(FabricItemType.NOTEBOOK, workspace_id, api_token)

    def upload(self, name:str, data:str) -> str:
        """
        Uploads a notebook to Fabric
        Args:
            name (str): Name of the notebook
            data (str): Notebook content
        Returns:
            str: Notebook ID
        """
        encode_data = Util.encode_base64(data)
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

        return self.rest_api_proxy.post(endpoint=self.uri, json=notebook_payload)
        
class FabricAPI:
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric API Client
        """
        self.Workspace = FabricWorkspace(workspace_id, api_token)
        self.Lakehouse = FabricLakehouse(workspace_id, api_token)
        self.Environment = FabricEnvironment(workspace_id, api_token)
        self.OpenMirroredDatabase = FabricOpenMirroredDatabase(workspace_id, api_token)
        self.Notebook = FabricNotebook(workspace_id, api_token)  