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
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Http import RestAPIProxy

class BaseFabricItem(ContextAwareBase):
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
    
    def create(self, name:str, data:Dict=None, update_hook:callable = None) -> str:
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

        response = self.rest_api_proxy.post(endpoint=self.uri, json=data, lro_update_hook=update_hook)

        try:
            response = response.json()
            return response["id"]
        except Exception:
            return None

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
        response = self.get_by_name(name)

        if response:
            return response["id"]
        
        return None
    
    def list(self) -> str:
        """
        Lists all Fabric items
        Returns:
            str: Fabric items
        """ 
        return self.rest_api_proxy.get(endpoint=self.uri).json()
    
    def _do_fabric_item_command(self, id:str, path:str) -> Response:
        """
        Executes a command on a Fabric item
        Args:
            id (str): ID of the Fabric item
            path (str): Path to the command
        Returns:
            str: Response from the Fabric API
        """
        return self.rest_api_proxy.post(endpoint=f"{self.uri}/{id}/{path}")

    def get_by_name(self, name:str) -> str:
        """
        Gets a Fabric item by name
        Args:
            name (str): Name of the Fabric item
        Returns:
            str: Fabric item
        """
        response = self.list()

        if response and "value" in response:
            filter = [x for x in response["value"] if x["displayName"].lower() == name.lower()]

            if filter:
                return filter[0]
        
        return None
    
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
    
    def __run_with_timeout(self, api_function:callable, id:str, result:Any, while_match:bool, 
                        timeout:int, poll_seconds:int, update_hook:callable=None): 
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
        thread = Thread(target=self.__poll_status, args=(api_function, id, result, 
                            while_match, poll_seconds, update_hook))
        thread.start()
        thread.join(timeout*60)
        
        if thread.is_alive():
            raise TimeoutError()

    def __poll_status(self, api_function:callable, id:str, result:Any, while_match:bool, 
        poll_seconds:int, update_hook:callable):
        """
        Polls the Fabric API for the status of the Fabric item
        Args:
            api_function (callable): API function to call
            id (str): ID of the Fabric item
            result (Any): Expected result
            while_match (bool): Flag to indicate if the status should match the expected result
            poll_seconds (int): Polling interval in seconds
            update_hook (callable): Update hook
        """

        complete = False

        while (True):
            state = api_function(id)
            self.Logger.debug(f"POLLING FABRIC API: {self.type} for {result} status State: {state}...")

            if state == result and not while_match:
                complete = True
                break
            elif state != result and while_match:
                complete = True
                break
            
            if update_hook:
                update_hook(state)

            time.sleep(poll_seconds)
        
        if complete and update_hook:
            update_hook("Completed")

    def _wait_status(self, api_function:callable, id:str, result:Any, while_match:bool=True, 
        timeout_minutes:int = 1, poll_seconds:int = 1, raise_error = False, update_hook:callable=None) -> str:
        """
        Waits for the status of the Fabric item to match the expected result or timeout
        Args:
            api_function (callable): API function to call
            id (str): ID of the Fabric item
            result (Any): Expected result
            while_match (bool): Flag to indicate if the status should match the expected result
            timeout_minutes (int): Timeout in minutes
            poll_seconds (int): Polling interval in seconds
            raise_error (bool): Flag to indicate if an error should be raised on timeout
            update_hook (callable): Update hook
        Returns:
            str: Status of the Fabric item
        """
        try:
            self.__run_with_timeout(api_function, id, result, while_match, timeout_minutes, poll_seconds, update_hook)
            self.Logger.debug(f"{self.type} STATUS POLLING FABRIC API: {result} complete")

            if not while_match:
                state = api_function(id)
            else:
                state = result
            
            return state
        except TimeoutError as e:
            self.Logger.debug(f"{self.type} STATUS POLLING FABRIC API: TIMEOUT")

            if raise_error:
                raise e
            
            return None

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
            "description": f"Fabric Sync Lakehouse: {name}"
        }

        if enable_schemas:
            data["creationPayload"] = {
                "enableSchemas" : enable_schemas
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

    def publish(self, id:str) -> None:
        """
        Publishes the environment
        Args:
            id (str): Environment ID
        Returns:
            str: Response from the Fabric API
        """
        self._do_fabric_item_command(id, "staging/publish")
    
    def stage_library(self, id:str, file:str, file_buffer) -> Response:
        """
        Stages a file to the environment
        Args:
            id (str): Environment ID
            file (str): File Name
            file_buffer (byte[]): File Buffer
        Returns:
            str: Response from the Fabric API
        """
        headers = {'Authorization': f'Bearer {self.token}'}
        files = {'file':(file,file_buffer)}
        response = self.rest_api_proxy.post(endpoint=f"{self.uri}/{id}/staging/libraries", files=files, headers=headers)
    
        return response
    
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
    
    def wait_for_publish(self, id) -> str:
        return self._wait_status(self.get_publish_state, id, "running", while_match=True, 
                            timeout_minutes=5, poll_seconds=15, raise_error=False)

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

    def create(self, name:str, data:Dict = None) -> str:
        """
        Creates a mirrored database
        Args:
            name (str): Name of the mirrored database
        Returns:
            str: Mirrored Database ID
        """

        if not data:
            mirror_db_definition = {
                    "properties": {
                        "source": {
                            "type": "GenericMirror",
                            "typeProperties": {}
                        },
                        "target": {
                            "type": "MountedRelationalDatabase",
                            "typeProperties": {
                                "defaultSchema": "dbo",
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
        else:
            payload = data

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
        return response.json()["status"]
    
    def wait_for_mirroring(self, id:str, status:str) -> str:
        return self._wait_status(self.get_mirroring_status, id, status, while_match=True, 
                            timeout_minutes=1, poll_seconds=5)

class FabricNotebook(BaseFabricItem):
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Notebook API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        """
        super().__init__(FabricItemType.NOTEBOOK, workspace_id, api_token)
    
    def get_definition(self, id:str) -> str:
        """
        Gets the definition of the notebook
        Args:
            id (str): Notebook ID
        Returns:
            str: Notebook definition
        """
        return self._do_fabric_item_command(id, "getDefinition?format=ipynb")

    def create(self, name:str, data:str, update_hook:callable) -> Response:
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
            "description": f"Fabric Sync: {name}",
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

        return super().create(name, notebook_payload, update_hook)
        
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