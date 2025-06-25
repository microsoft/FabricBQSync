from requests.models import Response
import json
import os
from threading import Thread
import time

from typing import (
    Dict, Any
)

from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Enum import FabricItemType
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Http import RestAPIProxy

class BaseFabricItem(ContextAwareBase):
    """
    Base class for Fabric item API clients    
    This class provides common functionality for creating, retrieving, and managing Fabric items.
    It is not intended to be instantiated directly, but rather serves as a base class for specific
    Fabric item types such as FabricWorkspace, FabricLakehouse, etc.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        token (str): The API token for authentication.
        type (FabricItemType): The type of Fabric item.
        rest_api_proxy (RestAPIProxy): The REST API proxy for making API calls.
    Methods:
        __init__(type:FabricItemType, workspace_id:str, api_token:str): Initializes
            the BaseFabricItem with the specified type, workspace ID, and API token.
        uri() -> str: Returns the URI of the Fabric item based on its type. 
        create(name:str, data:Dict=None, folder:str = None, update_hook:callable = None, **kwargs) -> str:
            Creates a Fabric item with the specified name and data, optionally in a folder.
        get(id:str) -> str: Retrieves a Fabric item by its ID.
        get_name(id:str) -> str: Retrieves the name of a Fabric item by its ID
        get_id(name:str) -> str: Retrieves the ID of a Fabric item by its name.
        list() -> str: Lists all Fabric items of the specified type.
        _do_fabric_item_command(id:str, path:str) -> Response: Executes a command on a Fabric item.
        get_by_name(name:str) -> str: Retrieves a Fabric item by its name.
        get_or_create(name:str, data:Dict = None, folder:str = None, **kwargs) -> str:
            Retrieves a Fabric item by its name or creates it if it does not exist.
        __run_with_timeout(api_function:callable, id:str, result:Any, while_match:bool, 
            timeout:int, poll_seconds:int, update_hook:callable=None): Runs the Fabric API function with a timeout.
        __poll_status(api_function:callable, id:str, result:Any, while_match:bool, 
            poll_seconds:int, update_hook:callable): Polls the Fabric API for the status of the Fabric item.
        _wait_status(api_function:callable, id:str, result:Any, while_match:bool=True, 
            timeout_minutes:int = 1, poll_seconds:int = 1, raise_error = False  , update_hook:callable=None) -> str:
            Waits for the status of the Fabric item to match the expected result or timeout.            
    """
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
    
    def create(self, name:str, data:Dict=None, folder:str = None, update_hook:callable = None, **kwargs) -> str:
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

        if folder:
            data["folderId"] = folder

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
    
    def get_or_create(self, name:str, data:Dict = None, folder:str = None, **kwargs) -> str:
        """
        Gets or creates a Fabric item
        Args:
            name (str): Name of the Fabric item
            data (Dict): Data to create the Fabric item
        Returns:
            Tuple[str, bool]: Fabric item ID and flag indicating if the item is new
        """
        id = self.get_id(name)

        if not id:
            id = self.create(name, data=data, folder=folder, **kwargs)
        
        return id
    
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
    """
    Fabric Workspace API Client
    This class provides methods to interact with Fabric Workspaces, including creating, retrieving,
    and managing workspaces.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricWorkspace with the specified workspace ID and API token.
    """
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
    """
    Fabric Lakehouse API Client
    This class provides methods to interact with Fabric Lakehouses, including creating, retrieving,
    and managing lakehouses.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricLakehouse with the specified workspace ID and API token.
        create(name:str, folder:str = None, **kwargs) -> str: Creates a lake
            house with the specified name and optional folder.
        has_schema(id:str) -> bool: Checks if the lakehouse has a default schema.
    """
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

    def create(self, name:str, folder:str = None, **kwargs) -> str:
        if kwargs and "enable_schemas" in kwargs:
            enable_schemas = kwargs["enable_schemas"]
        else:
            enable_schemas = False

        data = {
            "displayName": name,
            "description": f"Fabric Sync Lakehouse: {name}"
        }

        if enable_schemas:
            data["creationPayload"] = {
                "enableSchemas" : enable_schemas
                }

        return super().create(name, data=data, folder=folder)

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
    """
    Fabric Environment API Client
    This class provides methods to interact with Fabric Environments, including creating, retrieving,
    and managing environments.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricEnvironment with the specified workspace ID and API token.
        publish(id:str) -> None: Publishes the environment with the specified ID.
        stage_library(id:str, file:str, file_buffer) -> Response: Stages a file to the environment.
        get_publish_state(id) -> str: Returns the publish state of the environment with the specified ID.
        wait_for_publish(id) -> str: Waits for the environment to be published. 
    """
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
        """
        Waits for the environment to be published
        Args:
            id (str): Environment ID
        Returns:
            str: Publish state of the environment
        """
        return self._wait_status(self.get_publish_state, id, "running", while_match=True, 
                            timeout_minutes=5, poll_seconds=15, raise_error=False)

class FabricOpenMirroredDatabase(BaseFabricItem):
    """
    Fabric Open Mirrored Database API Client
    This class provides methods to interact with Fabric Open Mirrored Databases, including creating,
    starting, stopping mirroring operations, and checking the status of mirroring.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricOpenMirroredDatabase with the specified workspace ID and API token.
        create(name:str, data:Dict = None, folder:str = None) -> str:
            Creates a mirrored database with the specified name and optional data.
        start_mirroring(id:str) -> str: Starts the mirroring operation for the specified mirrored database ID.
        stop_mirroring(id:str) -> str: Stops the mirroring operation for the specified  mirrored database ID.
        get_mirroring_status(id:str) -> str: Returns the status of the mirroring operation for the specified mirrored database ID.
        wait_for_mirroring(id:str, status:str) -> str: Waits for the mirroring operation to reach the specified status for the given mirrored database ID.
    """
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

    def create(self, name:str, data:Dict = None, folder:str = None) -> str:
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

        return super().create(name, data=payload, folder=folder)

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
        """
        Waits for the mirroring operation to reach the specified status
        Args:
            id (str): Mirrored Database ID
            status (str): Expected status of the mirroring operation
        Returns:
            str: Status of the mirroring operation
        """
        return self._wait_status(self.get_mirroring_status, id, status, while_match=True, 
                            timeout_minutes=1, poll_seconds=5)

class FabricFolder(BaseFabricItem):
    """
    Fabric Folder API Client
    This class provides methods to interact with Fabric Folders, including creating, retrieving,
    and managing folders.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricFolder with the specified workspace ID and API token.
        create(name:str, data:Dict = None, folder:str = None) -> str:
            Creates a folder with the specified name and optional data.
    """
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Folder API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricFolder: Fabric Folder API Client
        """
        super().__init__(FabricItemType.FOLDER, workspace_id, api_token)
    
    def create(self, name:str, data:Dict = None, folder:str = None) -> str:
        """
        Creates a folder
        Args:
            name (str): Name of the folder
            data (Dict): Data to create the folder
        Returns:
            str: Folder ID
        """
        if data:
            payload = data

            if folder:
                payload["parentFolderId"] = folder
        else:
            payload = {
                    "displayName": name,
                    "parentFolderId": folder
                }
        
        return super().create(name, data=payload)

class FabricDataPipeline(BaseFabricItem):
    """
    Fabric Data Pipeline API Client
    This class provides methods to interact with Fabric Data Pipelines, including creating, retrieving, 
    and managing data pipelines.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricDataPipeline with the specified workspace ID and API token.
        create(name:str, data:Dict = None, folder:str = None) -> str:
            Creates a data pipeline with the specified name and optional data.
    """
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric Data Pipeline API Client
        Args:
            workspace_id (str): Workspace ID
            api_token (str): API Token
        Returns:
            FabricDataPipeline: Fabric Data Pipeline API Client
        """
        super().__init__(FabricItemType.DATA_PIPELINE, workspace_id, api_token)
    
    def create(self, name:str, data:Dict = None, folder:str = None) -> str:
        """
        Creates a data pipeline
        Args:
            name (str): Name of the data pipeline
            data (Dict): Data to create the data pipeline
        Returns:
            str: Data Pipeline ID
        """
        encoded_data = Util.encode_base64(json.dumps(data))
        payload = {
                "displayName": f"{name}_Data_Pipeline",
                "description": f"AUTOGENERATED - {name} Pipeline",
                "definition": {
                    "parts": [
                        {
                            "path": "pipeline-content.json",
                            "payload": encoded_data,
                            "payloadType": "InlineBase64"
                        }
                    ]
                }
            }
        
        return super().create(name, data=payload, folder=folder)

class FabricNotebook(BaseFabricItem):
    """
    Fabric Notebook API Client
    This class provides methods to interact with Fabric Notebooks, including creating, retrieving,
    and managing notebooks.
    Attributes:
        workspace_id (str): The ID of the Fabric workspace.
        api_token (str): The API token for authentication.
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricNotebook with the specified workspace ID and API token.
        get_definition(id:str) -> str: Gets the definition of the notebook with the specified ID.
        create(name:str, data:str, folder:str = None, update_hook:callable = None) -> Response:
            Uploads a notebook to Fabric with the specified name and content, optionally in a folder.
    """
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

    def create(self, name:str, data:str, folder:str = None, update_hook:callable = None) -> Response:
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
            "description": f"AUTOGENERATED - {name}",
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

        return super().create(name, data=notebook_payload, folder=folder, update_hook=update_hook)
        
class FabricAPI:
    """
    Fabric API Client
    This class provides a unified interface to interact with various Fabric API clients such as
    FabricWorkspace, FabricLakehouse, FabricEnvironment, FabricOpenMirroredDatabase, FabricNotebook
    FabricFolder, and FabricDataPipeline.
    Attributes:
        Workspace (FabricWorkspace): Fabric Workspace API Client
        Lakehouse (FabricLakehouse): Fabric Lakehouse API Client
        Environment (FabricEnvironment): Fabric Environment API Client
        OpenMirroredDatabase (FabricOpenMirroredDatabase): Fabric Open Mirrored Database API Client
        Notebook (FabricNotebook): Fabric Notebook API Client
        Folder (FabricFolder): Fabric Folder API Client
        DataPipeline (FabricDataPipeline): Fabric Data Pipeline API Client
    Methods:
        __init__(workspace_id:str, api_token:str): Initializes the FabricAPI with the specified workspace ID and API token.
    """
    def __init__(self, workspace_id:str, api_token:str):
        """
        Fabric API Client
        """
        self.Workspace = FabricWorkspace(workspace_id, api_token)
        self.Lakehouse = FabricLakehouse(workspace_id, api_token)
        self.Environment = FabricEnvironment(workspace_id, api_token)
        self.OpenMirroredDatabase = FabricOpenMirroredDatabase(workspace_id, api_token)
        self.Notebook = FabricNotebook(workspace_id, api_token)
        self.Folder = FabricFolder(workspace_id, api_token)
        self.DataPipeline = FabricDataPipeline(workspace_id, api_token)