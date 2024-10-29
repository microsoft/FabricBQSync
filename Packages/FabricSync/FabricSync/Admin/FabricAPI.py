
from sempy.fabric.exceptions import FabricHTTPException
import sempy.fabric as fabric
import base64

class FabricAPIUtil():
    def invoke_fabric_request(self, url:str, mode:str = "GET", payload:dict = None) -> str:
        response = None

        client = fabric.FabricRestClient()
        
        try:
            if mode.upper() == "POST":
                r = client.post(url, json= payload)
            else:
                r = client.get(url)

            if r.status_code not in [200, 201, 202]:
                raise FabricHTTPException(r)

            response = r
        except FabricHTTPException as e:
            print("Caught a FabricHTTPException. Check the API endpoint, authentication.")

        return response

    def get_workspace_id(self) -> str:
        return fabric.get_workspace_id()

    def get_workspace_name(self) -> str:
        return  fabric.resolve_workspace_name()
        
    def get_lakehouse_id(self, workspace_id, lakehouse_nm: str) -> str:
        lakehouse_id = None
        uri = f"v1/workspaces/{workspace_id}/lakehouses"
        response = self.invoke_fabric_request(uri)

        if response:
            json_resp = response.json()

            o = [x["id"] for x in json_resp["value"] if x["displayName"].lower() == lakehouse_nm.lower()]

            if o:
                lakehouse_id = o[0]

        return lakehouse_id

    def get_or_create_lakehouse(self, workspace_id, lakehouse_nm:str) -> str:
        lid = self.get_lakehouse_id(workspace_id, lakehouse_nm)

        if not lid:
            lid = fabric.create_lakehouse(lakehouse_nm)

        return lid

    def get_lakehouse_abfs_path(self, workspace_id:str, lakehouse_id:str) -> str:
        return f"abfss://{workspace_id}@msit-onelake.dfs.fabric.microsoft.com/{lakehouse_id}/"
    
    def encode_base64(self, val:str) -> str:
        b = base64.b64encode(bytes(val, 'utf-8'))
        return b.decode('utf-8')

    def upload_fabric_notebook(self, workspace_id:str, name:str, data:str):
        encode_data = self.encode_base64(data)
        uri = f"v1/workspaces/{workspace_id}/notebooks"

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

        r = self.invoke_fabric_request(uri, mode="POST", payload=notebook_payload)