import time
import json
import jwt
import requests
import httplib2

class BigQueryAccessPolicy:
    """
    BigQuery Access Policy Mode
    """
    def __init__(
            self, 
            json_row:str):
        """
        Initializes class from JSON object
        """
        self.ProjectId = json_row["projectId"]
        self.DatasetId = json_row["datasetId"]
        self.TableId = json_row["tableId"]
        self.PolicyId = json_row["policyId"]
        self.PolicyIAM:list[BigQueryAccessPolicyIAM] = []

    @property
    def ResourceId(self) -> str:
        """
        Returns the GCP formatted Resource Id
        """
        return f"projects/{self.ProjectId}/datasets/{self.DatasetId}/tables/{self.TableId}/rowAccessPolicies/{self.PolicyId}"
    
    def __str__(self):
        return self.ResourceId

class BigQueryAccessPolicyIAM:
    """
    BiqQuery Access Policy IAM Model
    """
    def __init__(
            self, 
            json_row:str):
        """
        Initializes class from JSON object
        """
        self.Role = json_row["role"]
        self.Members = [m for m in json_row["members"]]

class GCPRestAPI:
    """
    GCP Rest API Helper Class with Service Account Authentication
    """
    def __init__(
            self, 
            json_credentials:str):
        """
        Class Initialization with Service Account JSON Credential Path
        """
        self.JSONCredentials = json_credentials
        self.__Token:str = None
    
    @property
    def AccessToken(self) -> str:
        """
        Cached Authentication Access Token
        """
        if self.__Token is None:
            self.__Token = self.get_access_token()
        
        return self.__Token

    def load_json_credentials(
            self, 
            filename:str) -> str:
        """
        Reads Credential JSON file from the filesystem
        """
        with open(filename, 'r') as f:
            data = f.read()
        
        return json.loads(data)

    def load_private_key(
            self, 
            json_cred:str) -> str:
        """
        Extracts the private key from the credential file
        """
        return json_cred['private_key']

    def create_signed_jwt(
            self, 
            pkey:str, 
            pkey_id:str, 
            email:str, 
            scope:str) -> str:
        """
        Calls the Google API OAuth service to get a signed JWT token
        """
        # Google Endpoint for creating OAuth 2.0 Access Tokens from Signed-JWT
        auth_url = "https://www.googleapis.com/oauth2/v4/token"

        # Set how long this token will be valid in seconds
        expires_in = 3600   # Expires in 1 hour

        issued = int(time.time())
        expires = issued + expires_in   # expires_in is in seconds
        
        # Note: this token expires and cannot be refreshed. The token must be recreated
        
        # JWT Headers
        additional_headers = {
            'kid': pkey_id,
            "alg": "RS256",
            "typ": "JWT"    # Google uses SHA256withRSA
        }
        
        # JWT Payload
        payload = {
            "iss": email,       # Issuer claim
            "sub": email,       # Issuer claim
            "aud": auth_url,    # Audience claim
            "iat": issued,      # Issued At claim
            "exp": expires,     # Expire time
            "scope": scope      # Permissions
        }
        
        # Encode the headers and payload and sign creating a Signed JWT (JWS)
        sig = jwt.encode(payload, pkey, algorithm="RS256", headers=additional_headers)
        
        return sig

    def exchange_jwt_for_access_token(
            self, 
            signed_jwt:str) -> str:
        """
        Calls the Google OAuth service to exchange the JWT token for a Access Token 
        """
        auth_url = "https://www.googleapis.com/oauth2/v4/token"
        
        params = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt
        }
        
        r = requests.post(auth_url, data=params)
        
        if not r.ok:
            raise Exception(r.text)
        
        return r.json()['access_token']
        
    def execute_request(
            self, 
            url:str, 
            http_method:str) -> str:
        """
        Executes an authenticated API request
        """    
        headers = {
            "Host": "www.googleapis.com",
            "Authorization": "Bearer " + self.AccessToken,
            "Content-Type": "application/json"
        }
            
        h = httplib2.Http()
            
        resp, content = h.request(uri=url, method=http_method, headers=headers)
            
        status = int(resp.status)

        if status < 200 or status >= 300:
            raise Exception (f"Error: HTTP Request failed (Status Code: {status})")
        
        return json.loads(content.decode('utf-8').replace('\n', ''))
    
    def get_access_token(self) -> str:
        """
        Users JSON Service Account credentials to generate a valid Access Token for API access
        """
        cred = self.load_json_credentials(self.JSONCredentials)
            
        private_key = self.load_private_key(cred)
            
        s_jwt = self.create_signed_jwt(
            private_key,
            cred['private_key_id'],
            cred['client_email'],
            "https://www.googleapis.com/auth/cloud-platform")
            
        token = self.exchange_jwt_for_access_token(s_jwt)

        return token