from typing import Dict

from FabricSync.BQ.Utils import Util

class Credentials:
    def getToken(audience) -> str:
        """
        Returns a token for the given audience.
        """
        pass      

class TokenProvider:
    FABRIC_TOKEN_SCOPE = "https://api.fabric.microsoft.com/"
    STORAGE_TOKEN_SCOPE = "storage"
    
    def __init__(self, credential_provider:Credentials) -> None:
        """
        Initializes a new instance of the TokenProvider class.
        Args:
            credential_provider (Credentials): The credential provider.
        """
        self.credential_provider = credential_provider
        self.tokens:Dict[str,str] = {}
    
    def get_token(self, scope:str) -> str:
        """
        Returns a token for the given scope.
        Args:
            scope (str): The scope for which to get the token.
        Returns:
            str: The token.
        """
        if scope not in self.tokens:
            self.tokens[scope] = self.credential_provider.getToken(scope)

        return self.tokens[scope]

class GCPAuth:
    @staticmethod
    def get_encoded_credentials_from_path(path:str) -> str:
        """
        Returns the encoded credentials from the given path.
        Args:
            path (str): The path to the credentials file.
        Returns:
            str: The encoded credentials.
        """
        credential_data = Util.read_file_to_string(path)
        credential_data = [l.strip() for l in credential_data]
        credential_data = ''.join(credential_data)

        return Util.encode_base64(credential_data)