from typing import Dict

from FabricSync.BQ.Utils import Util
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Exceptions import SyncConfigurationError

class Credentials:
    def getToken(audience) -> str:
        """
        Returns a token for the given audience.
        """
        pass      

class TokenProvider:
    FABRIC_TOKEN_SCOPE = "https://api.fabric.microsoft.com/"
    
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
    
    def get_secret(self, key_vault:str, key:str) -> str:
        """
        Returns a secret from the given key vault.
        Args:
            key_vault (str): The key vault.
            key (str): The key.
        Returns:
            str: The secret.
        """
        return self.credential_provider.getSecret(key_vault, key)

class GCPAuth:
    @classmethod
    def get_encoded_credentials_from_path(cls, path:str) -> str:
        """
        Returns the encoded credentials from the given path.
        Args:
            path (str): The path to the credentials file.
        Returns:
            str: The encoded credentials.
        """
        credential_data = Util.read_file_to_string(path)
        return Util.encode_base64(credential_data)
    
    @classmethod
    def load_gcp_credential(cls, userConfig:ConfigDataset) -> str:
        """
        Loads the GCP credential from the user configuration settings.
        1. If the credential is base-64 encoded, it is returned as is.
        2. If the credential is a file path, it is read and encoded.
        3. If the credential is missing or invalid, an exception is raised.
        Returns:
            str: The GCP credential.
        Raises:
            SyncConfigurationError: If the GCP credential is missing or invalid.
        """
        credential = None
        if Util.is_base64(userConfig.GCP.GCPCredential.Credential):
            credential = userConfig.GCP.GCPCredential.Credential
        else:
            try:
                credential = cls.get_encoded_credentials_from_path(
                    userConfig.GCP.GCPCredential.CredentialPath)
            except Exception as e:
                raise SyncConfigurationError(f"Failed to parse GCP credential file: {e}") from e
        
        if not credential:
            raise SyncConfigurationError(f"GCP credential is missing or is in an invalid format.")
        
        return credential