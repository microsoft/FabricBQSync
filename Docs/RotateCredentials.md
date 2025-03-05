# Rotating GCP Service Account Credentials

One of the common practices to ensure that the Fabric Sync accelerator complies with organizational security is the rotation of the GCP Service Account credentials used for authentication and authorization.

### Updating Service Account Credentials
By default, GCP Service Account credentials are encoded and packaged into the Fabric Sync User Configuration file. When credentials are rotated the User Configuration file must be updated with the new encoded credentials.

To update the GCP Service Account Credentials use the following steps:

1. Upload the new GCP Service Account credential JSON file to the OneLake metadata lakehouse
2. Create a Fabric Notebook attached to the OneLake metadata lakehouse
3. PIP Install the Fabric Sync library:
    <code>
    %pip install FabricSync --quiet
    </code>
4. Add the code below:
    <code><pre>
    from FabricSync.BQ.Model.Config import ConfigDataset
    from FabricSync.BQ.Auth import GCPAuth

    config_json_path = "[PATH TO CONFIG FILE]"
    credential_file_path = "[PATH TO CREDENTIAL FILE]"

    config = ConfigDataset.from_json(config_json_path)
    encoded_credential = GCPAuth.get_encoded_credentials_from_path(credential_file_path)
    config.GCP.GCPCredential.Credential = encoded_credential
    config.GCP.GCPCredential.CredentialPath = None
    config.to_json(config_json_path)
    </pre></code>

5. Update the code provided below with:
    - File API path to the Fabric Sync User Configuration file 
    - File API path to the GCP credential file
6. Run the code

### Externalizing Service Account Credentials

An alternate approach to managing GCP Service Account credentials is to externalize the credentials. This approach is useful when credentials are frequently rotated or when multiple Fabric Sync configurations are used.

Externalizing the credentials requires the GCP Service Account credential file to be stored securely in the OneLake metadata lakehouse. The Fabric Sync accelerator then loads and encodes the credential at runtime.

To externalize your GCP Service Account credentials, use the following steps:

1. 1. Upload the GCP Service Account credential JSON file to the OneLake metadata lakehouse
2. Create a Fabric Notebook attached to the OneLake metadata lakehouse
3. PIP Install the Fabric Sync library:
    <code>
    %pip install FabricSync --quiet
    </code>
4. Add the code below:
    <code><pre>
    from FabricSync.BQ.Model.Config import ConfigDataset

    config_json_path = "[PATH TO CONFIG FILE]"
    credential_file_path = "[PATH TO CREDENTIAL FILE]"

    config = ConfigDataset.from_json(config_json_path)
    config.GCP.GCPCredential.CredentialPath = credential_file_path
    config.GCP.GCPCredential.Credential = None
    config.to_json(config_json_path)
    </pre></code>

5. Update the code provided below with:
    - File API path to the Fabric Sync User Configuration file 
    - File API path to the GCP credential file
6. Run the code