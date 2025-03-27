from pydantic import BaseModel

class SyncTimerError(Exception):
    pass

class SyncBaseError(Exception):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncBaseError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__()
        self.title = "BQ Sync Base Error"
        self.data = data
        self.msg = msg
    
    def __str__(self) -> str:
        """
        Returns the string representation of the error.
        Returns:
            str: The string representation of the error.
        """
        return self.title if not self.msg else f"{self.title} - {self.msg}"

class SyncInstallError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncInstallError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Installer Error" 

class SyncKeyVaultError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncKeyVaultError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Key Vault Error" 

class SyncConfigurationError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncConfigurationError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Configuration Error" 

class AutoDiscoveryError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the AutoDiscoveryError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync AutoDiscovery Error" 

class SchedulerError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SchedulerError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Scheduler Error" 

class MetadataSyncError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the MetadataSyncError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Metadata Sync Error" 

class SyncLoadError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncLoadError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Data Load Error"

class FabricLakehouseError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the FabricLakehouseError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Lakehouse Error"

class DataRetentionError(SyncBaseError):
    def __init__(self, msg:str = None, data:BaseModel = None) -> None:
        """
        Initializes a new instance of the DataRetentionError class.
        Args:
            msg (str): The message.
            data (SyncBaseModel): The data.
        """
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Sync Data Retention Error"

class BQConnectorError(SyncBaseError):
    def __init__(self, msg:str = None, query:BaseModel = None) -> None:
        """
        Initializes a new instance of the BQConnectorError class.
        Args:
            msg (str): The message.
            query (SyncBaseModel): The query.
        """
        super().__init__(msg=msg, data=query)
        self.title = "Fabric Sync BigQuery Connector Error"

class SyncDataMaintenanceError(SyncBaseError):
    def __init__(self, msg:str = None, query:BaseModel = None) -> None:
        """
        Initializes a new instance of the SyncDataMaintenanceError class.
        Args:
            msg (str): The message.
            query (SyncBaseModel): The query.
        """
        super().__init__(msg=msg, data=query)
        self.title = "Fabric Sync Data Maintenance Error"