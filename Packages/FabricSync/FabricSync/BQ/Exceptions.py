from .Model.Config import SyncBaseModel

class SyncTimerError(Exception):
    pass

class SyncBaseError(Exception):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__()
        self.title = "BQ Sync Base Error"
        self.data = data
        self.msg = msg
    
    def __str__(self):
        ex_msg = self.title if not self.msg else f"{self.title} - {self.msg}"

        if self.data:
            return f"{ex_msg} - Data: \r\n{self.data.model_dump_json(indent=4)}"
        else:
            return f"{ex_msg}"

class SyncInstallError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "BQ Sync Configuration Error" 

class SyncConfigurationError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "BQ Sync Configuration Error" 

class AutoDiscoveryError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "BQ AutoDiscovery Error" 

class SchedulerError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "Sync Scheduler Error" 

class MetadataSyncError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "Metadata Sync Error" 

class SyncLoadError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "BQ Sync Load Error"

class FabricLakehouseError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "Fabric Lakehouse Error"

class DataRetentionError(SyncBaseError):
    def __init__(self, msg:str = None, data:SyncBaseModel = None):
        super().__init__(msg=msg, data=data)
        self.title = "Data Retention Error"

class BQConnectorError(SyncBaseError):
    def __init__(self, msg:str = None, query:SyncBaseModel = None):
        super().__init__(msg=msg, data=query)
        self.title = "BigQuery Connector Error"

class SyncDataMaintenanceError(SyncBaseError):
    def __init__(self, msg:str = None, query:SyncBaseModel = None):
        super().__init__(msg=msg, data=query)
        self.title = "Data Maintenance Error"