import logging
from logging import Logger
import inspect
import json
import functools
import asyncio
import threading
import os
import sys
from pyspark.sql import SparkSession

from FabricSync.BQ.Enum import (
    SyncLogLevel, SyncStatus
)
from FabricSync.BQ.SessionManager import Session
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Http import RestAPIProxy
from FabricSync.Meta import Version

class SyncLogger:
    __context:SparkSession = None
    __logger:Logger = None
    
    def __init__(self) -> None:
        self.loop = None
    
    def _initialize_logger(self) -> None:
        self.__logger = logging.getLogger(SyncConstants.FABRIC_LOG_NAME)        
        
        LOG_LEVEL = (SyncLogLevel[Session.LogLevel]).value

        logging.addLevelName(SyncLogLevel.SYNC_STATUS.value, "SYNC_STATUS")
        logging.addLevelName(SyncLogLevel.TELEMETRY.value, "TELEMETRY")

        self.base_factory = logging.getLogRecordFactory()
        logging.setLogRecordFactory(self.record_factory)

        logging.Logger.telemetry = self.telemetry
        logging.Logger.sync_status = self.sync_status

        SYNC_LOG_HANDLER_NAME = "SYNC_LOG_HANDLER"

        os.makedirs(os.path.dirname(Session.LogPath), exist_ok=True)   

        handler = logging.handlers.TimedRotatingFileHandler(Session.LogPath, when="d", interval=1)
        handler.setFormatter(CustomJsonFormatter())
        handler.name = "SYNC_FILE_LOG_HANDLER"
        handler.setLevel(LOG_LEVEL)

        self.__logger.addHandler(SyncLogHandler(SYNC_LOG_HANDLER_NAME, handler))

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(LOG_LEVEL)
        self.__logger.addHandler(stdout_handler)

        self.__logger.setLevel(LOG_LEVEL)        

    def sync_status(self, message, verbose:bool=False, *args, **kwargs) -> None:
        if self.__logger.isEnabledFor(SyncLogLevel.SYNC_STATUS.value):
            if not verbose or (verbose and self.__logger.isEnabledFor(logging.DEBUG)):
                self.__logger._log(SyncLogLevel.SYNC_STATUS.value, message, args, **kwargs)

    def telemetry(self, message, *args, **kwargs) -> None:
        if (self.__logger.isEnabledFor(SyncLogLevel.TELEMETRY.value)):
            if Session.Telemetry:
                message["correlation_id"] = Session.ApplicationID
                message["sync_version"] = str(Version.CurrentVersion)

                self.send_telemetry(json.dumps(message))
                #self.__logger._log(SyncLogLevel.SYNC_STATUS.value, f"Telemetry: {message}", args, **kwargs)

    def send_telemetry(self, payload) -> None:
        ct = threading.current_thread()

        if not self.loop:
            if SyncConstants.THREAD_PREFIX in ct.name:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            else:
                self.loop = asyncio.get_event_loop()

        t = self.loop.create_task(self.send_telemetry_to_api(payload))

    async def send_telemetry_to_api(self, payload) -> None:
        try:
            api_proxy = RestAPIProxy(base_url=f"https://{Session.TelemetryEndpoint}")
            bound = functools.partial(api_proxy.post, endpoint="telemetry", data=payload)
            await self.loop.run_in_executor(None, bound)
        except Exception as e:
            self.__logger.error(f"Telemetry Send Failure: {e}")

    def record_factory(self, *args, **kwargs) -> logging.LogRecord:
        record = self.base_factory(*args, **kwargs)
        record.correlation_id = Session.ApplicationID
        return record

    @classmethod
    def getLogger(cls) -> logging.Logger:
        if not cls.__logger:
            cls.__logger = SyncLogger().Logger
        
        return cls.__logger

    @property
    def Logger(self) -> logging.Logger:
        if not self.__logger:
            if SyncConstants.FABRIC_LOG_NAME not in logging.Logger.manager.loggerDict:
                self._initialize_logger()
            
            self.__logger = logging.getLogger(SyncConstants.FABRIC_LOG_NAME)

        return self.__logger

    def reset_logging() -> None:
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name == SyncConstants.FABRIC_LOG_NAME]

        for logger in loggers:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True
    
    @property
    def Context(self) -> SparkSession:
        if not self.__context:
            self.__context = SparkSession.getActiveSession()

        return self.__context

class SyncLogHandler(logging.Handler):
    def __init__(self, name, target_handler) -> None:
        super().__init__()

        self.name = name
        self.target_handler = target_handler

    def emit(self, record) -> None:
        #Pass-through handler defined for future use

        self.target_handler.handle(record)

class CustomJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        output = {k: str(v) for k, v in record.__dict__.items()}
        return json.dumps(output)

class Telemetry(): 
    def log_telemetry(operation:str, result=None, data=None) -> None:
        payload = {
            "operation":operation,
            "result": result
        }

        if data:
            payload["operation_data"] = data

        SyncLogger.getLogger().telemetry(payload)

    def Install(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments
                r = func(*args, **kwargs)
                Telemetry.log_telemetry("Fabric Sync Accelerator Install", result=True)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator
    
    def Upgrade(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r =  func(*args, **kwargs)

                Telemetry.log_telemetry("Fabric Sync Accelerator Upgrade", result=True)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Mirror_DB_Sync(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                r =  func(*args, **kwargs)
                Telemetry.log_telemetry("Fabric Mirror Database Sync", r)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator
        
    def Metadata_Sync(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                r =  func(*args, **kwargs)
                Telemetry.log_telemetry("Fabric Metadata Sync", r)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Auto_Discover(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                r = func(*args, **kwargs)
                Telemetry.log_telemetry("Fabric Sync Auto Discover Configuration", result=True)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Scheduler(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r = func(*args, **kwargs)

                params = {
                    "schedule_type": str(func_args["schedule_type"]),
                    "schedule_id": str(r)
                }
                Telemetry.log_telemetry("Fabric Sync Scheduler", result=True, data=params)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Sync_Load(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r = func(*args, **kwargs)

                params = r.to_telemetry()
                result = (r.Status==SyncStatus.COMPLETE)

                Telemetry.log_telemetry("BQ to OneLake Sync", result=result, data=params)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Lakehouse_Inventory(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r =  func(*args, **kwargs)

                Telemetry.log_telemetry("Fabric Lakehouse Inventory", result=True)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Delta_Maintenance(func_=None, maintainence_type:str=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r =  func(*args, **kwargs)

                params = {
                    "maintainence_type":maintainence_type
                }

                Telemetry.log_telemetry("OneLake Delta Table Maintenance", result=True, data=params)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator

    def Data_Expiration(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r =  func(*args, **kwargs)

                row = func_args["row"]
                params = row.asDict()

                Telemetry.log_telemetry("BQ Data Expiration Enforcement", result=True, data=params)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator