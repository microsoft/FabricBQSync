import logging
import logging.handlers
import inspect
import json
import functools
import asyncio
import threading
from pyspark.sql import SparkSession

from .Enum import *
from .Constants import SyncConstants
from .Utils import *
from ..Meta import *

class SyncLogger:
    def __init__(self, context:SparkSession):
        self.loop = None

        self.TelemetryEndpoint = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.telemetry_endpoint")
        self.ApplicationID = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.application_id")
        self.ID = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.name")
        self.LogLevel = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.log_level")
        self.LogPath = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.log_path")
        self.Telemetry = context.conf.get(f"{SyncConstants.SPARK_CONF_PREFIX}.log_telemetry").lower() == "true"

        if self.ID not in logging.Logger.manager.loggerDict:
            self._initialize_logger()
        else:
            self.logger = logging.getLogger(self.ID)
    
    def _initialize_logger(self):
        self.logger = logging.getLogger(self.ID)        
        
        LOG_LEVEL = (Util.assign_enum_name(SyncLogLevel, self.LogLevel)).value

        logging.addLevelName(SyncLogLevel.SYNC_STATUS.value, "SYNC_STATUS")
        logging.addLevelName(SyncLogLevel.TELEMETRY.value, "TELEMETRY")

        self.base_factory = logging.getLogRecordFactory()
        logging.setLogRecordFactory(self.record_factory)

        logging.Logger.telemetry = self.telemetry
        logging.Logger.sync_status = self.sync_status

        SYNC_LOG_HANDLER_NAME = "SYNC_LOG_HANDLER"

        handler = logging.handlers.TimedRotatingFileHandler(self.LogPath, 
            when="d", interval=1)
        handler.setFormatter(CustomJsonFormatter())
        handler.name = "SYNC_FILE_LOG_HANDLER"
        handler.setLevel(LOG_LEVEL)

        self.logger.addHandler(SyncLogHandler(SYNC_LOG_HANDLER_NAME, handler))

        self.logger.setLevel(LOG_LEVEL)        

    def sync_status(self, message, *args, **kwargs):
        if (self.logger.isEnabledFor(SyncLogLevel.SYNC_STATUS.value)):
            self.logger._log(SyncLogLevel.SYNC_STATUS.value, message, args, **kwargs)

    def telemetry(self, message, *args, **kwargs):
        if (self.logger.isEnabledFor(SyncLogLevel.TELEMETRY.value)):
            if self.Telemetry:
                message["correlation_id"] = self.ApplicationID
                message["sync_version"] = Version.CurrentVersion 

                self.send_telemetry(json.dumps(message))
                #self.logger._log(SyncLogLevel.SYNC_STATUS.value, f"Telemetry: {message}", args, **kwargs)

    def send_telemetry(self, payload):
        ct = threading.current_thread()

        if not self.loop:
            if SyncConstants.THREAD_PREFIX in ct.name:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            else:
                self.loop = asyncio.get_event_loop()
        
        t = self.loop.create_task(self.send_telemetry_to_api(payload))

    async def send_telemetry_to_api(self, payload):
        try:
            api_proxy = RestAPIProxy(base_url=f"https://{self.TelemetryEndpoint}")
            bound = functools.partial(api_proxy.post, endpoint="telemetry", data=payload)
            await self.loop.run_in_executor(None, bound)
        except Exception as e:
            self.logger.error("Telemetry Send Failure")

    def record_factory(self, *args, **kwargs):
        record = self.base_factory(*args, **kwargs)
        record.correlation_id = self.ApplicationID
        return record

    def get_logger(self):
        return self.logger

    def reset_logging():
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        loggers.append(logging.getLogger())

        for logger in loggers:
            handlers = logger.handlers[:]
            for handler in handlers:
                logger.removeHandler(handler)
                handler.close()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True  

class SyncLogHandler(logging.Handler):
    def __init__(self, name, target_handler):
        super().__init__()

        self.name = name
        self.target_handler = target_handler

    def emit(self, record):
        log_entry = self.format(record)

        #Pass-through handler defined for future use

        self.target_handler.handle(record)

class CustomJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        output = {k: str(v) for k, v in record.__dict__.items()}
        return json.dumps(output)

class Telemetry(): 
    def log_telemetry(operation:str, result=None, data=None):
        import logging

        payload = {
            "operation":operation,
            "result": result
        }

        if data:
            payload["operation_data"] = data

        logging.Logger.telemetry(payload)

    def Install(func_=None):
        def _decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                func_args = inspect.signature(func).bind(*args, **kwargs).arguments

                r = func(*args, **kwargs)

                Telemetry.log_telemetry("BQ Sync Accelerator Install", result=True)

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

                Telemetry.log_telemetry("BQ Sync Accelerator Upgrade", result=True)

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
                Telemetry.log_telemetry("BQ Metadata Sync", r)

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
                Telemetry.log_telemetry("Auto Discover Configuration", result=True)

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
                Telemetry.log_telemetry("Scheduler", result=True, data=params)

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

                Telemetry.log_telemetry("Lakehouse Inventory", result=True)

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

                Telemetry.log_telemetry("Delta Table Maintenance", result=True, data=params)

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
                print(params)

                Telemetry.log_telemetry("BQ Data Expiration Enforcement", result=True, data=params)

                return r
            return wrapper

        if callable(func_):
            return _decorator(func_)
        elif func_ is None:
            return _decorator