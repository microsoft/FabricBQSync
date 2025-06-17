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
    """
    SyncLogger is a singleton class that manages logging for the Fabric Sync Accelerator.
    It initializes a logger with a specific name, sets up handlers for file and console output,
    and provides methods for logging sync status and telemetry messages.
    It also includes a custom log record factory to add a correlation ID to each log record.
    Attributes:
        loop (asyncio.AbstractEventLoop): The event loop for asynchronous operations.
        base_factory (logging.LogRecordFactory): The base log record factory for creating log records.
        __context (SparkSession): The Spark session context.
        __logger (Logger): The logger instance for the Fabric Sync Accelerator.
    """
    
    loop: asyncio.AbstractEventLoop = None
    __context:SparkSession = None
    __logger:Logger = None
    
    def __init__(self) -> None:
        """
        Initializes the SyncLogger instance, setting up the logger and its handlers.
        This method is called when an instance of SyncLogger is created.
        It sets the logger name to the Fabric Sync Accelerator log name and configures
        the logging level and handlers for file and console output.
        """
        self.loop = None
    
    def _initialize_logger(self) -> None:
        """
        Initializes the logger for the Fabric Sync Accelerator.
        This method sets up the logger with a specific name, configures the logging level,
        and adds handlers for file and console output. It also sets up a custom log record factory
        to include a correlation ID in each log record.
        """
        self.__logger = logging.getLogger(SyncConstants.FABRIC_LOG_NAME)

        logging.addLevelName(SyncLogLevel.SYNC_STATUS.value, "SYNC_STATUS")
        logging.addLevelName(SyncLogLevel.TELEMETRY.value, "TELEMETRY")

        self.base_factory = logging.getLogRecordFactory()
        logging.setLogRecordFactory(self.record_factory)

        logging.Logger.telemetry = self.telemetry
        logging.Logger.sync_status = self.sync_status

        os.makedirs(os.path.dirname(Session.LogPath), exist_ok=True)   

        log_level = (SyncLogLevel[Session.LogLevel]).value

        handler = logging.handlers.TimedRotatingFileHandler(Session.LogPath, when="d", interval=1)
        handler.setFormatter(CustomJsonFormatter())
        handler.name = f"{SyncConstants.FABRIC_LOG_NAME}_HANDLER"

        self.__logger.addHandler(SyncLogHandler(f"{SyncConstants.FABRIC_LOG_NAME}_HANDLER", handler))

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(ThreadingLogFormatter())
        
        self.__logger.addHandler(stdout_handler)

        SyncLogger.set_level(log_level)

    def sync_status(self, message, verbose:bool=False, *args, **kwargs) -> None:
        """
        Logs a sync status message at the SYNC_STATUS log level.
        Args:
            message (str): The message to log.
            verbose (bool, optional): If True, logs the message at DEBUG level if the logger is enabled for DEBUG.
            *args: Additional positional arguments to format the message.
            **kwargs: Additional keyword arguments to format the message.
        """
        if self.__logger.isEnabledFor(SyncLogLevel.SYNC_STATUS.value):
            if not verbose or (verbose and self.__logger.isEnabledFor(logging.DEBUG)):
                self.__logger._log(SyncLogLevel.SYNC_STATUS.value, message, args, **kwargs)

    def telemetry(self, message, *args, **kwargs) -> None:
        """
        Logs a telemetry message at the TELEMETRY log level.
        Args:
            message (str): The message to log.
            *args: Additional positional arguments to format the message.
            **kwargs: Additional keyword arguments to format the message.
        """
        if (self.__logger.isEnabledFor(SyncLogLevel.TELEMETRY.value)):
            if Session.Telemetry:
                message["correlation_id"] = Session.ApplicationID
                message["sync_version"] = str(Version.CurrentVersion)

                self.send_telemetry(json.dumps(message))
                #self.__logger._log(SyncLogLevel.SYNC_STATUS.value, f"Telemetry: {message}", args, **kwargs)

    def send_telemetry(self, payload) -> None:
        """
        Sends telemetry data to the Fabric Sync Accelerator API asynchronously.
        Args:
            payload (str): The telemetry data to send, formatted as a JSON string.
            This method checks if the current thread is a sync thread and creates a new event loop if necessary.
            It then creates an asynchronous task to send the telemetry data to the API.
        """
        ct = threading.current_thread()

        if not self.loop:
            if SyncConstants.THREAD_PREFIX in ct.name:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            else:
                self.loop = asyncio.get_event_loop()

        t = self.loop.create_task(self.send_telemetry_to_api(payload))

    async def send_telemetry_to_api(self, payload) -> None:
        """
        Asynchronously sends telemetry data to the Fabric Sync Accelerator API.
        Args:
            payload (str): The telemetry data to send, formatted as a JSON string.
            This method uses the RestAPIProxy to send a POST request to the telemetry endpoint.
            It handles any exceptions that may occur during the API call and logs an error message if the telemetry send fails.
        """
        try:
            api_proxy = RestAPIProxy(base_url=f"https://{Session.TelemetryEndpoint}")
            bound = functools.partial(api_proxy.post, endpoint="telemetry", data=payload)
            await self.loop.run_in_executor(None, bound)
        except Exception as e:
            self.__logger.error(f"Telemetry Send Failure: {e}")

    def record_factory(self, *args, **kwargs) -> logging.LogRecord:
        """
        Custom log record factory that adds a correlation ID to each log record.
        Args:
            *args: Positional arguments for the log record.
            **kwargs: Keyword arguments for the log record.
        Returns:
            logging.LogRecord: A log record with the correlation ID set to the current application ID.
        """
        record = self.base_factory(*args, **kwargs)
        record.correlation_id = Session.ApplicationID
        return record

    @classmethod
    def getLogger(cls) -> logging.Logger:
        """
        Returns the logger instance for the Fabric Sync Accelerator.
        If the logger has not been initialized, it initializes the logger first.
        Returns:
            logging.Logger: The logger instance for the Fabric Sync Accelerator.
        """
        if not cls.__logger:
            cls.__logger = SyncLogger().Logger
        
        return cls.__logger

    @property
    def Logger(self) -> logging.Logger:
        """
        Returns the logger instance for the Fabric Sync Accelerator.
        If the logger has not been initialized, it initializes the logger first.
        Returns:
            logging.Logger: The logger instance for the Fabric Sync Accelerator.
        """
        if not self.__logger:
            if SyncConstants.FABRIC_LOG_NAME not in logging.Logger.manager.loggerDict:
                self._initialize_logger()
            
            self.__logger = logging.getLogger(SyncConstants.FABRIC_LOG_NAME)

        return self.__logger

    @classmethod
    def set_level(cls, log_level:SyncLogLevel) -> None:
        """
        Sets the logging level for the Fabric Sync Accelerator logger and its handlers.
        Args:
            log_level (SyncLogLevel): The logging level to set for the logger and its handlers.
        Returns:
            None
        """
        cls.getLogger().setLevel(log_level)

        for handler in cls.getLogger().handlers[:]:
            handler.setLevel(log_level)        

    @classmethod
    def reset_logging(cls) -> None:
        """
        Resets the logger for the Fabric Sync Accelerator.
        This method removes all handlers from the logger, closes them, and sets the logger to None. 
        It is used to clean up the logger when it is no longer needed or before re-initializing it
        """
        if SyncConstants.FABRIC_LOG_NAME in logging.Logger.manager.loggerDict:
            loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict if name == SyncConstants.FABRIC_LOG_NAME]

            for logger in loggers:
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                    handler.close()
                logger.setLevel(logging.NOTSET)
                logger.propagate = True

            del logging.Logger.manager.loggerDict[SyncConstants.FABRIC_LOG_NAME]
    
    @property
    def Context(self) -> SparkSession:
        """
        Returns the Spark session context for the Fabric Sync Accelerator.
        If the context has not been set, it retrieves the active Spark session.
        Returns:
            SparkSession: The Spark session context for the Fabric Sync Accelerator.
        """
        if not self.__context:
            self.__context = SparkSession.getActiveSession()

        return self.__context

class ThreadingLogFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True):
        """
        Initializes the ThreadingLogFormatter with a specific format and date format.
        Args:
            fmt (str, optional): The format string for the log record. Defaults to None.
            datefmt (str, optional): The date format string for the log record. Defaults to None.
            style (str, optional): The style of the format string. Defaults to '%'.
            validate (bool, optional): Whether to validate the format string. Defaults to True.
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)
        self.__default_formatter = None

    @property
    def default_format(self):
        """
        Returns the default format for the log record.
        If the default formatter has not been set, it creates a new formatter with the specified format.
        Returns:
            logging.Formatter: The default formatter for the log record.
        """
        if not self.__default_formatter:
            self.__default_formatter = logging.Formatter(f"%(threadName)s - {self._fmt}")
        return self.__default_formatter

    def format(self, record):
        """
        Formats the log record based on its level.
        Args:
            record (logging.LogRecord): The log record to be formatted.
        Returns:
            str: The formatted log record as a string.
            If the record's level is DEBUG or SYNC_STATUS, it uses the default format.
        """
        match record.levelno:
            case SyncLogLevel.DEBUG:
                return self.default_format.format(record)
            case SyncLogLevel.SYNC_STATUS:
                if SyncLogger.getLogger().getEffectiveLevel() <= SyncLogLevel.DEBUG.value:
                    return self.default_format.format(record)
        
        return super().format(record)
    
class SyncLogHandler(logging.Handler):
    """
    SyncLogHandler is a custom logging handler that passes log records to a target handler.
    It is used to allow the logger to handle log records in a specific way, such as sending them to a remote API or processing them differently.
    Attributes:
        name (str): The name of the log handler.
        target_handler (logging.Handler): The target logging handler to which log records are passed.
    """
    def __init__(self, name, target_handler) -> None:
        """
        Initializes the SyncLogHandler with a name and a target handler.
        Args:
            name (str): The name of the log handler.
            target_handler (logging.Handler): The target logging handler to which log records are passed.
        """
        super().__init__()

        self.name = name
        self.target_handler = target_handler

    def emit(self, record) -> None:
        """
        Emits a log record by passing it to the target handler.
        Args:
            record (logging.LogRecord): The log record to be emitted.
            This method is called when a log record is created and needs to be processed.
        """
        #Pass-through handler defined for future use
        self.target_handler.handle(record)

class CustomJsonFormatter(logging.Formatter):
    """
    CustomJsonFormatter is a logging formatter that formats log records as JSON strings.
    It converts the log record's attributes into a dictionary and serializes it to a JSON string.
    Attributes:
        fmt (str): The format string for the log record.
        datefmt (str): The date format string for the log record.
    """
    def format(self, record: logging.LogRecord) -> str:
        """
        Formats a log record as a JSON string.
        Args:
            record (logging.LogRecord): The log record to be formatted.
        Returns:
            str: A JSON string representation of the log record.
            This method converts the log record's attributes into a dictionary and serializes it to a JSON string.
        """
        output = {k: str(v) for k, v in record.__dict__.items()}
        return json.dumps(output)

class Telemetry(): 
    """
    Telemetry is a class that provides methods for logging telemetry data related to the Fabric Sync Accelerator operations.
    It includes methods for logging telemetry messages for various operations such as installation, upgrade, synchronization, and maintenance.
    It uses the SyncLogger to log telemetry messages and sends them to the Fabric Sync Accelerator API.
    Attributes:
        None
    """
    def log_telemetry(operation:str, result=None, data=None) -> None:
        """
        Logs telemetry data for a specific operation.
        Args:
            operation (str): The name of the operation being logged.
            result (bool, optional): The result of the operation, indicating success or failure. Defaults to None.
            data (dict, optional): Additional data related to the operation, such as parameters or results. Defaults to None.
            This method creates a payload dictionary containing the operation name, result, and any additional data.
            It then uses the SyncLogger to log the telemetry message.
        """
        payload = {
            "operation":operation,
            "result": result
        }

        if data:
            payload["operation_data"] = data

        SyncLogger.getLogger().telemetry(payload)

    def Install(func_=None):
        """
        Decorator for logging telemetry data for the Fabric Sync Accelerator installation operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Sync Accelerator upgrade operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Mirror Database Sync operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Metadata Sync operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Sync Auto Discover Configuration operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Sync Scheduler operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Sync Load operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the Fabric Lakehouse Inventory operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the OneLake Delta Table Maintenance operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
            maintainence_type (str, optional): The type of maintenance being performed. Defaults to None.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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
        """
        Decorator for logging telemetry data for the BigQuery Data Expiration Enforcement operation.
        This decorator can be applied to a function to log telemetry data when the function is called.
        Args:
            func_ (callable, optional): The function to be decorated. If None, returns the decorator itself.
        Returns:
            callable: The decorated function that logs telemetry data when called.
        """
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