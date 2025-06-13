import base64 as b64
import time
import os
import requests
import re

from io import (
    BytesIO, StringIO
)
from pyspark.sql.types import StructType
from contextlib import ContextDecorator
from pyspark.sql import DataFrame

from dataclasses import (
    dataclass, field
)
from typing import (
    Any, Optional, Dict
)

from pyspark.sql.types import (
    ArrayType, MapType, StructType
)
from pyspark.sql.functions import (
    col, to_json
)
from FabricSync.BQ.Exceptions import SyncTimerError

@dataclass
class SyncTimer(ContextDecorator):
    """
    A simple timer class to measure elapsed time.
    It can be used as a context manager or as a regular class.
    Example usage:
        with SyncTimer() as timer:
            # Your code here
            time.sleep(2)
        print(f"Elapsed time: {timer.elapsed_time} seconds")
    """
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def start(self) -> None:
        """
        Start the timer.
        Raises:
            SyncTimerError: If the timer is already running.
        """
        if self._start_time is not None:
            raise SyncTimerError(f"Timer is already running.")

        self._start_time = time.perf_counter()

    def stop(self) -> float:
        """
        Stop the timer and return the elapsed time.
        Returns:
            float: The elapsed time in seconds.
        Raises:
            SyncTimerError: If the timer is not running.
        """
        if self._start_time is None:
            raise SyncTimerError(f"Timer is not running.")

        self.elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

        return self.elapsed_time

    def __enter__(self) -> None:
        """
        Start the timer when entering the context.
        Returns:
            SyncTimer: The instance of the timer.
        """
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """
        Stop the timer when exiting the context.
        Args:
            *exc_info: Exception information, if any.
        """
        self.stop()
    
    def __str__(self) -> str:
        """
        Return a string representation of the elapsed time in minutes.
        Returns:
            str: The elapsed time in minutes, formatted to 4 decimal places.
        """
        if self.elapsed_time:
            return f"{(self.elapsed_time/60):.4f} mins"
        else:
            return None
        
class Util():
    """
    A utility class providing various helper methods for file operations, data processing, and configuration handling.
    This class includes methods for reading files into buffers or strings, downloading files from URLs, ensuring directory paths exist,
    handling complex data types in DataFrames, encoding strings to base64, checking if a string is base64 encoded, and retrieving values from JSON objects.
    It also includes methods for assigning enum values and removing special characters from strings.
    Methods:
        read_file_to_buffer(path:str) -> BytesIO: Reads a file from the specified path and returns its content as a BytesIO buffer. 
        read_file_to_string(path:str) -> str: Reads a file from the specified path and returns its content as a string.
        download_file(url:str, path:str): Downloads a file from a URL and saves it to the specified path.
        download_file_to_buffer(url:str) -> BytesIO: Downloads a file from a URL and returns its content as a BytesIO buffer.
        download_file_to_string(url:str) -> str: Downloads a file from a URL and returns its content as a string.
        ensure_paths(paths): Ensures that the given paths exist, creating them if they do not.
        get_complex_types(df:DataFrame) -> Dict[str, str]: Gets the complex types in the DataFrame schema.
        convert_complex_types_to_json_str(df:DataFrame) -> DataFrame: Converts complex types in the DataFrame schema to JSON strings.
        encode_base64(val:str) -> str: Encodes a string to base64.
        is_base64(val) -> bool: Checks if a string is base64 encoded.
        get_config_value(json_data, json_path, default=None, raise_error=False): Gets a value from a JSON object using a path.
        assign_enum_val(enum_class, value): Assigns an enum value from a string.
        assign_enum_name(enum_class, name): Assigns an enum value from a string.
        remove_special_characters(val) -> str: Removes special characters from a string.
    """
    @staticmethod
    def read_file_to_buffer(path:str) -> BytesIO:
        """
        Read a file from the specified path and return its content as a BytesIO buffer.
        Args:
            path (str): The path to the file.
        Returns:
            BytesIO: A BytesIO buffer containing the content of the file.
        """
        buffer = BytesIO()

        with open(path, 'rb') as f:
            buffer = BytesIO(f.read())  

        return buffer
    
    @staticmethod
    def read_file_to_string(path:str) -> str:
        """
        Read a file from the specified path and return its content as a string.
        Args:
            path (str): The path to the file.
        Returns:
            str: The content of the file as a string.
        """
        buffer = StringIO()

        with buffer:
            with open(path, 'r') as f:
                buffer.write(f.read())
            
            return buffer.getvalue()

    @staticmethod
    def download_file(url:str, path:str):
        """
        Download a file from a URL and save it to the specified path.
        Args:
            url (str): The URL of the file to download.
            path (str): The local path where the file should be saved.
        """
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

    @staticmethod
    def download_file_to_buffer(url:str):
        """
        Download a file from a URL and return its content as a BytesIO buffer.
        Args:
            url (str): The URL of the file to download.
        Returns:
            BytesIO: A BytesIO buffer containing the content of the file.
        """
        buffer = BytesIO()
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk: 
                    buffer.write(chunk)
        else:
            raise Exception(f"Failed to download file from {url}")
        
        return buffer

    @staticmethod
    def download_file_to_string(url:str) -> str:
        """
        Download a file from a URL and return its content as a string.
        Args:
            url (str): The URL of the file to download.
        Returns:
            str: The content of the file as a string.
        """
        response = requests.get(url, stream=True)
        response.raise_for_status()

        buffer = StringIO()

        with buffer:
            for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
                if chunk:
                    buffer.write(chunk)
            
            return buffer.getvalue()
    
    @staticmethod
    def ensure_paths(paths):
        """
        Ensure that the given paths exist, creating them if they do not.
        Args:
            paths (list): A list of paths to ensure.
        """
        for p in paths:
            if not os.path.isdir(p):
                os.mkdir(p)

    @staticmethod
    def get_complex_types(df:DataFrame) -> Dict[str, str]:
        """
        Get the complex types in the DataFrame schema.
        Args:
            df: The DataFrame to check.
        Returns:
            Dict[str, str]: A dictionary of the complex types in the DataFrame schema
        """
        complex_types = {}
        for field in df.schema.fields:
            if isinstance(field.dataType, (ArrayType, MapType, StructType)):
                complex_types[field.name] = type(field.dataType).__name__
        return complex_types

    @staticmethod
    def convert_complex_types_to_json_str(df:DataFrame) -> DataFrame:
        """
        Convert complex types in the DataFrame schema to JSON strings.
        Args:
            df: The DataFrame to convert.
        Returns:
            DataFrame: The DataFrame with complex types converted to JSON strings.
        """
        complex_types = Util.get_complex_types(df)

        for c in complex_types.keys():
            df = df.withColumn(c, to_json(col(c)))
        
        return df

    @staticmethod
    def encode_base64(val:str) -> str:
        """
        Encode a string to base64.
        Args:
            val: The value to encode.
        Returns:
            str: The base64 encoded value.
        """
        b = b64.b64encode(bytes(val, 'utf-8'))
        return b.decode('utf-8')
    
    @staticmethod
    def is_base64(val) -> str:
        """
        Check if a string is base64 encoded.
        Args:
            val: The value to check.
        Returns:
            bool: True if the value is base64 encoded, False otherwise.
        """
        try:
            if isinstance(val, str):
                sb_bytes = bytes(val, 'utf-8')
            elif isinstance(val, bytes):
                sb_bytes = val
            else:
                return False

            return b64.b64encode(b64.b64decode(sb_bytes)) == sb_bytes
        except Exception as e:
            return False

    @staticmethod
    def get_config_value(json_data, json_path, default=None, raise_error=False):
        """
        Get a value from a JSON object using a path.
        Args:
            json_data: The JSON object to search.
            json_path: The path to the value.
            default: The default value to return if the key is not found.
            raise_error: If True, raise an error if the key is not found.
        Returns:
            Any: The value at the specified path.
        """
        paths = json_path.split('.')
        level = json_data

        for p in paths:
            if p in level:
                level = level[p]
                val = level
            else:
                val = None
                break
        
        if not val and raise_error:
            raise ValueError(f"Missing Key: {json_path}")
        elif not val:
            return default
        
        return val

    @staticmethod
    def assign_enum_val(enum_class, value):
        """
        Assign an enum value from a string.
        Args:
            enum_class: The enum class to assign.
            value: The value to assign.
        Returns:
                Any: The enum value.
        """
        try:
            return enum_class(value)
        except ValueError:
            return None
    
    @staticmethod
    def assign_enum_name(enum_class, name):
        """
        Assign an enum value from a string.
        Args:
            enum_class: The enum class to assign.
            value: The value to assign.
            Returns:
                Any: The enum value.
        """
        try:
            return enum_class[name]
        except ValueError:
            return None
    
    @staticmethod
    def remove_special_characters(val) -> str:
        """
        Remove special characters from a string.
        Args:
            val (str): The string from which to remove special characters.
        Returns:
            str: The string with special characters removed.
        """
        return re.sub(r'[^A-Za-z0-9]', '', val)