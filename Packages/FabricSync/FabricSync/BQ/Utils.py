import base64 as b64
import time
import os
import requests
import urllib.request
from delta.tables import DeltaTable
from pyspark.sql.types import StructType
from contextlib import ContextDecorator

from pyspark.sql import (
    Row, DataFrame
)

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
    col, to_json, max
)


from FabricSync.BQ.Exceptions import SyncTimerError
from FabricSync.BQ.Core import ContextAwareBase

class DeltaTableMaintenance(ContextAwareBase):
    __detail:Row = None

    def __init__(self, table_name:str, table_path:str=None) -> None:
        """
        Initializes a new instance of the DeltaTableMaintenance class.
        Args:
            table_name (str): The table name.
            table_path (str): The table path.
        """
        self.TableName = table_name

        if table_path:
            self.DeltaTable = DeltaTable.forPath(self.Context, table_path)
        else:
            self.DeltaTable = DeltaTable.forName(self.Context, table_name)
    
    @property
    def CurrentTableVersion(self) -> int:
        """
        Gets the current table version.
        Returns:
            int: The current table version.
        """
        history = self.DeltaTable.history() \
            .select(max(col("version")).alias("delta_version"))

        return [r[0] for r in history.collect()][0]

    @property
    def Detail(self) -> DataFrame:
        """
        Gets the table detail.
        Returns:
            DataFrame: The table detail.
        """
        if not self.__detail:
            self.__detail = self.DeltaTable.detail().collect()[0]
        
        return self.__detail
    
    def drop_partition(self, partition_filter:str) -> None:
        """
        Drops the partition.
        Args:
            partition_filter (str): The partition filter.
        """
        self.DeltaTable.delete(partition_filter)

    def drop_table(self) -> None:
        """
        Drops the table.
        """
        self.Context.sql(f"DROP TABLE IF EXISTS {self.TableName}")
    
    def optimize_and_vacuum(self, partition_filter:str = None) -> None:
        """
        Optimizes and vacuums the table.
        Args:
            partition_filter (str): The partition filter.
        """
        self.optimize(partition_filter)
        self.vacuum()
    
    def optimize(self, partition_filter:str = None) -> None:
        """
        Optimizes the table.
        Args:
            partition_filter (str): The partition filter.
        """
        if partition_filter:
            self.DeltaTable.optimize().where(partition_filter).executeCompaction()
        else:
            self.DeltaTable.optimize().executeCompaction()

    def vacuum(self) -> None:
        """
        Vacuums the table.
        """
        self.DeltaTable.vacuum(0)

@dataclass
class SyncTimer(ContextDecorator):
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._start_time is not None:
            raise SyncTimerError(f"Timer is already running.")

        self._start_time = time.perf_counter()

    def stop(self) -> float:
        if self._start_time is None:
            raise SyncTimerError(f"Timer is not running.")

        self.elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

        return self.elapsed_time

    def __enter__(self) -> None:
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()
    
    def __str__(self) -> str:
        if self.elapsed_time:
            return f"{(self.elapsed_time/60):.4f} mins"
        else:
            return None
        
class Util():
    @staticmethod
    def read_file_to_string(path:str) -> str:
        contents = ""

        with open(path, 'r') as f:
            contents = f.readlines()  

        return contents

    @staticmethod
    def download_file(url:str, path:str):
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

    @staticmethod
    def download_encoded_to_string(url:str) -> str:
        contents = ""

        for data in urllib.request.urlopen(url):
            contents += data.decode('utf-8')
        
        return contents
    
    @staticmethod
    def ensure_paths(paths):
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