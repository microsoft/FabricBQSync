import re
import os
import json

from pathlib import Path
from typing import List, Literal, Tuple
from py4j.java_gateway import (
    JavaObject, java_import
)
from pyspark.sql import SparkSession
#from builtins import max as b
import builtins as b
from io import BytesIO

from FabricSync.BQ.Model.Core import HDFSFile
from FabricSync.BQ.Enum import FileSystemType
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Core import ContextAwareBase

class HadoopFileSystem(ContextAwareBase):
    __FS_PATTERN = r"(s3\w*://|hdfs://|abfss://|dbfs://|file://|file:/).(.*)"

    def __init__(self: "HadoopFileSystem", pattern: str) -> None:
        """
        Initializes a new instance of the HadoopFileSystem class.
        Args:
            pattern (str): The Hadoop file system pattern to use.
        """
        hadoop, hdfs, fs_type = self.__get_hdfs(self.Context, pattern)
        self._hdfs = hdfs
        self._fs_type = fs_type
        self._hadoop = hadoop
        self._jvm = self.Context.sparkContext._jvm

    def write(self: "HadoopFileSystem", path: str, data, mode: Literal["a", "w"] = "w") -> None:
        """
        Writes data to a file in the Hadoop file system. 
        Args:
            path (str): The path to the file to write.
            data (Union[str, bytes, bytearray, BytesIO]): The data to write to the file.
            mode (Literal["a", "w"]): The write mode to use.
        Raises:
            TypeError: If the data is not a str, bytes-like object, or Bytes IO base.
            Exception: If an error occurs while writing the data.
        Returns:
            None
        """
        if not isinstance(data, (str, bytes, bytearray, BytesIO)):
            raise TypeError("data must be a str, bytes-like object or Bytes IO base is required, not '{}'".format(type(data).__name__))

        if isinstance(data, str):
            buffer = BytesIO(data.encode("utf-8"))
        elif isinstance(data, (bytes, bytearray)):
            buffer = BytesIO(data)
        else:
            buffer = data

        if mode.lower() == "w":
            output_stream = self._hdfs.create(self._hadoop.fs.Path(path), True)  # type: ignore
        elif mode.lower() == "a":
            output_stream = self._hdfs.append(self._hadoop.fs.Path(path))  # type: ignore

        try:
            with buffer as b:
                b.seek(0)

                while True:
                    chunk = b.read(1024 * 1024)

                    if not chunk:
                        break

                    output_stream.write(chunk)                

            output_stream.flush()
            output_stream.close()
        except Exception as e:
            output_stream.close()
            raise e
    
    def read(self: "HadoopFileSystem", path: str) -> BytesIO:
        """
        Reads data from a file in the Hadoop file system.
        Args:
            path (str): The path to the file to read.
        Returns:
            BytesIO: The data read from the file.
        """
        in_stream = self._hdfs.open(self._hadoop.fs.Path(path))  # type: ignore
        buffer = bytearray()

        try:
            byte = in_stream.read()
            while byte != -1:
                buffer.append(byte)
                byte = in_stream.read()
        except Exception as e:
            raise e
        finally:
            in_stream.close()

        return bytes(buffer)

    def delete(self, target:str, recurse:bool=False) -> bool:
        """
        Deletes a file or directory in the Hadoop file system.
        Args:
            target (str): The path to the file or directory to delete.
            recurse (bool): A boolean indicating whether to delete recursively.
        Returns:
            bool: A boolean indicating whether the file or directory was deleted successfully.
        """
        result = self._hdfs.delete(
            self._hadoop.fs.Path(target), recurse)
        return result

    def rename(self, target:str, destination:str) -> bool:
        """
        Renames a file or directory in the Hadoop file system.
        Args:
            target (str): The path to the file or directory to rename.
            destination (str): The new path for the file or directory.
        Returns:
            bool: A boolean indicating whether the file or directory was renamed successfully.
        """
        result = self._hdfs.rename(
            self._hadoop.fs.Path(target),
            self._hadoop.fs.Path(destination))

        return result

    def copyFromLocalFile(self, source:str, destination:str) -> None:
        """
        Copies a file from the local file system to the Hadoop file system.
        Args:
            source (str): The path to the source file.
            destination (str): The path to the destination file.
        Returns:
            None
        """
        self._hdfs.copyFromLocalFile(
            self._hadoop.fs.Path(source),
            self._hadoop.fs.Path(destination))

    def glob(self, pattern: str) -> List[HDFSFile]:
        """
        Searches for files or directories in the Hadoop file system using a glob pattern.
        Args:
            pattern (str): The glob pattern to use for the search.
        Returns:
            List[HDFSFile]: A list of HDFSFile objects representing the files or directories found.
        """
        statuses = self._hdfs.globStatus(self._hadoop.fs.Path(pattern))

        res = []
        for file_status in statuses:
            # org.apache.hadoop.fs.FileStatus
            res.append(
                HDFSFile(
                    name=file_status.getPath().getName(),
                    path=file_status.getPath().toString(),
                    mod_time=file_status.getModificationTime(),
                    is_dir=file_status.isDirectory(),
                    fs_type=self._fs_type,
                )
            )

        return res
    
    def __get_hdfs(self, context: SparkSession, pattern: str) -> Tuple[JavaObject, JavaObject, FileSystemType]:
        """
        Gets the Hadoop file system object for the specified pattern.
        Args:
            context (SparkSession): The Spark session context.
            pattern (str): The Hadoop file system pattern.
        Returns:
            Tuple[JavaObject, JavaObject, FileSystemType]: A tuple containing the Hadoop, HDFS, and file system type objects.
        Raises:
            ValueError: If the pattern is invalid.
        """
        match = re.match(self.__FS_PATTERN, pattern)

        if match is None:
            raise ValueError(
                f"Bad pattern or path. Got {pattern} but should be"
                " one of `abfss://`, `s3://`, `s3a://`, `dbfs://`, `hdfs://`, `file://`")

        java_import(context.sparkContext._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(context.sparkContext._jvm, "org.apache.hadoop.fs.Path")
        java_import(context.sparkContext._jvm, "org.apache.hadoop.fs.FileStatus")
        java_import(context.sparkContext._jvm, "org.apache.hadoop.fs.FSDataInputStream")
        java_import(context.sparkContext._jvm, "org.apache.hadoop.fs.FSDataOutputStream")

        fs_type = FileSystemType._from_pattern(match.groups()[0])

        # Java is accessible in runtime only and it is impossible to infer types here
        hadoop = context.sparkContext._jvm.org.apache.hadoop  # type: ignore
        hadoop_conf = context._jsc.hadoopConfiguration()  # type: ignore
        uri = hadoop.fs.Path(pattern).toUri()  # type: ignore
        hdfs = hadoop.fs.FileSystem.get(uri, hadoop_conf)  # type: ignore

        return (hadoop, hdfs, fs_type)  # type: ignore

class OneLakeUtil:
    _abfss_onelake_path_format = "abfss://{}@onelake.dfs.fabric.microsoft.com/{}/"

    @staticmethod
    def get_onelake_uri(workspace_id:str, lakehouse_id:str) -> str:
        """
        Gets the URI for a OneLake instance.
        Args:
            workspace_id (str): The workspace ID for the OneLake instance.
            lakehouse_id (str): The lakehouse ID for the OneLake instance.
        Returns:
            str: The URI for the OneLake instance.
        """
        return OneLakeUtil._abfss_onelake_path_format.format(workspace_id, lakehouse_id)

class OneLakeFileSystem(HadoopFileSystem):
    def __init__(self, workspace_id:str, lakehouse_id:str) -> None:
        """
        Initializes a new instance of the OneLakeFileSystem class.
        Args:
            workspace_id (str): The workspace ID for the OneLake instance.
            lakehouse_id (str): The lakehouse ID for the OneLake instance.
        """
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._base_uri = OneLakeUtil.get_onelake_uri(workspace_id, lakehouse_id)

        super().__init__(self._base_uri)
    
    def _get_onelake_path(self, path) -> str:
        """
        Gets the full path for a file in the OneLake file system.
        Args:
            path (str): The path to the file.
        Returns:
            str: The full path for the file in the OneLake file system.
        """
        return os.path.join(self._base_uri, path.lstrip("/"))

    def copyFromLocalFile(self, source:str, destination:str) -> None:
        """
        Copies a file from the local file system to the OneLake file system.
        Args:
            source (str): The path to the source file.
            destination (str): The path to the destination file.
        Returns:
            None
        """
        super().copyFromLocalFile(
            source,
            self._get_onelake_path(destination))

    def write(self, path: str, data, mode: Literal["a", "w"]="w") -> None:
        """
        Writes data to a file in the OneLake file system.
        Args:
            path (str): The path to the file to write.
            data (Union[str, bytes, bytearray, BytesIO]): The data to write to the file.
            mode (Literal["a", "w"]): The write mode to use.
        Raises:
            TypeError: If the data is not a str, bytes-like object, or Bytes IO base.
            Exception: If an error occurs while writing the data.  
        Returns:
            None
        """
        super().write(self._get_onelake_path(path), data, mode)
    
    def read_string(self: "HadoopFileSystem", path: str) -> str:
        """
        Reads data from a file in the OneLake file system as a string.
        Args:
            path (str): The path to the file to read.
        Returns:
            str: The data read from the file as a string.
        """
        buffer = self.read(self._get_onelake_path(path))
        return buffer.getvalue().decode("utf-8")
    
    def read(self, path: str) -> BytesIO:
        """
        Reads data from a file in the OneLake file system.
        Args:
            path (str): The path to the file to read.
        Returns:
            str: The data read from the file.
        """
        return super().read(self._get_onelake_path(path))
    
    def delete(self, target:str, recurse:bool=False) -> bool:
        """
        Deletes a file or directory in the OneLake file system.
        Args:
            target (str): The path to the file or directory to delete.
            recurse (bool): A boolean indicating whether to delete recursively.
        Returns:
            bool: A boolean indicating whether the file or directory was deleted successfully.
        """
        return super().delete(self._get_onelake_path(target), recurse)
    
    def rename(self, target:str, destination:str) -> bool:
        """
        Renames a file or directory in the OneLake file system.
        Args:
            target (str): The path to the file or directory to rename.
            destination (str): The new path for the file or directory.  
        Returns:
            bool: A boolean indicating whether the file or directory was renamed successfully.
        """
        return super().rename(self._get_onelake_path(target), self._get_onelake_path(destination))
    
    def glob(self, pattern: str) -> List[HDFSFile]:
        return super().glob(pattern)

class OpenMirrorLandingZone(OneLakeFileSystem):
    """
    A class representing a file system for an OpenMirror landing zone. 
    """
    _lz_path = "Files/LandingZone/"
    _lz_table_schema_format = "{}.schema"

    SCRATCH_PATH = "_scratch"

    def __init__(self, workspace_id:str, lakehouse_id:str, table_schema:str, table:str) -> None:
        """
        Initializes a new instance of the OpenMirrorLandingZone class.
        Args:
            workspace_id (str): The workspace ID for the OneLake instance.
            lakehouse_id (str): The lakehouse ID for the OneLake instance.
            table_schema (str): The schema for the table.
            table (str): The name of the table.
        """
        super().__init__(workspace_id, lakehouse_id)

        self._table_schema = table_schema
        self._table = table

    @property
    def _lz_relative_path(self) -> str:
        """
        Gets the relative path for the OpenMirror landing zone.
        Returns:
            str: The relative path for the OpenMirror landing zone.
        """
        if self._table_schema:
            return os.path.join(self._lz_path,
                self._lz_table_schema_format.format(self._table_schema),self._table)
        else:
            return os.path.join(self._lz_path, self._table)

    @property
    def _lz_uri(self) -> str:
        """
        Gets the URI for the OpenMirror landing zone.
        Returns:
            str: The URI for the OpenMirror landing zone.
        """
        if self._table_schema:
            return os.path.join(self._base_uri, self._lz_path,
                self._lz_table_schema_format.format(self._table_schema),self._table)
        else:
            return os.path.join(self._base_uri, self._lz_path, self._table)

    def _get_onelake_path(self, path) -> str:
        """
        Gets the full path for a file in the OpenMirror landing zone.
        Args:
            path (str): The path to the file.
        Returns:
            str: The full path for the file in the OpenMirror landing zone.
        """
        return os.path.join(self._lz_uri, path.lstrip("/"))

    def copyFromLocalFile(self, source:str, destination:str) -> None:
        """
        Copies a file from the local file system to the OpenMirror landing zone.
        Args:
            source (str): The path to the source file.
            destination (str): The path to the destination file.
        Returns:
            None
        """
        self.Logger.debug(f"Landing Zone Operation - COPYFROMLOCALFILE - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} " +
                          f"{source} -> {destination}")

        super().copyFromLocalFile(source, destination)

    def write(self, path: str, data, mode: Literal["a", "w"] = "w") -> None:
        """
        Writes data to a file in the OpenMirror landing zone.
        Args:
            path (str): The path to the file to write.
            data (Union[str, bytes, bytearray, BytesIO]): The data to write to the file.
            mode (Literal["a", "w"]): The write mode to use.
        Raises:
            TypeError: If the data is not a str, bytes-like object, or Bytes IO base.
            Exception: If an error occurs while writing the data.
        Returns:
            None
        """
        self.Logger.debug(f"Landing Zone Operation - WRITE ({mode}) - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {path}")
        super().write(path, data, mode)
    
    def read(self, path: str) -> BytesIO:
        """
        Reads data from a file in the OpenMirror landing zone.
        Args:
            path (str): The path to the file to read.
        Returns:
            BytesIO: The data read from the file.
        """
        self.Logger.debug(f"Landing Zone Operation - READ - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {path}")
        return super().read(path)

    def read_string(self: "HadoopFileSystem", path: str) -> str:
        """
        Reads data from a file in the OpenMirror landing zone as a string.
        Args:
            path (str): The path to the file to read.
        Returns:
            str: The data read from the file as a string.
        """
        self.Logger.debug(f"Landing Zone Operation - READ (STRING) - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {path}")
        return super().read(path)

    def delete(self, target:str, recurse:bool=False) -> bool:
        """
        Deletes a file or directory in the OpenMirror landing zone.
        Args:
            target (str): The path to the file or directory to delete.
            recurse (bool): A boolean indicating whether to delete recursively.
        Returns:
            bool: A boolean indicating whether the file or directory was deleted successfully.
        """
        self.Logger.debug(f"Landing Zone Operation - DELETE - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {target}")
        return super().delete(target, recurse)
    
    def rename(self, target:str, destination:str) -> bool:
        """
        Renames a file or directory in the OpenMirror landing zone.
        Args:
            target (str): The path to the file or directory to rename.
            destination (str): The new path for the file or directory.
        Returns:
            bool: A boolean indicating whether the file or directory was renamed successfully.
        """
        self.Logger.debug(f"Landing Zone Operation - RENAME - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {target} -> {destination}")
        return super().rename(target, destination)

    def glob(self, pattern: str) -> List[HDFSFile]:
        """
        Searches for files or directories in the OpenMirror landing zone using a glob pattern.
        Args:
            pattern (str): The glob pattern to use for the search.
        Returns:
            List[HDFSFile]: A list of HDFSFile objects representing the files or directories found.
        """
        self.Logger.debug(f"Landing Zone Operation - SEARCH - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {pattern}")
        return super().glob(f"/{self._lakehouse_id}/{self._lz_relative_path}/{pattern}")

    def delete_table(self) -> bool:
        """
        Deletes the OpenMirror landing zone for the table.
        Returns:
            bool: A boolean indicating whether the OpenMirror landing zone was deleted successfully.
        """
        return self.delete("", recurse=True)
    
    def get_last_file_index(self) -> int:
        """
        Gets the last file index in the OpenMirror landing zone.
        Returns:
            int: The last file index in the OpenMirror landing zone.
        """
        files = [
            int(Path(x.name).with_suffix('').stem) for x in self.glob(f"*.parquet") 
                if not x.name.endswith(".snappy.parquet")
            ]

        processed_files = [
            int(Path(x.name).with_suffix('').stem) for x in self.glob(f"_ProcessedFiles/*.parquet") 
                if not x.name.endswith(".snappy.parquet")
            ]

        files = files + processed_files
        
        file_index = b.max(files) if files else 0        
        self.Logger.debug(f"Landing Zone Operation - MIRROR INDEX - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {file_index}")
        return file_index
    
    def cleanup_staged_lz(self)-> None:
        """
        Cleans up staged files in the OpenMirror landing zone.
        Raises:
            Exception: If any files cannot be deleted.
        """
        self.delete(f"{self.SCRATCH_PATH}/", True)
        #self.delete("_SUCCESS")
        #
        #for f in  [x.name for x in self.glob(f"*.snappy.parquet")]:
        #    result = self.delete(f)
        #
        #    if not result:
        #        raise Exception(f"Failed to delete {LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} LZ stage file {f}")

    #def cleanup_lz(self)-> None:
    #    """
    #    Cleans up files in the OpenMirror landing zone.
    #    Raises:
    #        Exception: If any files cannot be deleted.
    #    """
    #    files = { 
    #        x.name: int(Path(x.name).with_suffix('').stem) 
    #            for x in self.glob(f"*.parquet") 
    #            if not x.name.endswith(".snappy.parquet") }
    #
    #    max_key = b.max(files, key=files.get)
    #
    #    for f in [f for f in files if f != max_key]:
    #        result = self.delete(f)
    #
    #        if not result:
    #            raise Exception(f"Failed to delete {LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} LZ stage file {f}")
    
    def __format_lz_filename(self, idx:int) -> str:
        """
        Formats a file name for the OpenMirror landing zone.
        Args:
            idx (int): The index for the file.
        Returns:
            str: The formatted file name for the OpenMirror landing zone.
        """
        return "%020d.parquet" % idx
    
    def generate_metadata_file(self, keys:List[str]) -> bool:
        """
        Generates a metadata file for a table using the provided key columns.
        Args:
            keys (List[str]): The key columns for the table.
        Returns:
            bool: A boolean indicating whether the metadata file was generated successfully
        """
        #Generate metadata file for the table
        if not keys:
            keys = []
            
        metadata = {"keyColumns" : keys}
        self.write("_metadata.json", json.dumps(metadata), "w")

    def stage_spark_output(self, mirror_index:int) -> int:
        """
        Stages and renames Spark output files to match the LZ naming convention.
        This method deletes the Spark _SUCCESS marker file, checks for consistency
        between the current file index and the provided mirror index, and then renames
        all applicable parquet files in incremental order.
        Args:
            mirror_index (int): The mirror index to use for the staging operation.
        Returns:
            int: The new mirror index after the staging operation.
        Raises:
            Exception: If the specified mirror index is inconsistent with the current state.
        """
        #Delete Spark _SUCCESS marker file
        #self.delete(f"{self.SCRATCH_PATH}/_SUCCESS")

        current_index = self.get_last_file_index()

        if (current_index + 1) != mirror_index:
            #Raise an exception if the specified mirror index is inconsistent with the current state
            raise Exception("Specified mirror index is inconsistent with current state: "+
                f"(Specified: {mirror_index} - Current: {(current_index + 1)})")

        files = sorted([x.name for x in self.glob(f"{self.SCRATCH_PATH}/*.snappy.parquet") ])

        for f in files:
            #Rename spark output files to LZ naming standard
            stage_file = self.__format_lz_filename(mirror_index)
            result = self.rename(f"{self.SCRATCH_PATH}/{f}", stage_file)

            if not result:
                raise Exception(f"Failed to stage {self._table} LZ stage file {f}")

            mirror_index += 1
        
        self.cleanup_staged_lz()

        return mirror_index