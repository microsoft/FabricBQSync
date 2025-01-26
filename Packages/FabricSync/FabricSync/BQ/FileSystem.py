import re
import os
import json

from pathlib import Path

from typing import List, Literal, Tuple

from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession

from FabricSync.BQ.Model.Core import HDFSFile
from FabricSync.BQ.Enum import FileSystemType
from FabricSync.BQ.Lakehouse import LakehouseCatalog
from FabricSync.BQ.Logging import SyncLogger

class HadoopFileSystem(object):
    __FS_PATTERN = r"(s3\w*://|hdfs://|abfss://|dbfs://|file://|file:/).(.*)"

    def __init__(self: "HadoopFileSystem", pattern: str) -> None:
        spark = SparkSession.builder.getOrCreate()
        hadoop, hdfs, fs_type = self.__get_hdfs(spark, pattern)
        self._hdfs = hdfs
        self._fs_type = fs_type
        self._hadoop = hadoop
        self._jvm = spark.sparkContext._jvm

    def write(self: "HadoopFileSystem", path: str, data: str, mode: Literal["a", "w"]) -> None:
        if mode == "w":
            # org.apache.hadoop.fs.FileSystem.create(Path f, boolean overwrite)
            output_stream = self._hdfs.create(self._hadoop.fs.Path(path), True)  # type: ignore
        elif mode == "a":
            # org.apache.hadoop.fs.FileSystem.append(Path f)
            output_stream = self._hdfs.append(self._hadoop.fs.Path(path))  # type: ignore

        # org.apache.hadoop.fs.FSDataOutputStream
        try:
            for b in data.encode("utf-8"):
                output_stream.write(b)
            output_stream.flush()
            output_stream.close()
        except Exception as e:
            output_stream.close()
            raise e

    def read(self: "HadoopFileSystem", path: str) -> str:
        res = []
        # org.apache.hadoop.fs.FileSystem.open
        in_stream = self._hdfs.open(self._hadoop.fs.Path(path))  # type: ignore

        # open returns us org.apache.hadoop.fs.FSDataInputStream
        try:
            while True:
                if in_stream.available() > 0:
                    res.append(in_stream.readByte())
                else:
                    in_stream.close()
                    break
        except Exception as e:
            in_stream.close()
            raise e

        return bytes(res).decode("utf-8")

    def delete(self, target:str, recurse:bool=False) -> bool:
        result = self._hdfs.delete(
            self._hadoop.fs.Path(target), recurse)
        return result

    def rename(self, target:str, destination:str) -> bool:
        result = self._hdfs.rename(
            self._hadoop.fs.Path(target),
            self._hadoop.fs.Path(destination))

        return result

    def glob(self, pattern: str) -> List[HDFSFile]:
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
    
    def __get_hdfs(self, spark: SparkSession, pattern: str) -> Tuple[JavaObject, JavaObject, FileSystemType]:
        match = re.match(self.__FS_PATTERN, pattern)

        if match is None:
            raise ValueError(
                f"Bad pattern or path. Got {pattern} but should be"
                " one of `abfss://`, `s3://`, `s3a://`, `dbfs://`, `hdfs://`, `file://`")

        fs_type = FileSystemType._from_pattern(match.groups()[0])

        # Java is accessible in runtime only and it is impossible to infer types here
        hadoop = spark.sparkContext._jvm.org.apache.hadoop  # type: ignore
        hadoop_conf = spark._jsc.hadoopConfiguration()  # type: ignore
        uri = hadoop.fs.Path(pattern).toUri()  # type: ignore
        hdfs = hadoop.fs.FileSystem.get(uri, hadoop_conf)  # type: ignore

        return (hadoop, hdfs, fs_type)  # type: ignore

class OneLakeFileSystem(HadoopFileSystem):
    _abfss_onelake_path_format = "abfss://{}@onelake.dfs.fabric.microsoft.com/{}/"

    def __init__(self, workspace_id:str, lakehouse_id:str) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_id = lakehouse_id
        self._base_uri = self._abfss_onelake_path_format.format(workspace_id, lakehouse_id)

        super().__init__(self._base_uri)
    
    def _get_onelake_path(self, path):
        return os.path.join(self._base_uri, path)

    def write(self, path: str, data: str, mode: Literal["a", "w"]) -> None:
        super().write(self._get_onelake_path(path), data, mode)
    
    def read(self, path: str) -> str:
        return super().read(self._get_onelake_path(path))
    
    def delete(self, target:str, recurse:bool=False) -> bool:
        return super().delete(self._get_onelake_path(target), recurse)
    
    def rename(self, target:str, destination:str) -> bool:
        return super().rename(self._get_onelake_path(target), self._get_onelake_path(destination))
    
    def glob(self, pattern: str) -> List[HDFSFile]:
        return super().glob(pattern)

class OpenMirrorLandingZone(OneLakeFileSystem):
    _lz_path = "Files/LandingZone/"
    _lz_table_schema_format = "{}.schema"
    __Logger = SyncLogger().get_logger()

    def __init__(self, workspace_id:str, lakehouse_id:str, table_schema:str, table:str) -> None:
        super().__init__(workspace_id, lakehouse_id)

        self._table_schema = table_schema
        self._table = table
    
    @property
    def Logger(self):
        return self.__logger

    @property
    def _lz_relative_path(self) -> str:
        if self._table_schema:
            return os.path.join(self._lz_path,
                self._lz_table_schema_format.format(self._table_schema),self._table)
        else:
            return os.path.join(self._lz_path, self._table)

    @property
    def _lz_uri(self) -> str:
        if self._table_schema:
            return os.path.join(self._base_uri, self._lz_path,
                self._lz_table_schema_format.format(self._table_schema),self._table)
        else:
            return os.path.join(self._base_uri, self._lz_path, self._table)

    def _get_onelake_path(self, path) -> str:
        return os.path.join(self._lz_uri, path)

    def write(self, path: str, data: str, mode: Literal["a", "w"]) -> None:
        self.Logger.debug(f"Landing Zone Operation - WRITE ({mode}) - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {path}")
        super().write(path, data, mode)
    
    def read(self, path: str) -> str:
        self.Logger.debug(f"Landing Zone Operation - READ - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {path}")
        return super().read(path)

    def delete(self, target:str, recurse:bool=False) -> bool:
        self.Logger.debug(f"Landing Zone Operation - DELETE - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {target}")
        return super().delete(target, recurse)
    
    def rename(self, target:str, destination:str) -> bool:
        self.Logger.debug(f"Landing Zone Operation - RENAME - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {target} -> {destination}")
        return super().rename(target, destination)

    def glob(self, pattern: str) -> List[HDFSFile]:
        self.Logger.debug(f"Landing Zone Operation - SEARCH - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {pattern}")
        return super().glob(f"/{self._lakehouse_id}/{self._lz_relative_path}/{pattern}")

    def delete_table(self) -> bool:
        return self.delete("", recurse=True)
    
    def get_last_file_index(self) -> int:
        files = [
            int(Path(x.name).with_suffix('').stem) for x in self.glob(f"*.parquet") 
                if not x.name.endswith(".snappy.parquet")
            ]
        
        file_index = max(files) if files else 0
        
        self.Logger.debug(f"Landing Zone Operation - MIRROR INDEX - " +
                          f"{LakehouseCatalog.resolve_table_name(self._table_schema, self._table)} - {file_index}")
        return file_index
    
    def cleanup_lz(self)-> None:
        files = { 
            x.name: int(Path(x.name).with_suffix('').stem) 
                for x in self.glob(f"*.parquet") 
                if not x.name.endswith(".snappy.parquet") }

        max_key = max(files, key=files.get)

        for f in [f for f in files if f != max_key]:
            result = self.delete(f)

            if not result:
                raise Exception(f"Failed to delete {self._table} LZ stage file {f}")
    
    def __format_lz_filename(self, idx:int) -> str:
        return "%020d.parquet" % idx
    
    def generate_metadata_file(self, keys:List[str]) -> bool:
        #Generate metadata file for the table
        metadata = {"keyColumns" : keys}
        self.write("_metadata.json", json.dumps(metadata), "w")

    def stage_spark_output(self, mirror_index:int) -> int:
        #Delete Spark _SUCCESS marker file
        self.delete("_SUCCESS")

        current_index = self.get_last_file_index()

        if (current_index + 1) != mirror_index:
            #Raise an exception if the specified mirror index is inconsistent with the current state
            raise Exception("Specified mirror index is inconsistent with current state: "+
                f"(Specified: {mirror_index} - Current: {(current_index + 1)})")

        files = sorted([x.name for x in self.glob(f"*.parquet") 
            if x.name.endswith(".snappy.parquet")
            ])

        for f in files:
            #Rename spark output files to LZ naming standard
            stage_file = self.__format_lz_filename(mirror_index)
            result = self.rename(f, stage_file)

            if not result:
                raise Exception(f"Failed to stage {self._table} LZ stage file {f}")

            mirror_index += 1
        
        return mirror_index
