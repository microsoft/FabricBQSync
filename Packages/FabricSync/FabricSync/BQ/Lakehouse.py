from typing import (
    List
)
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField
)
import time

from FabricSync.BQ.Threading import SparkProcessor
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Metastore import FabricMetastoreSchema
from FabricSync.BQ.Enum import FabricDestinationType
from FabricSync.BQ.SessionManager import Session

class LakehouseCatalog(ContextAwareBase):
    @classmethod
    def get_catalog_tables(cls, catalog:str) -> List[str]:
        """
        Retrieves the list of tables in the specified catalog from the Spark metastore.
        Args:
            catalog (str): The name of the catalog to retrieve tables from.
        Returns:
            List[str]: A list of table names in the specified catalog.
        """
        sql = f"SHOW TABLES IN {catalog}"
        df = cls.Context.sql(sql) \
            .filter(col("namespace")!="")

        return [r["tableName"] for r in df.collect()]
    
    @classmethod
    def optimize_sync_metastore(cls, enable_schemas:bool = False) -> None:
        """
        Optimize and vacuum metadata and information schema tables for the synchronization process.
        This function retrieves the required metadata and information schema tables, then
        triggers an optimization and vacuum operation to improve storage and query performance.
        Returns:
            None
        """
        table_schema = "dbo" if enable_schemas else None
        metadata_tbls = SyncConstants.get_metadata_tables() + SyncConstants.get_information_schema_tables()
        SparkProcessor.optimize_vacuum([cls.resolve_table_name(table_schema, t) for t in metadata_tbls])
    
    @classmethod
    def reset_workspace(cls, userConfig:ConfigDataset, optimize=False) -> None:
        """
        Resets the Fabric Sync environment by deleting all metadata tables and dropping all target tables.
        Args:
            userConfig (ConfigDataset): The user-defined configuration dataset.
            optimize (bool): A flag indicating whether to optimize the metastore after resetting the environment.
        Returns:
            None
        """
        sql_commands = []
        table_schema = "dbo" if userConfig.Fabric.EnableSchemas else None

        for t in cls.get_catalog_tables(userConfig.Fabric.get_metadata_lakehouse()):
            if t != "data_type_map":
                sql_commands.append(f"DELETE FROM {userConfig.Fabric.MetadataLakehouse}." +
                                    f"{cls.resolve_table_name(table_schema, t)} WHERE sync_id='{userConfig.ID}';")
        
        if userConfig.Fabric.TargetType == FabricDestinationType.LAKEHOUSE:
            for t in cls.get_catalog_tables(userConfig.Fabric.get_target_lakehouse()):
                sql_commands.append(f"DROP TABLE IF EXISTS {userConfig.Fabric.TargetLakehouse}.{t};")

        cls.Logger.sync_status("Resetting Fabric Sync Environment")
        SparkProcessor.process_command_list(sql_commands)

        if optimize:
            cls.Logger.sync_status("Optimizing Fabric Sync Metastore")
            cls.optimize_sync_metastore()
    
    @classmethod
    def create_metastore_tables(cls, table_path:str) -> None:
        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)
            df = cls.Context.createDataFrame(data=cls.Context.sparkContext.emptyRDD(),schema=schema)
            df.write.mode("OVERWRITE").format("delta").save(f"{table_path}/{tbl}")
        

    @classmethod
    def upgrade_metastore(cls, metadata_lakehouse:str, enable_schemas:bool = False) -> None:
        """
        Upgrades the specified metadata lakehouse by ensuring each required metadata table 
        exists and has the latest schema.
        Steps:
          1. Gets list of the current metadata tables for the Catalog
          2. Renames tables if necessary
          3. Retrieves the list of metadata tables from SyncConstants.
          4. For each table:
             - If it exists, synchronizes its schema with the defined schema.
             - If it does not exist, creates the table using the defined schema.
          5. Executes any necessary SQL commands to finalize the schema upgrade.
        :param metadata_lakehouse: The name of the lakehouse where metadata tables should be upgraded.
        :type metadata_lakehouse: str
        """
        rewrite_tables = []
        cmds = []
        table_schema = "dbo" if enable_schemas else None

        current_tables = cls.__rename_metastore_tables(metadata_lakehouse)

        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)

            if tbl in current_tables:
                df = cls.Context.table(tbl)
                schema_changes = cls.__sync_schema(table_schema, tbl,  df.schema, schema)

                if schema_changes:
                    cmds.extend(schema_changes)

                    result = list(filter(lambda s: "DROP" in s, schema_changes))

                    if result:
                        rewrite_tables.append(tbl)
            else:
                df = cls.Context.createDataFrame(data=cls.Context.sparkContext.emptyRDD(),schema=schema)
                df.write.mode("OVERWRITE").saveAsTable(cls.resolve_table_name(table_schema, tbl))

        if cmds:
            for cmd in cmds:
                cls.Logger.sync_status(f"Metastore Upgrade: {cmd}", verbose=True)
                cls.Context.sql(cmd)
        
        if rewrite_tables:
            cls.__rewrite_metastore_tables(rewrite_tables)

    @classmethod
    def __rewrite_metastore_tables(cls, tables:List) -> None:
        Session.set_spark_conf("spark.sql.sources.partitionOverwriteMode", "static")

        for tbl in tables:
            cls.Logger.sync_status(f"Metastore Upgrade - Rewriting table for version capability: {tbl}", verbose=True)
            df = cls.Context.table(tbl)
            df.write.partitionBy("sync_id").mode("OVERWRITE").saveAsTable(f"{tbl}_tmp")
            cls.Context.sql(f"DROP TABLE IF EXISTS {tbl};")
            time.sleep(10)
            cls.Context.sql(f"ALTER TABLE {tbl}_tmp RENAME TO {tbl};")
        
        Session.set_spark_conf("spark.sql.sources.partitionOverwriteMode", "dynamic")

    @classmethod
    def __rename_metastore_tables(cls, metadata_lakehouse:str) -> List[str]:
        """
        Renames the metadata tables in the specified lakehouse to remove the 'bq_' prefix.
        Args:
            metadata_lakehouse (str): The name of the lakehouse containing the metadata tables.
        Returns:
            List[str]: A list of the current metadata tables in the lakehouse after renaming.
        """
        current_tables = cls.get_catalog_tables(metadata_lakehouse)  

        renamed_tables = [
            f"ALTER TABLE {metadata_lakehouse}.{t} RENAME TO " +
                f"{metadata_lakehouse}.{t.replace('bq_', '')}"                
                    for t in current_tables if "bq_" in t]

        if renamed_tables:   
            SparkProcessor.process_command_list(renamed_tables)
            current_tables = cls.get_catalog_tables(metadata_lakehouse)
        
        return current_tables

    @classmethod
    def resolve_table_name(cls, table_schema:str, table_name:str) -> str:
        return f"{table_name}" if not table_schema else f"{table_schema}.{table_name}"
                                                                 
    @classmethod
    def __sync_schema(cls, table_schema:str, table_name:str, source_schema:StructType, target_schema:StructType) -> List[str]:
        """
        Generates SQL commands to synchronize a table's schema between a source and target definition.
        This function compares the columns in the source_schema and target_schema, then:
        • Updates the table's Delta properties if changes are detected.
        • Removes columns present in the source_schema but not in the target_schema.
        • Adds new columns that exist in the target_schema but not in the source_schema.
        Args:
            table_name (str): The name of the table to update.
            source_schema (Iterable): The current schema of the table.
            target_schema (Iterable): The desired schema for the table.
        Returns:
            List[str]: A list of SQL commands needed to perform the schema synchronization.
        """

        cmds = []

        target_diff = set(target_schema) - set(source_schema)
        source_diff = set(source_schema) - set(target_schema)

        ddl = f"ALTER TABLE {cls.resolve_table_name(table_schema, table_name)} "

        if source_diff:
            cmds.append(
                ddl + "SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');")

            cmds.extend([ddl + f"DROP COLUMN {f.name};" for f in source_diff])
        
        if target_diff:
            for tf in target_schema.fieldNames():
                for f in target_diff:
                    if tf == f.name:
                        cmds.append(cls.__create_schema_field_sql(table_schema, table_name, f, target_schema))
                        break
        
        return cmds

    @classmethod
    def __create_schema_field_sql(cls, table_schema:str, table_name:str, field:StructField, schema:StructType) -> str:
        """
        Generates an SQL statement that adds a column to a specified table based on the provided field and schema.
        Args:
            table_name (str): The name of the table to be altered.
            field: An object representing the column to add, containing 'name' and 'dataType'.
            schema (list): A list of field objects representing the existing table schema.
        Returns:
            str: The SQL statement to add the new column to the table. If the column is not the first one to add,
                the statement will include the 'AFTER <existing column>' clause based on the schema.
        """    
        after_col = None

        for f in schema:
            if f.name == field.name:
                break
            else:
                after_col = f.name

        ddl = f"ALTER TABLE {cls.resolve_table_name(table_schema, table_name)} " + \
                f"ADD COLUMN {field.name} {field.dataType.simpleString()} "
        
        if after_col:
            ddl += f"AFTER {after_col};"
  
        return ddl