from typing import (
    List
)
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField
)
from FabricSync.BQ.Threading import SparkProcessor
from FabricSync.BQ.Core import ContextAwareBase
from FabricSync.BQ.Model.Config import ConfigDataset
from FabricSync.BQ.Constants import SyncConstants
from FabricSync.BQ.Metastore import FabricMetastoreSchema
from FabricSync.BQ.Enum import FabricDestinationType

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
    def create_metastore_tables(cls, enable_schemas:bool = False) -> None:
        """
        Creates the required metadata tables in the Spark metastore using the defined schemas.
        Args:
            enable_schemas (bool): A flag indicating whether to use schemas in the table names.
        Returns:
            None
        """
        table_schema = "dbo" if enable_schemas else None

        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)
            LakehouseCatalog.create_metastore_table_from_schema(table_schema, tbl, schema)
            
    @classmethod
    def create_metastore_table_from_schema(cls, table_schema:str, table_name:str, schema:StructType) -> None:
        """
        Creates an empty table in the Spark metastore using the given schema.
        Args:
            table_name (str): The name of the metastore table to create.
            schema (pyspark.sql.types.StructType): The schema defining the table structure.
        Returns:
            None
        """
        df = cls.Context.createDataFrame(data=cls.Context.sparkContext.emptyRDD(),schema=schema)
        df.write.mode("OVERWRITE").saveAsTable(f"{cls.resolve_table_name(table_schema, table_name)}")

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
        cmds = []
        table_schema = "dbo" if enable_schemas else None

        current_tables = cls.__rename_metastore_tables(metadata_lakehouse, table_schema)

        for tbl in SyncConstants.get_metadata_tables():
            schema = getattr(FabricMetastoreSchema(), tbl)

            if tbl in current_tables:
                df = cls.Context.table(tbl)
                cmds.append(cls.__sync_schema(table_schema, tbl,  df.schema, schema))
            else:
                cls.create_metastore_table_from_schema(table_schema, tbl, schema)

        if cmds:
            SparkProcessor.process_command_list(cmds)
    
    @classmethod
    def __rename_metastore_tables(cls, metadata_lakehouse:str, table_schema:str) -> List[str]:
        """
        Renames the metadata tables in the specified lakehouse to remove the 'bq_' prefix.
        Args:
            metadata_lakehouse (str): The name of the lakehouse containing the metadata tables.
        Returns:
            List[str]: A list of the current metadata tables in the lakehouse after renaming.
        """
        current_tables = cls.get_catalog_tables(metadata_lakehouse)  

        renamed_tables = [
            f"ALTER TABLE {metadata_lakehouse}.{cls.resolve_table_name(table_schema, t)} RENAME TO " +
                f"{metadata_lakehouse}.{t.replace('bq_', '')}"                
                    for t in current_tables if "bq_" in t]

        if renamed_tables:   
            SparkProcessor.process_command_list(renamed_tables)
        
        return cls.get_catalog_tables(metadata_lakehouse)

    @classmethod
    def resolve_table_name(cls, table_schema:str, table_name:str) -> str:
        """
        Resolves the table name to use in SQL commands based on the schema and table name.
        Args:
            table_name (str): The name of the table to update.
            table_schema (str): The schema of the table to update.
        Returns:
            str: The resolved table name to use in SQL commands.
        """
        return table_name if not table_schema else f"{table_schema}.{table_name}"
                                                                 
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

        if target_diff or source_diff:
            cmds.append(
                ddl + "SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');")
        
        if source_diff:
            cmds.extend([ddl + f"DROP COLUMN {f.name};" for f in source_diff])
        
        if target_diff:
            cmds.extend([cls.__create_schema_field_sql(table_schema, table_name, f, target_schema) for f in target_diff])
        
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