from pyspark.sql.functions import *
from delta.tables import *
from datetime import datetime, timezone
import json
import base64
from pathlib import Path
import os

class ConfigBase():
    def __init__(self, config_path, gcp_credential):
        if config_path is None:
            raise ValueError("Missing Path to JSON User Config")
        
        if gcp_credential is None:
            raise ValueError("Missing GCP Credentials")

        self.ConfigPath = config_path
        self.UserConfig = None
        self.GCPCredential = None

        self.UserConfig = self.ensure_user_config()
        self.GCPCredential = self.load_gcp_credential(gcp_credential)
    
    def ensure_user_config(self):
        if self.UserConfig is None and self.ConfigPath is not None:
            config = self.load_user_config(self.ConfigPath)

            cfg = ConfigDataset(config)

            self.validate_user_config(cfg)
            
            return cfg
        else:
            return self.UserConfig
    
    def load_user_config(self, config_path):
        config_df = spark.read.option("multiline","true").json(config_path)
        config_df.createOrReplaceTempView("user_config_json")
        config_df.cache()
        return json.loads(config_df.toJSON().first())

    def validate_user_config(self, cfg):
        if cfg is None:
            raise RuntimeError("Invalid User Config")    
        return True

    def load_gcp_credential(self, credential):
        cred = None

        if self.is_base64(credential):
            cred = credential
        else:
            if os.path.exists(credential):
                file_contents = self.read_credential_file(credential)
                cred = self.convert_to_base64string(file_contents)
            else:
                raise ValueError("Invalid GCP Credential path supplied.")

        return cred

    def read_credential_file(self, credential_path):
        txt = Path(credential_path).read_text()
        txt = txt.replace("\n", "").replace("\r", "")

        return txt

    def convert_to_base64string(self, credential_val):
        credential_val_bytes = credential_val.encode("ascii") 
        
        base64_bytes = base64.b64encode(credential_val_bytes) 
        base64_string = base64_bytes.decode("ascii") 

        return base64_string

    def is_base64(self, val):
        try:
                if isinstance(val, str):
                        sb_bytes = bytes(val, 'ascii')
                elif isinstance(val, bytes):
                        sb_bytes = val
                else:
                        raise ValueError("Argument must be string or bytes")
                return base64.b64encode(base64.b64decode(sb_bytes)) == sb_bytes
        except Exception:
                return False

    def read_bq_to_dataframe(self, query, cache_results=False):
        df = spark.read \
            .format("bigquery") \
            .option("parentProject", self.UserConfig.ProjectID) \
            .option("credentials", self.GCPCredential) \
            .option("viewsEnabled", "true") \
            .option("materializationDataset", self.UserConfig.Dataset) \
            .load(query)
        
        if cache_results:
            df.cache()
        
        return df

    def write_lakehouse_table(self, df, lakehouse, tbl_nm, mode=SyncConstants.OVERWRITE):
        dest_table = self.UserConfig.get_lakehouse_tablename(lakehouse, tbl_nm)

        df.write \
            .mode(mode) \
            .saveAsTable(dest_table)
    
    def create_infosys_proxy_view(self, trgt):
        clean_nm = trgt.replace(".", "_")
        tbl = self.UserConfig.flatten_3part_tablename(clean_nm)
        lakehouse_tbl = self.UserConfig.get_lakehouse_tablename(self.UserConfig.MetadataLakehouse, tbl)

        sql = f"""
        CREATE OR REPLACE TEMPORARY VIEW BQ_{clean_nm}
        AS
        SELECT *
        FROM {lakehouse_tbl}
        """
        spark.sql(sql)

    def create_userconfig_tables_proxy_view(self):
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_tables
            AS
            SELECT
                project_id, dataset, tbl.table_name,
                tbl.enabled,tbl.load_priority,tbl.source_query,
                tbl.load_strategy,tbl.load_type,tbl.interval,
                tbl.watermark.column as watermark_column,
                tbl.partitioned.enabled as partition_enabled,
                tbl.partitioned.type as partition_type,
                tbl.partitioned.column as partition_column,
                tbl.partitioned.partition_grain,
                tbl.lakehouse_target.lakehouse,
                tbl.lakehouse_target.table_name AS lakehouse_target_table,
                tbl.keys
            FROM (SELECT project_id, dataset, EXPLODE(tables) AS tbl FROM user_config_json)
        """
        spark.sql (sql)

    def create_userconfig_tables_cols_proxy_view(self):
        sql = """
            CREATE OR REPLACE TEMPORARY VIEW user_config_table_keys
            AS
            SELECT
                project_id, dataset, table_name, pkeys.column
            FROM (
                SELECT
                    project_id, dataset, tbl.table_name, EXPLODE(tbl.keys) AS pkeys
                FROM (SELECT project_id, dataset, EXPLODE(tables) AS tbl FROM user_config_json)
            )
        """
        spark.sql(sql)

    def create_proxy_views(self):
        self.create_userconfig_tables_proxy_view()
        self.create_userconfig_tables_cols_proxy_view()
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLES)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_PARTITIONS)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_COLUMNS)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_TABLE_CONSTRAINTS)
        self.create_infosys_proxy_view(SyncConstants.INFORMATION_SCHEMA_KEY_COLUMN_USAGE)