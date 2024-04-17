import json

class ConfigDataset:
    def __init__(self, json_config):
        self.ProjectID = self.get_json_conf_val(json_config, "project_id", None)
        self.Dataset = self.get_json_conf_val(json_config, "dataset", None)
        self.LoadAllTables = self.get_json_conf_val(json_config, "load_all_tables", True)
        self.Autodetect = self.get_json_conf_val(json_config, "autodetect", True)
        self.MasterReset = self.get_json_conf_val(json_config, "master_reset", False)
        self.MetadataLakehouse = self.get_json_conf_val(json_config, "metadata_lakehouse", None)
        self.TargetLakehouse = self.get_json_conf_val(json_config, "target_lakehouse", None)
        self.GCPCredentialPath = self.get_json_conf_val(json_config, "gcp_credential_path", None)
        self.Tables = []

        if "async" in json_config:
            self.Async = ConfigAsync(
                self.get_json_conf_val(json_config["async"], "enabled", False),
                self.get_json_conf_val(json_config["async"], "parallelism", None),
                self.get_json_conf_val(json_config["async"], "notebook_timeout", None),
                self.get_json_conf_val(json_config["async"], "cell_timeout", None)
            )
        else:
            self.Async = ConfigAsync()

        if "tables" in json_config:
            for t in json_config["tables"]:
                self.Tables.append(ConfigBQTable(t))
    
    def get_delimited_tables_list(self):
            return ''.join([str(x.TableName) for x in self.Tables])

    def get_table_name_list(self):
        return [str(x.TableName) for x in self.Tables]

    def get_bq_table_fullname(self, tbl_name):
        return f"{self.ProjectID}.{self.Dataset}.{tbl_name}"

    def get_lakehouse_tablename(self, lakehouse, tbl_name):
        return f"{lakehouse}.{tbl_name}"

    def flatten_3part_tablename(self, tbl_name):
        clean_project_id = self.ProjectID.replace("-", "_")
        return f"{clean_project_id}_{self.Dataset}_{tbl_name}"
    
    def get_json_conf_val(self, json, config_key, default_val = None):
        if config_key in json:
            return json[config_key]
        else:
            return default_val

class ConfigAsync:
    def __init__(self, enabled = False, parallelism = 5, notebook_timeout = 1800, cell_timeout = 300):
        self.Enabled = enabled
        self.Parallelism = parallelism
        self.NotebookTimeout = notebook_timeout
        self.CellTimeout = cell_timeout

class ConfigTableColumn:
    def __init__(self, col = ""):
        self.Column = col

class ConfigLakehouseTarget:
    def __init__(self, lakehouse = "", table = ""):
        self.Lakehouse = lakehouse
        self.Table = table

class ConfigPartition:
    def __init__(self, enabled = False, partition_type = "", col = ConfigTableColumn(), grain = ""):
        self.Enabled = enabled
        self.PartitionType = partition_type
        self.PartitionColumn = col
        self.Granularity = grain

class ConfigBQTable:
    def __str__(self):
        return str(self.TableName)

    def __init__(self, json_config):
        self.TableName = self.get_json_conf_val(json_config, "table_name", "")
        self.Priority = self.get_json_conf_val(json_config, "priority", 100)
        self.SourceQuery = self.get_json_conf_val(json_config, "source_query", "")
        self.LoadStrategy = self.get_json_conf_val(json_config, "load_strategy" , SyncConstants.FULL)
        self.LoadType = self.get_json_conf_val(json_config, "load_type", SyncConstants.OVERWRITE)
        self.Interval =  self.get_json_conf_val(json_config, "interval", SyncConstants.AUTO)
        self.Enabled =  self.get_json_conf_val(json_config, "enabled", True)

        if "lakehouse_target" in json_config:
            self.LakehouseTarget = ConfigLakehouseTarget( \
                self.get_json_conf_val(json_config["lakehouse_target"], "lakehouse", ""), \
                self.get_json_conf_val(json_config["lakehouse_target"], "table_name", ""))
        else:
            self.LakehouseTarget = ConfigLakehouseTarget()
        
        if "watermark" in json_config:
            self.Watermark = ConfigTableColumn( \
                self.get_json_conf_val(json_config["watermark"], "column", ""))
        else:
            self.Watermark = ConfigTableColumn()

        if "partitioned" in json_config:
            self.Partitioned = ConfigPartition( \
                self.get_json_conf_val(json_config["partitioned"], "enabled", False), \
                self.get_json_conf_val(json_config["partitioned"], "type", ""), \
                self.get_json_conf_val(json_config["partitioned"], "column", ""), \
                self.get_json_conf_val(json_config["partitioned"], "partition_grain", ""))
        else:
            self.Partitioned = ConfigPartition()
        
        self.Keys = []

        if "keys" in json_config:
            for c in json_config["keys"]:
                self.Keys.append(ConfigTableColumn( \
                    self.get_json_conf_val(c, "column", "")))
        
    def get_json_conf_val(self, json, config_key, default_val = None):
        if config_key in json:
            return json[config_key]
        else:
            return default_val