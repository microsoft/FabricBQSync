from FabricSync.BQ.Enum import (
    BigQueryObjectType, SyncLoadStrategy, SyncLoadType,
    CalendarInterval, BQDataType, BQPartitionType
)
from FabricSync.BQ.Model.Config import (
    ConfigBQTableDefault, ConfigBQTable, ConfigTableColumn,
    ConfigPartition, ConfigDataset
)
from FabricSync.BQ.Setup import Installer

class Configurator:
    def __init__(cls, installer:Installer):
        cls.__installer = installer

    def __write_config_to_onelake(cls, user_config:ConfigDataset) -> str:
        config_path = cls.__installer.Data["user_config_file_path"]
        cls.__installer.Onelake.write(config_path, user_config.model_dump_json(exclude_none=True, exclude_unset=True, indent=4))
        return config_path

    def Configure_GCP_Billing_Export(cls, billing_account_id:str = None, 
                                          use_standard_export:bool = False, 
                                          use_detailed_export:bool = False, 
                                          use_pricing_export:bool = False,
                                          focus_view_name:str = None):

        print("Creating Fabric Sync -> GCP Billing Export Configuration..")
        user_config = cls.__installer.UserConfig
        
        user_config.AutoDiscover.Tables.Enabled = True
        user_config.AutoDiscover.Tables.LoadAll = False

        user_config.AutoDiscover.Views.Enabled = True
        user_config.AutoDiscover.Views.LoadAll = False

        user_config.TableDefaults = ConfigBQTableDefault(
            ProjectID = cls.__installer.Data["gcp_project_id"],
            Dataset = cls.__installer.Data["gcp_dataset_id"],
            ObjectType = BigQueryObjectType.BASE_TABLE,
            Priority = 100,
            Enabled = True,
            AllowSchemaEvolution = True
        )

        if not user_config.Tables:
            user_config.Tables = []

        if any([use_standard_export, use_detailed_export, use_pricing_export]):
            if not billing_account_id:
                raise ValueError("billing_account_id is required.")

            gcp_billing_tables = []

            if use_pricing_export:
                gcp_billing_tables.append("cloud_pricing_export")
            
            if use_standard_export:
                gcp_billing_tables.append(f"gcp_billing_export_v1_{billing_account_id}")
            
            if use_detailed_export:
                gcp_billing_tables.append(f"gcp_billing_export_resource_v1_{billing_account_id}")

            for tbl in gcp_billing_tables:
                print(f"Adding {tbl} to User Configuration...")
                user_config.Tables.append(
                    ConfigBQTable(
                        TableName = tbl,
                        LoadStrategy = SyncLoadStrategy.TIME_INGESTION,
                        LoadType = SyncLoadType.APPEND,
                        Watermark = ConfigTableColumn(Column="export_time"),
                        BQPartition = ConfigPartition(
                            Enabled = True,
                            PartitionType = BQPartitionType.TIME,
                            PartitionColumn = "_PARTITIONTIME",
                            Granularity = CalendarInterval.DAY,
                            PartitionDataType = BQDataType.TIMESTAMP
                        )
                    )
                )

        if focus_view_name:
            print(f"Adding {focus_view_name} VIEW to User Configuration...")
            user_config.Tables.append(
                ConfigBQTable(
                    TableName = focus_view_name,
                    ObjectType = BigQueryObjectType.VIEW,
                    LoadStrategy = SyncLoadStrategy.WATERMARK,
                    LoadType = SyncLoadType.APPEND,
                    Watermark = ConfigTableColumn(Column="x_ExportTime")
                    )
                )
        
        path = cls.__write_config_to_onelake(user_config)
        print(f"GCP Billing Export Configuration created at: {path}")