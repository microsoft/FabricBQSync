from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import json
import base64 as b64

from google.cloud import bigquery
from google.oauth2.service_account import Credentials as gcpCredentials # type: ignore

from FabricSync.BQ.Core import ContextAwareBase

class BigQueryClient(ContextAwareBase):
    def __init__(self, project_id:str, credentials:str) -> None:
        """
        Initializes a new instance of the BigQueryClient class.
        Args:
            project_id (str): The project ID.
            credentials (str): The credentials.
        """
        key = json.loads(b64.b64decode(credentials))
        bq_credentials = gcpCredentials.from_service_account_info(key)

        self.project_id = project_id
        self.client = bigquery.Client(project=project_id, credentials=bq_credentials)

    def read_to_dataframe(self, sql:str, schema:StructType = None) -> DataFrame:
        """
        Reads the data from the given SQL query into a DataFrame.
        Args:
            sql (str): The SQL query.
            schema (StructType): The schema.
        Returns:
            DataFrame: The DataFrame.
        """
        query = self.client.query(sql)
        bq = query.to_dataframe()
        
        if not bq.empty:
            return self.Context.createDataFrame(bq, schema=schema)
        else:
            return None