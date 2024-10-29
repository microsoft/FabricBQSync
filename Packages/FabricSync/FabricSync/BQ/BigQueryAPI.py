import time
import json
import jwt
import requests

class BigQueryAccessPolicy:
    """
    BigQuery Access Policy Mode
    """
    def __init__(
            self, 
            json_row:str):
        """
        Initializes class from JSON object
        """
        self.ProjectId = json_row["projectId"]
        self.DatasetId = json_row["datasetId"]
        self.TableId = json_row["tableId"]
        self.PolicyId = json_row["policyId"]
        self.PolicyIAM:list[BigQueryAccessPolicyIAM] = []

    @property
    def ResourceId(self) -> str:
        """
        Returns the GCP formatted Resource Id
        """
        return f"projects/{self.ProjectId}/datasets/{self.DatasetId}/tables/{self.TableId}/rowAccessPolicies/{self.PolicyId}"
    
    def __str__(self):
        return self.ResourceId

class BigQueryAccessPolicyIAM:
    """
    BiqQuery Access Policy IAM Model
    """
    def __init__(
            self, 
            json_row:str):
        """
        Initializes class from JSON object
        """
        self.Role = json_row["role"]
        self.Members = [m for m in json_row["members"]]