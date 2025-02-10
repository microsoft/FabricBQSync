import requests
from requests import RequestException
from requests.models import Response
import time
import logging
from logging import Logger

from FabricSync.BQ.Constants import SyncConstants

class RestAPIProxy:
    __logger:Logger = None

    def __init__(self, base_url, headers=None) -> None:
        """
        Rest API Proxy
        Args:
            base_url (str): Base URL
            headers (Dict): Headers
        """
        self.base_url = base_url
        self.headers = headers

    @property
    def Logger(self) -> Logger:
        """
        Logger
        Returns:
            Logger: Logger
        """
        if not self.__logger:
            self.__logger = logging.getLogger(SyncConstants.FABRIC_LOG_NAME)

        return self.__logger
    
    def get(self, endpoint, params=None, headers=None) -> Response:
        """
        Gets a resource from the API
        Args:
            endpoint (str): API endpoint
            params (Dict): Parameters
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        self.Logger.debug(f"API GET REQUEST: {endpoint}")
        response = requests.get(f"{self.base_url}/{endpoint}", params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None, files=None, headers=None, 
                retry_attempts:int=5, lro_update_hook:callable = None) -> Response:
        """
        Posts a resource to the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            files (Dict): Files
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        self.Logger.debug(f"API POST REQUEST: {endpoint}")
        response = requests.post(f"{self.base_url}/{endpoint}", data=data, json=json, files=files, headers=headers)
        
        response = self._handle_response(response)

        #Support for LRO operations
        if response.status_code == 202:
            result = {}
            timeout = True

            for _ in range(retry_attempts):
                self.Logger.debug(f"API POST REQUEST: LRO - {endpoint}")
                retry_after = int(response.headers["Retry-After"])
                location = response.headers["Location"]

                if lro_update_hook:
                    lro_update_hook("Processing...")

                time.sleep(retry_after)

                lro_response = requests.get(url=location, headers=headers)
                result = lro_response.json()

                if result["status"] in ["NotStarted", "Running", "Undefined"]:
                    retry_after = int(lro_response.headers["Retry-After"])

                    if "percentComplete" in result:
                        if lro_update_hook:
                            lro_update_hook(f"Processing - {result['percentComplete']} complete...")

                    continue
                elif result["status"] == "Succeeded":
                    timeout = False

                    if lro_update_hook:
                        lro_update_hook(f"Processing complete.")

                    if "location" in lro_response.headers:
                        location = lro_response.headers["Location"]
                        lro_response = requests.get(url=location, headers=headers)
                        result = lro_response.json()

                    break
                else:
                    break

            if timeout:
                raise Exception("Fabric LRO Operaiton Timeout: Max retry attempts exceeded")
            
            return result
        
        return response
            

    def put(self, endpoint, data=None, json=None, headers=None) -> Response:
        """
        Puts a resource in the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        self.Logger.debug(f"API PUT REQUEST: {endpoint}")
        response = requests.put(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)
    
    def patch(self, endpoint, data=None, json=None, headers=None) -> Response:
        """
        Patches a resource in the API
        Args:
            endpoint (str): API endpoint
            data (Dict): Data
            json (Dict): JSON
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers

        self.Logger.debug(f"API PATCH REQUEST: {endpoint}")
        response = requests.patch(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint, headers=None) -> Response:
        """
        Deletes a resource from the API
        Args:
            endpoint (str): API endpoint
            headers (Dict): Headers
        Returns:
            Response: Response from the API
        """
        if not headers:
            headers = self.headers
        
        self.Logger.debug(f"API DELETE REQUEST: {endpoint}")
        response = requests.delete(f"{self.base_url}/{endpoint}", headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response) -> Response:
        """
        Handles the response from the API
        Args:
            response (Response): Response from the API
        Returns:
            Response: Response from the API
        """
        try:
            self.Logger.debug(f"API {response.request.method.upper()} RESPONSE: {response.status_code} - {response.url}")
            if response.status_code >= 200 and response.status_code < 400:
                return response
            else:
                response.raise_for_status()
        except RequestException:
            raise RequestException(response.text)