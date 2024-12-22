import requests

class Util():
    def get_config_value(json_data, json_path, default=None, raise_error=False):
        paths = json_path.split('.')
        level = json_data

        for p in paths:
            if p in level:
                level = level[p]
                val = level
            else:
                val = None
                break
        
        if not val and raise_error:
            raise ValueError(f"Missing Key: {json_path}")
        elif not val:
            return default
        
        return val

    def assign_enum_val(enum_class, value):
        try:
            return enum_class(value)
        except ValueError:
            return None
    
    def assign_enum_name(enum_class, name):
        try:
            return enum_class[name]
        except ValueError:
            return None

class RestAPIProxy:
    def __init__(self, base_url):
        self.base_url = base_url

    def get(self, endpoint, params=None, headers=None):
        response = requests.get(f"{self.base_url}/{endpoint}", params=params, headers=headers)
        return self._handle_response(response)

    def post(self, endpoint, data=None, json=None, headers=None):
        response = requests.post(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)

    def put(self, endpoint, data=None, json=None, headers=None):
        response = requests.put(f"{self.base_url}/{endpoint}", data=data, json=json, headers=headers)
        return self._handle_response(response)

    def delete(self, endpoint, headers=None):
        response = requests.delete(f"{self.base_url}/{endpoint}", headers=headers)
        return self._handle_response(response)

    def _handle_response(self, response):
        if response.status_code == 200:
            return response
        else:
            response.raise_for_status()