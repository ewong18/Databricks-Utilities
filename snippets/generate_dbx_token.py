import requests

class DBXToken:
    """
    Documenation:
    https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/app-aad-token

    """
    def __init__(self,
                 spn_client_id,
                 spn_client_secret,
                 tenant_id,
                 ):
        self._spn_client_id = spn_client_id
        self._spn_client_secret = spn_client_secret
        self._tenant_id = tenant_id
        self.DBX_RESOURCE_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d%2F.default"
        self.access_token = self.getAccessToken()

    def getAccessToken(self):
        token_url = f"https://login.microsoftonline.com/{self._tenant_id}"
        params = {
            "grant_type":"client_credentials",
            "client_id": self._spn_client_id,
            "client_secret": self._spn_client_secret,
            "scope": self.DBX_RESOURCE_SCOPE
          }
        response = requests.post(token_url,data=params)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get token: {response.text}")
        else:
            return response.json().get("access_token")
    
    def getDBXToken(self, dbx_wks_url:str, purpose:str, lifetime:int):
        url = f"https://{dbx_wks_url}/api/2.0/token/create"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
          }
        params = {
            "comment": purpose,
            "lifetime_seconds": lifetime
          }

        response = requests.post(url, headers=headers, data=params)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get dbx token: {response.text}")
        else:
            return response.json().get("token")

