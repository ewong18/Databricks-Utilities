import requests

class SharePointDownloadURLClient():
    def __init__(self, job_config):
        self._tenant_id = job_config['tenantID']
        self._key_vault_scope = job_config['keyVaultName']
        self._client_id_kv = job_config['intermediateStoreConnID']
        self._client_secret_kv = job_config['intermediateStoreSecretName']
        self._sourceURL = job_config['sourceURL'] #Sharepoint site drive URL
        self._jobPath = job_config['jobPath'] #Folder path within the drive
        self._sourceFileName = (job_config['sourceFileName']
                                    .replace("^%", "")
                                    .replace("%^", ""))

        self.client_id = dbutils.secrets.get(scope=self._key_vault_scope, key=self._client_id_kv)
        self.client_secret = dbutils.secrets.get(scope=self._key_vault_scope, key=self._client_secret_kv)
        self.token_url = f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/v2.0/token"

        self.access_token = self.get_graph_token()



    def get_graph_token(self):
        body = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        response = requests.post(self.token_url, data=body)
        if response.status_code != 200:
            raise Exception(f"Failed to get token: {response.text}")
        else:
            return response.json().get("access_token")
    
    def get_site_id(self):
        url_parts = self._sourceURL.split("/")
        root = url_parts[2] #bp365.sharepoint.com
        site = "/".join(url_parts[3:5]) #sites/{site_name}
        relative_path = f"{root}:/{site}"

        site_response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{relative_path}", 
            headers={"Authorization": f"Bearer {self.access_token}"})
        if site_response.status_code != 200:
            raise Exception(f"Failed to get site: {site_response.text}")
        else:
            site_id = site_response.json().get("id").split(",")[1]
            return site_id
        
    def get_drive_id(self, site_id):
        drive_response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", 
                        headers={"Authorization": f"Bearer {self.access_token}"})

        if drive_response.status_code != 200:
            raise Exception(f"Failed to get drive: {drive_response.text}")
        else:
            filtered_drive = [drive for drive in drive_response.json()['value'] 
                            if drive['webUrl'] == self._sourceURL]
            if len(filtered_drive) == 1:
                drive_id = filtered_drive[0]['id']
            return drive_id
    
    def get_download_urls(self):
        site_id = self.get_site_id()
        drive_id = self.get_drive_id(site_id)
        download_response = requests.get(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{self._jobPath}:/children", 
                                         headers={"Authorization": f"Bearer {self.access_token}"})
        if download_response.status_code != 200:
            raise Exception(f"Failed to get download urls: {download_response.text}")
        filtered_urls = [item['@microsoft.graph.downloadUrl'] for item in download_response.json()['value'] 
                            if self._sourceFileName in item['name']]
        return filtered_urls

