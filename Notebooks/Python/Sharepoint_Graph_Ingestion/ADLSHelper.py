# Databricks notebook source
#%pip install azure-storage-file-datalake azure-identity


# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential, DefaultAzureCredential

class ADLSHelper:
    def __init__(self, storage_account_name, container_name, tenant_id, client_id, client_secret):
        if ".dfs.core.windows.net" not in storage_account_name:
            raise ValueError("Invalid storage url. Please use DFS-enabled endpoint.")
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.credential = self.get_spn_credentials()
        
    def get_spn_credentials(self):
        # Create the credential using service principal
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        return credential

    def upload_binary_to_adls(self,
        binary_data,
        file_path,
        file_name
    ):
        """
        Upload binary data to Azure Data Lake Storage Gen2 using service principal authentication
        
        Args:
            binary_data: The binary data to upload
            storage_account_name: Azure Storage account name
            tenant_id: Azure AD tenant ID
            client_id: Service principal client ID
            client_secret: Service principal client secret
            file_system_name: Name of the file system (container)
            file_path: Path where the file should be stored (end with '/')
            file_name: File name
        """
        try:
            
            
            # Create the DataLakeServiceClient
            service_client = DataLakeServiceClient(
                account_url=f"https://{self.storage_account_name}",
                credential=self.credential
            )
            
            # Get the file system client
            file_system_client = service_client.get_file_system_client(self.container_name)
            
            # Get the file client
            full_file_path = f"{file_path}{file_name}"
            file_client = file_system_client.get_file_client(full_file_path)
            
            # Upload the binary data
            file_client.upload_data(
                binary_data,
                overwrite=True
            )
            
            print(f"Successfully uploaded file to {full_file_path} in file system {self.container_name}")
            
        except Exception as e:
            print(f"Error uploading to ADLS: {str(e)}")
