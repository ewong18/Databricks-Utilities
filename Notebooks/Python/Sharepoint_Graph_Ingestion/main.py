# Databricks notebook source
# MAGIC %md
# MAGIC # Sharepoint File Ingestion v1
# MAGIC Contains 2 parts:
# MAGIC 1. [PYTHON] Get files from Sharepoint
# MAGIC     a. Get download URLs for desired files using Graph API
# MAGIC     b. Upload content to ADLSg2 storage account
# MAGIC        - Requires DFS endpoint and Service Principal
# MAGIC 2. [Scala] Call File Ingestion JAR
# MAGIC
# MAGIC For future versions, it would be good to convert this code to Scala so it's consistent.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Files to Azure Storage

# COMMAND ----------

import json
from typing import Dict, List

# COMMAND ----------

# MAGIC %run ./ADLSHelper

# COMMAND ----------

# MAGIC %run ./SharePointDownloadURLClient

# COMMAND ----------

# DBTITLE 1,Get Download URL and Target Location
#Create Unique Download Config for each file
def create_download_config(job_config: List[Dict]) -> List[Dict]:
    """
    Create Download Config dictionary for each unique file (ignores duplicates)
    Args: 
        job_config: List of dictionaries containing audit db metadata from ADF
    
    Returns:
        List of dictionaries containing:
            targetFilePath: target path to file in ADLS
            targetFileName: target name of file in ADLS
            downloadUrl: download url for file
    """
    config = []
    for job in job_config:
        sp_client = SharePointDownloadURLClient(job)
        urls = sp_client.get_download_urls()
        for x in set(urls):
            if x.split("&")[0] not in [d.get("downloadUrl").split("&")[0] 
                                    for d in config]:
                config.append({
                    "targetFilePath": job['sourceFilePath'],                 
                    "targetFileName": job['sourceFileName'],
                    "downloadUrl": x
                    })
                print(f"URL for {job['sourceFileName']} added.") 
            else:
                #Don't include duplicate files
                print(f"Skipped: {job['sourceFileName']} already exists")
    return config

def upload_files_to_blob(download_config: List[Dict],
                         storageAccount: str,
                         container:str,
                         tenant_id:str,
                         client_id:str,
                         client_secret:str) -> None:
    adls_upload_client = ADLSHelper(storageAccount, 
                                container, 
                                tenant_id, 
                                client_id, 
                                client_secret)
    for config in download_config:
        data_response = requests.get(config['downloadUrl'])
        if data_response.status_code != 200:
            raise Exception(f"Error downloading file {config['downloadUrl']}")
        data = data_response.content
        adls_upload_client.upload_binary_to_adls(data, 
                                                config['targetFilePath'], 
                                                config['targetFileName'])
    return 


# COMMAND ----------

# DBTITLE 1,Configs
job_config = json.loads(dbutils.widgets.get("jobConfig"))
job_group = dbutils.widgets.get("jobGroup")
job_order = dbutils.widgets.get("jobOrder")
job_num = dbutils.widgets.get("jobNum")

if job_num and int(job_num) > 0:
    job_config = [item for item in job_config if item['jobNum'] == int(job_num)]

keyVaultName = job_config[0]['keyVaultName']
storageAccount = job_config[0]['sourceStorageAccount']
container = job_config[0]['sourceContainer']
client_id = dbutils.secrets.get(scope = keyVaultName, 
                                key = job_config[0]['sourceConnID'])
client_secret = dbutils.secrets.get(scope = keyVaultName,
                                    key = job_config[0]['sourceSecretName']),
tenant_id = job_config[0]['tenantID']

# COMMAND ----------

download_config = create_download_config(job_config)
upload_files_to_blob(download_config, 
                      storageAccount, 
                      container, 
                      tenant_id, 
                      client_id, 
                      client_secret)
