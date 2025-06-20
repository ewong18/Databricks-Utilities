# Databricks notebook source
from datetime import datetime
import json
import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, Row
import requests
from tableUtils import tableUtils



# COMMAND ----------

# MAGIC %run ../../../../Utilities/Python/common/utils

# COMMAND ----------

class iAuditorClient:
    def __init__(self, 
                 job_metadata_df: Row,
                 catalog_name:str):
        """
        Client for ingestion data from iAuditor.

        Attributes:
            _base_url (str): base url for API (e.g. https://api.safetyculture.io)
            _source_url_path (str): path for datafeed endpoint (e.g. /feed/schedules)
            _url_params (str): ';' separated list of additional params for API request (e.g. key1=val1;key2=val2 )
            _token_kv (str): token name in key vault
            _key_vault (str): keyvault name
            token (str): token for API
            _filter_date_key (str): key for modified_after filter (e.g. modified_after)
            _last_loaded_date (str): latest value for modified_after (e.g. 2025-06-18T19:08:21.351Z)
            _catalog_name (str): catalog name for target table
            _targetdbname (str): database name for target table
            _targettblname (str): table name for target table
            _loadtype (str): load type for target table
            _inscopeColumnList (list): list of columns to be ingested
            _column_change_schema (str): ';'-separated list of column and data types for casting
            _rename_columns(str): ';'-separated list of old and new column names
            _source_timestamp_format (str): timestamp format for source data
            _additional_cols_in_target (dict): additional columns to be ingested
            source_pk_cols (list): list of primary key columns for target table

        Methods:
            get_response(pagination_url:str): get response from API
            create_dataframe(): create dataframe from schema and API response
            ingest_data(): get data and save into target table
            
        """
        
        #api request details
        self._base_url = job_metadata_df["sourceURL"] #https://api.safetyculture.io
        self._source_url_path = job_metadata_df["sourceTblName"]
        self._url_params = job_metadata_df["filterQuery"]
        self._token_kv = job_metadata_df["srcAuthCredentialKVNameList"]
        self._key_vault = job_metadata_df["keyVaultName"]
        self.token = dbutils.secrets.get(key=self._token_kv, scope = self._key_vault)
        self._filter_date_key = job_metadata_df["sourceChangeKeyCols"] #modified_after
        self._last_loaded_date = job_metadata_df["sourceChgKeyLatestValues"] 
        #load details
        self._catalog_name = catalog_name
        self._targetdbname = job_metadata_df["targetDBName"]
        self._targettblname = job_metadata_df["targetTblName"]
        self._loadtype = job_metadata_df["fkLoadType"]
        #transformation details
        self._inscopeColumnList = job_metadata_df["inscopeColumnList"].split(",")
        self._column_change_schema = job_metadata_df["transformedColumnsInTarget"].split("|")[0]
        self._rename_columns = job_metadata_df["transformedColumnsInTarget"].split("|")[1]
        self._source_timestamp_format = job_metadata_df["sourceTimestampFormat"]
        self._additional_cols_in_target = job_metadata_df["additionalColumnsInTarget"]
        self.source_pk_cols = (job_metadata_df["sourcePKCols"].split(",") 
                                    if job_metadata_df["sourcePKCols"] 
                                    else None)
        
        #logger
        self.logger = logging.getLogger("iAuditorClient")
        self.logger.setLevel(logging.INFO)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.stream_handler.setFormatter(self.formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(self.stream_handler)
            
    def create_pagination_url(self):
        """
        Uses optional params and optional filter date to create pagination url

        Returns:
            str: endpoint url for datafeed and subsequent pages
        """
        pagination_url = f"{self._source_url_path}"
        #add filter params
        if self._url_params:
            pagination_url = pagination_url + "?" + "&".join(self._url_params.split(";"))
        #add modified_after filter if present
        if self._filter_date_key:
            if self._url_params: #if appending to existing params
                pagination_url+="&"
            else:
                pagination_url+="?" #if first param
            pagination_url += f"{self._filter_date_key}={self._last_loaded_date}"
        return pagination_url

    def get_response(self, pagination_url:str) -> requests.models.Response:
        """
        Get response from API.

        Args:
            pagination_url (str): endpoint url for datafeed and subsequent pages

        Returns:
            requests.models.Response: API response type
        """
        header = {"Authorization": f"Bearer {self.token}"}
        url = f"{self._base_url}{pagination_url}"
        self.logger.info(f"Sending request to {url}")
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}")
        return response
    
    def create_dataframe(self) -> DataFrame:
        data = []
        schema = StructType([StructField(name, StringType(), True) 
                             for name in self._inscopeColumnList])
        paginationURL = self.create_pagination_url()
        while paginationURL:
            response = self.get_response(paginationURL)
            json_payload = json.loads(response.text)
            data.extend(json_payload["data"])
            paginationURL = json_payload["metadata"]["next_page"]     
        df = spark.createDataFrame(data, schema)
        return df
    
    def ingest_data(self) -> dict:
        start_time = datetime.now()
        status = "R"        
        try:
            df = self.create_dataframe()
            recProcessed = df.count()
            if recProcessed > 0:
                if self._column_change_schema:
                    df = Utils.change_df_column_schema(df,
                                                    self._column_change_schema,
                                                    self._source_timestamp_format)
                if self._rename_columns:
                    df = Utils.rename_columns(df, self._rename_columns)
                if self._additional_cols_in_target:
                    df = Utils.add_columns(df, self._additional_cols_in_target)
                Utils.write_data(df=df,
                                 target_catalog=self._catalog_name,
                                 target_database=self._targetdbname,
                                 target_tbl_name=self._targettblname,
                                 load_type=self._loadtype,
                                 upsert_keys=self.source_pk_cols)
                processing_details = f"API Load Successful: {self._targettblname}"
                self.logger.info(processing_details)

            else:
                processing_details = "No new records to ingest."
                self.logger.info(processing_details)
            
            #run details
            status = "S"
            end_time = datetime.now()
            recIngested = recProcessed

            run_log_dict ={
                "startTime": start_time,
                "endTime": end_time,
                "status": status,
                "processingDetails": processing_details,
                "errorMsg": "N/A",
                "recInSource" : None, #cannot accurately determine from api
                "recIngested": recIngested,
                "recProcessed" : recProcessed,
                "recFailed": 0                   
            }
        except Exception as e:
            self.logger.error(f"Error while ingesting data for: {self._targettblname}: {str(e)}")
            #run details
            status = "F"
            end_time = datetime.now()
            processing_details = f"Error while ingesting data for: {self._targettblname}"
            error_msg = str(e)

            run_log_dict ={
                "startTime": start_time,
                "endTime": end_time,
                "status": status,
                "processingDetails": processing_details,
                "errorMsg": error_msg,
                "recInSource" : None, #cannot accurately determine from api
                "recIngested": 0,
                "recProcessed" : 0,
                "recFailed": None                   
            }
        return run_log_dict
    
