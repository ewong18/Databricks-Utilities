# Databricks notebook source
import msal
import pyodbc
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import struct


_AUDIT_DB_FETCH_JOB_METADATA_QUERY = """
        SELECT 
        {control_tbl_fields} 
        FROM audit.tblJobQueue jq
        LEFT JOIN audit.tblJobGroup jg
        ON jq.jobGroup = jg.pkTblJobGroup
        LEFT JOIN audit.tblJobQueueExtn jqe
        ON jq.pkTblJobQueue = jqe.fkJobQueue
        WHERE jobGroup = {job_group}
        AND jobOrder = {job_order}
    """

_RUN_LOG_COLUMNS = [
    "runID",
    "fkJobQueue",
    "jobGroup",
    "jobNum",
    "jobOrder",
    "jobStepNum",
    "startTime",
    "endTime",
    "status",
    "processingDetails",
    "errorMsg",
    "recInSource",
    "recIngested",
    "recProcessed",
    "recFailed"
    ]

class AuditDBClient:
    """
    A client for interacting with an audit database.

    Attributes:
        _service_principal_id (str): The ID of the service principal.
        _service_principal_pwd (str): The password of the service principal.
        _token_authority_url (str): The URL of the token authority.
        _resource_app_id_uri (str): The URI of the resource app.
        connection_properties (dict): Connection properties for the audit database.
        _audit_db_jdbc_url (str): The JDBC URL of the audit database.
    """
    def __init__(self, 
                 service_principal_id: str, 
                 service_principal_pwd: str, 
                 token_authority_url: str, 
                 resource_app_id_uri: str, 
                 audit_db_server: str,
                 audit_db_database:str, 
                 audit_db_port:str,
                 audit_db_driver:str):
        self._service_principal_id = service_principal_id
        self._service_principal_pwd = service_principal_pwd
        self._token_authority_url = token_authority_url
        self._resource_app_id_uri = resource_app_id_uri
        self.connection_properties = self._get_connection()
        self._audit_db_server = audit_db_server
        self._audit_db_database = audit_db_database
        self._audit_db_port = audit_db_port
        self._audit_db_driver = audit_db_driver
        self.audit_db_jdbc_url = f"jdbc:sqlserver://{self._audit_db_server}:{self._audit_db_port};database={self._audit_db_database}"

        self.logger = logging.getLogger("AuditDBClient")
        self.logger.setLevel(logging.INFO)
        self.stream_handler = logging.StreamHandler()
        self.stream_handler.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.stream_handler.setFormatter(self.formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(self.stream_handler)

    def _get_connection(self) -> dict:
        """
        Retrieves connection properties for accessing the audit database.

        Returns:
            dict: Connection properties including access token and host name.
        """
        context = msal.ConfidentialClientApplication(
            self._service_principal_id, self._service_principal_pwd, self._token_authority_url
        )
        token = context.acquire_token_for_client([self._resource_app_id_uri])
        connection_properties = {
            "accessToken": token.get("access_token"),
            "hostNameInCertificate": "*.database.windows.net",
            "encrypt": "true",
        }
        return connection_properties


    def fetch_ingestion_job_metadata(self, 
                                     control_tbl_fields: str, 
                                     job_group: int, 
                                     job_order: int, 
                                     job_num: int,
                                     spark_session: SparkSession) -> DataFrame:
        """
        Fetches ingestion job metadata from the audit database.

        Args:
            control_tbl_fields: The list of all columns to be selected.
            job_group (int): The job group identifier
            job_order (int): The job order to ingest.
            job_num(int): The job num to ingest (0 if all jobs in job order)

        Returns:
            DataFrame: DataFrame containing the fetched metadata.

        """
        query = _AUDIT_DB_FETCH_JOB_METADATA_QUERY.format(control_tbl_fields=control_tbl_fields, job_group=job_group, job_order=job_order)
        if job_num != 0:
            query += " AND jobNum = {job_num}".format(job_num=job_num)
        return spark_session.read.jdbc(
            url=self.audit_db_jdbc_url,
            table=f"({query}) as tab",
            properties=self.connection_properties,
        )
    
    def insert_run_log(self, full_run_log_df: DataFrame) -> None:
        """
        Inserts run log data into the audit database.
        Args:
            full_run_log_df (DataFrame): DataFrame containing the run log data.
        Returns:
            None
        """

        full_run_log_df = full_run_log_df.select(_RUN_LOG_COLUMNS)
        full_run_log_df = (full_run_log_df.withColumn("fkJobQueue", col("fkJobQueue").cast("integer"))
                   .withColumn("jobGroup", col("jobGroup").cast("integer"))
                   .withColumn("jobNum", col("jobNum").cast("integer"))
                   .withColumn("jobOrder", col("jobOrder").cast("integer"))
                   .withColumn("jobStepNum", col("jobStepNum").cast("integer"))
                   .withColumn("startTime", col("startTime").cast("timestamp"))
                   .withColumn("endTime", col("endTime").cast("timestamp"))
                   .withColumn("status", col("status").cast("string"))
                   .withColumn("processingDetails", col("processingDetails").cast("string"))
                   .withColumn("errorMsg", col("errorMsg").cast("string"))
                   .withColumn("recInSource", col("recInSource").cast("long"))
                   .withColumn("recIngested", col("recIngested").cast("long"))
                   .withColumn("recProcessed", col("recProcessed").cast("long"))
                   .withColumn("recFailed", col("recFailed").cast("long"))) 
                                                      
        full_run_log_df.write.mode("append").jdbc(
                url=self.audit_db_jdbc_url, 
                table="audit.tblrunlog", 
                properties=self.connection_properties
        
        )
        self.logger.info("Run log details inserted.")

    def get_pyodbc_connection(self) -> pyodbc.Connection:
        token = self.connection_properties['accessToken']
        self.logger.info('Access token generated. Encoding the token now....')
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        tokenb = bytes(token, "UTF-8")
        exptoken = b''
        for i in tokenb:
            exptoken += bytes({i})
            exptoken += bytes(1)
    
        tokenStruct = struct.pack("=i", len(exptoken)) + exptoken;
        
        connectionString = f"DRIVER={self._audit_db_driver};SERVER={self._audit_db_server};DATABASE={self._audit_db_database};PORT={self._audit_db_port}"
        connection = pyodbc.connect(connectionString, 
                                    attrs_before = {SQL_COPT_SS_ACCESS_TOKEN:tokenStruct})
        return connection
    
    def update_watermark(self, timestamp:str, pkTblJobQueue:str) -> None:
        """
        Updates sourceChgKeyLatestValues in jobQueue table

        Args: 
            timestamp(str): The timestamp to be updated in the sourceChgKeyLatestValues column.
            pkTblJobQueue(str): primary key of jobQueue
    
        Returns:
            None

        """
        import struct

        try:
        
            update_query = F"UPDATE audit.tbljobqueue SET sourceChgKeyLatestValues = '{timestamp}' WHERE pkTblJobQueue = '{pkTblJobQueue}'"
            connection = self.get_pyodbc_connection()
            cursor = connection.cursor()
            cursor.execute(update_query)
            connection.commit()
            cursor.close()

            self.logger.info('sourceChgKeyLatestValues updated in the audit.tbljobQueue...')
        
        except Exception as e:
            raise Exception(f'Exception Occured while updating the sourceChgKeyLatestValues in audit.tbljobQueue.Exception details: ' + str(e))
        


# COMMAND ----------


