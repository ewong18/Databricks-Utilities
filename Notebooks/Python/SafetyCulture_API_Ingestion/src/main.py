# Databricks notebook source
from datetime import datetime, timezone
import logging
from pyspark.sql.functions import col
from tableUtils import tableUtils
from typing import Tuple

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ../../../../Utilities/Python/common/audit_db

# COMMAND ----------

# MAGIC %run ../../../../Utilities/Python/common/email

# COMMAND ----------

# MAGIC %run ./iauditor_client

# COMMAND ----------

# Connect to AuditDB for Job metadata
def get_job_metadata(job_group:int, 
                     job_order:int, 
                     job_num:int, 
                     INGESTION_CONGIF:dict) -> Tuple[AuditDBClient, DataFrame]:
    """
    Get job metadata from audit DB

    Args:
        job_group: int - jobGroup
        job_order: int - jobOrder
        job_num: int - jobNum (set 0 if all jobNums in a jobOrder)
        INGESTION_CONFIG: dict - config for ingestion
    
    Returns:
        Tuple[AuditDBClient, DataFrame]
            AuditDBClient: AuditDBClient object
            DataFrame: DataFrame with job metadata
    """
    audit_db_service_principal_id = dbutils.secrets.get(
        scope=INGESTION_CONFIG["KEY_VAULT_SCOPE"], key=INGESTION_CONFIG['AUDIT_DB_SPN_ID_KV_SECRET']
    )
    audit_db_service_principal_pwd = dbutils.secrets.get(
        scope=INGESTION_CONFIG["KEY_VAULT_SCOPE"], key=INGESTION_CONFIG['AUDIT_DB_SPN_PWD_KV_SECRET']
    )
    audit_db_client = AuditDBClient(
        service_principal_id=audit_db_service_principal_id,
        service_principal_pwd=audit_db_service_principal_pwd,
        token_authority_url=INGESTION_CONFIG["TOKEN_AUTHORITY_HOST"] + INGESTION_CONFIG["TENANT_ID"],
        resource_app_id_uri=INGESTION_CONFIG["RESOURCE_APP_ID_URI"],
        audit_db_server = INGESTION_CONFIG["AUDIT_DB_SERVER"],
        audit_db_database =INGESTION_CONFIG["AUDIT_DB_DATABASE"],
        audit_db_port = INGESTION_CONFIG["AUDIT_DB_PORT"],
        audit_db_driver = INGESTION_CONFIG["AUDIT_DB_DRIVER"]
    )

    logger.info("Fetching job metadata")
    api_ingestion_job_metadata_df = audit_db_client.fetch_ingestion_job_metadata(
                control_tbl_fields=INGESTION_CONFIG["CONTROL_TBL_FIELDS"],
                job_group=job_group, 
                job_order=job_order,
                job_num =job_num,
                spark_session=spark)
    display(api_ingestion_job_metadata_df)            
    return audit_db_client, api_ingestion_job_metadata_df


# COMMAND ----------

#Run Job and Log Results
def run_job_and_log(api_ingestion_job_metadata_df:DataFrame, 
                    audit_db_client:AuditDBClient, 
                    INGESTION_CONFIG:dict, 
                    runID:str) -> DataFrame:
    logging_metadata = []
    """
    Run Job and Log Results

    Args:
        api_ingestion_job_metadata_df: DataFrame - DataFrame with job metadata
        audit_db_client: AuditDBClient - AuditDBClient object
        INGESTION_CONFIG: dict - config for ingestion
        runID: str - runID for job run
        
    Returns:
        DataFrame: DataFrame with run results for each job
    """
    for job in api_ingestion_job_metadata_df.collect():
        iauditor_client = iAuditorClient(job, INGESTION_CONFIG["CATALOG_NAME"])
        logger.info(f">>> Starting Ingestion for {job['targetTblName']}")
        job_results = iauditor_client.ingest_data()
        job_runlog_dict = {"runID": runID,
                        "fkJobQueue": job["pkTblJobQueue"],
                        "jobGroup": job["jobGroup"],
                        "jobNum": job["jobNum"],
                        "jobOrder": job["jobOrder"],
                        "jobStepNum": job["jobStepNum"],
                        "targetTblName": job["targetTblName"],
                        "startTime": job_results["startTime"],
                        "endTime": job_results["endTime"],
                        "status": job_results["status"],
                        "processingDetails": job_results["processingDetails"],
                        "errorMsg": job_results["errorMsg"],
                        "recInSource" : None, #cannot accurately determine from api
                        "recIngested": job_results["recIngested"],
                        "recProcessed" : job_results["recProcessed"],
                        "recFailed": job_results["recFailed"]
                        }
        logging_metadata.append(job_runlog_dict)
        watermark_ts = (job_results["startTime"]
                            .astimezone(timezone.utc)
                            .isoformat(timespec="milliseconds")
                            .replace("+00:00", "Z"))
        audit_db_client.update_watermark(watermark_ts,job["pkTblJobQueue"])
        logger.info(f"Ingestion for {job['targetTblName']} Completed with Status: {job_results['status']}")
    job_logging_df = Utils.dict_to_df(logging_metadata)
    audit_db_client.insert_run_log(job_logging_df)
    return job_logging_df

# COMMAND ----------

#Send Email Alert
def send_job_email_alert(job_metadata_df, job_logging_df, INGESTION_CONFIG:dict, runID:str) -> None:
    """
    Send Email Alert with job run results
    Args:
        job_metadata_df: DataFrame - DataFrame with job metadata
        job_logging_df: DataFrame - DataFrame with run results for each job
        INGESTION_CONFIG: dict - config for ingestion
    
    Returns:
        None
    """
    successful_tables = [x.targetTblName for x in 
                            job_logging_df.filter(col("status") == "S")
                                        .select("targetTblName").collect()]
    failed_tables = [x.targetTblName for x in 
                            job_logging_df.filter(col("status") == "F")
                                        .select("targetTblName").collect()]

    email_client_id = dbutils.secrets.get(scope = INGESTION_CONFIG["KEY_VAULT_SCOPE"], 
                                        key = INGESTION_CONFIG["EMAIL_CLIENT_ID_KV_SECRET"])
    email_client_secret = dbutils.secrets.get(scope = INGESTION_CONFIG["KEY_VAULT_SCOPE"], 
                                            key = INGESTION_CONFIG["EMAIL_CLIENT_SECRET_KV_SECRET"])

    email_client = EmailClient(email_client_id, 
                            email_client_secret, 
                            tenant_id = INGESTION_CONFIG["TENANT_ID"], 
                            from_email_addr = INGESTION_CONFIG["FROM_EMAIL_ADDRESS"])

    subject, message = email_client.create_email_message(
            successful_tables = successful_tables,
            failed_tables = failed_tables,
            job_group_name="iAuditor API Ingestion",
            job_group = job_group,
            job_order = job_order,
            job_url = runID
        )

    to_email_addrs = (job_metadata_df.select("failureNotificationEmailIDs")
                     .collect()[0][0].split(","))
    email_client.send_email(to_email_addrs,subject, message)
    return

# COMMAND ----------

def main(job_group,job_order, job_num,INGESTION_CONFIG,runID ):
    audit_db_client, job_metadata = get_job_metadata(job_group, job_order, job_num, INGESTION_CONFIG)
    job_run_results = run_job_and_log(job_metadata, audit_db_client, INGESTION_CONFIG, runID)
    send_job_email_alert(job_metadata,job_run_results, INGESTION_CONFIG, runID)
    if job_run_results.filter(col("status") == "F").count() > 0:
        raise Exception("One or more jobs failed. See logs for details.")
    return 

# COMMAND ----------

logger = logging.getLogger(name="__iAuditor_Ingestion__")
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(stream_handler)

job_group = int(dbutils.widgets.get("jobGroup"))
job_order = int(dbutils.widgets.get("jobOrder"))
job_num = int(dbutils.widgets.get("jobNum"))
runID = Utils.get_job_url()

logger.info(f"Running jobGroup: {job_group}, jobOrder: {job_order}, jobNum: {job_num}")

# COMMAND ----------

main(job_group,job_order, job_num,INGESTION_CONFIG,runID)


