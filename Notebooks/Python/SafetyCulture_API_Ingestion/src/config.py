# Databricks notebook source
DEV_ENV_PARAMS = {
    "TENANT_ID": "",
    "TOKEN_AUTHORITY_HOST": "https://login.windows.net/",
    "RESOURCE_APP_ID_URI": "https://database.windows.net/.default",
    "AUDIT_DB_SERVER": "",
    "AUDIT_DB_DATABASE": "",
    "AUDIT_DB_PORT": "1433", 
    "AUDIT_DB_DRIVER": "{ODBC Driver 18 for SQL Server}",
    "KEY_VAULT_SCOPE": "",
    "AUDIT_DB_SPN_ID_KV_SECRET": "",
    "AUDIT_DB_SPN_PWD_KV_SECRET": "",
    "EMAIL_CLIENT_ID_KV_SECRET": "",
    "EMAIL_CLIENT_SECRET_KV_SECRET": "",
    "FROM_EMAIL_ADDRESS": "",
    "TARGET_DB_NAME": "",
    "CONTROL_TBL_FIELDS" : "jq.pkTblJobQueue,jq.jobGroup,jq.jobNum,jq.jobOrder,jq.jobStepNum,jq.fkJobType,jq.fkJobStep,jq.fkLoadType,jq.fkSourceApplication,jq.sourceTblName,jq.sourcePKCols,jq.sourceChangeKeyCols,jq.excludeColumns,jq.targetDBName,jq.targetExtTblSchemaName,jq.targetTblName,jq.targetStorageAccount,jq.targetContainer,jq.extTbDataSource,jq.forceTblRecreate,jq.targetFilePath,jq.targetFileName,jq.fkTargetFileFormat,jq.successNotificationEmailIDs,jq.failureNotificationEmailIDs,jq.IsActive,jq.keyVaultName,jq.inscopeColumnList,jq.sourceURL,jq.sourceFilePath ,jq.sourceFileName,jq.additionalColumnsInTarget,jqe.tokenURL,jq.filterQuery,jqe.tokenCredentialKeyList,jqe.tokenCredentialKVNameList,jqe.fkTokenRequestMethod,jqe.fkTokenAuthType,jqe.fkTokenAuthParamPassingMethod,jqe.tokenRespContentType,jqe.srcAuthCredentialKeyList,jqe.srcAuthCredentialKVNameList,jqe.fkSrcRequestMethod,jqe.fkSrcAuthType,jqe.fkSrcAuthParamPassingMethod,jqe.fkSrcResponseType,jqe.fkSrcResponseFormat,jqe.srcRespContentType,jqe.hasPagination,jqe.paginationURLKeyword,jqe.paginationURLLocation,jqe.paginationType,jqe.paginationAdditionalParams,jq.sourceChgKeyLatestValues,jq.deleteOperationTableName,jq.XMLNodesTillDataNode,jqe.APIPostRequestBody,jq.sourceTimestampFormat,jq.targetStoreKeyName,jq.target2StorageAccount,jq.target2Container,jq.target2FilePath,jq.target2FileName,jq.transformedColumnsInTarget,jqe.sourceURL2,jqe.excludeFlattenArrayList,jq.targetStoreSecretName,jq.targetDB2Server,jq.targetDB2Name,jq.targetDB2ConnID,jq.targetDB2SecretName,jq.clusterPermissionGroupName,jq.clusterPermissionLevel",
    "CATALOG_NAME": ""
}

PROD_ENV_PARAMS = {
    "TENANT_ID": "",
    "TOKEN_AUTHORITY_HOST": "https://login.windows.net/",
    "RESOURCE_APP_ID_URI": "https://database.windows.net/.default",
    "AUDIT_DB_SERVER": "",
    "AUDIT_DB_DATABASE": "",
    "AUDIT_DB_PORT": "1433", 
    "AUDIT_DB_DRIVER": "{ODBC Driver 18 for SQL Server}",
    "KEY_VAULT_SCOPE": "" ,
    "AUDIT_DB_SPN_ID_KV_SECRET": "" ,
    "AUDIT_DB_SPN_PWD_KV_SECRET": "" ,
    "EMAIL_CLIENT_ID_KV_SECRET": "",
    "EMAIL_CLIENT_SECRET_KV_SECRET": "",
    "FROM_EMAIL_ADDRESS": "",
    "TARGET_DB_NAME": "",
    "CONTROL_TBL_FIELDS" : "jq.pkTblJobQueue,jq.jobGroup,jq.jobNum,jq.jobOrder,jq.jobStepNum,jq.fkJobType,jq.fkJobStep,jq.fkLoadType,jq.fkSourceApplication,jq.sourceTblName,jq.sourcePKCols,jq.sourceChangeKeyCols,jq.excludeColumns,jq.targetDBName,jq.targetExtTblSchemaName,jq.targetTblName,jq.targetStorageAccount,jq.targetContainer,jq.extTbDataSource,jq.forceTblRecreate,jq.targetFilePath,jq.targetFileName,jq.fkTargetFileFormat,jq.successNotificationEmailIDs,jq.failureNotificationEmailIDs,jq.IsActive,jq.keyVaultName,jq.inscopeColumnList,jq.sourceURL,jq.sourceFilePath ,jq.sourceFileName,jq.additionalColumnsInTarget,jqe.tokenURL,jq.filterQuery,jqe.tokenCredentialKeyList,jqe.tokenCredentialKVNameList,jqe.fkTokenRequestMethod,jqe.fkTokenAuthType,jqe.fkTokenAuthParamPassingMethod,jqe.tokenRespContentType,jqe.srcAuthCredentialKeyList,jqe.srcAuthCredentialKVNameList,jqe.fkSrcRequestMethod,jqe.fkSrcAuthType,jqe.fkSrcAuthParamPassingMethod,jqe.fkSrcResponseType,jqe.fkSrcResponseFormat,jqe.srcRespContentType,jqe.hasPagination,jqe.paginationURLKeyword,jqe.paginationURLLocation,jqe.paginationType,jqe.paginationAdditionalParams,jq.sourceChgKeyLatestValues,jq.deleteOperationTableName,jq.XMLNodesTillDataNode,jqe.APIPostRequestBody,jq.sourceTimestampFormat,jq.targetStoreKeyName,jq.target2StorageAccount,jq.target2Container,jq.target2FilePath,jq.target2FileName,jq.transformedColumnsInTarget,jqe.sourceURL2,jqe.excludeFlattenArrayList,jq.targetStoreSecretName,jq.targetDB2Server,jq.targetDB2Name,jq.targetDB2ConnID,jq.targetDB2SecretName,jq.clusterPermissionGroupName,jq.clusterPermissionLevel",
    "CATALOG_NAME": ""
}

workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
dev_workspace_id = ""
prod_workspace_id = ""
INGESTION_CONFIG = PROD_ENV_PARAMS if workspace_id == prod_workspace_id else DEV_ENV_PARAMS





