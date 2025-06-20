// Databricks notebook source
/*dbutils.widgets.removeAll();
dbutils.widgets.text("jdbcServerName","")
dbutils.widgets.text("jdbcPort","1433")
dbutils.widgets.text("jdbcDatabaseName","")

dbutils.widgets.text("UserName","")
dbutils.widgets.text("Password KV","")

dbutils.widgets.text("SPN id KV","")
dbutils.widgets.text("SPN secret KV","")*/


// COMMAND ----------

// MAGIC %md
// MAGIC # Instructions:
// MAGIC ###### For Connecting to SQL DB via SPN:
// MAGIC 1. Fill in `SPN id KV` and `SPN secret KV` with their Key Vault Secret Names and keep `UserName` and `Password` blank
// MAGIC 1. Fill in `jdbcServerName` and `jdbcDatabaseName`. (`jdbcPort` is `1433` by default)
// MAGIC 1. Run the command below
// MAGIC 1. call `querySQL()` with your SQL query statement
// MAGIC 
// MAGIC ###### For Connecting to SQL DB via UserName Password:
// MAGIC 1. Fill in `UserName` with actual value and `Password` with the Key Vault Secret Name and keep `SPN id KV` and `SPN secret KV` blank
// MAGIC 1. Fill in `jdbcServerName` and `jdbcDatabaseName`. (`jdbcPort` is `1433` by default)
// MAGIC 1. Run the command below
// MAGIC 1. call `querySQL()` with your SQL query statement



// COMMAND ----------

import java.util.Collections
import com.microsoft.aad.msal4j._


import org.apache.spark.sql.DataFrame

val keyVaultScope = [REDACTED-KEYVAULT_NAME]
val tenantId = [REDACTED-TENANT_ID]

val jdbcHostname = dbutils.widgets.get("jdbcServerName")
val jdbcPort = dbutils.widgets.get("jdbcPort")
val jdbcDatabase = dbutils.widgets.get("jdbcDatabaseName")

val jdbcUsername = dbutils.widgets.get("UserName")
val pwdSecretName = dbutils.widgets.get("Password KV")

var spidKV = dbutils.widgets.get("SPN id KV")
var sppwdKV = dbutils.widgets.get("SPN secret KV")


def getAccessToken():String = {
  var ServicePrincipalId = dbutils.secrets.get(scope = keyVaultScope, key = spidKV) 
  var ServicePrincipalPwd = dbutils.secrets.get(scope = keyVaultScope, key = sppwdKV) 
  
  val scope = "https://database.windows.net/.default"
  val authority = "https://login.windows.net/" + tenantId
  
  val app = ConfidentialClientApplication.builder(ServicePrincipalId,
                                                  ClientCredentialFactory.createFromSecret(ServicePrincipalPwd))
                                         .authority(authority).build()
  val clientCredentialParam = ClientCredentialParameters.builder(Collections.singleton(scope)).build()
  val accessToken = app.acquireToken(clientCredentialParam).get().accessToken()

  return accessToken    
}

def querySQL(query: String) : DataFrame={
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
  var jdbc_df = spark.emptyDataFrame
  //If authenticating with Username and Password
  if(jdbcUsername != "" & pwdSecretName != "" & spidKV == "" & sppwdKV == ""){
    println("Using Username and Password...")  
    val jdbcPassword = dbutils.secrets.get(scope = keyVaultScope, key = pwdSecretName)

    jdbc_df = spark.read.format("jdbc") 
              .option("url", jdbcUrl) 
              .option("query", query) 
              .option("username", jdbcUsername) 
              .option("password", jdbcPassword) 
              .option("databaseName", jdbcDatabase) 
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
              .load() 
    
  }
  else if (jdbcUsername == "" & pwdSecretName == "" & spidKV != "" & sppwdKV != ""){
    println("Using Service Principal...")
    jdbc_df = spark.read.format("jdbc") 
              .option("url", jdbcUrl) 
              .option("query", query) 
              .option("accessToken", getAccessToken()) 
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") 
              .load() 
  }
 
  return jdbc_df
}

// COMMAND ----------

//Example query to list all table names in a Schema
val query = """SELECT table_name, table_schema, table_type
FROM information_schema.tables
where table_schema like '%[SCHEMANAME]%'"""

// COMMAND ----------

display(querySQL(query))
