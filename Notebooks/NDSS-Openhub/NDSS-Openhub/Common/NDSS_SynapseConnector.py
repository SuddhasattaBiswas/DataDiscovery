# Databricks notebook source
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "9a5a044f-c757-4926-825a-d7f459ede977")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV",key= "AZ-AS-NDH-SPN-DEV-Key"))
blobaccesskey=dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV",key= "AZ-AS-devndssstgacct-access-key")
spark.conf.set("fs.azure.account.key.devndssstgacct.blob.core.windows.net",blobaccesskey)

# COMMAND ----------

# DBTITLE 1,method for setting up connections
def setconnections():
  spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "9a5a044f-c757-4926-825a-d7f459ede977")
  spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV",key= "AZ-AS-NDH-SPN-DEV-Key"))
  blobaccesskey=dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV",key= "AZ-AS-devndssstgacct-access-key")
  spark.conf.set("fs.azure.account.key.devndssstgacct.blob.core.windows.net",blobaccesskey)
  

# COMMAND ----------

# DBTITLE 1,1.get last update date from synapse table
import datetime 
def readLastUpdateDate(sourcetable):
  lquery=f"select distinct(update_date) from {sourcetable}"
  print(lquery)
  lastupdatedate=datetime.datetime(1000, 1, 1, 0, 0, 0, 0)
  try:
    updatedates=spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
    .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
    .option("enableServicePrincipalAuth", "true") \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("query", lquery) \
    .load().collect()
    print(updatedates)
    lastupdatedate=max(updatedates)[0]
    print(f"Last Update Date : {lastupdatedate}")
    
  except Exception as e :
    #print(e)
    if ('Invalid object name' in str(e)):
      print("table is Not present in Synapse")
      lastupdatedate=datetime.datetime(1001, 1, 1, 0, 0, 0, 0)
  return lastupdatedate

  
#print(readLastUpdateDate("dbo.SITE_PERFORMANCE_RATIO_NDA_test"))

# COMMAND ----------

# DBTITLE 1,2.get incremental records from NDH delta table
def getIncrementalRecords(sourcetable,lastupdatedate):
  #lastupdatedate=readLastUpdateDate(table)
  #deltaquery=f"select *,now() as NDSS_REFRESH_DATE from {sourcetable} where update_date > \"{lastupdatedate}\" "
  deltaquery = f"SELECT *,now() AS NDSS_REFRESH_DATE FROM {sourcetable} WHERE FISCAL_PERIOD IN (SELECT DISTINCT FISCAL_PERIOD FROM {sourcetable} WHERE UPDATE_DATE > \"{lastupdatedate}\")"
  print(deltaquery)
  df=spark.sql(deltaquery)
  return df
#getIncrementalRecords("OPERATING_COST_NDT","1000-10-26 10:46:29.034000").display()

# COMMAND ----------

# DBTITLE 1,3.Append the df to Synapse table
def appendToSynapse(df,table):
  df.write \
  .mode("append")\
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
  .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
  .option("enableServicePrincipalAuth", "true") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", table) \
  .save()

# COMMAND ----------

# DBTITLE 1,4.Overwrite the df to synapse  table
def overwriteToSynapse(df,table):
  df.write \
  .mode("overwrite")\
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
  .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
  .option("enableServicePrincipalAuth", "true") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", table) \
  .save()
  

# COMMAND ----------

# DBTITLE 1,MASTER:  Method to copy incremental data to synapse temp incremental table
def writeIncrementalToSynapseTemp(sourceTable,destTable,incrementalTable):
  print("set all connections")
  setconnections()
  print("Source table is ::"+sourceTable)
  print("Destination  table is :: "+destTable)
  print("Synapse incremental table is ::  "+incrementalTable)
  lastUpdateDate=readLastUpdateDate(destTable)
  print(f"lastupdatedate ={lastUpdateDate}")
  df=getIncrementalRecords(sourceTable,lastUpdateDate)
  overwriteToSynapse(df,incrementalTable)
  


# COMMAND ----------

# DBTITLE 1,Read a table from synapse
def readTableFromSynapse(table):
  
  try:
    df=spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
    .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
    .option("enableServicePrincipalAuth", "true") \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("dbtable", table) \
    .load()
        
  except Exception as e :
    #print(e)
    if ('Invalid object name' in str(e)):
      print("table is Not present in Synapse")
      df=spark.createDataFrame
  return df

# COMMAND ----------

# DBTITLE 1,Read a Query from synapse
def queryFromSynapse(query):
  
  try:
     df=spark.read \
    .format("com.databricks.spark.sqldw") \
    .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
    .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
    .option("enableServicePrincipalAuth", "true") \
    .option("forwardSparkAzureStorageCredentials", "true") \
    .option("query", query) \
    .load()
        
  except Exception as e :
    print(e)
    if ('Invalid object name' in str(e)):
      print("table is Not present in Synapse")
    
  return df
      


# COMMAND ----------

def overwriteToSynapseCustom(df,table):
  df.write \
   .option("maxStrLength", 2000) \
  .mode("overwrite")\
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://az-as-sql-srv-ex-n-seq02217-ndh-dev.database.windows.net:1433;database=AZ-AS-SQL-DW-EX-N-SEQ02217-NDH-DEV") \
  .option("tempdir", "wasbs://dataload@devndssstgacct.blob.core.windows.net/ADB_SYNAPSE_TEMP")\
  .option("enableServicePrincipalAuth", "true") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", table) \
  .save()