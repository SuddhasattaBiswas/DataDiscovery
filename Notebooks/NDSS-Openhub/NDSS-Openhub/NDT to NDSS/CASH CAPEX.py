# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df_Maxdt = spark.sql("""SELECT  max(substr(Ingestion_Revision_Date,1,10)) as max_date from NDH.Cash_Capex_NDA""").collect()[0][0]
print(df_Maxdt)
var1=df_Maxdt+"%"
print(var1)
spark.conf.set("c.var1", var1)

# COMMAND ----------

SourceTable = 'NDH.Cash_Capex_NDA'
#TargetTable = 'dbo.Cash_Capex_NDA'
TargetTable = 'stg.Cash_Capex_NDA_12345'
Actual_Exchange_Rate_NDT_df = spark.sql("""SELECT * from NDH.Cash_Capex_NDA where Ingestion_Revision_Date like '${c.var1}' """).withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
Actual_Exchange_Rate_NDT_df.display()
setconnections();
overwriteToSynapseCustom(Actual_Exchange_Rate_NDT_df,TargetTable)

# COMMAND ----------

# DBTITLE 1,Clear cache
sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
