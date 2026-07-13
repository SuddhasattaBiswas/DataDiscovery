# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector
# MAGIC

# COMMAND ----------

# MAGIC %run /Shared/NDH/Commons/NB_CommonFunction

# COMMAND ----------

spark.catalog.clearCache()


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,when,max
from datetime import datetime

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
RDD = spark_session.sparkContext.emptyRDD()

# COMMAND ----------

# # Check if the table exists
# if spark.catalog.tableExists("ndh.FIXED_ASSET_NBV_NDT"):
#     backup_table_name = "ndh.FIXED_ASSET_NBV_NDT_backup"
#     spark.sql(f"CREATE TABLE {backup_table_name} AS SELECT * FROM ndh.FIXED_ASSET_NBV_NDT")
#     print(f"Backup table '{backup_table_name}' created successfully.")
# else:
#     print("Table 'ndh.FIXED_ASSET_NBV_NDT' does not exist.")

# COMMAND ----------

previous_timestamp = datetime(2020, 1, 1, 0, 0, 0)

if spark.catalog.tableExists("ndh.Corporate_tax"):
    max_timestamp_df = spark.table("ndh.Corporate_tax").select(max("CREATE_DATE").alias("max_timestamp"))
    max_timestamp_row = max_timestamp_df.first()

    if max_timestamp_row and max_timestamp_row["max_timestamp"]:
        max_timestamp = max_timestamp_row["max_timestamp"]

        if max_timestamp > previous_timestamp:
            previous_timestamp = max_timestamp

print(previous_timestamp)

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

list_trans=['OM','PK','PH'] #country list
mergedf = spark.read.option("mergeSchema", "true").parquet("/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_POWERPLAN/Sensitive/Corporate_Tax_Rate/*/*/*/*.parquet")
dict_trans = [
        ('OM',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_POWERPLAN_OM/Sensitive/Corporate_Tax_Rate/'}),
        ('PH',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_POWERPLAN_PH/Sensitive/Corporate_Tax_Rate/'}),
        ('PK',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_POWERPLAN_PK/Sensitive/Corporate_Tax_Rate/'})
        ]
#Union of Restricted and Unrestricted country data
for i in dict_trans:
    path = i[1]["path_name"]
    newMegred=spark.read.option("mergeSchema", "true").parquet(path+i[0]+"/*/*/*.parquet")
    mergedf=mergedf.union(newMegred).drop_duplicates()
mergedf=mergedf.withColumnRenamed('Ingestion_Revision_Date','CREATE_DATE')
mergedf=mergedf[mergedf['CREATE_DATE']>previous_timestamp]
mergedf.createOrReplaceTempView("Corporate_tax")

# COMMAND ----------

spark.sql("""select CONCAT('0',substring(add_months((date_format(current_date(), 'yyyy-MM-dd')),-1),6,2), '.', substring(add_months((date_format(current_date(), 'yyyy-MM-dd')),-1),0,4)) as FISCAL_PERIOD,
CREATE_DATE ,
Company_code,
Country ,
Percentage ,
Year 
-- current_timestamp() as CREATE_DATE,
-- current_timestamp() as UPDATE_DATE
from Corporate_tax""").createOrReplaceTempView('Corporate_tax')

# COMMAND ----------

# MAGIC %sql select * from Corporate_tax

# COMMAND ----------

# df_write_incr = spark.sql("""Insert into ndh.FIXED_ASSET_NBV_NDT (select * from result_table)""")
spark.sql("""Insert into ndh.Corporate_tax (select * from Corporate_tax)""")

# COMMAND ----------

# DBTITLE 1,MAKE AN ENTRY IN AUDIT TABLE
AuditTableUpdate('Corporate_tax')

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')