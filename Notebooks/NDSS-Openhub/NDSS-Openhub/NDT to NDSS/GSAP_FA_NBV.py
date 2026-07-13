# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector


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

if spark.catalog.tableExists("ndh.FIXED_ASSET_NBV_NDT"):
    max_timestamp_df = spark.table("ndh.FIXED_ASSET_NBV_NDT").select(max("CREATE_DATE").alias("max_timestamp"))
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

list_trans=['CASH_CAPEX','OM','PK','PH'] #country list
mergedf = spark.read.option("mergeSchema", "true").parquet("/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/FA_NBV/*/*/*/*.parquet")

dict_trans = [
        ('OM',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_OM/Sensitive/FA_NBV/'}),
        ('PH',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PH/Sensitive/FA_NBV/'}),
        ('PK',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PK/Sensitive/FA_NBV/'})
        ]
#Union of Restricted and Unrestricted country data
for i in dict_trans:
    path = i[1]["path_name"]
    newMegred=spark.read.option("mergeSchema", "true").parquet(path+i[0]+"/*/*/*.parquet")
    mergedf=mergedf.union(newMegred).drop_duplicates()
mergedf.createOrReplaceTempView("final_table")    
master_df=mergedf.withColumn('SITE_CODE',when(col('SITE_CODE').substr(1,2) == '00',col('SITE_CODE').substr(3,1000)).otherwise(col('SITE_CODE')))
master_df=master_df.withColumnRenamed('Ingestion_Revision_Date','CREATE_DATE')
master_df=master_df.withColumnRenamed('Fixed_Asset_NBV','FIXED_ASSET_NET_BOOK_VALUE')
final_df=master_df[master_df['CREATE_DATE']>previous_timestamp]
final_df.createOrReplaceTempView("final_table")
final_df.count()

# COMMAND ----------

country_df=spark.sql(""" select distinct COMP_CODE,COUNTRY from NDH.COMP_CODE_ATTRIBUTES_NDT where COUNTRY!=' ' AND COUNTRY is not null """)
country_df=country_df.withColumnRenamed('COMP_CODE','COMPANY_CODE')
country_df.display()

# COMMAND ----------

result_df=final_df.join(country_df,on='COMPANY_CODE', how='left')
result_df.display()
result_df.count()

# COMMAND ----------

table = 'dbo.FIXED_ASSET_NBV_NDT'
appendToSynapse(result_df,table)

# COMMAND ----------

result_df.createOrReplaceTempView('result_table')

# COMMAND ----------

df_write_incr = spark.sql("""Insert into ndh.FIXED_ASSET_NBV_NDT (select * from result_table)""")

# COMMAND ----------

AuditTableUpdate('FIXED_ASSET_NBV')

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
