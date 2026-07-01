# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
RDD = spark_session.sparkContext.emptyRDD()

# COMMAND ----------

columns = StructType([StructField('Ingestion_Revision_Date', TimestampType(), False),
                      StructField('Country', StringType(), False),
                      StructField('Country Code', IntegerType(), False),
                      StructField('Company Code', StringType(), False),
                      StructField('CAPIN ID', IntegerType(), False),
                      StructField('Fiscal Period', StringType(), False),
                      StructField('Date - Year', StringType(), False),
                      StructField('Date - Month', IntegerType(), False),
                      StructField('Amount @ActualRate', IntegerType(), False),
                      StructField('Amount @PlanRate', StringType(), False),
                      StructField('Currency Code', StringType(), False),
                      StructField('Project Code', StringType(), False),
                      StructField('Project Text', StringType(), False),
                      StructField('CAPIN Type Project Code', StringType(), False),
                      StructField('CAPIN Type Project Text', StringType(), False),
                      StructField('CAPIN Type Child Code', StringType(), False),
                      StructField('CAPIN Type Child Text', StringType(), False),
                      StructField('Entry Name', StringType(), False),
                      StructField('Project_Country', StringType(), False)])

# COMMAND ----------

master_df = spark_session.createDataFrame(data=RDD,schema=columns)

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year
print(currentMonth,currentYear)

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/CASH_CAPEX/'):
  path=i.path+str(currentYear)+'/'+str(currentMonth)+'/CAPIN.parquet'
  try:
    print(path)
    path=i.path+str(currentYear)+'/'+str(currentMonth)+'/*.parquet'
    df=spark.read.parquet(path)
    master_df=master_df.union(df)
  except:
    pass
  dict_trans = [
        ('OM',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_OM/Sensitive/CASH_CAPEX/'}),
        ('PH',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PH/Sensitive/CASH_CAPEX/'}),
        ('PK',{'path_name':'/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PK/Sensitive/CASH_CAPEX/'})
        ]
  #Union of Restricted and Unrestricted country data
for i in dict_trans:
    try:
        path = i[1]["path_name"]
        #print(path)
        master_df_new = spark.read.option("mergeSchema", "true").parquet(path+i[0]+"/"+str(currentYear)+"/"+str(currentMonth)+"/*.parquet")
        master_df=master_df.union(master_df_new)
    except:
        pass

# COMMAND ----------

 final_table=master_df.createOrReplaceTempView('final_table')

# COMMAND ----------

# DBTITLE 1,Fail the notebook if records doesn't exists
view_cnt = spark.sql(""" select count(*) from final_table""").collect()[0][0]
print(view_cnt)
if (view_cnt == 0): 
  raise Exception("No data is availabe")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into NDH.cash_capex_NDA
# MAGIC select * from final_table

# COMMAND ----------

spark.sql("DESCRIBE DETAIL NDH.cash_capex_NDA").select("location").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Clearing cache
sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success') 
