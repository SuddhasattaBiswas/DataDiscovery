# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()
RDD = spark_session.sparkContext.emptyRDD()

# COMMAND ----------

columns = StructType([
                      StructField('IngestionDate', TimestampType(), False),
                      StructField('FISCAL_PERIOD', StringType(), False),
                      StructField('COUNTRY_CODE', StringType(), False), 
                      StructField('COST_CENTER_COUNTRY_CODE', StringType(), False),                     
                      StructField('COST_CENTER_COMPANY_CODE', StringType(), False),
                      StructField('COMPANY_CODE', StringType(), False),
                      StructField('COST_CENTER_CODE', StringType(), False),
                      StructField('COST_CENTER_DESCRIPTION', StringType(), False),
                      StructField('COST_ELEMENT_CODE', StringType(), False),
                      StructField('COST_ELEMENT_DESCRIPTION', StringType(), False),
                      StructField('CONTROLLING_AREA_CODE', StringType(), False),
                      StructField('PROFIT_CENTER_CODE', StringType(), False),
                      StructField('PROFIT_CENTER_DESCRIPTION', StringType(), False),
                      StructField('OPEX_LINE_ITEM_CODE', StringType(), False),
                      StructField('OPEX_LINE_ITEM_CODE_DESCRIPTION', StringType(), False),
                      StructField('ZCO_ITMTX', StringType(), False),
                      StructField('CURRENCY_CODE', StringType(), False),
                      StructField('UNIT', StringType(), False),
                      StructField('CURRENCY_TYPE', StringType(), False),                    
                      StructField('ZVCCURTYP', StringType(), False),
                      StructField('VALUE_TYPE_CODE', StringType(), False),
                      StructField('VERSION_NUMBER', StringType(), False),
                      StructField('GL_ACCOUNT_CODE', StringType(), False),
                      StructField('CHRT_ACCTS', StringType(), False),
                      StructField('INFO_PROVIDER', StringType(), False),
                      StructField('TOTAL_VOLUME_QUANTITY', DoubleType(), False),
                      StructField('ACTUAL_AMOUNT', DoubleType(), False),
                      StructField('PLAN_CO_AMOUNT', DoubleType(), False),
                      StructField('PLAN_COMPANY_AMOUNT', DoubleType(), False),
                      StructField('PLAN_HARD_AMOUNT', DoubleType(), False),
                      StructField('CREATE_DATE', TimestampType(), False)])

# COMMAND ----------

master_df = spark_session.createDataFrame(data=RDD,schema=columns)

# COMMAND ----------

import datetime
currentMonth = datetime.datetime.now().strftime('%m')
currentYear = datetime.datetime.now().year

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/CC_POS/'):
  #print (i.path)
  path=i.path+str(currentYear)+'/'+str(currentMonth)+'/POS.parquet'
  try:
    #print(path)
    #print('try')
    df=spark.read.parquet(path)
  except:
    #print(path)
    #print('excep')
    continue
  master_df=master_df.union(df)
  
  #print(df.count())
  #print(i.name)

# COMMAND ----------

try:
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_OM/Sensitive/CC_POS/OM/'+str(currentYear)+'/'+str(currentMonth)+'/POS.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass
try:
  path=''
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PH/Sensitive/CC_POS/PH/'+str(currentYear)+'/'+str(currentMonth)+'/POS.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass
try:
  path=''
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PK/Sensitive/CC_POS/PK/'+str(currentYear)+'/'+str(currentMonth)+'/POS.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass

# COMMAND ----------

final_table=master_df.createOrReplaceTempView('final_table')

# COMMAND ----------


CAPIN_df = spark.sql("SELECT * from final_table ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(CAPIN_df,'dbo.POS_OPEX_PLAN_ACTUAL_NDT')

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')