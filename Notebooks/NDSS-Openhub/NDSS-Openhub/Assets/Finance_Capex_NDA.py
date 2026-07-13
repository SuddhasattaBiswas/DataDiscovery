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
                      StructField('Country_Name', IntegerType(), False), 
                      StructField('Country_Code', StringType(), False),                     
                      StructField('Company_Code', StringType(), False),
                      StructField('CAPIN_Unique_Id', StringType(), False),
                      StructField('FISCAL_Period', StringType(), False),
                      StructField('Date_Year', StringType(), False),
                      StructField('Date_Month', StringType(), False),
                      StructField('ActualRate', StringType(), False),
                      StructField('PlanRate', StringType(), False),
                      StructField('Currency_Code', StringType(), False),
                      StructField('Project_Code', StringType(), False),
                      StructField('Project_Text', IntegerType(), False),
                      StructField('CAPIN_Type_Project_Code', IntegerType(), False),
                      StructField('CAPIN_Type_Project_Text', StringType(), False),
                      StructField('CAPIN_Type_Child_Code', StringType(), False),
                      StructField('CAPIN_Type_Child_Text', StringType(), False),
                      StructField('Entry_Text', IntegerType(), False),                    
                      StructField('Project_Country', StringType(), False)])

# COMMAND ----------

master_df = spark_session.createDataFrame(data=RDD,schema=columns)

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/FINANCE_CAPEX/'):
  #print (i.path)
  path=i.path+str(currentYear)+'/'+str(currentMonth)+'/Finance_Capex_Actuals.parquet'
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
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_OM/Sensitive/FINANCE_CAPEX/OM/'+str(currentYear)+'/'+str(currentMonth)+'/Finance_Capex_Actuals.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass
try:
  path=''
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PH/Sensitive/FINANCE_CAPEX/PH/'+str(currentYear)+'/'+str(currentMonth)+'/Finance_Capex_Actuals.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass
try:
  path=''
  path='/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP_PK/Sensitive/FINANCE_CAPEX/PK/'+str(currentYear)+'/'+str(currentMonth)+'/Finance_Capex_Actuals.parquet'
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
except:
  #print(path)
  pass

# COMMAND ----------

final_table=master_df.createOrReplaceTempView('final_table')

# COMMAND ----------

view_cnt = spark.sql(""" select count(*) from final_table""").collect()[0][0]
print(view_cnt)
if (view_cnt == 0): 
  raise Exception("No data is availabe")

# COMMAND ----------


finance_df = spark.sql("SELECT * from final_table ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(finance_df,'stg.Finance_Capex_Actuals')

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
