# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col
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
                      StructField('Company_Code', StringType(), False),
                      StructField('Entry_Text', IntegerType(), False),
                      StructField('Country_Code', StringType(), False),
                      StructField('Country_Name', IntegerType(), False),
                      StructField('CAPIN_Unique_Id', StringType(), False),
                      StructField('Project_Code', StringType(), False),
                      StructField('Project_Text', IntegerType(), False),
                      StructField('CAPIN_Type_Project_Code', IntegerType(), False),
                      StructField('CAPIN_Type_Project_Text', StringType(), False),
                      StructField('CAPIN_Type_Child_Code', StringType(), False),
                      StructField('CAPIN_Type_Child_Text', StringType(), False),
                      StructField('FISCAL_Period', StringType(), False),
                      StructField('Actual_LC_Amount', StringType(), False),
                      StructField('Currency_Code', StringType(), False)])

# COMMAND ----------

master_df = spark_session.createDataFrame(data=RDD,schema=columns)

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

# DBTITLE 1,Getting complete data from MDH Prep
list_trans=['CASH_CAPEX','OM','PK','PH'] #country list
master_df = spark.read.option("mergeSchema", "true").parquet("/mnt/ADLS1/PREP/1stParty/GSAP_BW/Sensitive/CASH_CAPEX/*/*/*/*.parquet")

dict_trans = [
        ('OM',{'path_name':'/mnt/ADLS1/PREP/1stParty/GSAP_BW_OM/Sensitive/CASH_CAPEX/'}),
        ('PH',{'path_name':'/mnt/ADLS1/PREP/1stParty/GSAP_BW_PH/Sensitive/CASH_CAPEX/'}),
        ('PK',{'path_name':'/mnt/ADLS1/PREP/1stParty/GSAP_BW_PK/Sensitive/CASH_CAPEX/'})
        ]
#Union of Restricted and Unrestricted country data
for i in dict_trans:
    path = i[1]["path_name"]
    newMegred=spark.read.option("mergeSchema", "true").parquet(path+i[0]+"/*/*/*.parquet")
    master_df=master_df.union(newMegred)
    


# COMMAND ----------

final_table=master_df.createOrReplaceTempView('final_table')

# COMMAND ----------

view_cnt = spark.sql(""" select count(*) from final_table""").collect()[0][0]
print(view_cnt)
if (view_cnt == 0): 
  raise Exception("No data is availabe")

# COMMAND ----------

CAPIN_df = spark.sql("SELECT * from final_table ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(CAPIN_df,'stg.Cash_Capex_Interim')

# COMMAND ----------

# DBTITLE 1,Clearing cache
sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
