# Databricks notebook source
# MAGIC %run /NDSS/Common/NDSS_SynapseConnector

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

columns = StructType([StructField('Ingestion_Revision_Date', TimestampType(), False),
                      StructField('Company_Code', StringType(), False),
                      StructField('Vendor_Account_Code', IntegerType(), False),
                      StructField('FISCAL_Period', StringType(), False),
                      StructField('Fiscal_Year_Variant', IntegerType(), False),
                      StructField('Document_Number', StringType(), False),
                      StructField('Refrence_Key', IntegerType(), False),
                      StructField('Item_Number', IntegerType(), False),
                      StructField('Item_Due_Date', IntegerType(), False),
                      StructField('Country_Code', StringType(), False),
                      StructField('Reference_Dcoument_Number', StringType(), False),
                      StructField('Allocation_Number', StringType(), False),
                      StructField('HDR_REF_KEY1', StringType(), False),
                      StructField('Header_Text', StringType(), False),
                      StructField('Item_Text', StringType(), False),
                      StructField('RT_TAXCODE', StringType(), False),
                      StructField('PMTMTHSUPL', StringType(), False),
                      StructField('ZHWAE2', StringType(), False),
                      StructField('DCINDIC', StringType(), False),
                      StructField('LOAD_DT', StringType(), False),
                      StructField('Dcoument_Type_Code', StringType(), False),
                      StructField('Paymend_Method_Code', StringType(), False),
                      StructField('Paymend_Block_Code', StringType(), False),
                      StructField('Create_User_Id', StringType(), False),
                      StructField('Base_Date', IntegerType(), False),
                      StructField('Net_Due_Date', IntegerType(), False),
                      StructField('Clearing_Document_Number', StringType(), False),
                      StructField('Clearing_Date', IntegerType(), False),
                      StructField('Document_Created_Date', IntegerType(), False),
                      StructField('Document_Date', IntegerType(), False),
                      StructField('Posting_Date', IntegerType(), False),
                      StructField('DISC_BASE', DoubleType(), False),
                      StructField('Discount_DC_Amount', DoubleType(), False),
                      StructField('Document_Currency_Amount', DoubleType(), False),
                      StructField('Document_Currency_Code', StringType(), False),
                      StructField('Local_Currency_Amount', DoubleType(), False),
                      StructField('Local_Currency_Code', StringType(), False),
                      StructField('Discount_LC_Amount', DoubleType(), False),
                      StructField('ZBVTYP', StringType(), False),
                      StructField('ZQSSHB', DoubleType(), False),
                      StructField('ZDMBE2', DoubleType(), False)])

# COMMAND ----------

master_df = spark_session.createDataFrame(data=RDD,schema=columns)

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/ADLS1/PREP/1stParty/GSAP_BW/Sensitive/LA_PAYABLE/'):
  #print (i.path)
  path=i.path+'ZOH_APLI.parquet'
  #print(path)
  df=spark.read.parquet(path)
  master_df=master_df.union(df)
  #print(df.count())
  #print(i.name)

# COMMAND ----------

final_table=master_df.createOrReplaceTempView('final_table')

# COMMAND ----------


LA_PAYABLET_df = spark.sql("SELECT * from final_table").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(LA_PAYABLET_df,'dbo.LA_PAYABLE')
