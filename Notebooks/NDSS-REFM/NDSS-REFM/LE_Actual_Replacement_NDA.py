# Databricks notebook source
# MAGIC %md
# MAGIC DETAILS: LE_Actual_Replacement_NDA
# MAGIC
# MAGIC CREATED ON: 28/11/2022

# COMMAND ----------

from pyspark.sql.functions import (col,lit,current_timestamp,upper,to_date,round,date_format,when,filter,regexp_replace,from_unixtime,unix_timestamp)
from pyspark.sql import Window


# COMMAND ----------

# DBTITLE 1,Widgets Read
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'

# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/LE_Actual_Replacement_NDA"
spark.sql(f""" DROP TABLE IF EXISTS {Database}.LE_Actual_Replacement_NDA """)

# COMMAND ----------

# DBTITLE 1,Table Creation Script
spark.sql(f"""
  CREATE TABLE {Database}.LE_Actual_Replacement_NDA
  (  
    COMPANY_CODE string,
    LEASE_CLASSIFICATION_CODE string,
    VALUE_TYPE string,
    SITE_ID string,
    SITE_NAME string,
    LEASE_ID string,
    LOCAL_CURRENCY_CODE string,
    CALENDAR_MONTH string,
    CALENDAR_YEAR string,
    LE_ACTUAL_PLAN1 string,
    LE_AMOUNT double,
    LE_USD_AMOUNT double,
    NO_MONTH  int,
    TRANSACTION_ID string,
    LEGALLY_COMMITTED_INDICATOR string,    
    LEASE_SITE_COUNT int,
    AUTO_RENEWAL_INDICATOR string,
    KPI_TYPE_TEXT string,
    LE_Type_Code string,
    LE_ACT string,
    CREATE_DATE  date,
    UPDATE_DATE date
    )
    
 USING DELTA LOCATION '{TableLoc}'
 """)

# COMMAND ----------


LE_ACT_df = spark.sql('''select   COMPANY_CODE,
    LEASE_CLASSIFICATION_CODE,
    VALUE_TYPE,
    SITE_ID,
    SITE_NAME,
    LEASE_ID,
    LOCAL_CURRENCY_CODE,
    CALENDAR_MONTH,
    CALENDAR_YEAR,
    LE_ACTUAL_PLAN1,
    ADVANCE_PAYMENT_LE as LE_AMOUNT,
    ADVANCE_PAYMENT_LE_USD as LE_USD_AMOUNT,
    NO_MONTH,
    TRANSACTION_ID,
    LEGALLY_COMMITTED_INDICATOR,    
    LEASE_SITE_COUNT,
    AUTO_RENEWAL_INDICATOR,
    KPI_TYPE_TEXT,
    LE_Type_Code,
    LE_ACT
    from delta.`/mnt/ADLS2/NDH/NonSensitive/NDSS/REFM/LE_ACT_DATA` ''')\
                            .withColumn ("CREATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))\
                            .withColumn("UPDATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))

# COMMAND ----------

LE_ACT_df.createOrReplaceTempView("LE_ACT_TABLE")

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into ndh.LE_Actual_Replacement_NDA (COMPANY_CODE,
# MAGIC     LEASE_CLASSIFICATION_CODE,
# MAGIC     VALUE_TYPE,
# MAGIC     SITE_ID,
# MAGIC     SITE_NAME,
# MAGIC     LEASE_ID,
# MAGIC     LOCAL_CURRENCY_CODE,
# MAGIC     CALENDAR_MONTH,
# MAGIC     CALENDAR_YEAR,
# MAGIC     LE_ACTUAL_PLAN1,
# MAGIC     LE_AMOUNT,
# MAGIC     LE_USD_AMOUNT,
# MAGIC     NO_MONTH,
# MAGIC     TRANSACTION_ID,
# MAGIC     LEGALLY_COMMITTED_INDICATOR,    
# MAGIC     LEASE_SITE_COUNT,
# MAGIC     AUTO_RENEWAL_INDICATOR,
# MAGIC     KPI_TYPE_TEXT,
# MAGIC     LE_Type_Code,
# MAGIC     LE_ACT,
# MAGIC     CREATE_DATE,
# MAGIC     UPDATE_DATE)
# MAGIC     select
# MAGIC     COMPANY_CODE,
# MAGIC     LEASE_CLASSIFICATION_CODE,
# MAGIC     VALUE_TYPE,
# MAGIC     SITE_ID,
# MAGIC     SITE_NAME,
# MAGIC     LEASE_ID,
# MAGIC     LOCAL_CURRENCY_CODE,
# MAGIC     CALENDAR_MONTH,
# MAGIC     CALENDAR_YEAR,
# MAGIC     LE_ACTUAL_PLAN1,
# MAGIC     LE_AMOUNT,
# MAGIC     LE_USD_AMOUNT,
# MAGIC     NO_MONTH,
# MAGIC     TRANSACTION_ID,
# MAGIC     LEGALLY_COMMITTED_INDICATOR,    
# MAGIC     LEASE_SITE_COUNT,
# MAGIC     AUTO_RENEWAL_INDICATOR,
# MAGIC     KPI_TYPE_TEXT,
# MAGIC     LE_Type_Code,
# MAGIC     LE_ACT,
# MAGIC     CREATE_DATE,
# MAGIC     UPDATE_DATE
# MAGIC     from LE_ACT_TABLE

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE {Database}.LE_Actual_Replacement_NDA
  (  
    COMPANY_CODE string,
    LEASE_CLASSIFICATION_CODE string,
    VALUE_TYPE string,
    SITE_ID string,
    SITE_NAME string,
    LEASE_ID string,
    LOCAL_CURRENCY_CODE string,
    CALENDAR_MONTH string,
    CALENDAR_YEAR string,
    LE_ACTUAL_PLAN1 string,
    LE_AMOUNT double,
    LE_USD_AMOUNT double,
    NO_MONTH  int,
    TRANSACTION_ID string,
    LEGALLY_COMMITTED_INDICATOR string,    
    LEASE_SITE_COUNT int,
    AUTO_RENEWAL_INDICATOR string,
    KPI_TYPE_TEXT string,
    LE_Type_Code string,
    LE_ACT string,
    CREATE_DATE  date,
    UPDATE_DATE date
    )
    
 USING DELTA LOCATION '{TableLoc}'
 """)
