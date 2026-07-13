# Databricks notebook source
# DBTITLE 1,Widgets Read
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'

# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/REFM_AC_NDT"

spark.sql(""" DROP TABLE IF EXISTS {0}.REFM_AC_NDT """.format(Database))

# COMMAND ----------

# DBTITLE 1,Table Creation Script
spark.sql("""
  CREATE TABLE {0}.REFM_AC_NDT
  (  
     KPI string,
SITE_ID string,
SITE_NAME string,    
LEASE_ID string,
TRANSACTION_ID string,
COMPANY_CODE string,
YEAR int,
MONTH string,
LEASE_CLASSIFICATION string,
ACTUAL_AMOUNT double,
ACTUAL_AMOUNT_USD double,
LEGALLY_COMMITTED string,
LOCAL_CURRENCY string,
LOAD_DATE timestamp,
UPDATE_DATE timestamp,
NO_OF_LEASE_SITES int,
TEXT string,
POSTING_PERIOD INT,
PROFIT_CENTER INT,
CONTRACT_NUMBER STRING,
G_L_ACCOUNT STRING,
ASSIGNMENT STRING,
FLOW_TYPE STRING,
DOCUMENT_TYPE STRING,
GROWTH_SUSTAIN_SPLIT_TEXT string
     )
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))
