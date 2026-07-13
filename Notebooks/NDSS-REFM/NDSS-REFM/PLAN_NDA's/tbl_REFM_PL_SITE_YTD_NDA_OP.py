# Databricks notebook source
# DBTITLE 1,Widgets Read
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'


# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/REFM_PL_SITE_YTD_NDA_OP"
spark.sql(f""" DROP TABLE IF EXISTS {Database}.REFM_PL_SITE_YTD_NDA_OP """)

# COMMAND ----------

# DBTITLE 1,Table Creation Script
spark.sql(f"""
  CREATE TABLE {Database}.REFM_PL_SITE_YTD_NDA_OP
  (  
     KPI string,
    COMPANY_CODE string,
    SITE_ID int,
    LEASE_ID int,
    TRANSACTION_ID int,
    LEASE_CLASSIFICATION string,
    LOCAL_CURRENCY string,
    YEAR int,
    MONTH string,
    NO_OF_LEASE_SITES int,
    PLAN_AMOUNT float,
    PLAN_AMOUNT_USD float,
    PLAN_YTD_AMOUNT float,
    PLAN_YTD_AMOUNT_USD float,
    LEGALLY_COMMITTED string,
    AUTO_RENEWAL_Y_N string,
    CREATE_DATE  date,
    UPDATE_DATE date,
    OP_SUBMISSIONS string)
 USING DELTA LOCATION '{TableLoc}'
 """)

# COMMAND ----------

#dbutils.fs.rm("/mnt/ADLS2/NDH/Sensitive/REFM_PL_SITE_FY_NDA",recurse=True)

# COMMAND ----------

dbutils.notebook.exit('Success')