# Databricks notebook source
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'

# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/REFM_PL_SITE_FY_NDA_OP"
spark.sql(""" DROP TABLE IF EXISTS {0}.REFM_PL_SITE_FY_NDA_OP """.format(Database))

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE {Database}.REFM_PL_SITE_FY_NDA_OP
  (  
    KPI string,
    COMPANY_CODE string,
    SITE_ID int,
    LEASE_ID int,
    Transaction_ID int,
    PLAN_AMOUNT Float,
    PLAN_AMOUNT_USD Float,
    LEASE_CLASSIFICATION string,
    Legally_Committed string,
    YEAR int,
    CREATE_DATE Date,
    UPDATE_DATE Date,
    LOCAL_CURRENCY string,
    NO_OF_LEASE_SITES int,
    Auto_Renewal_Y_N string,
    OP_SUBMISSIONS string
  )
 USING DELTA LOCATION '{TableLoc}'
 """)

# COMMAND ----------

dbutils.notebook.exit('Success')