# Databricks notebook source
# DBTITLE 1,Widgets Read
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'


# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/REFM_PL_OU_YTD_NDA_OP"
spark.sql(f""" DROP TABLE IF EXISTS {Database}.REFM_PL_OU_YTD_NDA_OP """)

# COMMAND ----------

# DBTITLE 1,Table Creation Script
spark.sql(f"""
  CREATE TABLE {Database}.REFM_PL_OU_YTD_NDA_OP
  (  
     KPI string,
    COMPANY_CODE string,
    YEAR int,
    PLAN_AMOUNT double,
    PLAN_AMOUNT_USD double,
    PLAN_YTD_AMOUNT double,
    PLAN_YTD_AMOUNT_USD double,
    LEASE_CLASSIFICATION string,
    MONTH string,
    LOCAL_CURRENCY string,
    CREATE_DATE  date,
    UPDATE_DATE date,
    NO_OF_LEASE_SITES int,
    OP_SUBMISSIONS string
  )
 USING DELTA LOCATION '{TableLoc}'
 """)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ndh.REFM_PL_OU_YTD_NDA