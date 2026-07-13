# Databricks notebook source
# DBTITLE 1,Widgets Read
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'


# COMMAND ----------

# DBTITLE 1,Drop table if exists
TableLoc = DBLoc + "/REFM_PL_OU_FY_NDA_OP"
spark.sql(""" DROP TABLE IF EXISTS {0}.REFM_PL_OU_FY_NDA_OP """.format(Database))

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE {Database}.REFM_PL_OU_FY_NDA_OP
  (  
      KPI string,
    COMPANY_CODE string,
    YEAR int,
    PLAN_AMOUNT double,
    PLAN_AMOUNT_USD double,
    LEASE_CLASSIFICATION string,
    LOCAL_CURRENCY string,
    CREATE_DATE timestamp,
    UPDATE_DATE timestamp,
    NO_OF_LEASE_SITES int,
    OP_SUBMISSIONS string
  )
 USING DELTA LOCATION '{TableLoc}'
 """)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe NDH.REFM_PL_OU_FY_NDA_OP

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from NDH.REFM_PL_OU_FY_NDA