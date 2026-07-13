# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("MountPath","")
MountPath = dbutils.widgets.get("MountPath")

dbutils.widgets.text("Database","")
Database = dbutils.widgets.get("Database")

dbutils.widgets.text("ADLSFolderPath","")
ADLSFolderPath = dbutils.widgets.get("ADLSFolderPath") 

DBLoc = MountPath + ADLSFolderPath
print(DBLoc)

# COMMAND ----------

TableLoc = DBLoc + "/ROU_ASSET_NBV_NDT"
print(TableLoc)
spark.sql(""" DROP TABLE IF EXISTS {0}.ROU_ASSET_NBV_NDT """.format(Database))
dbutils.fs.rm(TableLoc,True)

# COMMAND ----------

spark.sql("""
  CREATE TABLE {0}.ROU_ASSET_NBV_NDT 
  (  
     COMPANY_CODE STRING,
     CREATE_DATE TIMESTAMP,
     SITE_CODE STRING,
     PERIOD STRING,
     ROU_ASSET_NET_BOOK_VALUE decimal(18,5),
     CURRENCY_CODE STRING,
     COUNTRY STRING
  )
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

dbutils.notebook.exit('Success')