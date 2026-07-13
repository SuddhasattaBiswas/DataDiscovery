# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("MountPath","")
MountPath = dbutils.widgets.get("MountPath")

dbutils.widgets.text("Database","")
Database = dbutils.widgets.get("Database")

dbutils.widgets.text("ADLSFolderPath","")
ADLSFolderPath = dbutils.widgets.get("ADLSFolderPath") 

DBLoc = MountPath + ADLSFolderPath

# COMMAND ----------

TableLoc = DBLoc + "/Actual_Exchange_Rate_NDT"
spark.sql(""" DROP TABLE IF EXISTS {0}.Actual_Exchange_Rate_NDT """.format(Database))

# COMMAND ----------

spark.sql("""
  CREATE TABLE {0}.Actual_Exchange_Rate_NDT 
  (  
     Exchange_Rate_Period_Type_Code STRING,
     From_Currency_Code STRING,
     To_Currency_Code STRING,
     Effective_From_Date date,
     Exchanage_Rate Double,
     Ratio_From_Currency_Unit_Number Double,
     Ratio_To_Currency_Unit_Number Double,
     Create_Date TIMESTAMP)
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

dbutils.notebook.exit('Success')