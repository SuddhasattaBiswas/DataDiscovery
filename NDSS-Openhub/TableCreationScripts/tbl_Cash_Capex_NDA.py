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

TableLoc = DBLoc + "/Cash_Capex_NDA"
spark.sql(""" DROP TABLE IF EXISTS {0}.Cash_Capex_NDA """.format(Database))

# COMMAND ----------

spark.sql("""
  CREATE TABLE {0}.Cash_Capex_NDA 
  (  
    Ingestion_Revision_Date Timestamp,
    Country_Name STRING,
    Country_Code STRING,
    Company_Code STRING,
    CAPIN_Unique_Id STRING,
    FISCAL_Period STRING,
    Date_Year STRING,
    Date_Month STRING,
    ActualRate STRING,
    PlanRate STRING,
    Currency_Code STRING,
    Project_Code STRING,
    Project_Text STRING,
    CAPIN_Type_Project_Code STRING,
    CAPIN_Type_Project_Text STRING,
    CAPIN_Type_Child_Code STRING,
    CAPIN_Type_Child_Text STRING,
    Entry_Text STRING,
    Project_Country STRING)
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

dbutils.notebook.exit('Success')