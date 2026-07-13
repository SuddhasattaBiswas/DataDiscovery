# Databricks notebook source
Database = 'NDH'
DBLoc = '/mnt/ADLS2/NDH/Sensitive'

# COMMAND ----------

TableLoc = DBLoc + "/Baseload_Yearly_Company_Code_NDT"
spark.sql(""" DROP TABLE IF EXISTS {0}.Baseload_Yearly_Company_Code_NDT """.format(Database))

# COMMAND ----------

spark.sql("""
  CREATE TABLE {0}.Baseload_Yearly_Company_Code_NDT
  (  
     KPI_Type_Text string,
Company_Code string,
BaseLoad_LC_Amount double,
BaseLoad_USD_Amount double,
Growth_Sustain_Split_Text string,
Calendar_Year int,
Create_Date timestamp,
Update_Date timestamp,
Local_Currency_Code string,
BaseLoad_Type int
     )
 USING DELTA 
 LOCATION '{1}'
"""
.format(Database,TableLoc))

# COMMAND ----------

dbutils.notebook.exit('Success')
