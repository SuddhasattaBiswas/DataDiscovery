# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

PLAN_GSAP_MANUAL=spark.read.format("parquet").option("header", "true").load("/mnt/ADLS1//PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/CAPEX_PLAN/GLOBAL/"+str(currentYear)+"/"+str(currentMonth)+"/PLAN_FILE.parquet").createOrReplaceTempView('PLAN_GSAP_MANUAL')

# COMMAND ----------

df_Pivot = spark.sql(''' SELECT
                    Capex_Category_Type_Code AS Capex_Category_Type_Code,
                    Project_Name AS Project_Name,
                    stack(12, 'Jan', Jan_Month, 
                              'Feb', Feb_Month, 
                              'Mar', Mar_Month, 
                              'Apr', Apr_Month,
                              'May', May_Month, 
                              'Jun', Jun_Month,
                              'Jul', Jul_Month, 
                              'Aug', Aug_Month,
                              'Sep', Sep_Month, 
                              'Oct', Oct_Month,
                              'Nov', Nov_Month,
                              'Dec', Dec_Month                              
                          ) as (Calender_MONTH,Capex_PLAN_USD_Amount),
                          --Year(current_timestamp()) as Calendar_year
                            Year(Ingestion_Revision_Date) as Calendar_year
                        FROM PLAN_GSAP_MANUAL
                  ''')

df_Pivot.createOrReplaceTempView("df_PivotTempView") 

# COMMAND ----------

TargetTable = 'STG.CAPEX_PLAN'

PLAN_FILE = spark.sql("SELECT * FROM df_PivotTempView").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(PLAN_FILE,TargetTable)

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
