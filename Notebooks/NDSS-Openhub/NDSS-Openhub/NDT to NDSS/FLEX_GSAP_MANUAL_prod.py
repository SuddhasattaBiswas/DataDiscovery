# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

FLEX_GSAP_MANUAL=spark.read.format("parquet").option("header", "true").load("/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/CAPEX_FLEX/GLOBAL/"+str(currentYear)+"/"+"/"+str(currentMonth)+"/FLEX_FILE.parquet").createOrReplaceTempView('FLEX_GSAP_MANUAL')

# COMMAND ----------

df_Pivot = spark.sql(''' SELECT
                     Ingestion_Revision_Date,
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
                          ) as (Calender_MONTH,Capex_Flex_USD_Amount),
                          --Year(current_timestamp()) as Calendar_year
                            Year(Ingestion_Revision_Date) as Calendar_year
                        FROM FLEX_GSAP_MANUAL
                  ''')

df_Pivot.createOrReplaceTempView("df_PivotTempView")    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from df_PivotTempView ;

# COMMAND ----------

df_final = spark.sql('''select *,
case when MONTH(Ingestion_Revision_Date) in(1,2,3) then 'Q1'
 when MONTH(Ingestion_Revision_Date) in(4,5,6) then 'Q2'
 when MONTH(Ingestion_Revision_Date) in(7,8,9) then 'Q3'
 when MONTH(Ingestion_Revision_Date) in(10,11,12) then 'Q4' end as Quarter_Number
 from df_PivotTempView''')
df_final.createOrReplaceTempView("df_finalView")

# COMMAND ----------

view_cnt = spark.sql(""" select count(*) from df_finalView""").collect()[0][0]
print(view_cnt)
if (view_cnt == 0): 
  raise Exception("No data is availabe")

# COMMAND ----------

TargetTable = 'STG.CAPEX_FLEX'

FLEX_FILE = spark.sql("SELECT * FROM df_finalView").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(FLEX_FILE,TargetTable)

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')
