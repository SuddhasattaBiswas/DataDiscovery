# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

# COMMAND ----------

currentMonth = datetime.now().strftime('%m')
currentYear = datetime.now().year

# COMMAND ----------

LE_GSAP_MANUAL=spark.read.format("parquet").option("header", "true").load("/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_GSAP/Sensitive/CAPEX_LE/GLOBAL/"+str(currentYear)+"/"+str(currentMonth)+"/LE_FILE.parquet").createOrReplaceTempView('LE_GSAP_MANUAL')

# COMMAND ----------

df_Pivot = spark.sql(''' SELECT
                    Ingestion_Revision_Date AS Ingestion_DateTime, 
                    Capex_Category_Type_Code AS Capex_Category_Type_Code,
                    Project_Name AS Project_Name,
                    Currency_Code AS Currency_Code,
                    stack(12, 'Jan', Jan_Amount, 
                              'Feb', Feb_Amount, 
                              'Mar', Mar_Amount, 
                              'Apr', Apr_Amount,
                              'May', May_Amount, 
                              'Jun', Jun_Amount,
                              'Jul', Jul_Amount, 
                              'Aug', Aug_Amount,
                              'Sep', Sep_Amount, 
                              'Oct', Oct_Amount,
                              'Nov', Nov_Amount,
                              'Dec', Dec_Amount                              
                          ) as (Calender_MONTH, VALUE),
                          Year(Ingestion_Revision_Date) as Calendar_year
                        FROM LE_GSAP_MANUAL
                  ''')

df_Pivot.createOrReplaceTempView("df_PivotTempView")                  

# COMMAND ----------

df_final = spark.sql('''select Capex_Category_Type_Code,
                              Project_Name,
                              Currency_Code,
                              Calender_MONTH,
                              VALUE,
                              Calendar_year,
                              case when Calender_MONTH='Jan' and VALUE is not null then 1
                              when Calender_MONTH='Feb' and VALUE is not null  then 2
                              when Calender_MONTH='Mar' and VALUE is not null  then 3
                              when Calender_MONTH='Apr' and VALUE is not null  then 4
                              when Calender_MONTH='May' and VALUE is not null  then 5
                              when Calender_MONTH='Jun' and VALUE is not null  then 6
                              when Calender_MONTH='Jul' and VALUE is not null  then 7
                              when Calender_MONTH='Aug' and VALUE is not null  then 8
                              when Calender_MONTH='Sep' and VALUE is not null  then 9
                              when Calender_MONTH='Oct' and VALUE is not null  then 10
                              when Calender_MONTH='Nov' and VALUE is not null  then 11
                              when Calender_MONTH='Dec' and VALUE is not null  then 12 else 99 end as LE_month_no,
                              Ingestion_DateTime 
                              from df_PivotTempView
                        ''')
df_final.createOrReplaceTempView("df_finalTempView")

# COMMAND ----------

df_final = spark.sql('''select concat('LE',4 ) as LE,
case  --month(current_timestamp())  in(1,2,3) then 3 
 when LE_month_no in(4,5,6) then 6
 when LE_month_no in(7,8,9) then 9
 when LE_month_no in(10,11,12) then 12 else 3 end as Q_month,
 year(Ingestion_DateTime)  as Q_year,
 * from df_finalTempView where LE_month_no!=99
 ''')
df_final.createOrReplaceTempView("df_finalView1")

# COMMAND ----------

df_quarter = spark.sql('''select  *,
 case when month(current_timestamp())   in(4,5,6) then 6
 when month(current_timestamp()) in(7,8,9) then 9
 when month(current_timestamp()) in(10,11,12) then 12 else 3 end as current_qtr from df_finalView1
 ''')
df_quarter.display()
df_quarter.createOrReplaceTempView("df_finalView")

# COMMAND ----------

df_final = spark.sql('''
select case when er.EXCHANGE_RATE is null then 1 else er.EXCHANGE_RATE end AS Plan_USD,replace(aer.Exchanage_Rate,'-','') as Actual_USD,aer.Effective_From_Date,le.* from df_finalView le
left join ndh.exchange_rates_ndt er on le.Currency_Code=er.FROM_CURRENCY and er.TO_CURRENCY='USD' and right(EXCHANGE_DATE,4)=Q_year
left join NDH.Actual_Exchange_Rate_NDT aer on le.Currency_Code=aer.from_currency_code and aer.to_currency_code='USD' and Exchange_Rate_Period_Type_Code='ZQ1' and year(Effective_From_Date)=Q_year where month(Effective_From_Date)=current_qtr''')
df_final1=df_final.drop("current_qtr")
df_final1.createOrReplaceTempView("df_finalSQL")    

# COMMAND ----------

view_cnt = spark.sql(""" select count(*) from df_finalSQL""").collect()[0][0]
print(view_cnt)
if (view_cnt == 0): 
  raise Exception("No data is availabe")

# COMMAND ----------

TargetTable = 'STG.CAPEX_LE'

CAPEX_LE = spark.sql("SELECT * FROM df_finalSQL").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(CAPEX_LE,TargetTable)

# COMMAND ----------

# DBTITLE 1,Clearing cache
sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')

