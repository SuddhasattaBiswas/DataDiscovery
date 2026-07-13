# Databricks notebook source
# MAGIC %md
# MAGIC Details ::::::::::::::: Creating logic for Le replacement with actuals
# MAGIC
# MAGIC
# MAGIC Created By::::::::::::: Prabalya Suresh
# MAGIC
# MAGIC Modified By::::::::::::
# MAGIC
# MAGIC Modified On::::::::::::
# MAGIC
# MAGIC Modificaiton Details:::
# MAGIC
# MAGIC Dependent Tables::: 

# COMMAND ----------

# DBTITLE 1,Import 
from pyspark.sql.functions import col, lit, current_timestamp, current_date, sum, row_number,concat,lit,when,length,split,unix_timestamp,from_unixtime
from pyspark.sql.types import IntegerType, StringType, StructType,StructField, Row
from pyspark.sql import Window
from calendar import monthrange
import sys

# COMMAND ----------

# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

ACT_LE_DATA = "/mnt/ADLS2/NDH/Sensitive/REFM_LE_Actual_NDA"

# COMMAND ----------

tempTableName = 'adv_ac'
df = spark.sql('''select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         --LE_TYPE_CODE,
                         Lease_Classification_Code,
                         SITE_ID,
                         LEASE_ID,
                         SITE_NAME,
                         TRANSACTION_ID,
                         LEASE_SITE_COUNT,
                         --VALUE_TYPE_CODE,
                         'ADVANCE_PAYMENT' AS VALUE_TYPE,
                         Local_Currency_Code,
                         Calendar_Month,
                         --year(Calendar_Year) as Calendar_Year, changed in prod
                         Calendar_Year as Calendar_Year,
                         'ACTUAL' as LE_ACTUAL_PLAN,
                         Actual_LC_Amount,
                         Actual_USD_Amount,
                         Flow_Type_Code,
                         PROFIT_CENTER,
                         LEGALLY_COMMITTED_INDICATOR,
                         GROWTH_SUSTAIN_SPLIT_TEXT
                         FROM ndh.actual_ndt 
                         ''')

df.createOrReplaceTempView(tempTableName)


# COMMAND ----------

col_list1 = ['Actual_LC_Amount','Actual_USD_Amount']
df_window = df

windowSpec = (Window.partitionBy('LEASE_ID','COMPANY_CODE','SITE_ID','Calendar_Month','Calendar_Year','Lease_Classification_Code','LE_ACTUAL_PLAN'))
grouping_window = Window.partitionBy('LEASE_ID','COMPANY_CODE','SITE_ID','Calendar_Month','Calendar_Year','Lease_Classification_Code','LE_ACTUAL_PLAN').orderBy('Actual_LC_Amount','Actual_USD_Amount')

# COMMAND ----------

for c in col_list1: 
              df_window = df_window.withColumn(c,sum(c).over(windowSpec))
                                   

# COMMAND ----------

df_window = df_window.withColumn('grprank',row_number().over(grouping_window)).where(col('grprank') == 1)\
                                    .drop(col('grprank'))

# COMMAND ----------

from datetime import date

tod_date = date.today()

curr_year = tod_date.year
print(curr_year)

# COMMAND ----------

month_year = [{"month":"JAN","year":curr_year},	{"month":"FEB","year":curr_year},	{"month":"MAR","year":curr_year},	{"month":"APR","year":curr_year},	{"month":"MAY","year":curr_year},	{"month":"JUN","year":curr_year},	{"month":"JUL","year":curr_year},	{"month":"AUG","year":curr_year},	{"month":"SEP","year":curr_year},	{"month":"OCT","year":curr_year},	{"month":"NOV","year":curr_year},	{"month":"DEC","year":curr_year}]

# COMMAND ----------

df2 = spark.createDataFrame(month_year)

# COMMAND ----------

df2.withColumn("year",df2.year.cast('int'))

# COMMAND ----------

df2.createOrReplaceTempView('table_mon')
df_window.createOrReplaceTempView('ac_data')

# COMMAND ----------

tempTableName = 'adv_ac2'
dfActual = spark.sql(''' select distinct ac.company_code,
 ac.site_id,
 ac.value_type,
 ac.Lease_Classification_Code,
 ac.le_actual_plan, 
 ac.lease_id,
 ac.Local_Currency_Code,
 ac.KPI_TYPE_TEXT,
 ac.SITE_NAME,
 ac.TRANSACTION_ID,
 ac.LEASE_SITE_COUNT,
 trim(upper(substring(mn.month,0,3))) as month1, mn.year,
 case when ac.Calendar_Month = trim(upper(substring(mn.month,0,3))) then ac.Actual_LC_Amount
 else 0 end as Actual_Loc_Curr1, 
 case when ac.Calendar_Month = trim(upper(substring(mn.month,0,3))) then ac.Actual_USD_Amount
 else 0 end as Actual_USD1,
 ac.Flow_Type_Code,
 ac.PROFIT_CENTER,
 ac.LEGALLY_COMMITTED_INDICATOR,
 ac.GROWTH_SUSTAIN_SPLIT_TEXT
from table_mon mn
 left join 
(select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         --LE_TYPE_CODE,
                         Lease_Classification_Code,
                         SITE_ID,
                         LEASE_ID,
                         SITE_NAME,
                         TRANSACTION_ID,
                         LEASE_SITE_COUNT,
                         --VALUE_TYPE_CODE,
                         VALUE_TYPE,
                         Local_Currency_Code,
                         Calendar_Month,
                         Calendar_Year,
                         LE_ACTUAL_PLAN,
                         Actual_LC_Amount,
                         Actual_USD_Amount,
                         Flow_Type_Code,
                         PROFIT_CENTER,
                         LEGALLY_COMMITTED_INDICATOR,
                         GROWTH_SUSTAIN_SPLIT_TEXT  from ac_data )ac
on 
 ac.Calendar_Year = ac.Calendar_Year 
''').registerTempTable(tempTableName)


# COMMAND ----------

# tempTableName = 'adv_ac2'
# df3 = spark.sql(''' SELECT * , 
#                 CASE 
#                 when Calendar_Month = 'JAN' then 01
#                 when Calendar_Month = 'FEB' then 02
#                 when Calendar_Month = 'MAR' then 03
#                 when Calendar_Month = 'APR' then 04
#                 when Calendar_Month = 'MAY' then 05
#                 when Calendar_Month = 'JUN' then 06
#                 when Calendar_Month = 'JUL' then 07
#                 when Calendar_Month = 'AUG' then 08
#                 when Calendar_Month = 'SEP' then 09
#                 when Calendar_Month = 'OCT' then 10
#                 when Calendar_Month = 'NOV' then 11
#                 when Calendar_Month = 'DEC' then 12
#                 END as NO_MONTH
#                 from adv_ac
#  ''').registerTempTable(tempTableName)

# COMMAND ----------

tempTableName = 'adv_ac3'
df3 = spark.sql(''' SELECT * , 
                CASE 
                when month1 = 'JAN' then 01
                when month1 = 'FEB' then 02
                when month1 = 'MAR' then 03
                when month1 = 'APR' then 04
                when month1 = 'MAY' then 05
                when month1 = 'JUN' then 06
                when month1 = 'JUL' then 07
                when month1 = 'AUG' then 08
                when month1 = 'SEP' then 09
                when month1 = 'OCT' then 10
                when month1 = 'NOV' then 11
                when month1 = 'DEC' then 12
                END as NO_MONTH
                from adv_ac2
 ''').registerTempTable(tempTableName)

# COMMAND ----------

# this is when we remove current year logic in actuals
# final_adv_ac = spark.sql('''select ac.company_code,
#  ac.site_id,
#  ac.value_type,
#  ac.Lease_Classification_Code,
#  ac.le_actual_plan, 
#  ac.lease_id,
#  ac.Local_Currency_Code,
#  ac.Calendar_Month, ac.Calendar_Year,
#  sum(ac.Actual_LC_Amount) AS Actual_Loc_Curr1, 
#  sum(ac.Actual_USD_Amount) AS Actual_USD_Amount,
#  ac.Flow_Type_Code,
#  ac.profit_center,no_month
#  from adv_ac2 ac group by ac.company_code, ac.site_id, ac.value_type,
#  ac.Lease_Classification_Code,
#  ac.le_actual_plan, 
#  ac.lease_id,
#  ac.Local_Currency_Code,
#  ac.Calendar_Month, ac.Calendar_Year,ac.Flow_Type_Code,ac.profit_center,no_month order by ac.company_code, ac.site_id,ac.lease_id,ac.Lease_Classification_Code,ac.Calendar_Year,ac.Calendar_Month''').createOrReplaceTempView('final_adv_ac')

# COMMAND ----------

final_adv_ac = spark.sql('''select ac.company_code,
ac.KPI_TYPE_TEXT,
 ac.site_id,
 ac.SITE_NAME,
 ac.value_type,
 ac.Lease_Classification_Code,
 ac.le_actual_plan, 
 ac.lease_id,
 ac.Local_Currency_Code,
 ac.month1, ac.year,
 sum(ac.Actual_Loc_Curr1) AS Actual_Loc_Curr1, 
 sum(ac.Actual_USD1) AS Actual_USD1,
 ac.Flow_Type_Code,
 ac.profit_center,no_month,
 ac.LEGALLY_COMMITTED_INDICATOR,
 ac.GROWTH_SUSTAIN_SPLIT_TEXT,
 ac.TRANSACTION_ID,
 ac.LEASE_SITE_COUNT
 from adv_ac3 ac group by ac.company_code, ac.site_id,ac.SITE_NAME, ac.value_type,
 ac.Lease_Classification_Code,
 ac.le_actual_plan, 
 ac.lease_id,
 ac.Local_Currency_Code,ac.KPI_TYPE_TEXT,
 ac.month1, ac.year,ac.Flow_Type_Code,ac.profit_center,no_month,ac.LEGALLY_COMMITTED_INDICATOR,
 ac.GROWTH_SUSTAIN_SPLIT_TEXT,ac.TRANSACTION_ID,ac.LEASE_SITE_COUNT order by ac.company_code, ac.site_id,ac.lease_id,ac.Lease_Classification_Code,ac.year,ac.month1''')
# .createOrReplaceTempView('final_adv_ac')

# COMMAND ----------

final_adv_ac.select(["*", lit(None).alias("Auto_Renewal_Indicator"), lit(None).alias("LE_Type_Code")]).createOrReplaceTempView('final_adv_ac')

# COMMAND ----------

tempTableName = 'adv_le1'
df4 = spark.sql('''select ac.COMPANY_CODE,
                        ac.SITE_ID,
                        CASE WHEN UPPER(ac.VALUE_TYPE_CODE) in ('ADVANCE_PAYMENTS','ADVANCE PAYMENTS','ADVANCE_PAYMENT','ADVANCE PAYMENT') OR ac.VALUE_TYPE_CODE is null THEN 'ADVANCE_PAYMENT'
                        ELSE ac.VALUE_TYPE_CODE
                        END AS VALUE_TYPE,
                        CASE WHEN ac.LEASE_CLASSIFICATION_CODE IN ('LR','LPG Equipment','LNG Equipment','CON','OPT','Tap UP','Non RE Equipment','CPI','EV OTG FC','LPG EQ','LNG EQ','NON RE') THEN 'LR'
                        WHEN ac.LEASE_CLASSIFICATION_CODE IN ('NTI','EV','NTS','NFR','EV  DEST','EV SFS','EV OTG Hub','EV DEST') THEN 'NTI'
                        ELSE NULL END AS LEASE_CLASSIFICATION_CODE,
                        'LE' AS LE_ACTUAL_PLAN,
                        ac.LEASE_ID,
                        ac.LOCAL_CURRENCY_CODE,
                        ac.CALENDAR_MONTH,
                        --year(ac.CALENDAR_YEAR) as CALENDAR_YEAR, changed in prod
                        ac.CALENDAR_YEAR as CALENDAR_YEAR,
                        CASE WHEN ac.LE_LC_AMOUNT IS NOT NULL THEN (ac.LE_LC_AMOUNT)
                        ELSE 0
                        END AS Actual_Loc_Curr,
                        CASE WHEN ac.LE_USD_AMOUNT IS NOT NULL THEN (ac.LE_USD_AMOUNT)
                        ELSE 0
                        END AS Actual_USD,
                        ac.LE_TYPE_CODE,
                        ac.KPI_TYPE_TEXT,
                        ac.site_name,
                        ac.TRANSACTION_ID,
                        ac.COMPANY_CODE,
                        ac.LEGALLY_COMMITTED_INDICATOR,
                        AUTO_RENEWAL_INDICATOR,
                        LEASE_SITE_COUNT
                       FROM NDH.LE_NDT ac 
                       --where KPI_TYPE_TEXT like 'ADVANCE_PAYMENT' 
                       --AND YEAR = YEAR(current_timestamp())
                       ''')

df4.createOrReplaceTempView(tempTableName)

# COMMAND ----------

tempTableName = 'adv_le2'
df = spark.sql(''' SELECT * , 
                CASE 
                when CALENDAR_MONTH = 'JAN' then 01
                when CALENDAR_MONTH = 'FEB' then 02
                when CALENDAR_MONTH = 'MAR' then 03
                when CALENDAR_MONTH = 'APR' then 04
                when CALENDAR_MONTH = 'MAY' then 05
                when CALENDAR_MONTH = 'JUN' then 06
                when CALENDAR_MONTH = 'JUL' then 07
                when CALENDAR_MONTH = 'AUG' then 08
                when CALENDAR_MONTH = 'SEP' then 09
                when CALENDAR_MONTH = 'OCT' then 10
                when CALENDAR_MONTH = 'NOV' then 11
                when CALENDAR_MONTH = 'DEC' then 12
                END as NO_MONTH
                from adv_le1
 ''').registerTempTable(tempTableName)

# COMMAND ----------

# #changing on 22 nov
# final_adv_le = spark.sql('''select ac.COMPANY_CODE,
#                         ac.SITE_ID,
#                         ac.VALUE_TYPE,
#                         ac.LEASE_CLASSIFICATION_CODE,
#                         ac.LE_ACTUAL_PLAN,
#                         ac.LEASE_ID,
#                         ac.LOCAL_CURRENCY_CODE,
#                         ac.CALENDAR_MONTH,
#                         ac.CALENDAR_YEAR,
#                         --SUM(ac.Actual_Loc_Curr) AS Actual_Loc_Curr,
#                         ac.Actual_Loc_Curr AS Actual_Loc_Curr,
#                         --SUM(ac.Actual_USD) AS Actual_USD,
#                         ac.Actual_USD AS Actual_USD,
#                         ac.LE_TYPE_CODE,
#                         ac.NO_MONTH,
#                         ac.KPI_TYPE_TEXT,
#                         ac.site_name,
#                         ac.TRANSACTION_ID,
#                         ac.LEGALLY_COMMITTED_INDICATOR,
#                         ac.LEASE_SITE_COUNT,
#                         ac.AUTO_RENEWAL_INDICATOR
#  from adv_le2 ac 
#  group by ac.LE_TYPE_CODE,ac.company_code, ac.site_id, ac.value_type,
#  ac.LEASE_CLASSIFICATION_CODE,
#  ac.le_actual_plan, 
#  ac.lease_id,
#  ac.LOCAL_CURRENCY_CODE,
#  ac.CALENDAR_MONTH, ac.CALENDAR_YEAR,no_month,KPI_TYPE_TEXT,site_name,TRANSACTION_ID,LEGALLY_COMMITTED_INDICATOR,LEASE_SITE_COUNT,AUTO_RENEWAL_INDICATOR order by ac.LE_TYPE_CODE,ac.company_code, ac.site_id,ac.lease_id,ac.LEASE_CLASSIFICATION_CODE,ac.CALENDAR_YEAR,ac.CALENDAR_MONTH''').createOrReplaceTempView('final_adv_le')


# COMMAND ----------

#changing on 22 nov
final_adv_le = spark.sql('''select ac.COMPANY_CODE,
                        ac.SITE_ID,
                        ac.VALUE_TYPE,
                        ac.LEASE_CLASSIFICATION_CODE,
                        ac.LE_ACTUAL_PLAN,
                        ac.LEASE_ID,
                        ac.LOCAL_CURRENCY_CODE,
                        ac.CALENDAR_MONTH,
                        ac.CALENDAR_YEAR,
                        --SUM(ac.Actual_Loc_Curr) AS Actual_Loc_Curr,
                        ac.Actual_Loc_Curr AS Actual_Loc_Curr,
                        --SUM(ac.Actual_USD) AS Actual_USD,
                        ac.Actual_USD AS Actual_USD,
                        ac.LE_TYPE_CODE,
                        ac.NO_MONTH,
                        ac.KPI_TYPE_TEXT,
                        ac.site_name,
                        ac.TRANSACTION_ID,
                        ac.LEGALLY_COMMITTED_INDICATOR,
                        ac.LEASE_SITE_COUNT,
                        ac.AUTO_RENEWAL_INDICATOR
 from adv_le2 ac 
  order by ac.LE_TYPE_CODE,ac.company_code, ac.site_id,ac.lease_id,ac.LEASE_CLASSIFICATION_CODE,ac.CALENDAR_YEAR,ac.CALENDAR_MONTH''').createOrReplaceTempView('final_adv_le')


# COMMAND ----------

# MAGIC %sql select sum(Actual_Loc_Curr),sum(Actual_USD) from final_adv_le where LE_TYPE_CODE = 'LE2' and CALENDAR_YEAR = 2022 and CALENDAR_MONTH NOT in  ('JAN','FEB','MAR')

# COMMAND ----------

#USING UNION

ADV_LE1_FINAL1 = spark.sql('''
                         select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         LEASE_CLASSIFICATION_CODE,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         LOCAL_CURRENCY_CODE,
                         CALENDAR_MONTH,
                         CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         Actual_Loc_Curr AS ADVANCE_PAYMENT_LE,
                         Actual_USD AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_TYPE_CODE,
                         'LE1_ACT' as LE_ACT
                         from FINAL_ADV_LE 
                         WHERE LE_TYPE_CODE='LE1'
                         and CALENDAR_YEAR = YEAR(current_timestamp())''')

ADV_LE1_FINAL1.registerTempTable('ADV_LE1_FINAL')

# COMMAND ----------

#USING UNION
#added current_year filter in final_adv_le 22 nov 
#removed union all, gave union
ADV_LE2_FINAL1 = spark.sql('''
             select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         LEASE_CLASSIFICATION_CODE,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         LOCAL_CURRENCY_CODE,
                         MONTH1 as CALENDAR_MONTH,
                         YEAR as CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=3 then Actual_Loc_Curr1
                         else
                         0 END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=3 then Actual_USD1
                         else 0 END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH, 
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE2_ACT' as LE_ACT
                         
                         from FINAL_ADV_AC
                         
                         union 
                         
                         select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         LEASE_CLASSIFICATION_CODE,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         LOCAL_CURRENCY_CODE,
                         CALENDAR_MONTH,
                         CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=3 then 0
                         else
                         Actual_Loc_Curr END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=3 then 0
                         else Actual_USD END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE2_ACT' as LE_ACT
                         from FINAL_ADV_LE 
                         WHERE LE_TYPE_CODE='LE2'
                         and CALENDAR_YEAR = YEAR(current_timestamp())''')

ADV_LE2_FINAL1.registerTempTable('ADV_LE2_FINAL')

# COMMAND ----------

#USING UNION

ADV_LE3_FINAL1 = spark.sql('''
             select COMPANY_CODE,
             KPI_TYPE_TEXT,
                         Lease_Classification_Code,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         Local_Currency_Code,
                         MONTH1 as CALENDAR_MONTH,
                         YEAR as CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=6 then Actual_Loc_Curr1
                         else
                         0 END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=6 then Actual_USD1
                         else 0 END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE3_ACT' as LE_ACT
                         from FINAL_ADV_AC
                         
                         union 
                         
                         select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         Lease_Classification_Code,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         Local_Currency_Code,
                         CALENDAR_MONTH,
                         CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=6 then 0
                         else
                         Actual_Loc_Curr END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=6 then 0
                         else Actual_USD END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE3_ACT' as LE_ACT
                         from FINAL_ADV_LE
                         WHERE LE_TYPE_CODE='LE3'
                         and CALENDAR_YEAR = YEAR(current_timestamp())''')

ADV_LE3_FINAL1.registerTempTable('ADV_LE3_FINAL')

# COMMAND ----------

#USING UNION

ADV_LE4_FINAL1 = spark.sql('''
             select COMPANY_CODE,
             KPI_TYPE_TEXT,
                         Lease_Classification_Code,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         Local_Currency_Code,
                         MONTH1 as CALENDAR_MONTH,
                         YEAR as CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=9 then Actual_Loc_Curr1
                         else
                         0 END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=9 then Actual_USD1
                         else 0 END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE4_ACT' as LE_ACT
                         from FINAL_ADV_AC
                         
                         union 
                         
                         select COMPANY_CODE,
                         KPI_TYPE_TEXT,
                         Lease_Classification_Code,
                         VALUE_TYPE,
                         SITE_ID,
                         SITE_NAME,
                         LEASE_ID,
                         Local_Currency_Code,
                         CALENDAR_MONTH,
                         CALENDAR_YEAR,
                         'LE' as LE_ACTUAL_PLAN1,
                         case 
                         when no_month <=9 then 0
                         else
                         Actual_Loc_Curr END AS ADVANCE_PAYMENT_LE,
                         case
                         when no_month <=9 then 0
                         else Actual_USD END AS ADVANCE_PAYMENT_LE_USD,
                         NO_MONTH,
                         TRANSACTION_ID,
                         LEGALLY_COMMITTED_INDICATOR,
                         LEASE_SITE_COUNT,
                         AUTO_RENEWAL_INDICATOR,
                         LE_Type_Code,
                         'LE4_ACT' as LE_ACT
                         from FINAL_ADV_LE 
                         WHERE LE_TYPE_code='LE4'
                         and CALENDAR_YEAR = YEAR(current_timestamp())''')

ADV_LE4_FINAL1.registerTempTable('ADV_LE4_FINAL')

# COMMAND ----------

try:
  LE = []
  LE = spark.sql('''
                  Select distinct LE_TYPE_code from ndh.le_ndt 
                  --where KPI_TYPE_TEXT = 'ADVANCE_PAYMENT' 
                  order by LE_TYPE_code desc
                  '''
                ).rdd.flatMap(lambda x: x).collect()
  print(LE)
  
except Exception as e:
  msg = str(e)
  LE = [] 
  print(msg)
  dbutils.notebook.exit("no data for LE is available")

# COMMAND ----------

if LE[0] == 'LE1':
  ADV_LE_FINAL1 = ADV_LE1_FINAL1
  print('LE1')
if LE[0] == 'LE2':
  ADV_LE_FINAL1 = ADV_LE2_FINAL1
  print('LE2')
if LE[0] == 'LE3':
  ADV_LE_FINAL1 = ADV_LE3_FINAL1
  print('LE3')
if LE[0] == 'LE4':
  ADV_LE_FINAL1 = ADV_LE4_FINAL1
  print('LE4')


# COMMAND ----------

ADV_LE_FINAL1 = ADV_LE1_FINAL1.union(ADV_LE2_FINAL1).union(ADV_LE3_FINAL1).union(ADV_LE4_FINAL1)


# COMMAND ----------

# ADV_LE_FINAL1 = ADV_LE_FINAL1.withColumn("CALENDAR_MONTH",when ((length(ADV_LE_FINAL1.CALENDAR_MONTH)) < 1 , "DEC" ).otherwise(ADV_LE_FINAL1.CALENDAR_MONTH)).fillna('DEC',subset= ['CALENDAR_MONTH']).drop_duplicates()

# COMMAND ----------

ADV_LE_FINAL1.drop_duplicates()

# COMMAND ----------

ADV_LE_FINAL1.registerTempTable('ADV_LE_FINAL')

# COMMAND ----------


 ADV_LE_FINAL1.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(ACT_LE_DATA)

# COMMAND ----------

# MAGIC %sql desc delta.`/mnt/ADLS2/NDH/Sensitive/REFM_LE_Actual_NDA`

# COMMAND ----------

#  %sql delete from delta.`/mnt/ADLS2/NDH/NonSensitive/NDSS/REFM/LE_ACT_DATA`

# COMMAND ----------

# tempTableName = 'ACT_LE'

# df = spark.sql(''' SELECT COMPANY_CODE AS company_code,
#                    LEASE_ID AS LEASE_VERSION_ID,
#                    VALUE_TYPE AS value_type,
#                    SITE_ID as site_id,
#                    SITE_NAME as site_name,
#                    LOCAL_CURRENCY_CODE AS Currency,
#                    LEASE_CLASSIFICATION_CODE AS Lease_Classification,
#                    ADVANCE_PAYMENT_LE AS Advance_Payment_LE,
#                    ADVANCE_PAYMENT_LE_USD AS Advance_Payment_LE_USD,
#                    CALENDAR_MONTH AS month,
#                    Calendar_YEAR AS Year,
#                    NO_MONTH 
#                    FROM ADV_LE_FINAL
# ''')

# df.registerTempTable(tempTableName)

# COMMAND ----------

# def toStagingNDT(assetName, ndhSchema,assetType , Schema , TableName):
#   assetQuery = '''select * from ''' +assetName
#   print(assetQuery)
#   inputDf=spark.sql(assetQuery)
#   inputDf.write.mode('overwrite').jdbc(url=jdbcUrlNvm, table=Schema +'.'+ TableName, properties=None)
#   return inputDf
  

# COMMAND ----------

# toStagingNDT('ACT_LE','NDH','NDA','REFM','DB1_ADV_LE')


# COMMAND ----------

dbutils.notebook.exit('Success')
