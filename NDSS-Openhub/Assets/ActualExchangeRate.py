# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

#NDH Database name
NDH_DB = "NDH"

#Delta table names
Actual_Exchange_Rate_NDT_tbl = "Actual_Exchange_Rate_NDT"

#Source locations of Tririga PREP layer data
ZOH_TCURR_MDH = "/mnt/ADLS1/PREP/1stParty/GSAP_BW/NonSensitive/MASTER_FILES/ZOH_TCURR/GLOBAL/ZOH_TCURR.parquet"
ZOH_TCURR_df = spark.read.parquet(ZOH_TCURR_MDH)


# COMMAND ----------

ZOH_TCURR_df.createOrReplaceTempView('ZOH_TCURR')

# COMMAND ----------

df_TcurrNDT = spark.sql('''
select concat(left(e_date, 4),'-',right(left(e_date,6), 2),'-',right(e_date,2) ) as effective_date,* from (
select *,cast(99999999-effective_from_date as int)e_date  from ZOH_TCURR
)a''')

# COMMAND ----------

df_TcurrNDT.createOrReplaceTempView('df_tcurr')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct
# MAGIC Exchanage_Rate_Type_Period 
# MAGIC from df_tcurr
# MAGIC where Exchanage_Rate<0

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE table NDH.Actual_Exchange_Rate_NDT

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO NDH.Actual_Exchange_Rate_NDT
# MAGIC (Exchange_Rate_Period_Type_Code,
# MAGIC From_Currency_Code,
# MAGIC To_Currency_Code,
# MAGIC Effective_From_Date,
# MAGIC Exchanage_Rate,
# MAGIC Ratio_From_Currency_Unit_Number,
# MAGIC Ratio_To_Currency_Unit_Number,
# MAGIC Create_Date)
# MAGIC select 
# MAGIC Exchanage_Rate_Type_Period ,
# MAGIC From_Currency_Code,
# MAGIC To_Currency_Code,
# MAGIC effective_date,
# MAGIC --Exchanage_Rate,
# MAGIC case when To_Currency_Code='USD' then 1/replace(Exchanage_Rate,'-','') else Exchanage_Rate end as Exchanage_Rate,
# MAGIC Ratio_From_Currency_Unit as Ratio_From_Currency_Unit_Number,
# MAGIC Ratio_To_Currency_Unit as Ratio_To_Currency_Unit_Number,
# MAGIC current_timestamp() as Create_Date
# MAGIC from df_tcurr

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE NDH.Actual_Exchange_Rate_NDT

# COMMAND ----------

sqlContext.clearCache()

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC   %sql select * from NDH.Actual_Exchange_Rate_NDT where Effective_From_Date like '%2025%' and To_Currency_Code like '%IDR%'
# MAGIC  
# MAGIC
