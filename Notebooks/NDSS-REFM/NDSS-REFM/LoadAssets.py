# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

# DBTITLE 1,Read data from Source Table as Delta and Write to Synapse DWH - NDT: baseload_yearly_company_code_ndt
from pyspark.sql.functions import current_timestamp

SourceTable = 'ndh.Baseload_Yearly_Company_Code_NDT'
TargetTable = 'dbo.Baseload_Yearly_Company_Code_NDT'

read_Baseload_Yearly_Company_Code_NDT = spark.sql("select * from "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == read_Baseload_Yearly_Company_Code_NDT.Company_Code]
df_final = read_Baseload_Yearly_Company_Code_NDT.join(df_comp_code_Attr,cond).select("KPI_Type_Text","COMPANY_CODE","country","BaseLoad_LC_Amount","BaseLoad_USD_Amount","Growth_Sustain_Split_Text","Calendar_Year","CREATE_DATE","UPDATE_DATE","Local_Currency_Code","BaseLoad_Type","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

SourceTable = 'NDH.ACTUAL_NDA'
TargetTable = 'STG.ACTUAL_NDA'

actual_nda_df = spark.sql("SELECT * FROM "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == actual_nda_df.COMPANY_CODE]
df_final = actual_nda_df.join(df_comp_code_Attr,cond,"left").select("KPI_TYPE_TEXT","SITE_ID","SITE_NAME","LEASE_ID","COMPANY_CODE","country","CALENDAR_YEAR","CALENDAR_MONTH","LEASE_CLASSIFICATION_CODE","ACTUAL_LC_AMOUNT","ACTUAL_USD_AMOUNT","LEGALLY_COMMITTED_INDICATOR","LOCAL_CURRENCY_CODE","LEASE_SITE_COUNT","COMMENTS_TEXT","POSTING_PERIOD","PROFIT_CENTER","G_L_ACCOUNT","FLOW_TYPE_CODE","DOCUMENT_TYPE_CODE","GROWTH_SUSTAIN_SPLIT_TEXT","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


SourceTable = 'NDH.LE_NDA'
TargetTable = 'STG.LE_NDA'

le_nda_df = spark.sql("SELECT * FROM "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == le_nda_df.COMPANY_CODE]
df_final = le_nda_df.join(df_comp_code_Attr,cond,"left").select("KPI_TYPE_TEXT","LE_TYPE_CODE","SITE_ID","SITE_NAME","LEASE_ID","TRANSACTION_ID","COMPANY_CODE","country","VALUE_TYPE_CODE","CALENDAR_YEAR","CALENDAR_MONTH","LEASE_CLASSIFICATION_CODE","LE_LC_AMOUNT","LE_USD_AMOUNT","LEGALLY_COMMITTED_INDICATOR","LOCAL_CURRENCY_CODE","LEASE_START_DATE","LEASE_EXPIRY_DATE","LEASE_SITE_COUNT","AUTO_RENEWAL_INDICATOR","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable)

# COMMAND ----------

