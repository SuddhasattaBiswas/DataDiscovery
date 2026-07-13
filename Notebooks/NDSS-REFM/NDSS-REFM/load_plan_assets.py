# Databricks notebook source
# MAGIC %run Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

SourceTable = 'ndh.refm_pl_ou_ytd_nda_op'
TargetTable = 'stg.Plan_Ytd_Company_Code_NDA_OP'

read_refm_pl_ou_ytd_nda = spark.sql("select * from "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
read_refm_pl_ou_ytd_nda_ColRenamed = read_refm_pl_ou_ytd_nda.withColumnRenamed('KPI','KPI_Type_Text')\
                                                              .withColumnRenamed('Year','Calendar_Year')\
                                                              .withColumnRenamed('Plan_Amount','Plan_LC_Amount')\
                                                              .withColumnRenamed('Plan_Amount_USD','Plan_USD_Amount')\
                                                              .withColumnRenamed('Plan_YTD_Amount','Plan_Ytd_LC_Amount')\
                                                              .withColumnRenamed('Plan_YTD_Amount_USD','Plan_Ytd_USD_Amount')\
                                                              .withColumnRenamed('Lease_Classification','Lease_Classification_Code')\
                                                              .withColumnRenamed('Month','Calendar_Month')\
                                                              .withColumnRenamed('Local_Currency','Local_Currency_Code')\
                                                              .withColumnRenamed('No_Of_Lease_Sites','Lease_Site_Count')\
                                                              .withColumnRenamed('OP_SUBMISSIONS','OP_SUBMISSIONS')

df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == read_refm_pl_ou_ytd_nda_ColRenamed.COMPANY_CODE]

df_final = read_refm_pl_ou_ytd_nda_ColRenamed.join(df_comp_code_Attr,cond).select("KPI_Type_Text","COMPANY_CODE","country","Local_Currency_Code","Calendar_Year","Calendar_Month","Lease_Classification_Code","Lease_Site_Count","Plan_LC_Amount","Plan_USD_Amount","Plan_Ytd_LC_Amount","Plan_Ytd_USD_Amount","OP_SUBMISSIONS","CREATE_DATE","UPDATE_DATE","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable) 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

SourceTable = 'ndh.refm_pl_ou_fy_nda_op'
TargetTable = 'stg.Plan_Yearly_Company_Code_NDA_OP'

read_refm_pl_ou_fy_nda = spark.sql("select * from "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
read_refm_pl_ou_fy_nda_ColRenamed = read_refm_pl_ou_fy_nda.withColumnRenamed('KPI','KPI_Type_Text')\
                                                            .withColumnRenamed('Year','Calendar_Year')\
                                                            .withColumnRenamed('Plan_Amount','Plan_LC_Amount')\
                                                            .withColumnRenamed('Plan_Amount_USD','Plan_USD_Amount')\
                                                            .withColumnRenamed('Lease_Classification','Lease_Classification_Code')\
                                                            .withColumnRenamed('Local_Currency','Local_Currency_Code')\
                                                            .withColumnRenamed('No_Of_Lease_Sites','Lease_Site_Count')\
                                                            .withColumnRenamed('OP_SUBMISSIONS','OP_SUBMISSIONS')\


df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == read_refm_pl_ou_fy_nda_ColRenamed.COMPANY_CODE]
df_final = read_refm_pl_ou_fy_nda_ColRenamed.join(df_comp_code_Attr,cond).select("KPI_Type_Text","COMPANY_CODE","country","Calendar_Year","Plan_LC_Amount","Plan_USD_Amount","Lease_Classification_Code","Local_Currency_Code","CREATE_DATE","UPDATE_DATE","Lease_Site_Count","OP_SUBMISSIONS","NDSS_REFRESH_DATE") 





# cond = [dmf.COUNTRY_CODE == dfp.COUNTRY_CODE, dmf.FISCAL_PERIOD == dfp.FISCAL_PERIOD, dmf.KPI == dfp.KPI]
# df_final = dfp.join(dmf, cond, "leftouter")



setconnections();
overwriteToSynapse(df_final,TargetTable)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

SourceTable = 'NDH.REFM_PL_SITE_YTD_NDA_OP'
TargetTable = 'STG.Plan_YTD_By_Site_NDA_OP'

read_refm_pl_site_ytd_nda = spark.sql("select * from "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
read_refm_pl_site_ytd_nda_ColRenamed = read_refm_pl_site_ytd_nda.withColumnRenamed('KPI','KPI_Type_Text')\
                                                                  .withColumnRenamed('Month','Calendar_Month')\
                                                                  .withColumnRenamed('Plan_Amount','Plan_LC_Amount')\
                                                                  .withColumnRenamed('Plan_Amount_USD','Plan_USD_Amount')\
                                                                  .withColumnRenamed('Plan_YTD_Amount','Plan_Ytd_LC_Amount')\
                                                                  .withColumnRenamed('Plan_YTD_Amount_USD','Plan_Ytd_USD_Amount')\
                                                                  .withColumnRenamed('Legally_Committed','Legally_Committed_Indicator')\
                                                                  .withColumnRenamed('Lease_Classification','Lease_Classification_Code')\
                                                                  .withColumnRenamed('Year','Calendar_Year')\
                                                                  .withColumnRenamed('Local_Currency','Local_Currency_Code')\
                                                                  .withColumnRenamed('No_Of_Lease_Sites','Lease_Site_Count')\
                                                                  .withColumnRenamed('Auto_Renewal_Y_N','Auto_Renewal_Indicator')\
                                                                  .withColumnRenamed('OP_SUBMISSIONS','OP_SUBMISSIONS')

df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == read_refm_pl_site_ytd_nda_ColRenamed.COMPANY_CODE]

df_final = read_refm_pl_site_ytd_nda_ColRenamed.join(df_comp_code_Attr,cond).select("KPI_Type_Text","COMPANY_CODE","country","SITE_ID","LEASE_ID","TRANSACTION_ID","Lease_Classification_Code","Local_Currency_Code","Calendar_Year","Calendar_Month","Lease_Site_Count","Plan_LC_Amount","Plan_USD_Amount","Plan_Ytd_LC_Amount","Plan_Ytd_USD_Amount","Legally_Committed_Indicator","Auto_Renewal_Indicator","OP_SUBMISSIONS","CREATE_DATE","UPDATE_DATE","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

SourceTable = 'NDH.REFM_PL_SITE_FY_NDA_OP'
TargetTable = 'STG.Plan_Yearly_By_Site_NDA_OP'

read_refm_pl_site_fy_nda = spark.sql("select * from "+SourceTable+" ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))
read_refm_pl_site_fy_nda_ColRenamed = read_refm_pl_site_fy_nda.withColumnRenamed('KPI','KPI_Type_Text')\
                                                                .withColumnRenamed('Legally_Committed','Legally_Committed_Indicator')\
                                                                .withColumnRenamed('Lease_Classification','Lease_Classification_Code')\
                                                                .withColumnRenamed('Year','Calendar_Year')\
                                                                .withColumnRenamed('Local_Currency','Local_Currency_Code')\
                                                                .withColumnRenamed('No_Of_Lease_Sites','Lease_Site_Count')\
                                                                .withColumnRenamed('Auto_Renewal_Y_N','Auto_Renewal_Indicator')\
                                                                .withColumnRenamed('Plan_Amount','Plan_LC_Amount')\
                                                                .withColumnRenamed('Plan_Amount_USD','Plan_USD_Amount')\
                                                                .withColumnRenamed('OP_SUBMISSIONS','OP_SUBMISSIONS')

df_comp_code_Attr = spark.sql("select distinct company,country from NDH.COMP_CODE_ATTRIBUTES_NDT")
cond = [df_comp_code_Attr.company == read_refm_pl_site_fy_nda_ColRenamed.COMPANY_CODE]

df_final = read_refm_pl_site_fy_nda_ColRenamed.join(df_comp_code_Attr,cond).select("KPI_Type_Text","COMPANY_CODE","country","SITE_ID","LEASE_ID","TRANSACTION_ID","Plan_LC_Amount","Plan_USD_Amount","Lease_Classification_Code","Legally_Committed_Indicator","Calendar_Year","CREATE_DATE","UPDATE_DATE","Local_Currency_Code","Lease_Site_Count","Auto_Renewal_Indicator","OP_SUBMISSIONS","NDSS_REFRESH_DATE")

setconnections();
overwriteToSynapse(df_final,TargetTable)