# Databricks notebook source
# DBTITLE 1,Common functions notebook
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

# DBTITLE 1,Package imports
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,REOPS.DQS_DEFECT_NDT --> STG.DQS_DEFECT_NDT
SourceTable = 'REOPS.DQS_DEFECT_NDT '
TargetTable = 'STG.DQS_DEFECT_NDT'

DQS_DEFECT_NDT_df = spark.sql("SELECT DQ.*,r.COUNTRY_CODE FROM REOPS.DQS_DEFECT_NDT DQ inner join (select case when country_name='USA' then  'United States of America' when country_name=  'UK' then 'United Kingdom' when country_name='Hong Kong' then 'Hongkong' when country_name= 'Czech' then 'Czech Republic' else country_name end as Country_name_modified,* from ndh.region_ndt) r on DQ.country_name =r.Country_name_modified ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(DQS_DEFECT_NDT_df,TargetTable)

# COMMAND ----------

# DBTITLE 1,REOPS.DQS_GLOBAL_NDT --> STG.DQS_GLOBAL_NDT
SourceTable = 'REOPS.DQS_GLOBAL_NDT '
TargetTable = 'STG.DQS_GLOBAL_NDT'

DQS_GLOBAL_NDT_df = spark.sql("SELECT DQ.*,r.COUNTRY_CODE FROM REOPS.DQS_GLOBAL_NDT DQ inner join (select case when country_name='USA' then  'United States of America' when country_name=  'UK' then 'United Kingdom' when country_name='Hong Kong' then 'Hongkong' when country_name= 'Czech' then 'Czech Republic' else country_name end as Country_name_modified,* from ndh.region_ndt) r on DQ.country_name =r.Country_name_modified ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(DQS_GLOBAL_NDT_df,TargetTable)

# COMMAND ----------

# DBTITLE 1,REOPS.PLANNED_TRANSACTION_NDT --> STG.PLANNED_TRANSACTION_NDT
SourceTable = 'REOPS.PLANNED_TRANSACTION_NDT'
TargetTable = 'STG.PLANNED_TRANSACTION_NDT'

PLANNED_TRANSACTION_NDT = spark.sql("SELECT DQ.*,r.COUNTRY_CODE FROM REOPS.PLANNED_TRANSACTION_NDT DQ inner join (select case when country_name='USA' then  'United States of America' when country_name=  'UK' then 'United Kingdom' when country_name='Hong Kong' then 'Hongkong' when country_name= 'Czech' then 'Czech Republic' else country_name end as Country_name_modified,* from ndh.region_ndt) r on DQ.country_name =r.Country_name_modified ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(PLANNED_TRANSACTION_NDT,TargetTable)

# COMMAND ----------

# DBTITLE 1,REOPS.PLANNED_VALUE_NDT --> STG.PLANNED_VALUE_NDT
SourceTable = 'REOPS.PLANNED_VALUE_NDT'
TargetTable = 'STG.PLANNED_VALUE_NDT'

PLANNED_VALUE_NDT = spark.sql("SELECT DQ.*,r.COUNTRY_CODE FROM REOPS.PLANNED_VALUE_NDT DQ inner join (select case when country_name='USA' then  'United States of America' when country_name=  'UK' then 'United Kingdom' when country_name='Hong Kong' then 'Hongkong' when country_name= 'Czech' then 'Czech Republic' else country_name end as Country_name_modified,* from ndh.region_ndt) r on DQ.country_name =r.Country_name_modified ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapse(PLANNED_VALUE_NDT,TargetTable)

# COMMAND ----------

# DBTITLE 1,REOPS.TM_TRIRIGA_NDT --> STG.TM_TRIRIGA_NDT
SourceTable = 'REOPS.TM_TRIRIGA_NDT'
TargetTable = 'STG.TM_TRIRIGA_NDT'

TM_TRIRIGA_NDT = spark.sql("SELECT DQ.*,r.COUNTRY_CODE FROM REOPS.TM_TRIRIGA_NDT DQ inner join (select case when country_name='USA' then  'United States of America' when country_name=  'UK' then 'United Kingdom' when country_name='Hong Kong' then 'Hongkong' when country_name= 'Czech' then 'Czech Republic' else country_name end as Country_name_modified,* from ndh.region_ndt) r on DQ.country_name =r.Country_name_modified ").withColumn("NDSS_REFRESH_DATE",current_timestamp().cast("timestamp"))

setconnections();
overwriteToSynapseCustom(TM_TRIRIGA_NDT,TargetTable)

# COMMAND ----------

