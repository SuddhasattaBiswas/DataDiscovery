# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC DETAILS: REFM_PL_OU_FY_NDA
# MAGIC
# MAGIC CREATED ON: 12/09/2021

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp,upper,to_date,round,date_format

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table NDH.REFM_PL_OU_FY_NDA_OP

# COMMAND ----------

# Aggregating columns PLAN_AMOUNT,PLAN_AMOUNT_USD according to the logic.
NDA_PL_FY = spark.sql(''' select KPI,
                          Company_Code AS COMPANY_CODE,
                          Year As YEAR,
                          round(Sum(PLAN_AMOUNT),2) AS PLAN_AMOUNT,
                          round(Sum(PLAN_AMOUNT_USD),2) AS PLAN_AMOUNT_USD,
                          Lease_Classification AS LEASE_CLASSIFICATION,
                          MAX(LOCAL_CURRENCY) AS LOCAL_CURRENCY,
                          Count(Lease_Classification) as NO_OF_LEASE_SITES,
                          OP_SUBMISSIONS AS OP_SUBMISSIONS
                          from ndh.refm_pl_ndt
                          --where OP_SUBMISSIONS=concat('OP', substring(year(current_date()),3,4))
                          group by KPI,
                          Company_Code,Year,Lease_Classification,OP_SUBMISSIONS
                          order by year asc''').withColumn("CREATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))\
                                               .withColumn("UPDATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))
 

# COMMAND ----------

#sorting data
NDA_PL_FY = NDA_PL_FY.sort(['KPI','Company_Code','Year'],ascending = True)

#Filter to include only 10 years of Data
NDA_PL_FY.createOrReplaceTempView("REFM_PL_OU_FY_NDA_TBL_TEMP")
NDA_PL_FY = spark.sql('''SELECT KPI,
                                COMPANY_CODE,
                                YEAR,
                                round(PLAN_AMOUNT,2) AS PLAN_AMOUNT,
                                round(PLAN_AMOUNT_USD,2) AS PLAN_AMOUNT_USD,
                                LEASE_CLASSIFICATION,
                                LOCAL_CURRENCY,
                                CREATE_DATE,
                                UPDATE_DATE,
                                NO_OF_LEASE_SITES,
                                OP_SUBMISSIONS
                                FROM REFM_PL_OU_FY_NDA_TBL_TEMP 
                                WHERE Year BETWEEN year(CREATE_DATE)-1 AND year(CREATE_DATE)+10''')



# COMMAND ----------

#Fill NA with 0
NDA_PL_FY = NDA_PL_FY.fillna(0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

#temp table
NDA_PL_FY.createOrReplaceTempView("REFM_PL_OU_FY_NDA_TBL")

# COMMAND ----------

# DBTITLE 1,Insert Data into ndh.refm_pl_ou_fy_nda delta table
# MAGIC %sql
# MAGIC  MERGE INTO ndh.refm_pl_ou_fy_nda_op AB
# MAGIC  USING REFM_PL_OU_FY_NDA_TBL BA 
# MAGIC        ON 1 = 2 
# MAGIC  WHEN NOT MATCHED THEN  
# MAGIC    INSERT ( AB.KPI,
# MAGIC             AB.COMPANY_CODE,
# MAGIC             AB.YEAR,
# MAGIC             AB.PLAN_AMOUNT,
# MAGIC             AB.PLAN_AMOUNT_USD,
# MAGIC             AB.LEASE_CLASSIFICATION,
# MAGIC             AB.LOCAL_CURRENCY,
# MAGIC             AB.CREATE_DATE,
# MAGIC             AB.UPDATE_DATE,
# MAGIC             AB.NO_OF_LEASE_SITES,
# MAGIC             AB.OP_SUBMISSIONS
# MAGIC )
# MAGIC    VALUES 
# MAGIC      (
# MAGIC               BA.KPI,
# MAGIC               BA.COMPANY_CODE,
# MAGIC               BA.YEAR,
# MAGIC               BA.PLAN_AMOUNT,
# MAGIC               BA.PLAN_AMOUNT_USD,
# MAGIC               BA.LEASE_CLASSIFICATION,
# MAGIC               BA.LOCAL_CURRENCY,
# MAGIC               Cast(BA.CREATE_DATE as Date),
# MAGIC               Cast(BA.UPDATE_DATE as Date),
# MAGIC               BA.NO_OF_LEASE_SITES,
# MAGIC               BA.OP_SUBMISSIONS
# MAGIC      )

# COMMAND ----------

dbutils.notebook.exit("Success")