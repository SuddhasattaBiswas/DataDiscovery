# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC DETAILS: REFM_PL_SITE_FY_NDA
# MAGIC
# MAGIC CREATED ON: 12/15/2021

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp,upper,to_date,round,date_format,to_date

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table NDH.REFM_PL_SITE_FY_NDA_OP

# COMMAND ----------

#Pull required columns from the table ndh.refm_pl_ndt
NDA_PL_SITE_FY = spark.sql('''select  KPI,
                                      Company_Code,
                                      SITE_ID,
                                      LEASE_ID,
                                      TRANSACTION_ID,
                                      PLAN_AMOUNT,
                                      PLAN_AMOUNT_USD,
                                      Lease_Classification,
                                      Legally_Committed,
                                      YEAR,
                                      LOCAL_CURRENCY,
                                      NO_OF_LEASE_SITES,
                                      Auto_Renewal_Y_N,
                                      OP_SUBMISSIONS
                                      from 
                                      ndh.refm_pl_ndt
                                      --where OP_SUBMISSIONS=concat('OP', substring(year(current_date()),3,4))
                                      ''')

NDA_PL_SITE_FY.createOrReplaceTempView("REFM_PL_SITE_FY_NDA_TBL_TEMP")

# COMMAND ----------

# %sql
# select * from REFM_PL_SITE_FY_NDA_TBL_TEMP

# COMMAND ----------

# %sql
# select distinct(OP_submissions)

# COMMAND ----------

NDA_PL_SITE_FY.count()

# COMMAND ----------

# aggregating on amount columns
NDA_PL_SITE_FY = spark.sql('''select KPI,
                                    Company_Code,
                                    SITE_ID,
                                    LEASE_ID,
                                    round(SUM(PLAN_AMOUNT),2)as PLAN_AMOUNT,
                                    round(SUM(PLAN_AMOUNT_USD),2) as PLAN_AMOUNT_USD,
                                    Lease_Classification,
                                    YEAR,
                                    LOCAL_CURRENCY,
                                    NO_OF_LEASE_SITES
                                    from ndh.refm_pl_ndt 
                                    group by KPI,Company_Code,SITE_ID,LEASE_ID,Lease_Classification,YEAR,NO_OF_LEASE_SITES,LOCAL_CURRENCY
                                    order by YEAR,COMPANY_CODE,KPI''')

#Sort the data
NDA_PL_SITE_FY = NDA_PL_SITE_FY.sort(['KPI','Company_Code','Year'],ascending = True)

NDA_PL_SITE_FY.createOrReplaceTempView("REFM_PL_SITE_FY_NDA_TBL_TEMP2")

# COMMAND ----------

# %sql
# select * from REFM_PL_SITE_FY_NDA_TBL_TEMP2

# COMMAND ----------

NDA_PL_SITE_FY.count()

# COMMAND ----------

# gather all columns 
NDA_PL_SITE_FY = spark.sql('''select b.*, 
                                     a.TRANSACTION_ID,
                                     a.Legally_Committed,
                                     a.Auto_Renewal_Y_N,
                                     a.OP_SUBMISSIONS
                                     from REFM_PL_SITE_FY_NDA_TBL_TEMP2 b
                                     LEFT OUTER JOIN REFM_PL_SITE_FY_NDA_TBL_TEMP a
                                     ON a.KPI = b.KPI and a.Company_Code = b.Company_Code 
                                     and a.SITE_ID = b.SITE_ID and a.LEASE_ID = b.LEASE_ID and a.year = b.year
                                     and a.Lease_Classification = b.Lease_Classification 
                                     and a.Local_Currency = b.Local_Currency ''')

# COMMAND ----------

NDA_PL_SITE_FY.count()

# COMMAND ----------

# fill na with 0 values
NDA_PL_SITE_FY = NDA_PL_SITE_FY.fillna(0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

#temp table
NDA_PL_SITE_FY.createOrReplaceTempView("REFM_PL_SITE_FY_NDA_TEMP_TBL")

# COMMAND ----------

#sorting data
NDA_PL_SITE_FY = NDA_PL_SITE_FY.sort(['KPI','Company_Code','Year'],ascending = True)
                                     

#Create Date columns
NDA_PL_SITE_FY = NDA_PL_SITE_FY.withColumn ("CREATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))\
                               .withColumn("UPDATE_DATE", to_date(current_timestamp(),"yyyy-MM-dd"))

# COMMAND ----------

NDA_PL_SITE_FY.createOrReplaceTempView('REFM_PL_SITE_FY_NDA_TEMP_TBL')

# COMMAND ----------

#Filter to include only 10 years of Data
NDA_PL_SITE_FY = spark.sql('''SELECT * FROM REFM_PL_SITE_FY_NDA_TEMP_TBL WHERE Year BETWEEN year(CREATE_DATE)-1 AND year(CREATE_DATE)+10''')

#fill na values with 0
NDA_PL_SITE_FY = NDA_PL_SITE_FY.fillna(0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])
#NDA_PL_SITE_FY=NDA_PL_SITE_FY.distinct()
#temp table
NDA_PL_SITE_FY.createOrReplaceTempView("refm_pl_site_fy_nda_TBL")

#sorting data
NDA_PL_SITE_FY = NDA_PL_SITE_FY.sort(['KPI','Company_Code','Year'],ascending = True)


# COMMAND ----------

NDA_PL_SITE_FY.count()   #562776

# COMMAND ----------

# DBTITLE 1,Insert data into NDA Delta table
# MAGIC %sql
# MAGIC  MERGE INTO ndh.refm_pl_site_fy_nda_op AB
# MAGIC  USING refm_pl_site_fy_nda_tbl BA 
# MAGIC        ON 1 = 2 
# MAGIC  WHEN NOT MATCHED THEN  
# MAGIC    INSERT (  AB.KPI
# MAGIC             ,AB.Company_Code
# MAGIC             ,AB.Site_ID
# MAGIC             ,AB.Lease_ID
# MAGIC             ,AB.Transaction_Id
# MAGIC             ,AB.PLAN_AMOUNT
# MAGIC             ,AB.PLAN_AMOUNT_USD
# MAGIC             ,AB.Lease_Classification
# MAGIC             ,AB.Legally_Committed
# MAGIC             ,AB.Year
# MAGIC             ,AB.CREATE_DATE 
# MAGIC             ,AB.UPDATE_DATE
# MAGIC             ,AB.LOCAL_CURRENCY
# MAGIC             ,AB.NO_OF_LEASE_SITES
# MAGIC             ,AB.Auto_Renewal_Y_N
# MAGIC             ,AB.OP_SUBMISSIONS
# MAGIC
# MAGIC )
# MAGIC    VALUES 
# MAGIC      (
# MAGIC                BA.KPI
# MAGIC               ,BA.Company_Code
# MAGIC               ,BA.Site_ID
# MAGIC               ,BA.Lease_ID
# MAGIC               ,BA.Transaction_Id
# MAGIC               ,BA.PLAN_AMOUNT
# MAGIC               ,BA.PLAN_AMOUNT_USD
# MAGIC               ,BA.Lease_Classification
# MAGIC               ,BA.Legally_Committed
# MAGIC               ,BA.Year
# MAGIC               ,BA.CREATE_DATE 
# MAGIC               ,BA.UPDATE_DATE
# MAGIC               ,BA.LOCAL_CURRENCY
# MAGIC               ,BA.NO_OF_LEASE_SITES
# MAGIC               ,BA.Auto_Renewal_Y_N
# MAGIC               ,BA.OP_SUBMISSIONS
# MAGIC
# MAGIC      )

# COMMAND ----------

dbutils.notebook.exit("Success")