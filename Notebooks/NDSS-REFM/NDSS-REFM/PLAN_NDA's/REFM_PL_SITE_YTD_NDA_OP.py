# Databricks notebook source
# Import Libraries
from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table NDH.REFM_PL_SITE_YTD_NDA_OP

# COMMAND ----------

# %sql
# select * from ndh.refm_pl_ndt

# COMMAND ----------

# Select required columns from PL_NDT table for "REFM_PL_OU_YTD_NDA" creation
ndt_df_subset = spark.sql('''
select KPI,
Company_Code, 
Site_ID, 
Lease_ID,
Transaction_Id, 
Month, 
PLAN_AMOUNT, 
PLAN_AMOUNT_USD, 
Lease_Classification, 
Legally_Committed, 
Year,
LOCAL_CURRENCY, 
Auto_Renewal_Y_N,
OP_SUBMISSIONS from NDH.REFM_PL_NDT 
--where OP_SUBMISSIONS=concat('OP', substring(year(current_date()),3,4)) 
''')

# Create month id for respective months
ndt_df_subset = ndt_df_subset.withColumn("Month_ID",f.from_unixtime(f.unix_timestamp(f.col("Month"),'MMM'),'MM'))

# COMMAND ----------

ndt_df_subset.count()

# COMMAND ----------

ndt_cols_sub = ndt_df_subset.select(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Transaction_Id","Legally_Committed","Auto_Renewal_Y_N","OP_SUBMISSIONS"]).distinct()

# COMMAND ----------

# Map Country with its currency
dict1 = {row['Company_Code']:row['Local_Currency'] for row in ndt_df_subset.select(["Company_Code","Local_Currency"]).distinct().collect()}
#print(dict1)

# COMMAND ----------

# Create a column named "NO_OF_LEASE_SITES" based on the number of record count
ndt_df_subset_wls = ndt_df_subset.groupby(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).count().withColumnRenamed('count', "NO_OF_LEASE_SITES").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Create a column named "PLAN_AMOUNT" after aggregation for months and year
ndt_df_subset_pa = ndt_df_subset.groupby(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).sum("PLAN_AMOUNT").withColumnRenamed('sum(PLAN_AMOUNT)', "PLAN_AMOUNT").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Create a column named "PLAN_AMOUNT_USD" after aggregation for months and year
ndt_df_subset_pa_usd = ndt_df_subset.groupby(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).sum("PLAN_AMOUNT_USD").withColumnRenamed('sum(PLAN_AMOUNT_USD)', "PLAN_AMOUNT_USD").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Join all the tables to get the consolidated data in one table
ndt_df_subset_combined = ndt_df_subset_wls.join(ndt_df_subset_pa, ["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"], "left").join(ndt_df_subset_pa_usd, ["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"], "left").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Create a "Local Currency" column and replacing the values with respect to Company code
ndt_df_subset_combined = ndt_df_subset_combined.withColumn("LOCAL_CURRENCY",ndt_df_subset_combined.Company_Code)
ndt_df_subset_combined = ndt_df_subset_combined.replace(dict1,1,"LOCAL_CURRENCY")

# Fill 0 for null values
ndt_df_subset_combined = ndt_df_subset_combined.na.fill(value=0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

# COMMAND ----------

ndt_df_subset_combined.count()

# COMMAND ----------

# Calculate rolling sum for PLAN_YTD_AMOUNT and PLAN_YTD_AMOUNT_USD
partition = (Window
             .partitionBy(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","OP_SUBMISSIONS"])
             .orderBy(['Year','Month_ID'])
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df = ndt_df_subset_combined.withColumn('PLAN_YTD_AMOUNT', f.sum('PLAN_AMOUNT').over(partition))
PL_SITE_YTD_NDA_DF = df.withColumn('PLAN_YTD_AMOUNT_USD', f.sum('PLAN_AMOUNT_USD').over(partition))

# COMMAND ----------

PL_SITE_YTD_NDA_DF.count()

# COMMAND ----------

PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.join(ndt_cols_sub,on=["KPI","Company_Code","Lease_ID","Site_ID",
                                             "Lease_Classification","Year","Month","OP_SUBMISSIONS"],how='leftouter').drop_duplicates()

# COMMAND ----------

PL_SITE_YTD_NDA_DF=PL_SITE_YTD_NDA_DF.withColumn("Transaction_Id",f.lit(None).cast(StringType()))\
        .withColumn("Legally_Committed",f.lit(None).cast(StringType()))\
        .withColumn("Auto_Renewal_Y_N",f.lit(None).cast(StringType()))

# COMMAND ----------

PL_SITE_YTD_NDA_DF.display()

# COMMAND ----------

# Create subset of NL_PDT table to get full year calculation
df1 = spark.sql('''select KPI,
                          Company_Code,
                          Lease_ID,
                          Site_ID,
                          Lease_Classification,
                          Year,
                          round(Sum(PLAN_AMOUNT),2) AS PLAN_AMOUNT,
                          round(Sum(PLAN_AMOUNT_USD),2) AS PLAN_AMOUNT_USD,
                          MAX(LOCAL_CURRENCY) AS LOCAL_CURRENCY,
                          Count(Lease_Classification) as NO_OF_LEASE_SITES,
                          OP_SUBMISSIONS
                    from ndh.refm_pl_ndt
                    where Year in (select distinct Year from ndh.refm_pl_ndt A where A.Month = 'DEC')
                    group by KPI, Company_Code,Year,Lease_ID,Site_ID,LEASE_CLASSIFICATION,OP_SUBMISSIONS''')

# # Adding columns
df1 = df1.withColumn("Month",f.lit("FY"))\
        .withColumn("Month_ID",f.lit(13))\
        .withColumn("LOCAL_CURRENCY",df1.Company_Code)\
        .withColumn("PLAN_YTD_AMOUNT",f.lit(0))\
        .withColumn("PLAN_YTD_AMOUNT_USD",f.lit(0))\
        .withColumn("Transaction_Id",f.lit(None).cast(StringType()))\
        .withColumn("Legally_Committed",f.lit(None).cast(StringType()))\
        .withColumn("Auto_Renewal_Y_N",f.lit(None).cast(StringType()))

# Replace Local currency with respect to its country
df1 = df1.replace(dict1,1,"Local_Currency")

# Replacing null values with 0 for amount columns
df1 = df1.fillna(0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

# Sort the dataframe
df1 = df1.sort(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month_ID"])

# COMMAND ----------

df1.count()

# COMMAND ----------

# Rearrange columns
df1 = df1.select(['KPI',
                   'Company_Code',
                   'Lease_ID',
                   'Site_ID',
                   'Lease_Classification',
                   'Year',
                   'Month',
                   'Month_ID',
                   'OP_SUBMISSIONS',
                   'NO_OF_LEASE_SITES',
                   'PLAN_AMOUNT',
                   'PLAN_AMOUNT_USD',
                   'LOCAL_CURRENCY',
                   'PLAN_YTD_AMOUNT',
                   'PLAN_YTD_AMOUNT_USD',
                   'Transaction_Id',
                   'Legally_Committed',
                   'Auto_Renewal_Y_N'])

# COMMAND ----------

# PL_SITE_YTD_NDA_DF.display()

# COMMAND ----------

# Union of Months dataframe with Full Year dataframe
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.unionByName(df1).sort(["KPI","Company_Code","Lease_ID","Site_ID","Lease_Classification","Year","Month_ID"])

# Create two date columns
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.withColumn("CREATE_DATE",f.current_date()).withColumn("UPDATE_DATE",f.current_date())

# COMMAND ----------

# Convert all column names to uppercase.
for col in PL_SITE_YTD_NDA_DF.columns:
    PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.withColumnRenamed(col, col.upper())

# COMMAND ----------

# Round the amount columns to 2 decimal places
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.select("*", f.round(f.col('PLAN_AMOUNT'),2))
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.select("*", f.round(f.col('PLAN_AMOUNT_USD'),2))
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.select("*", f.round(f.col('PLAN_YTD_AMOUNT'),2))
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.select("*", f.round(f.col('PLAN_YTD_AMOUNT_USD'),2))

#Dropping old amount columns
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.drop("PLAN_AMOUNT","PLAN_AMOUNT_USD","PLAN_YTD_AMOUNT","PLAN_YTD_AMOUNT_USD")

#Renaming rounded columns
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.withColumnRenamed("round(PLAN_AMOUNT, 2)","PLAN_AMOUNT")\
                                   .withColumnRenamed("round(PLAN_AMOUNT_USD, 2)","PLAN_AMOUNT_USD")\
                                   .withColumnRenamed("round(PLAN_YTD_AMOUNT, 2)","PLAN_YTD_AMOUNT")\
                                   .withColumnRenamed("round(PLAN_YTD_AMOUNT_USD, 2)","PLAN_YTD_AMOUNT_USD")

# COMMAND ----------

PL_SITE_YTD_NDA_DF.count()

# COMMAND ----------

# Rearrange columns based on NDA Delta table
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.select("KPI","COMPANY_CODE","SITE_ID","LEASE_ID", "TRANSACTION_ID", "LEASE_CLASSIFICATION",
                                               "LOCAL_CURRENCY", "YEAR","MONTH","NO_OF_LEASE_SITES",
                                               "PLAN_AMOUNT","PLAN_AMOUNT_USD","PLAN_YTD_AMOUNT","PLAN_YTD_AMOUNT_USD",
                                               "LEGALLY_COMMITTED", "AUTO_RENEWAL_Y_N","CREATE_DATE","UPDATE_DATE","OP_SUBMISSIONS")

# COMMAND ----------

# Filter data for current year and next one year
PL_SITE_YTD_NDA_DF = PL_SITE_YTD_NDA_DF.filter((PL_SITE_YTD_NDA_DF.YEAR == f.year(f.add_months(f.current_date(), -12))) | (PL_SITE_YTD_NDA_DF.YEAR == f.year(f.current_date())) | (PL_SITE_YTD_NDA_DF.YEAR == f.year(f.add_months(f.current_date(), 12))))

# COMMAND ----------

# Create temp view of dataframe in order to insert data in delta table
PL_SITE_YTD_NDA_DF.createOrReplaceTempView("PL_SITE_YTD_NDA_DF")

# COMMAND ----------

# MAGIC  %sql
# MAGIC  select  count(*) from PL_SITE_YTD_NDA_DF

# COMMAND ----------

# %sql
# select * from NDH.REFM_PL_SITE_YTD_NDA

# COMMAND ----------

# MAGIC %sql
# MAGIC  MERGE INTO ndh.REFM_PL_SITE_YTD_NDA_OP AB
# MAGIC  USING PL_SITE_YTD_NDA_DF BA 
# MAGIC        ON 1 = 2 
# MAGIC  WHEN NOT MATCHED THEN  
# MAGIC    INSERT (  AB.KPI,
# MAGIC             AB.COMPANY_CODE,
# MAGIC             AB.SITE_ID,
# MAGIC             AB.LEASE_ID,
# MAGIC             AB.TRANSACTION_ID,
# MAGIC             AB.LEASE_CLASSIFICATION,
# MAGIC             AB.LOCAL_CURRENCY,
# MAGIC             AB.YEAR,
# MAGIC             AB.MONTH,
# MAGIC             AB.NO_OF_LEASE_SITES,
# MAGIC             AB.PLAN_AMOUNT,
# MAGIC             AB.PLAN_AMOUNT_USD,
# MAGIC             AB.PLAN_YTD_AMOUNT,
# MAGIC             AB.PLAN_YTD_AMOUNT_USD,
# MAGIC             AB.LEGALLY_COMMITTED,
# MAGIC             AB.AUTO_RENEWAL_Y_N,
# MAGIC             AB.CREATE_DATE,
# MAGIC             AB.UPDATE_DATE,
# MAGIC             AB.OP_SUBMISSIONS
# MAGIC
# MAGIC )
# MAGIC    VALUES 
# MAGIC      (
# MAGIC                BA.KPI,
# MAGIC               BA.COMPANY_CODE,
# MAGIC               BA.SITE_ID,
# MAGIC               BA.LEASE_ID,
# MAGIC               BA.TRANSACTION_ID,
# MAGIC               BA.LEASE_CLASSIFICATION,
# MAGIC               BA.LOCAL_CURRENCY,
# MAGIC               BA.YEAR,
# MAGIC               BA.MONTH,
# MAGIC               BA.NO_OF_LEASE_SITES,
# MAGIC               BA.PLAN_AMOUNT,
# MAGIC               BA.PLAN_AMOUNT_USD,
# MAGIC               BA.PLAN_YTD_AMOUNT,
# MAGIC               BA.PLAN_YTD_AMOUNT_USD,
# MAGIC               BA.LEGALLY_COMMITTED,
# MAGIC               BA.AUTO_RENEWAL_Y_N,
# MAGIC               BA.CREATE_DATE,
# MAGIC               BA.UPDATE_DATE,
# MAGIC               BA.OP_SUBMISSIONS
# MAGIC      )

# COMMAND ----------

dbutils.notebook.exit("Success")