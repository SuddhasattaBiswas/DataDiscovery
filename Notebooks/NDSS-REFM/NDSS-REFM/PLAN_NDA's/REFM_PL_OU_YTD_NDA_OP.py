# Databricks notebook source
from pyspark.sql.functions import col,lit,current_timestamp,upper,to_date,round,date_format,to_date

# COMMAND ----------

# Import Libraries
from pyspark.sql import Window
from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table NDH.REFM_PL_OU_YTD_NDA_OP

# COMMAND ----------

# Select required columns from PL_NDT table for "REFM_PL_OU_YTD_NDA" creation
ndt_df_subset = spark.sql('''
select KPI,
Company_Code,
Year,
round(PLAN_AMOUNT,2) as PLAN_AMOUNT,
round(PLAN_AMOUNT_USD,2) as PLAN_AMOUNT_USD,
Lease_Classification,
Month,
LOCAL_CURRENCY,
OP_SUBMISSIONS from NDH.REFM_PL_NDT
--where OP_SUBMISSIONS=concat('OP', substring(year(current_date()),3,4)) 
 ''')

# Create month id for respective months
ndt_df_subset = ndt_df_subset.withColumn("Month_ID",f.from_unixtime(f.unix_timestamp(f.col("Month"),'MMM'),'MM'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select KPI,Company_Code,Year,round(PLAN_AMOUNT,2) as PLAN_AMOUNT,round(PLAN_AMOUNT_USD,2) as PLAN_AMOUNT_USD,Lease_Classification,Month,LOCAL_CURRENCY,OP_SUBMISSIONS,concat('OP', substring(year(current_date()),3,4)) from NDH.REFM_PL_NDT where OP_SUBMISSIONS='OP23'

# COMMAND ----------

ndt_df_subset.select('OP_SUBMISSIONS').distinct().display()

# COMMAND ----------

# %sql
# select * from ndh.REFM_PL_NDT where year=2023

# COMMAND ----------

# %sql
# select * from ndh.REFM_PL_OU_YTD_NDA

# COMMAND ----------

# Map Country with its currency
dict1 = {row['Company_Code']:row['Local_Currency'] for row in ndt_df_subset.select(["Company_Code","Local_Currency"]).distinct().collect()}
#print(dict1)

# COMMAND ----------

# Create a column named "NO_OF_LEASE_SITES" based on the number of record count
ndt_df_subset_wls = ndt_df_subset.groupby(["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).count().withColumnRenamed('count', "NO_OF_LEASE_SITES").sort(["KPI","Lease_Classification","Year","Month_ID",])

# Create a column named "PLAN_AMOUNT" after aggregation for months and year
ndt_df_subset_pa = ndt_df_subset.groupby(["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).sum("PLAN_AMOUNT").withColumnRenamed('sum(PLAN_AMOUNT)', "PLAN_AMOUNT").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Create a column named "PLAN_AMOUNT_USD" after aggregation for months and year
ndt_df_subset_pa_usd = ndt_df_subset.groupby(["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"]).sum("PLAN_AMOUNT_USD").withColumnRenamed('sum(PLAN_AMOUNT_USD)', "PLAN_AMOUNT_USD").sort(["KPI","Lease_Classification","Year","Month_ID"])

# Join all the tables to get the consolidated data in one table
ndt_df_subset_combined = ndt_df_subset_wls.join(ndt_df_subset_pa, ["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"], "left").join(ndt_df_subset_pa_usd, ["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID","OP_SUBMISSIONS"], "left").sort(["KPI","Lease_Classification","Year","Month_ID"])

# COMMAND ----------

ndt_df_subset_wls = ndt_df_subset.groupby(["KPI","Company_Code","Lease_Classification","Year","Month","Month_ID"]).count().withColumnRenamed('count', "NO_OF_LEASE_SITES").sort(["KPI","Lease_Classification","Year","Month_ID",])

# COMMAND ----------

ndt_df_subset_wls.createOrReplaceTempView("df_PivotTempView")

# COMMAND ----------

# %sql
# select  count(*) from ndh.refm_pl_site_fy_nda

# COMMAND ----------

# %sql
# select  count(*) from ndh.refm_pl_site_fy_nda_op

# COMMAND ----------

      # ndt_df_subset_combined.select('OP_SUBMISSIONS').distinct().display()

# COMMAND ----------

# ndt_df_subset_combined.display()

# COMMAND ----------

# Create a "Local Currency" column and replacing the values with respect to Company code
ndt_df_subset_combined = ndt_df_subset_combined.withColumn("Local_Currency",ndt_df_subset_combined.Company_Code)
ndt_df_subset_combined = ndt_df_subset_combined.replace(dict1,1,"Local_Currency")

# Fill 0 for null values
ndt_df_subset_combined = ndt_df_subset_combined.na.fill(value=0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

# COMMAND ----------

ndt_df_subset_combined.select('OP_SUBMISSIONS').distinct().display()

# COMMAND ----------

# Calculate rolling sum for PLAN_YTD_AMOUNT and PLAN_YTD_AMOUNT_USD
partition = (Window
             .partitionBy(["KPI","Company_Code","Lease_Classification","Year"])
             .orderBy(['Year','Month_ID'])
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
df = ndt_df_subset_combined.withColumn('PLAN_YTD_AMOUNT', f.sum('PLAN_AMOUNT').over(partition))
PL_OU_YTD_NDA_DF = df.withColumn('PLAN_YTD_AMOUNT_USD', f.sum('PLAN_AMOUNT_USD').over(partition))

# COMMAND ----------

PL_OU_YTD_NDA_DF.display()

# COMMAND ----------

#PL_OU_YTD_NDA_DF.write.csv('/mnt/ADLS2/NDH/NDA_data.csv',header=True, mode='overwrite')
#ndt_df_subset.write.csv('/mnt/ADLS2/NDH/ndt_data_check.csv',header=True, mode='overwrite')

# COMMAND ----------

# Create subset of NL_PDT table to get full year calculation
df1 = spark.sql('''select KPI,
            Company_Code,
            Year,
            round(Sum(PLAN_AMOUNT),2) AS PLAN_AMOUNT,
            round(Sum(PLAN_AMOUNT_USD),2) AS PLAN_AMOUNT_USD,
            Lease_Classification,
            MAX(LOCAL_CURRENCY) AS LOCAL_CURRENCY,
            Count(Lease_Classification) as NO_OF_LEASE_SITES,
            OP_SUBMISSIONS
            from ndh.refm_pl_ndt
            where Year in (select distinct Year from ndh.refm_pl_ndt A where A.Month = 'DEC')
            group by KPI, Company_Code,Year,Lease_Classification,OP_SUBMISSIONS
''')

# Adding columns
df1 = df1.withColumn("Month",f.lit("FY"))\
        .withColumn("Month_ID",f.lit(13))\
        .withColumn("Local_Currency",df1.Company_Code)\
        .withColumn("PLAN_YTD_AMOUNT",f.lit(0))\
        .withColumn("PLAN_YTD_AMOUNT_USD",f.lit(0))

# Replace Local currency with respect to its country
df1 = df1.replace(dict1,1,"Local_Currency")

# Replacing null values with 0 for amount columns
df1 = df1.fillna(0,subset=["PLAN_AMOUNT","PLAN_AMOUNT_USD"])

# Sort the dataframe
df1 = df1.sort(["KPI","Company_Code","Lease_Classification","Year","Month_ID"])

# Rearrange the columns
df1 = df1.select(["KPI", "Company_Code", "Lease_Classification", "Year","Month","Month_ID","NO_OF_LEASE_SITES",
           "PLAN_AMOUNT","PLAN_AMOUNT_USD","Local_Currency","PLAN_YTD_AMOUNT","PLAN_YTD_AMOUNT_USD","OP_SUBMISSIONS"])

# COMMAND ----------

# df1.display()

# COMMAND ----------

df1.select('OP_SUBMISSIONS').distinct().display()

# COMMAND ----------

# Union of Months dataframe with Full Year dataframe
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.unionByName(df1).sort(["KPI","Company_Code","Lease_Classification","Year","Month_ID"])

# Create two date columns
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.withColumn("CREATE_DATE",f.current_date()).withColumn("UPDATE_DATE",f.current_date())

# COMMAND ----------

PL_OU_YTD_NDA_DF.select('OP_SUBMISSIONS').distinct().display()

# COMMAND ----------

# Convert all column names to uppercase.
for col in PL_OU_YTD_NDA_DF.columns:
    PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.withColumnRenamed(col, col.upper())

# COMMAND ----------

# Round the amount columns to 2 decimal places
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.select("*", f.round(f.col('PLAN_AMOUNT'),2))
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.select("*", f.round(f.col('PLAN_AMOUNT_USD'),2))
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.select("*", f.round(f.col('PLAN_YTD_AMOUNT'),2))
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.select("*", f.round(f.col('PLAN_YTD_AMOUNT_USD'),2))

#Dropping old amount columns
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.drop("PLAN_AMOUNT","PLAN_AMOUNT_USD","PLAN_YTD_AMOUNT","PLAN_YTD_AMOUNT_USD")

#Renaming rounded columns
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.withColumnRenamed("round(PLAN_AMOUNT, 2)","PLAN_AMOUNT")\
                                   .withColumnRenamed("round(PLAN_AMOUNT_USD, 2)","PLAN_AMOUNT_USD")\
                                   .withColumnRenamed("round(PLAN_YTD_AMOUNT, 2)","PLAN_YTD_AMOUNT")\
                                   .withColumnRenamed("round(PLAN_YTD_AMOUNT_USD, 2)","PLAN_YTD_AMOUNT_USD")

# COMMAND ----------

# Rearrange columns based on NDA Delta table
PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.select("KPI",
                                            "COMPANY_CODE",
                                            "LOCAL_CURRENCY",
                                            "YEAR",
                                            "MONTH",
                                            "LEASE_CLASSIFICATION",
                                            "NO_OF_LEASE_SITES",
                                            "PLAN_AMOUNT",
                                            "PLAN_AMOUNT_USD",
                                            "PLAN_YTD_AMOUNT",
                                            "PLAN_YTD_AMOUNT_USD",
                                            "CREATE_DATE" ,
                                            "UPDATE_DATE",
                                            "OP_SUBMISSIONS")

# COMMAND ----------

# Filter data for current year and next one year
#PL_OU_YTD_NDA_DF = PL_OU_YTD_NDA_DF.filter((PL_OU_YTD_NDA_DF.YEAR == f.year(f.current_date())) | (PL_OU_YTD_NDA_DF.YEAR == f.year(f.add_months(f.current_date(), 12)))| (PL_OU_YTD_NDA_DF.YEAR == f.year(f.add_months(f.current_date(), -12))))

# COMMAND ----------

# Create temp view of dataframe in order to insert data in delta table
PL_OU_YTD_NDA_DF.createOrReplaceTempView("PL_OU_YTD_NDA_DF")

# COMMAND ----------

# %sql
# select * from PL_OU_YTD_NDA_DF

# COMMAND ----------

# Write Intermediate file into Delta Table

# PL_OU_YTD_NDA_DF.write.format("delta").mode("overwrite").save("/mnt/ADLS2/NDH/Sensitive/REFM_PL_OU_YTD_NDA_OP")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO NDH.REFM_PL_OU_YTD_NDA_OP
# MAGIC   select KPI,
# MAGIC         COMPANY_CODE,
# MAGIC         YEAR,
# MAGIC         PLAN_AMOUNT,
# MAGIC         PLAN_AMOUNT_USD,
# MAGIC         PLAN_YTD_AMOUNT,
# MAGIC         PLAN_YTD_AMOUNT_USD,
# MAGIC         LEASE_CLASSIFICATION,
# MAGIC         MONTH,
# MAGIC         LOCAL_CURRENCY,
# MAGIC         CREATE_DATE,
# MAGIC         UPDATE_DATE,
# MAGIC         NO_OF_LEASE_SITES,
# MAGIC         OP_SUBMISSIONS
# MAGIC from PL_OU_YTD_NDA_DF

# COMMAND ----------

dbutils.notebook.exit("Success")