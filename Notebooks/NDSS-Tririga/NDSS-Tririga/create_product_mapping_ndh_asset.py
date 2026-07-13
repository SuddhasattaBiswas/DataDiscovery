# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#NDH Database name
NDH_DB = "NDH"

#Delta table names
PRODUCT_SALES_CATEGORY_tbl = "PRODUCT_SALES_CATEGORY_NDT"

#Source location of mapping table
#to be changed
PRODUCT_SALES_CATEGORY_mdh = "/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_TRIRIGA_GSAP/NonSensitive/SALES_CATEGORY_PRODUCT_MAPPING/GLOBAL/PRODUCT_SALES_CATEGORY.parquet"

#Destination locations of Curated layer
PRODUCT_SALES_CATEGORY_cur = "/mnt/ADLS2/NDH/NonSensitive/NDSS/NVM/PRODUCT_SALES_CATEGORY_Delta"

# COMMAND ----------

PRODUCT_SALES_CATEGORY_df = spark.read.parquet(PRODUCT_SALES_CATEGORY_mdh)

# COMMAND ----------

PRODUCT_SALES_CATEGORY_df = PRODUCT_SALES_CATEGORY_df.select(
  concat(PRODUCT_SALES_CATEGORY_df.Country_Code,PRODUCT_SALES_CATEGORY_df.Calendar_Year,PRODUCT_SALES_CATEGORY_df.Calendar_Month,PRODUCT_SALES_CATEGORY_df.Sales_Category,PRODUCT_SALES_CATEGORY_df.Product_Group_Code,PRODUCT_SALES_CATEGORY_df.Product_Family_Code).alias("Product_Sales_Category_Id")
  ,col("Country_Code").alias("Country_Code")
  ,col("Calendar_Year").alias("Calendar_Year")
  ,col("Calendar_Month").alias("Calendar_Month")
  ,col("Sales_Category").alias("Sales_Category")
  ,col("Product_Group_Code").alias("Product_Group_Code")
  ,col("Product_Group_Name").alias("Product_Group_Name")
  ,col("Product_Family_Code").alias("Product_Family_Code")
  ,col("Product_Family_Name").alias("Product_Family_Name")
  ,current_timestamp().cast("timestamp").alias("CREATE_DATE")
)

# COMMAND ----------

existing_count_df = spark.sql("SELECT COUNT(*) AS ROWCNT FROM NDH.COUNTRY_SPACE_COUNT_NDA")
#PRODUCT_SALES_CATEGORY_NDT")

# COMMAND ----------

existing_count_df = spark.sql("SELECT COUNT(*) AS ROWCNT FROM NDH.PRODUCT_SALES_CATEGORY_NDT")
existing_count = existing_count_df.select("ROWCNT").collect()[0][0]
print(existing_count)

if (existing_count == 0):
  PRODUCT_SALES_CATEGORY_df.write.format('delta').mode('overwrite').save(PRODUCT_SALES_CATEGORY_cur)
  print("Overwite success")
else:
  existing_records_df = spark.sql("SELECT * FROM NDH.PRODUCT_SALES_CATEGORY_NDT")
  PRODUCT_SALES_CATEGORY_df = PRODUCT_SALES_CATEGORY_df.unionAll(existing_records_df).createOrReplaceTempView('PRODUCT_SALES_CATEGORY_vw')
  df_partition = spark.sql("SELECT *, DENSE_RANK() OVER (PARTITION BY Product_Sales_Category_Id ORDER BY CREATE_DATE DESC) AS NUMBER FROM PRODUCT_SALES_CATEGORY_vw")
  PRODUCT_SALES_CATEGORY_df = df_partition.filter("NUMBER == 1").drop("NUMBER")
  PRODUCT_SALES_CATEGORY_df.write.format('delta').mode('overwrite').save(PRODUCT_SALES_CATEGORY_cur)
  print("Merge success")

# COMMAND ----------

# src = "/mnt/ADLS2/NDH/NonSensitive/NDSS/NVM/PRODUCT_SALES_CATEGORY_Delta/PRODUCT_SALES_CATEGORY.parquet"
# dst = "/mnt/ADLS2/NDH/NonSensitive/NDSS/NVM/PRODUCT_SALES_CATEGORY.parquet"

# dbutils.fs.cp(src,dst)

# COMMAND ----------

# dbutils.fs.rm("/mnt/ADLS2/NDH/NonSensitive/NDSS/NVM/PRODUCT_SALES_CATEGORY_Delta/PRODUCT_SALES_CATEGORY.parquet",True)
