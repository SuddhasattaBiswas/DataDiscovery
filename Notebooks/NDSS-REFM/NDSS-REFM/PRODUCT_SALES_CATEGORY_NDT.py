# Databricks notebook source
#NDH Database name
NDH_DB = "NDH"
#Delta table names
PRODUCT_SALES_CATEGORY_tbl = "PRODUCT_SALES_CATEGORY_NDT"

# COMMAND ----------

#Source location of mapping table

#to be changed

PRODUCT_SALES_CATEGORY_mdh = "/mnt/ADLS1/PREP/1stParty/BUSINESS_FEED_TRIRIGA_GSAP/NonSensitive/SALES_CATEGORY_PRODUCT_MAPPING/GLOBAL/PRODUCT_SALES_CATEGORY.parquet"


#Destination locations of Curated layer

PRODUCT_SALES_CATEGORY_cur = "/mnt/ADLS2/NDH/NonSensitive/NDSS/NVM/PRODUCT_SALES_CATEGORY_Delta"

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS {0} LOCATION '{1}'""".format(NDH_DB,PRODUCT_SALES_CATEGORY_cur))
spark.sql("""CREATE TABLE IF NOT EXISTS {0}.{1} USING DELTA LOCATION '{2}'""".format(NDH_DB,PRODUCT_SALES_CATEGORY_tbl,PRODUCT_SALES_CATEGORY_cur))
