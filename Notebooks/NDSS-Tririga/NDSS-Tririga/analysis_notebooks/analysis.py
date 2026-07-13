# Databricks notebook source
SBR_QUANTITY_RULES_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet"
df = spark.read.parquet(SBR_QUANTITY_RULES_mdh)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM NDH.RETAIL_SALES_NDT

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM NDH.AUDITTABLE WHERE SOURCESYSTEM = 'RSTS'

# COMMAND ----------

rsts_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/RSTS/NonSensitive/AggregatedSales/DHC/BE")
display(rsts_df)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import substring
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import when

#NDH Database name
NDH_DB = "NDH"

#Delta table names
LEASE_DATA_tbl = "LEASE_DATA_NDT"
SBR_QUANTITY_RULES_tbl = "SBR_QUANTITY_RULES_NDT"
SBR_VALUE_RULES_tbl = "SBR_VALUE_RULES_NDT"

#Source locations of Tririga PREP layer data
LEASE_DATA_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA.parquet"
SBR_QUANTITY_RULES_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet"
SBR_VALUE_RULES_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_VALUE_RULES/Global/TRIRIGA_SBR_VALUE_RULES.parquet"
PEOPLE_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_PEOPLE/Global/TRIRIGA_PEOPLE.parquet"
LOCATION_PROPERTY_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LOCATION_PROPERTY/Global/TRIRIGA_LOCATION_PROPERTY.parquet"
LOCATION_SPACE_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_LOCATION_SPACE/Global/TRIRIGA_LOCATION_SPACE.parquet"
LOCATION_BUILDING_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_LOCATION_BUILDING/Global/TRIRIGA_LOCATION_BUILDING.parquet"

#Destination locations of Curated layer
LEASE_DATA_cur = "/mnt/ADLS2/NDH/sensitive/TRIRIGA/LEASE_DATA_NDT"
SBR_QUANTITY_RULES_cur = "/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_QUANTITY_RULES_NDT"
SBR_VALUE_RULES_cur = "/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_VALUE_RULES_NDT"

#to get the company codes from NDH.COMP_CODE_ATTRIBUTES_NDT table
QUERY_COMP_CODE = "SELECT COMP_CODE,COUNTRY FROM NDH.COMP_CODE_ATTRIBUTES_NDT"
QUERY_LEASE_DATA = "SELECT Company_code,Country_code,Lease_Id FROM NDH.LEASE_DATA_NDT"

LEASE_DATA_df = spark.read.parquet(LEASE_DATA_mdh)
SBR_QUANTITY_RULES_df = spark.read.parquet(SBR_QUANTITY_RULES_mdh)
SBR_VALUE_RULES_df = spark.read.parquet(SBR_VALUE_RULES_mdh)
PEOPLE_df = spark.read.parquet(PEOPLE_mdh)
LOCATION_PROPERTY_df = spark.read.parquet(LOCATION_PROPERTY_mdh)
LOCATION_SPACE_df = spark.read.parquet(LOCATION_SPACE_mdh)
LOCATION_BUILDING_df = spark.read.parquet(LOCATION_BUILDING_mdh)

# COMMAND ----------

COMP_CODE_ATTRIB_df = spark.sql(QUERY_COMP_CODE)

cols = ("Ingestion_Revision_Date","GEOGRAPHY_NAME","TRI_GEOGRAPHY_LOOKUP_TX","TRI_HIERARCHY_PATH_TX","TRI_PEOPLE_TEMPLATE_TX","TRI_ROLE_NAME_TX","TRI_USER_GROUP_TX","TRI_USER_LICENCE_TX","TRI_STATUS_CL","TRI_USER_STATUS_CL","TRI_WORK_PHONE_TX","TRI_CONTROL_NUMBER_CN","TRI_ID_TX","TRI_MODIFIED_DATE_SY")

PEOPLE_df = PEOPLE_df.drop(*cols)
PEOPLE_df = PEOPLE_df.distinct()

LEASE_DATA_df = LEASE_DATA_df.join(COMP_CODE_ATTRIB_df,LEASE_DATA_df.TRI_BUSINESS_UNIT_ORG_TX == COMP_CODE_ATTRIB_df.COMP_CODE,"inner")\
                             .join(PEOPLE_df,LEASE_DATA_df.CST_PEOPLE_TX == PEOPLE_df.TRI_USER_NAME_TX,"inner")

LEASE_DATA_df = LEASE_DATA_df.select(
  col("TRI_NAME_TX").alias("Lease_Name")
  ,concat(LEASE_DATA_df.TRI_FIRST_NAME_TX,LEASE_DATA_df.TRI_LAST_NAME_TX).alias("DCH_Validator_Name")
  ,concat(LEASE_DATA_df.TRI_FIRST_NAME_TX,LEASE_DATA_df.TRI_LAST_NAME_TX).alias("Portfolio_Administrator_Name")
  ,concat(LEASE_DATA_df.TRI_FIRST_NAME_TX,LEASE_DATA_df.TRI_LAST_NAME_TX).alias("PA_Validator_Name")
  ,concat(LEASE_DATA_df.TRI_FIRST_NAME_TX,LEASE_DATA_df.TRI_LAST_NAME_TX).alias("Property_Manager_Name")
  ,col("CST_PEOPLE_TX").alias("DCH_Validator_Email_Id")
  ,col("CST_PEOPLE_TX1").alias("Portfolio_Administrator_Email_Id")
  ,col("CST_PEOPLE_TX2").alias("PA_Validator_Email_Id")
  ,col("CST_PEOPLE_TX3").alias("Property_Manager_Email_Id")
  ,col("CST_PREMISE_LOCATION_ID").alias("Premise_Location_Id")
  ,col("CST_PROPERTY_ID_TX").alias("Property_Id")
  ,substring(col("CST_PROPERTY_ID_TX"),6,15).alias("Site_Id")
  ,col("TRI_LEGACY_LEASE_ID_TX").alias("Legacy_Lease_Id")
  ,col("CST_SECONDARY_LEASE_TYPE_CL").alias("Secondary_Lease_Type_Text")
  ,col("CST_SPIDER_CONTRACT_APPROACH_LI").alias("Spider_Type_Text")
  ,col("CST_TENANT_ID_TX").alias("Tenant_Name")
  ,col("TRI_CONVERSION_GROUP_LI").alias("Conversion_Group_Text")
  ,col("TRI_LEASE_TERM_TX").alias("Lease_Term_Period")
  ,col("TRI_LEASE_TYPE_CL").alias("Lease_Type_Code")
  ,col("TRI_TOTAL_CONTRACT_AMOUNT_NU").alias("Total_Contract_LC_Amount")
  ,col("TRI_CONTACT_STATUS_TX").alias("Contact_Status_Code")
  ,col("TRI_CONTRACT_STATUS_CL").alias("Contract_Status_Code")
  ,col("TRI_STATUS_CL").alias("Lease_Status_Code")
  ,col("CST_LANDLORD_ID_TX").alias("Landlord_Name")
  ,col("TRI_TENANT_CONTACT_ORG_LOOKUP_TX").alias("Tenant_Organization_Text")
  ,col("TRI_EXPIRATION_DA").alias("Lease_Expiration_Date")
  ,col("TRI_ORIGINAL_EXPIRATION_DA").alias("Original_Contract_Expiration_Date")
  ,col("TRI_ORIGINAL_START_DA").alias("Original_Contract_Start_Date")
  ,col("TRI_RENT_COMMENCE_DA").alias("Rent_Commencement_Date")
  ,col("TRI_START_DA").alias("Contract_Start_Date")
  ,col("CST_SALES_BASED_RENT_BL").alias("SBR_Relevant_Status_Code")
  ,col("TRI_BUSINESS_UNIT_ORG_TX").alias("Company_Code")
  ,col("CST_CURRENCY_TX").alias("Local_Currency_Code")
  ,col("COUNTRY").alias("Country_Code")
  ,col("CST_BRAND_CATEGORY_TX").alias("Brand_Category_Text")
  ,col("CST_BRAND_SUB_CATEGORY_TX").alias("Brand_Sub_Category_Text")
  ,col("CST_BRAND_TX").alias("Brand_Name")
  ,col("CST_OFFER_TYPE_TX").alias("Offer_Type_Text")
  ,col("CST_OTHER_BRAND_TX").alias("Other_Brand_Text")
  ,col("CST_FORMAT_TYPE_CL").alias("Format_Type_Text")
  ,col("CST_BRAND_OPERATOR_OPEX_CL").alias("Brand_Operator_Text")
  ,col("CST_BUILDING_INVESTOR_CAPEX_CL").alias("Building_Investor_Text")
  ,col("CST_EQUIPMENT_INVESTOR_CAPEX_CL").alias("Equipment_Investor_Text")
  ,col("CST_OTHER_BRAND_SUB_CATEGORY_TX").alias("Other_Brand_Sub_Category_Text")
  ,col("TRI_MINOR_REVISION_NU").alias("Minor_Revision_Number")
  ,col("TRI_REVISION_NU").alias("Revision_Number")
  ,col("TRI_CONTROL_NUMBER_CN").alias("Control_Number")
  ,col("TRI_ID_TX").alias("Lease_Id")
  ,col("TRI_COLOCATION_BL").alias("CoLocator_Indicator")
  ,col("CST_SPIDER_TX").alias("Spider_Text")
  ,col("CST_EVERGREEN_BL").alias("Evergreen_Indicator")
  ,col("TRI_CURRENCY_UO").alias("Currency_Name")
  ,col("CST_CAPEX_APPLICABLE_LI").alias("Capex_Applicable_Indicator")
  ,col("CST_FINANCING_CAPEX_CLASS_LI").alias("Financing_Capex_Class_Code")
  ,col("CST_FINANCING_CAPEX_GROWTH_SUSTAIN_LI").alias("Financing_Capex_GrowthSustain_Type_Code")
#   ,when(LEASE_DATA_df.CST_BRAND_OPERATOR_OPEX_CL == ' ','null')
#   .when(LEASE_DATA_df.CST_BUILDING_INVESTOR_CAPEX_CL == ' ','null')
#   .when(LEASE_DATA_df.CST_EQUIPMENT_INVESTOR_CAPEX_CL == ' ','null')
#   .when(LEASE_DATA_df.CST_OTHER_BRAND_SUB_CATEGORY_TX == ' ','null')
).distinct()


# COMMAND ----------

LEASE_DATA_df.createOrReplaceTempView('LEASE_DATA')
LOCATION_PROPERTY_df.createOrReplaceTempView('LOCATION_PROPERTY')
LOCATION_SPACE_df.createOrReplaceTempView('LOCATION_SPACE')
LOCATION_BUILDING_df.createOrReplaceTempView('LOCATION_BUILDING')
PEOPLE_df.createOrReplaceTempView('PEOPLE')

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM LOCATION_BUILDING WHERE TRI_CONTROL_NUMBER_CN = 1011170 
# MAGIC
# MAGIC --SELECT DISTINCT * FROM LEASE_DATA WHERE Lease_Id IN (1010695,1015879,1015885) AND Lease_Type_Code = 'Expense Lease' AND Property_Id IS null
# MAGIC
# MAGIC --SELECT DISTINCT * FROM LEASE_DATA WHERE  Lease_Type_Code = 'Expense Lease' AND Property_Id IS null
# MAGIC
# MAGIC --SELECT * FROM LEASE_DATA WHERE TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null
# MAGIC --> TRI_ID_TX : 1010695 1015879 1015885 
# MAGIC --> CST_PREMISE_LOCATION_ID: 1016644 1024606 1024653
# MAGIC
# MAGIC --SELECT CST_PREMISE_LOCATION_ID,TRI_ID_TX,TRI_LEASE_TYPE_CL,CST_PROPERTY_ID_TX FROM LEASE_DATA WHERE TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null
# MAGIC
# MAGIC
# MAGIC --SELECT * FROM LEASE_DATA WHERE TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null
# MAGIC
# MAGIC --SELECT * FROM LOCATION_PROPERTY WHERE TRI_CONTROL_NUMBER_CN IN (1016644,1024606,1024653)
# MAGIC
# MAGIC --SELECT COUNT(*) FROM LOCATION_PROPERTY
# MAGIC
# MAGIC --SELECT * FROM LOCATION_PROPERTY WHERE TRI_CONTROL_NUMBER_CN IN (SELECT Premise_Location_Id FROM LEASE_DATA WHERE Lease_Type_Code = 'Expense Lease' AND Property_Id IS null)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ndh.SITE_BUSINESS_PERFORMANCE_NDA WHERE CREATE_DATE > '2022-07-14 19:09:48.4470000'

# COMMAND ----------

sbr_qr_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet")
sbr_vr_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_VALUE_RULES/Global/TRIRIGA_SBR_VALUE_RULES.parquet")

print(sbr_qr_df.count())
print(sbr_vr_df.count())

sbr_qr_df.createOrReplaceTempView('SBR_QR_VW')
sbr_vr_df.createOrReplaceTempView('SBR_VR_VW')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM SBR_QR_VW WHERE TRI_EFFECTIVE_FROM_DA IS null AND TRI_EFFECTIVE_TO_DA IS null

# COMMAND ----------

sbr_qr_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_SBR_QUANTITY_RULES/Global/TRIRIGA_SBR_QUANTITY_RULES.parquet")
print("Before Distinct & removing the columns : ",sbr_qr_df.count())

#sbr_qr_df.createOrReplaceTempView('SBR_QR_VW')

#display(sbr_qr_df)

cols = ("TRI_CLAUSE_ID_TX"
,"TRI_MODIFIED_DATE_SY"
,"Ingestion_Revision_Date")

#cols = ("TRI_MODIFIED_DATE_SY","Ingestion_Revision_Date")

sbr_qr_df = sbr_qr_df.drop(*cols)
sbr_qr_df = sbr_qr_df.distinct()





print("After Distinct : ",sbr_qr_df.count())

#display(sbr_qr_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM SBR_QR_VW WHERE TRI_LEASE_ID_TX = '1000789'
# MAGIC --SELECT Effective_From_Date,Effective_To_Date FROM NDH.SBR_QUANTITY_RULES_NDT

# COMMAND ----------

lease_data_df = spark.read.parquet("/mnt/ADLS1/PREP/1stParty/TRIRIGA/Sensitive/TRIRIGA_LEASE_DATA/Global/TRIRIGA_LEASE_DATA.parquet")
lease_data_df.createOrReplaceTempView('LEASE_DATA_VW')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM LEASE_DATA_VW WHERE TRI_ID_TX = '1015879' AND TRI_MODIFIED_DATE_SY = '2022-06-30T18:12:36.000+0000'
# MAGIC
# MAGIC --SELECT * FROM NDH.LEASE_DATA_NDT
# MAGIC
# MAGIC --1015879

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT CST_PROPERTY_ID_TX,
# MAGIC CASE 
# MAGIC 	WHEN TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null THEN 'True'
# MAGIC     ELSE 'False'
# MAGIC END AS CST_PROPERTY_ID_TX
# MAGIC FROM LEASE_DATA
# MAGIC
# MAGIC --SELECT CST_PROPERTY_ID_TX,TRI_LEASE_TYPE_CL FROM LEASE_DATA WHERE TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SELECT * FROM LEASE_DATA
# MAGIC
# MAGIC SELECT * FROM LEASE_DATA (CASE WHEN TRI_LEASE_TYPE_CL = 'Expense Lease' AND CST_PROPERTY_ID_TX IS null THEN SELECT LOCATION_PROPERTY.TRI_ID_TX WHERE LEASE_DATA.CST_PREMISE_LOCATION_ID =  LOCATION_PROPERTY.TRI_CONTROL_NUMBER_CN AS CST_PROPERTY_ID_TX)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SELECT * FROM ndh.SITE_PERFORMANCE_RATIO_NDA ORDER BY CREATE_DATE DESC
# MAGIC
# MAGIC --SELECT * FROM ndh.SITE_PERFORMANCE_RATIO_NDA
# MAGIC
# MAGIC --SELECT * FROM ndh.SITE_BUSINESS_PERFORMANCE_NDA ORDER BY CREATE_DATE DESC
# MAGIC --SELECT * FROM ndh.SITE_BUSINESS_PERFORMANCE_NDA
# MAGIC
# MAGIC --SELECT * FROM ndh.RETAIL_SALES_NDT ORDER BY CREATE_DATE DESC
# MAGIC
# MAGIC --SELECT * FROM ndh.RETAIL_SALES_NDT
# MAGIC
# MAGIC SELECT * FROM ndh.SITE_OFFER_NDA ORDER BY CREATE_DATE DESC
# MAGIC
# MAGIC --SELECT * FROM ndh.SITE_OFFER_NDA
