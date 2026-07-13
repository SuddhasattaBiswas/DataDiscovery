# Databricks notebook source
# DBTITLE 1,Package imports
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import substring
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Parameters used to create delta tables
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
LOCATION_LAND_mdh = "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_LOCATION_LAND/Global/TRIRIGA_LOCATION_LAND.parquet"
LEASE_CLAUSE_mdh= "/mnt/ADLS1/PREP/1stParty/TRIRIGA/NonSensitive/TRIRIGA_LEASE_CLAUSE/Global/TRIRIGA_LEASE_CLAUSE.parquet"

#Destination locations of Curated layer
LEASE_DATA_cur = "/mnt/ADLS2/NDH/Sensitive/TRIRIGA/LEASE_DATA_NDT"
SBR_QUANTITY_RULES_cur = "/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_QUANTITY_RULES_NDT"
SBR_VALUE_RULES_cur = "/mnt/ADLS2/NDH/NonSensitive/TRIRIGA/SBR_VALUE_RULES_NDT"

# COMMAND ----------

# DBTITLE 1,Queries used
#to get the company codes from NDH.COMP_CODE_ATTRIBUTES_NDT table
QUERY_COMP_CODE = "SELECT COMP_CODE,COUNTRY FROM NDH.COMP_CODE_ATTRIBUTES_NDT WHERE COUNTRY != ' '"

#to get the CST_PROPERTY_ID_TX value for LEASE_DATA table
QUERY_CST_PROPERTY_ID_TX = """SELECT
	LD.TRI_NAME_TX
	,LD.CST_PEOPLE_TX
	,LD.CST_PEOPLE_TX1
	,LD.CST_PEOPLE_TX2
	,LD.CST_PEOPLE_TX3
	,LD.CST_PREMISE_LOCATION_ID
	,COALESCE(LD.CST_PROPERTY_ID_TX,LP.TRI_ID_TX,LB.TRI_PROPERTY_ID,LL.TRI_PROPERTY_ID) AS CST_PROPERTY_ID_TX
	,LD.TRI_LEGACY_LEASE_ID_TX
	,LD.CST_SECONDARY_LEASE_TYPE_CL
	,LD.CST_SPIDER_CONTRACT_APPROACH_LI
	,LD.CST_TENANT_ID_TX
	,LD.TRI_CONVERSION_GROUP_LI
	,LD.TRI_LEASE_TERM_TX
	,LD.TRI_LEASE_TYPE_CL
	,LD.TRI_TOTAL_CONTRACT_AMOUNT_NU
	,LD.TRI_CONTACT_STATUS_TX
	,LD.TRI_CONTRACT_STATUS_CL
	,LD.TRI_STATUS_CL
	,LD.CST_LANDLORD_ID_TX
	,LD.TRI_TENANT_CONTACT_ORG_LOOKUP_TX
	,LD.TRI_EXPIRATION_DA
	,LD.TRI_ORIGINAL_EXPIRATION_DA
	,LD.TRI_ORIGINAL_START_DA
	,LD.TRI_RENT_COMMENCE_DA
	,LD.TRI_START_DA
	,LD.CST_SALES_BASED_RENT_BL
	,LD.TRI_BUSINESS_UNIT_ORG_TX
	,LD.CST_CURRENCY_TX
	,LD.CST_BRAND_CATEGORY_TX
	,LD.CST_BRAND_SUB_CATEGORY_TX
	,LD.CST_BRAND_TX
	,LD.CST_OFFER_TYPE_TX
	,LD.CST_OTHER_BRAND_TX
	,LD.CST_FORMAT_TYPE_CL
	,LD.CST_BRAND_OPERATOR_OPEX_CL
	,LD.CST_BUILDING_INVESTOR_CAPEX_CL
	,LD.CST_EQUIPMENT_INVESTOR_CAPEX_CL
	,LD.CST_OTHER_BRAND_SUB_CATEGORY_TX
	,LD.TRI_MINOR_REVISION_NU
	,LD.TRI_REVISION_NU
	,LD.TRI_CONTROL_NUMBER_CN
	,LD.TRI_ID_TX
	,LD.TRI_COLOCATION_BL
	,LD.CST_SPIDER_TX
	,LD.CST_EVERGREEN_BL
	,LD.TRI_CURRENCY_UO
	,LD.CST_CAPEX_APPLICABLE_LI
	,LD.CST_FINANCING_CAPEX_CLASS_LI
	,LD.CST_FINANCING_CAPEX_GROWTH_SUSTAIN_LI
    ,LD.TRI_MODIFIED_DATE_SY
FROM LEASE_DATA LD 
LEFT JOIN LOCATION_PROPERTY LP 
ON LD.CST_PREMISE_LOCATION_ID=LP.TRI_CONTROL_NUMBER_CN 
LEFT JOIN LOCATION_BUILDING LB 
ON LD.CST_PREMISE_LOCATION_ID=LB.TRI_CONTROL_NUMBER_CN 
LEFT JOIN LOCATION_LAND LL
ON LD.CST_PREMISE_LOCATION_ID=LL.TRI_CONTROL_NUMBER_CN  
WHERE TRI_LEASE_TYPE_CL='Expense Lease' AND LD.CST_PROPERTY_ID_TX IS null
UNION ALL
SELECT 
	LD2.TRI_NAME_TX
	,LD2.CST_PEOPLE_TX
	,LD2.CST_PEOPLE_TX1
	,LD2.CST_PEOPLE_TX2
	,LD2.CST_PEOPLE_TX3
	,LD2.CST_PREMISE_LOCATION_ID
	,COALESCE(LB2.TRI_PROPERTY_ID,LS.TRI_PROPERTY_ID) AS CST_PROPERTY_ID_TX
	,LD2.TRI_LEGACY_LEASE_ID_TX
	,LD2.CST_SECONDARY_LEASE_TYPE_CL
	,LD2.CST_SPIDER_CONTRACT_APPROACH_LI
	,LD2.CST_TENANT_ID_TX
	,LD2.TRI_CONVERSION_GROUP_LI
	,LD2.TRI_LEASE_TERM_TX
	,LD2.TRI_LEASE_TYPE_CL
	,LD2.TRI_TOTAL_CONTRACT_AMOUNT_NU
	,LD2.TRI_CONTACT_STATUS_TX
	,LD2.TRI_CONTRACT_STATUS_CL
	,LD2.TRI_STATUS_CL
	,LD2.CST_LANDLORD_ID_TX
	,LD2.TRI_TENANT_CONTACT_ORG_LOOKUP_TX
	,LD2.TRI_EXPIRATION_DA
	,LD2.TRI_ORIGINAL_EXPIRATION_DA
	,LD2.TRI_ORIGINAL_START_DA
	,LD2.TRI_RENT_COMMENCE_DA
	,LD2.TRI_START_DA
	,LD2.CST_SALES_BASED_RENT_BL
	,LD2.TRI_BUSINESS_UNIT_ORG_TX
	,LD2.CST_CURRENCY_TX
	,LD2.CST_BRAND_CATEGORY_TX
	,LD2.CST_BRAND_SUB_CATEGORY_TX
	,LD2.CST_BRAND_TX
	,LD2.CST_OFFER_TYPE_TX
	,LD2.CST_OTHER_BRAND_TX
	,LD2.CST_FORMAT_TYPE_CL
	,LD2.CST_BRAND_OPERATOR_OPEX_CL
	,LD2.CST_BUILDING_INVESTOR_CAPEX_CL
	,LD2.CST_EQUIPMENT_INVESTOR_CAPEX_CL
	,LD2.CST_OTHER_BRAND_SUB_CATEGORY_TX
	,LD2.TRI_MINOR_REVISION_NU
	,LD2.TRI_REVISION_NU
	,LD2.TRI_CONTROL_NUMBER_CN
	,LD2.TRI_ID_TX
	,LD2.TRI_COLOCATION_BL
	,LD2.CST_SPIDER_TX
	,LD2.CST_EVERGREEN_BL
	,LD2.TRI_CURRENCY_UO
	,LD2.CST_CAPEX_APPLICABLE_LI
	,LD2.CST_FINANCING_CAPEX_CLASS_LI
	,LD2.CST_FINANCING_CAPEX_GROWTH_SUSTAIN_LI
    ,LD2.TRI_MODIFIED_DATE_SY
FROM LEASE_DATA LD2
LEFT JOIN LOCATION_BUILDING LB2
ON LD2.CST_PREMISE_LOCATION_ID=LB2.TRI_CONTROL_NUMBER_CN
LEFT JOIN LOCATION_SPACE LS
ON LD2.CST_PREMISE_LOCATION_ID=LS.TRI_CONTROL_NUMBER_CN
WHERE TRI_LEASE_TYPE_CL='Income Lease' AND LD2.CST_PROPERTY_ID_TX IS null
UNION ALL
SELECT 
	TRI_NAME_TX
	,CST_PEOPLE_TX
	,CST_PEOPLE_TX1
	,CST_PEOPLE_TX2
	,CST_PEOPLE_TX3
	,CST_PREMISE_LOCATION_ID
	,CST_PROPERTY_ID_TX
	,TRI_LEGACY_LEASE_ID_TX
	,CST_SECONDARY_LEASE_TYPE_CL
	,CST_SPIDER_CONTRACT_APPROACH_LI
	,CST_TENANT_ID_TX
	,TRI_CONVERSION_GROUP_LI
	,TRI_LEASE_TERM_TX
	,TRI_LEASE_TYPE_CL
	,TRI_TOTAL_CONTRACT_AMOUNT_NU
	,TRI_CONTACT_STATUS_TX
	,TRI_CONTRACT_STATUS_CL
	,TRI_STATUS_CL
	,CST_LANDLORD_ID_TX
	,TRI_TENANT_CONTACT_ORG_LOOKUP_TX
	,TRI_EXPIRATION_DA
	,TRI_ORIGINAL_EXPIRATION_DA
	,TRI_ORIGINAL_START_DA
	,TRI_RENT_COMMENCE_DA
	,TRI_START_DA
	,CST_SALES_BASED_RENT_BL
	,TRI_BUSINESS_UNIT_ORG_TX
	,CST_CURRENCY_TX
	,CST_BRAND_CATEGORY_TX
	,CST_BRAND_SUB_CATEGORY_TX
	,CST_BRAND_TX
	,CST_OFFER_TYPE_TX
	,CST_OTHER_BRAND_TX
	,CST_FORMAT_TYPE_CL
	,CST_BRAND_OPERATOR_OPEX_CL
	,CST_BUILDING_INVESTOR_CAPEX_CL
	,CST_EQUIPMENT_INVESTOR_CAPEX_CL
	,CST_OTHER_BRAND_SUB_CATEGORY_TX
	,TRI_MINOR_REVISION_NU
	,TRI_REVISION_NU
	,TRI_CONTROL_NUMBER_CN
	,TRI_ID_TX
	,TRI_COLOCATION_BL
	,CST_SPIDER_TX
	,CST_EVERGREEN_BL
	,TRI_CURRENCY_UO
	,CST_CAPEX_APPLICABLE_LI
	,CST_FINANCING_CAPEX_CLASS_LI
	,CST_FINANCING_CAPEX_GROWTH_SUSTAIN_LI
    ,TRI_MODIFIED_DATE_SY
FROM LEASE_DATA
WHERE CST_PROPERTY_ID_TX IS NOT null"""

# COMMAND ----------

# DBTITLE 1,Read data from ADLS layer - Source
LEASE_DATA_df = spark.read.parquet(LEASE_DATA_mdh)
SBR_QUANTITY_RULES_df = spark.read.parquet(SBR_QUANTITY_RULES_mdh)
SBR_VALUE_RULES_df = spark.read.parquet(SBR_VALUE_RULES_mdh)
PEOPLE_df = spark.read.parquet(PEOPLE_mdh)
LOCATION_PROPERTY_df = spark.read.parquet(LOCATION_PROPERTY_mdh)
LOCATION_SPACE_df = spark.read.parquet(LOCATION_SPACE_mdh)
LOCATION_BUILDING_df = spark.read.parquet(LOCATION_BUILDING_mdh)
LOCATION_LAND_df = spark.read.parquet(LOCATION_LAND_mdh)
LEASE_CLAUSE_df = spark.read.parquet(LEASE_CLAUSE_mdh)

# COMMAND ----------

# DBTITLE 1,Dropping columns that are not required
#LEASE_CLAUSE
cols = ("CST_LEASE_CONTRACT_ID_TX"
,"TRI_LEGACY_CLAUSE_ID_TX"
,"TRI_CLAUSE_TYPE_CL"
,"TRI_DESCRIPTION_TX"
,"TRI_CONTROL_NUMBER_CN"
,"TRI_LEASE_ID_TX"
,"TRI_MODIFIED_DATE_SY"
,"CST_SYSTEM_GEO_TX"
,"Ingestion_Revision_Date")

LEASE_CLAUSE_df = LEASE_CLAUSE_df.drop(*cols)
LEASE_CLAUSE_df = LEASE_CLAUSE_df.distinct()

#SBR_VALUE_RULES
cols = ("TRI_CURRENCY_UO"
,"CST_CURRENCY_TX"
,"Ingestion_Revision_Date")

SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.drop(*cols)
SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.distinct()

#SBR_QUANTITY_RULES
cols = ("Ingestion_Revision_Date")

SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.drop(*cols)
SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.distinct()

#LEASE_DATA
cols = ("CST_REMIT_TO_ID_TX"
,"TRI_LAND_LORD_OWNER_ORG_LOOKUP_TX"
,"TRI_CONTACT_NAME_TX"
,"TRI_CONTACT_ROLE_TX"
,"CST_SYSTEM_GEO_TX"
,"Ingestion_Revision_Date")

LEASE_DATA_df = LEASE_DATA_df.drop(*cols)
LEASE_DATA_df = LEASE_DATA_df.distinct()

#PEOPLE
cols = ("Ingestion_Revision_Date"
,"GEOGRAPHY_NAME"
,"TRI_GEOGRAPHY_LOOKUP_TX"
,"TRI_HIERARCHY_PATH_TX"
,"TRI_PEOPLE_TEMPLATE_TX"
,"TRI_ROLE_NAME_TX"
,"TRI_USER_GROUP_TX"
,"TRI_USER_LICENCE_TX"
,"TRI_STATUS_CL"
,"TRI_USER_STATUS_CL"
,"TRI_WORK_PHONE_TX"
,"TRI_CONTROL_NUMBER_CN"
,"TRI_ID_TX"
,"TRI_MODIFIED_DATE_SY")

PEOPLE_df = PEOPLE_df.drop(*cols)
PEOPLE_df = PEOPLE_df.distinct()

#LOCATION_PROPERTY
cols = ("TRI_ADDRESS_TX"
,"HIERARCHY_PATH"
,"TRI_GEOGRAPHY_LOOKUP_TX"
,"TRI_NAME_TX"
,"TRI_REGION_TX"
,"TRI_STATE_PROV_TX"
,"CST_OPERATING_STATUS_LI"
,"CST_REAL_ESTATE_STATUS_LI"
,"TRI_CITY_TX"
,"TRI_LOCATION_TYPE"
,"TRI_PRIMARY_AREA_UNIT_UO"
,"TRI_PROPERTY_CLASS_CL"
,"TRI_TENURE_TYPE_CL"
,"TRI_ZIP_POSTAL_TX"
,"TRI_STATUS_CL"
,"CST_AREA_NU"
,"CST_OBJECT_VALID_FROM_DA"
,"CST_OBJECT_VALID_TO_DA"
,"TRI_MODIFIED_DATE_SY"
,"CST_SYSTEM_GEO_TX"
,"Ingestion_Revision_Date"
,"EndDate"
,"IsActive")

LOCATION_PROPERTY_df = LOCATION_PROPERTY_df.drop(*cols)
LOCATION_PROPERTY_df = LOCATION_PROPERTY_df.distinct()

#LOCATION_BUILDING
cols = ("TRI_ADDRESS_TX"
,"HIERARCHY_PATH"
,"CST_LEGACY_BUILDING_ID_TX"
,"TRI_NAME_TX"
,"CST_HERITABLE_RIGHT_LI"
,"CST_USAGE_TYPE_LI"
,"TRI_BUILDING_CLASS_CL"
,"TRI_CITY_TX"
,"TRI_PRIMARY_AREA_UNIT_UO"
,"TRI_ZIP_POSTAL_TX"
,"TRI_STATUS_CL"
,"TRI_COST_CENTER_TX"
,"TRI_ID_TX"
,"TRI_ACTIVE_START_DA"
,"TRI_MODIFIED_DATE_SY"
,"CST_TOTAL_AREA_NU"
,"TRI_GROSS_AREA_NU"
,"CST_SYSTEM_GEO_TX"
,"Ingestion_Revision_Date"
,"EndDate"
,"IsActive")

LOCATION_BUILDING_df = LOCATION_BUILDING_df.drop(*cols)
LOCATION_BUILDING_df = LOCATION_BUILDING_df.distinct()

#LOCATION_LAND
cols = ("TRI_ADDRESS_TX"
,"TRI_NAME_TX"
,"HIERARCHY_PATH"
,"CST_LEGACY_LAND_ID_TX"
,"CST_LAND_USAGE_TYPE_LI"
,"LAND_TENURE"
,"TRI_CITY_TX"
,"TRI_COMMON_NAME_TX"
,"TRI_LAND_CLASS_CL"
,"TRI_PRIMARY_AREA_UNIT_UO"
,"TRI_ZIP_POSTAL_TX"
,"CST_LAND_VALUE_NU"
,"CST_PURCHASE_PRICE_NU"
,"TRI_STATUS_CL"
,"TRI_ID_TX"
,"CST_HERITABLE_RIGHT_LI"
,"CST_IN_SERVICE_DATE_DA"
,"CST_VALUATION_DATE_DA"
,"TRI_ACTIVE_START_DA"
,"TRI_MODIFIED_DATE_SY"
,"CST_TOTAL_AREA_NU"
,"TRI_LAND_AREA_NU"
,"TRI_COST_CENTER_TX"
,"CST_SYSTEM_GEO_TX"
,"Ingestion_Revision_Date"
,"EndDate"
,"IsActive")

LOCATION_LAND_df = LOCATION_LAND_df.drop(*cols)
LOCATION_LAND_df = LOCATION_LAND_df.distinct()

#LOCATION_SPACE
cols = ("HIERARCHY_PATH"
,"TRI_NAME_TX"
,"TRI_SPACE_CLASS_TX"
,"CST_LEGACY_ID_TX"
,"CST_SPACE_USAGE_TYPE_LI"
,"TRI_PLAN_SC_CL"
,"TRI_STATUS_CL"
,"CST_PROFIT_CENTER_TX"
,"TRI_COST_CENTER_TX"
,"TRI_ID_TX"
,"TRI_ACTIVE_END_DA"
,"TRI_ACTIVE_START_DA"
,"TRI_CREATED_SY"
,"TRI_MODIFIED_DATE_SY"
,"TRI_MEAS_AREA_AMT_NU"
,"TRI_PRIMARY_AREA_UNIT_UO"
,"CST_SYSTEM_GEO_TX"
,"TRI_FLOOR_ID"
,"Ingestion_Revision_Date"
,"EndDate"
,"IsActive")

LOCATION_SPACE_df = LOCATION_SPACE_df.drop(*cols)
LOCATION_SPACE_df = LOCATION_SPACE_df.distinct()

# COMMAND ----------

# DBTITLE 1,Creating temp views
LEASE_DATA_df.createOrReplaceTempView('LEASE_DATA')
LOCATION_PROPERTY_df.createOrReplaceTempView('LOCATION_PROPERTY')
LOCATION_SPACE_df.createOrReplaceTempView('LOCATION_SPACE')
LOCATION_BUILDING_df.createOrReplaceTempView('LOCATION_BUILDING')
PEOPLE_df.createOrReplaceTempView('PEOPLE')
LOCATION_LAND_df.createOrReplaceTempView('LOCATION_LAND')

# COMMAND ----------

# DBTITLE 1,NDH.LEASE_DATA_NDT
COMP_CODE_ATTRIB_df = spark.sql(QUERY_COMP_CODE)

print("Initial count : ",LEASE_DATA_df.count())

LEASE_DATA_df = spark.sql(QUERY_CST_PROPERTY_ID_TX)
LEASE_DATA_df = LEASE_DATA_df.join(COMP_CODE_ATTRIB_df,LEASE_DATA_df.TRI_BUSINESS_UNIT_ORG_TX == COMP_CODE_ATTRIB_df.COMP_CODE,"left")
LEASE_DATA_df = LEASE_DATA_df.join(PEOPLE_df,LEASE_DATA_df.CST_PEOPLE_TX == PEOPLE_df.TRI_USER_NAME_TX,"left")

print("After getting country codes, usernames, property IDs : ",LEASE_DATA_df.count())

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
  ,col("TRI_ID_TX").alias("Lease_Transaction_Id")
  ,col("TRI_COLOCATION_BL").alias("CoLocator_Indicator")
  ,col("CST_SPIDER_TX").alias("Spider_Text")
  ,col("CST_EVERGREEN_BL").alias("Evergreen_Indicator")
  ,col("TRI_CURRENCY_UO").alias("Currency_Name")
  ,col("CST_CAPEX_APPLICABLE_LI").alias("Capex_Applicable_Indicator")
  ,col("CST_FINANCING_CAPEX_CLASS_LI").alias("Financing_Capex_Class_Code")
  ,col("CST_FINANCING_CAPEX_GROWTH_SUSTAIN_LI").alias("Financing_Capex_GrowthSustain_Type_Code")
  ,col("TRI_MODIFIED_DATE_SY").alias("Source_Last_Modified_Date")
  ,current_timestamp().cast("timestamp").alias("Create_Date")
)

print("Final count : ",LEASE_DATA_df.count())

LEASE_DATA_df.write.format('delta').mode('overwrite').save(LEASE_DATA_cur)

# COMMAND ----------

# DBTITLE 1,Dropping columns that are not required
#Re-using LEASE_DATA dataframe for getting country name and code, to avoid data duplication issue removing the columns that are not required.

cols = ("Lease_Name"
,"DCH_Validator_Name"
,"Portfolio_Administrator_Name"
,"PA_Validator_Name"
,"Property_Manager_Name"
,"DCH_Validator_Email_Id"
,"Portfolio_Administrator_Email_Id"
,"PA_Validator_Email_Id"
,"Property_Manager_Email_Id"
,"Premise_Location_Id"
,"Property_Id"
,"Site_Id"
,"Legacy_Lease_Id"
,"Secondary_Lease_Type_Text"
,"Spider_Type_Text"
,"Tenant_Name"
,"Conversion_Group_Text"
,"Lease_Term_Period"
,"Lease_Type_Code"
,"Total_Contract_LC_Amount"
,"Contact_Status_Code"
,"Contract_Status_Code"
,"Lease_Status_Code"
,"Landlord_Name"
,"Tenant_Organization_Text"
,"Lease_Expiration_Date"
,"Original_Contract_Expiration_Date"
,"Original_Contract_Start_Date"
,"Rent_Commencement_Date"
,"Contract_Start_Date"
,"SBR_Relevant_Status_Code"
,"Local_Currency_Code"
,"Brand_Category_Text"
,"Brand_Sub_Category_Text"
,"Brand_Name"
,"Offer_Type_Text"
,"Other_Brand_Text"
,"Format_Type_Text"
,"Brand_Operator_Text"
,"Building_Investor_Text"
,"Equipment_Investor_Text"
,"Other_Brand_Sub_Category_Text"
,"Minor_Revision_Number"
,"Revision_Number"
,"Control_Number"
,"CoLocator_Indicator"
,"Spider_Text"
,"Evergreen_Indicator"
,"Currency_Name"
,"Capex_Applicable_Indicator"
,"Financing_Capex_Class_Code"
,"Financing_Capex_GrowthSustain_Type_Code"
,"Source_Last_Modified_Date"
,"Create_Date")

LEASE_DATA_df = LEASE_DATA_df.drop(*cols)
LEASE_DATA_df = LEASE_DATA_df.distinct()

# COMMAND ----------

# DBTITLE 1,Renaming TRI_ID_TX Column
#Renaming TRI_ID_TX column to avoid the ambiguous issue.

LEASE_CLAUSE_df = LEASE_CLAUSE_df.select(
  col("TRI_ID_TX").alias("TRI_ID_TX_lc")
  ,col("TRI_STATUS_CL").alias("TRI_STATUS_CL")
).distinct()

# COMMAND ----------

# DBTITLE 1,NDH.SBR_QUANTITY_RULES
print("Initial count : ",SBR_QUANTITY_RULES_df.count())

SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.join(LEASE_DATA_df,SBR_QUANTITY_RULES_df.TRI_LEASE_ID_TX == LEASE_DATA_df.Lease_Transaction_Id,"left")

print("After getting country code : ",SBR_QUANTITY_RULES_df.count())

SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.join(LEASE_CLAUSE_df,SBR_QUANTITY_RULES_df.TRI_CLAUSE_ID_TX == LEASE_CLAUSE_df.TRI_ID_TX_lc,"left")

print("After joining with LEASE_CLAUSE : ",SBR_QUANTITY_RULES_df.count())

SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.filter((SBR_QUANTITY_RULES_df.TRI_STATUS_CL == 'Active') | (SBR_QUANTITY_RULES_df.TRI_STATUS_CL == 'Retired'))

print("After status filter : ",SBR_QUANTITY_RULES_df.count())

SBR_QUANTITY_RULES_df = SBR_QUANTITY_RULES_df.select(
  col("TRI_CLAUSE_ID_TX").alias("Lease_Clause_Legacy_Id")
  	,col("TRI_SALES_CATEGORY_CL").alias("Sales_Category")
	,col("CST_MAX_QUANTITY_NU").alias("Max_Quantity")
	,col("CST_MIN_AMOUNT_NU").alias("Min_LC_Amount")
	,col("CST_MIN_QUANTITY_NU").alias("Min_Quantity")
	,col("CST_QUANTITY_CALC_NU").alias("Multiplier_Value")
	,col("CST_QUANTITY_CALC_LI").alias("Quantity_Type_Code")
	,col("CST_PAYMENT_METHOD_LI").alias("Payment_Method_Type_Text")
	,col("TRI_LEASE_ID_TX").alias("Lease_Transaction_Id")
	,col("Country_Code").alias("Country_Code")
	,col("Company_Code").alias("Company_Code")
	,col("TRI_EFFECTIVE_FROM_DA").alias("Effective_From_Date")
	,col("TRI_EFFECTIVE_TO_DA").alias("Effective_To_Date")
	,col("TRI_CONTROL_NUMBER_CN").alias("Control_Number")
	,col("TRI_ID_TX").alias("Transaction_Id")
	,col("CST_SLIDING_GRADE_BL").alias("Sliding_Grade_Indicator")
    ,col("TRI_MODIFIED_DATE_SY").alias("Source_Last_Modified_Date")
    ,current_timestamp().cast("timestamp").alias("Create_Date")
)

print("Final Count : ",SBR_QUANTITY_RULES_df.count())

SBR_QUANTITY_RULES_df.write.format('delta').mode('overwrite').save(SBR_QUANTITY_RULES_cur)

# COMMAND ----------

# DBTITLE 1,NDH.SBR_VALUE_RULES
print("Initial count : ",SBR_VALUE_RULES_df.count())

SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.join(LEASE_DATA_df,SBR_VALUE_RULES_df.TRI_LEASE_ID_TX == LEASE_DATA_df.Lease_Transaction_Id,"left")

print("After getting country code : ",SBR_VALUE_RULES_df.count())

SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.join(LEASE_CLAUSE_df,SBR_VALUE_RULES_df.TRI_CLAUSE_ID_TX == LEASE_CLAUSE_df.TRI_ID_TX_lc,"left")

print("After joining with LEASE_CLAUSE : ",SBR_VALUE_RULES_df.count())

SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.filter((SBR_VALUE_RULES_df.TRI_STATUS_CL == 'Active') | (SBR_VALUE_RULES_df.TRI_STATUS_CL == 'Retired'))

print("After status filter : ",SBR_VALUE_RULES_df.count())

SBR_VALUE_RULES_df = SBR_VALUE_RULES_df.select(
	 col("TRI_CLAUSE_ID_TX").alias("Lease_Clause_Legacy_Id")
	,col("TRI_SALES_CATEGORY_CL").alias("Sales_Category")
	,col("CST_MIN_AMOUNT_NU").alias("Custom_Min_LC_Amount")
	,col("TRI_MAX_AMOUNT_NU").alias("Max_LC_Amount")
	,col("TRI_MIN_AMOUNT_NU").alias("Min_LC_Amount")
	,col("TRI_PERCENT_RENT_NU").alias("Percentage_Value")
	,col("CST_PAYMENT_METHOD_LI").alias("Payment_Method_Type_Text")
	,col("TRI_LEASE_ID_TX").alias("Lease_Transaction_Id")
	,col("Country_Code").alias("Country_Code")
	,col("Company_Code").alias("Company_Code")
	,col("TRI_EFFECTIVE_FROM_DA").alias("Effective_From_Date")
	,col("TRI_EFFECTIVE_TO_DA").alias("Effective_To_Date")
	,col("TRI_CONTROL_NUMBER_CN").alias("Control_Number")
	,col("TRI_ID_TX").alias("Transaction_Id")
	,col("CST_SLIDING_GRADE_BL").alias("Sliding_Grade_Indicator")
    ,col("TRI_MODIFIED_DATE_SY").alias("Source_Last_Modified_Date")
    ,current_timestamp().cast("timestamp").alias("Create_Date")
)

print("Final count : ",SBR_VALUE_RULES_df.count())

SBR_VALUE_RULES_df.write.format('delta').mode('overwrite').save(SBR_VALUE_RULES_cur)

# COMMAND ----------

# DBTITLE 1,Create Database and Table - NDH
spark.sql("""CREATE DATABASE IF NOT EXISTS {0} LOCATION '{1}'""".format(NDH_DB,LEASE_DATA_cur))          
spark.sql("""CREATE TABLE IF NOT EXISTS {0}.{1} USING DELTA LOCATION '{2}'""".format(NDH_DB,LEASE_DATA_tbl,LEASE_DATA_cur))

spark.sql("""CREATE DATABASE IF NOT EXISTS {0} LOCATION '{1}'""".format(NDH_DB,SBR_QUANTITY_RULES_cur))          
spark.sql("""CREATE TABLE IF NOT EXISTS {0}.{1} USING DELTA LOCATION '{2}'""".format(NDH_DB,SBR_QUANTITY_RULES_tbl,SBR_QUANTITY_RULES_cur))

spark.sql("""CREATE DATABASE IF NOT EXISTS {0} LOCATION '{1}'""".format(NDH_DB,SBR_VALUE_RULES_cur))          
spark.sql("""CREATE TABLE IF NOT EXISTS {0}.{1} USING DELTA LOCATION '{2}'""".format(NDH_DB,SBR_VALUE_RULES_tbl,SBR_VALUE_RULES_cur))
