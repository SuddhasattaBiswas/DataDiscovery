# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM NDH.SBR_QUANTITY_RULES_NDT WHERE Lease_Id = '2000001076'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT DISTINCT Site_Id,Country_Code,Lease_ID,Lease_Name,Lease_Type_Code,Legacy_Lease_Id,Control_Number
# MAGIC  FROM NDH.LEASE_DATA_NDT WHERE Legacy_Lease_Id = 'DE01-2000001076' --WHERE Lease_Id = '2000001076'  
# MAGIC  
# MAGIC  -- Legacy_Lease_Id consider Lease_Id
# MAGIC  -- Lease_Id --> FK in SBR_QUANTITY_RULES
# MAGIC  
# MAGIC  --> SBR_QR (Lease_Id) & LEASE_DATA (Control Number)
# MAGIC  
# MAGIC  -- SBR_QUANTITY_RULES && PRODUCT_MAPPING && PNL_GENERAL_REPORTING_NDA (Match with Site_Id of Lease_data) ==> 
# MAGIC   
# MAGIC    
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM NDH.SBR_QUANTITY_RULES_NDT WHERE Lease_Id = '1001318'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM NDH.PNL_GENERAL_REPORTING_NDA WHERE SITE_NAME LIKE '%10025191%'
