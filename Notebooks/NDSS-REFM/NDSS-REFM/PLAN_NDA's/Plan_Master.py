# Databricks notebook source
from pyspark.sql.functions import (expr, lit)
retainDataInDataBricksDB = True

# COMMAND ----------

notebooksToExecute = {
   'REFM_PL_OU_FY_NDA_OP' : { 'asset_name': 'REFM_PL_OU_FY_NDA_OP'}, 
   'REFM_PL_OU_YTD_NDA_OP' : { 'asset_name': 'REFM_PL_OU_YTD_NDA_OP'},
   'REFM_PL_SITE_FY_NDA_OP' : { 'asset_name': 'REFM_PL_SITE_FY_NDA_OP'},
   'REFM_PL_SITE_YTD_NDA_OP' : { 'asset_name': 'REFM_PL_SITE_YTD_NDA_OP'}
}

# COMMAND ----------

status=[]
for assetNotebook in notebooksToExecute.keys():
  msg = ''
  notebookToRun = assetNotebook
  try:
    msg = dbutils.notebook.run(f"./{notebookToRun}", 0, { **notebooksToExecute.get(assetNotebook)})
  except Exception as e:
    msg = str(e)
  status.append((assetNotebook,msg))

# COMMAND ----------

checkDf = (spark.createDataFrame(status,schema= ['ASSET','STATUS'])
           .withColumn('STATUS_CHECK',expr("case when STATUS like 'Success%' then 'Success' else 'Failed' end"))
          )
display(checkDf)

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# %run /Shared/NDSS-REFM/PLAN_NDA's/REFM_PL_OU_FY_NDA_OP

# COMMAND ----------

# %run /Shared/NDSS-REFM/PLAN_NDA's/REFM_PL_OU_YTD_NDA_OP

# COMMAND ----------

# %run /Shared/NDSS-REFM/PLAN_NDA's/REFM_PL_SITE_FY_NDA_OP

# COMMAND ----------

# %run /Shared/NDSS-REFM/PLAN_NDA's/REFM_PL_SITE_YTD_NDA_OP

# COMMAND ----------

