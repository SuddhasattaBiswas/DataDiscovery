# Databricks notebook source
# MAGIC %run /Shared/NDSS/Common/NDSS_SynapseConnector

# COMMAND ----------

import pandas as pd
from datetime import datetime

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

setconnections();
tables=queryFromSynapse("select sourcetable,DisposalDateColumn from cfg.DataDisposalTables where loadtype='Delta' ").collect()
cols = ['Tables', 'NDH_Table_Record_Count','DataDisposedBefore','DataDisposalRunYear']
lst=[]
for item in tables:
  sourceTable=item.sourcetable
  currentYear = datetime.now().year
  currentYear = currentYear-4
  DisposalDateColumn=item.DisposalDateColumn
  df=spark.sql(f"select count(*) as Count_of_records from {sourceTable} where {DisposalDateColumn} < {currentYear} ")
  lst.append([sourceTable,df.collect()[0][0],currentYear,datetime.now().year])
  df=spark.sql(f"delete from {sourceTable} where {DisposalDateColumn} < {currentYear} ")
  #print(f"{sourceTable} :{df.collect()[0][0]}")
PDF_DataDisposal = pd.DataFrame(lst, columns=cols)


# COMMAND ----------

from pyspark.sql import SparkSession
DF=spark.createDataFrame(PDF_DataDisposal)
DF.createOrReplaceTempView('PDF_DataDisposal')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ndh.DataDisposalAudit
# MAGIC select * from PDF_DataDisposal

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ndh.DataDisposalAudit