# Databricks notebook source
# DBTITLE 1,Importing Required Modules
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType #, TimestampType, IntegerType, DoubleType, ArrayType, MapType, DataType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import datetime

# COMMAND ----------

# DBTITLE 1,Creating Widgets
dbutils.widgets.text("Root_Folder","","Root Folder")
dbutils.widgets.text("Site_Info_Table","",label="Site Info Table Name")
dbutils.widgets.text("Transaction_Table","",label="Transaction Table Name")
dbutils.widgets.text("TDH_TABLE","",label="TDH TABLE NAME")
dbutils.widgets.text("Target_Table","",label="Target Table Name")
Root_Folder = dbutils.widgets.get("Root_Folder")
Site_Info_Table = dbutils.widgets.get("Site_Info_Table")
Transaction_Table = dbutils.widgets.get("Transaction_Table")
tdh_table = dbutils.widgets.get("TDH_TABLE")
Target_Table = dbutils.widgets.get("Target_Table")

# COMMAND ----------

# DBTITLE 1,Creating Variables
Root_Path = f"/mnt/ADLS2/{Root_Folder}"
Site_Info_Path = f"{Root_Path}/{Site_Info_Table}"
Transaction_Path = f"{Root_Path}/{Transaction_Table}"
Log_Folder = f"/mnt/ADLS2/{Root_Folder}/LOG/Log_Table"
print(Root_Path,Site_Info_Path,Transaction_Path,Log_Folder)

# COMMAND ----------

# DBTITLE 1,creating delta log table if not exists
try:
    dbutils.fs.ls(Log_Folder)
    print("Table Exists")
except Exception:
    print("Delta table Doesn't exist")
    Log_Schema = StructType([
        StructField("Notebook_Name",StringType(),True),
        StructField("Country",StringType(),True),
        StructField("Run_Date",StringType(),True)
    ])
    dbutils.fs.mkdirs(Log_Folder)
    spark.createDataFrame([],Log_Schema).write.format("delta").mode("overwrite").save(Log_Folder)
    print("Table Has been Created")

# COMMAND ----------

# DBTITLE 1,Getting Notebook Path
Notebook_Name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(Notebook_Name)

# COMMAND ----------

# DBTITLE 1,Read Log Table
'''
Read log Table and check whether we have record for this notebook
'''
delta_table = spark.read.format("delta").load(Log_Folder).filter(F.col("Notebook_Name")==Notebook_Name)
display(delta_table)
delta_len = delta_table.count()

# COMMAND ----------

# DBTITLE 1,Getting List of Dates to Process Data
'''
1. defining Transaction_Path variable and assigning the path of transaction files
2. checking whether we have records in delta log table
3. if we have records, then select Run_Date field and generate list of dates based on last run date.
4. if we don't have records in log table which means we haven't loaded any data, so select all the folders in the path and generate list of dates
'''
date_lis = []
if delta_len > 0:
   last_run_date = delta_table.select("Run_Date").collect()
   last_run_date = datetime.datetime.strptime(last_run_date[0].Run_Date, '%Y-%m-%d').date()
   today = datetime.datetime.today().date()
   current_date_one = last_run_date+datetime.timedelta(days=1)
   while current_date_one <= today:
       date_lis.append(current_date_one.strftime("%Y-%m-%d"))
       current_date_one += datetime.timedelta(days=1)
   print(date_lis)
else:
    try:
        items = dbutils.fs.ls(Transaction_Path)
        date_lis = [x.name.replace("/","") for x in items if x.isDir()]
        print(date_lis)
    except:
        print("No Data Found")


  

# COMMAND ----------

# DBTITLE 1,Exit Notebook based on Data Availability
## Exit the Notebook if we don't have any data to process
if len(date_lis) == 0:
    dbutils.notebook.exit("No Data Found")

# COMMAND ----------

# DBTITLE 1,Listing Out Valid Files
df_List_Path = []
all_paths = []
final_count = 0
for i in date_lis: #looping list of dates
    date_path = f"{Transaction_Path}/{i}" # concatenating path with Date
    # print(date_path)
    try: ### using try block because if we don't have any files today in ADLS if we run notebook it will fail while listing files
        Country_Paths = dbutils.fs.ls(date_path) # getting all sub directories in each date folder
        Country_Lis = [x.name.replace("/","") for x in Country_Paths if x.isDir()] # extracting country names from each date folder
        for j in Country_Lis: # looping country Name
            Country_Path = f"{date_path}/{j}" #concatenating Date path with country
            # print(Country_Path)
            try:
                Country_Path_lis = dbutils.fs.ls(Country_Path) # getting all files in each country folder
                if len(Country_Path_lis)>0: ### Check if we have file inside country folder, if yes proceed
                    Country_Parquet_File = Country_Path_lis[0].path ### Getting File path
                    df = spark.read.parquet(Country_Parquet_File)
                    df_count = df.count()
                    imp_fields = ["pumpNumber","nozzleNumber","quantity"]
                    if any(col in df.columns for col in imp_fields): # Check if we have "pumpNumber","nozzleNumber","quantity" any Column, if yes append Path to list
                        print(f"Parquet File Path : {Country_Parquet_File}")
                        final_count = final_count+df_count
                        df_List_Path.append(Country_Parquet_File)
                        print(f"Parquet File has : {df_count} Records")
                    else:
                        print(f"Parquet File doesn't have {imp_fields} Fields : {Country_Parquet_File}")
            except Exception as e:
                print(f"Country Path Does Not Exists : {Country_Path}")
                print(f"Actual Error is : str({e})")

    except Exception as e:
         print(f"Date Path Does Not Exists : {date_path} ===> ADF Load might not run or it might failed")
        #  print(f"Actual Error is : str({e})")


# COMMAND ----------

if len(df_List_Path) == 0:
    dbutils.notebook.exit("Notebook is exited as there is no latest WSMA data.... Check ADF Pipeline run")

# COMMAND ----------

# DBTITLE 1,Reading List of Valid Files
'''
1. Reading list of Parquet Files
2. check if len of delta log table, if it has > 0 then read data if not read all data and apply the filter from 2023-01-01 
'''
all_site_df = spark.read.parquet(*df_List_Path).distinct()

all_site_df = all_site_df.withColumn("transactionEndTime", F.from_utc_timestamp('transactionEndTime', 'Europe/Paris')) \
    .withColumn("RequestDateTime",F.from_utc_timestamp('RequestDateTime', 'Europe/Paris')) \
    .withColumn("transactionStartTime",F.from_utc_timestamp('transactionStartTime', 'Europe/Paris'))

if delta_len > 0:
    pass
else:
    all_site_df = all_site_df.filter(F.to_date("transactionEndTime") >= F.lit("2023-01-01")) #.filter(F.to_date("transactionEndTime") <= F.lit("2024-12-31"))
    
# all_site_df = all_site_df.filter("PartitionKey='DE'") #  and SiteId='0002'
all_site_df_count = all_site_df.count()
all_site_df.createOrReplaceTempView("v_all_site_df")

# COMMAND ----------

# DBTITLE 1,exiting Notebook based on Transactional data count
if all_site_df_count == 0:
    dbutils.notebook.exit("Notebook is exited as there is no latest WSMA data.... Check Curated Notebook for more details")

# COMMAND ----------

# DBTITLE 1,Reading Site Info Data
Site_Info_df = spark.read.parquet(f"{Site_Info_Path}/{Site_Info_Table}.parquet")
Site_Info_df.createOrReplaceTempView("v_Site_Info_df")
display(Site_Info_df)

# COMMAND ----------

# DBTITLE 1,Merging WSMA Transactional data and Site Info Data
Merge_Trans_Siteinfo_df = spark.sql(""" SELECT a.PartitionKey,
                                trim(a.RowKey) as RowKey,
                                a.RequestDateTime,
                                trim(a.SiteId) as SiteId,
                                cast(a.deviceID as int) as deviceID,
                                a.additionalProductInfo,
                                cast(a.productCode as int) as productCode,
                                cast(a.pumpNumber as int) as pumpNumber,
                                cast(a.nozzleNumber as int) as nozzleNumber,
                                a.quantity,
                                a.counterReading,
                                a.transactionStartTime,
                                a.transactionEndTime,
                                a.deliveryType,
                                a.lubricantQuantity,
                                cast(a.transactionNumber as int) as transactionNumber,
                                a.transactionStatus,
                                b.Cluster,
                                trim(b.GlobalSiteId) as GlobalSiteId,
                                b.IsWSMASupportedSite,
                                b.SiteSimulation,
                                b.SiteStatus,
                                b.Name as siteName,
                                b.City
                                FROM v_all_site_df a LEFT JOIN v_Site_Info_df b
                                ON trim(a.PartitionKey)=trim(b.PartitionKey) AND
                                trim(a.SiteId)=trim(b.SiteId) 
                                AND trim(a.pumpNumber)=trim(b.pumpNumber) AND trim(a.nozzleNumber)=trim(b.nozzleNumber)
                                """)

# COMMAND ----------

Merge_Trans_Siteinfo_df_wodu = Merge_Trans_Siteinfo_df.dropDuplicates(['SiteId','deviceID','additionalProductInfo', 'productCode', 'pumpNumber', 'nozzleNumber','quantity', 'counterReading', 'transactionStartTime', 'transactionEndTime', 'deliveryType', 'lubricantQuantity', 'transactionNumber', 'transactionStatus']) 
Merge_Trans_Siteinfo_df_wodu_count = Merge_Trans_Siteinfo_df_wodu.count()

# COMMAND ----------

print(f"With Duplicates count is : {all_site_df_count} and Without Duplicates count is : {Merge_Trans_Siteinfo_df_wodu_count} and Merge df count is {Merge_Trans_Siteinfo_df.count()}==== diff is : {all_site_df_count-Merge_Trans_Siteinfo_df_wodu_count}")

# COMMAND ----------

# DBTITLE 1,Creating New Columns for feature joins
'''
Create START_DATE,TRANSACTION_HOUR, TRANSACTION_MINUTE Columns by using transactionEndTime Field
'''
Merge_Trans_Siteinfo_df_New = Merge_Trans_Siteinfo_df_wodu.withColumn('START_DATE', F.to_date('transactionEndTime')) \
    .withColumn('SALES_QUANTITY',F.col('quantity').cast("decimal(20,2)")) \
    .withColumn('TRANSACTION_HOUR', F.hour('transactionEndTime')) \
    .withColumn('TRANSACTION_MINUTE', F.minute('transactionEndTime'))

# COMMAND ----------

# DBTITLE 1,Reading TDH Data
'''
1. get the list of START_DATE from Transactional Table
2. concatenate list of dates as string format by seperating with comma and find the min date from the list
3. check if we have record for this Notebook in delta table, if we don't have record which menas it's an first load
4. if it's first load, read TDH data by applying list of dates which we get from Transactional data
5. if it's ot an first run, then get the list of dates from Transactional data and get the min date from the list and apply the filter
'''

date_lis_read_data = sorted([f"'{i['START_DATE']}'" for i in Merge_Trans_Siteinfo_df_New.select("START_DATE").distinct().collect()])
date_lis_str = ",".join(date_lis_read_data)
min_date = min(date_lis_read_data)
max_date = max(date_lis_read_data)
if delta_len > 0:
    TDH_DATA = spark.table(tdh_table).filter(f"START_DATE in ({date_lis_str})") # AND COUNTRY_CODE = 'DE'
    TDH_DATA_COUNT = TDH_DATA.count()
    print(f"Data Loaded from TDH Table between {date_lis_str} dates, Count is {TDH_DATA_COUNT}") 
else:
    TDH_DATA = spark.table(tdh_table).filter(f"START_DATE>={min_date} AND START_DATE<={max_date}") #  AND COUNTRY_CODE = 'DE'
    TDH_DATA_COUNT = TDH_DATA.count()
    print(f"Data Loaded from TDH Table between {min_date} and {max_date}, Count is {TDH_DATA_COUNT}")
    
TDH_DATA = TDH_DATA.select(['COUNTRY_NAME', 'DAY_INT', 'EVENT_START_DATE', 'START_DATE', 'TRANSACTION_HOUR', 'RETAIL_SITE_ID', 'SALES_QUANTITY', 'SALES_AMOUNT', 'COUNTRY_CODE', 'CLASS_NAME', 'CALENDAR_YEAR', 'SALES_OR_RETURN_INDICATOR'])
# TDH_DATA.display()

# COMMAND ----------

# DBTITLE 1,exiting Notebook based on Transactional data count
if TDH_DATA_COUNT == 0:
    dbutils.notebook.exit("Notebook is exited as there is no new data in TDH Table")

# COMMAND ----------

# DBTITLE 1,Filtering TDH Data
tdh_df=TDH_DATA.filter(F.col('CLASS_NAME') =='FUELS') \
    .filter(F.col('SALES_OR_RETURN_INDICATOR') == 'S')
tdh_df=tdh_df.withColumn('SALES_QUANTITY',F.col('SALES_QUANTITY').cast("decimal(20,2)")) \
    .withColumnRenamed('COUNTRY_CODE','PartitionKey') \
    .withColumnRenamed('RETAIL_SITE_ID','GlobalSiteId') \
    .withColumn('TRANSACTION_MINUTE', F.minute(F.from_utc_timestamp('EVENT_START_DATE', 'Europe/Paris'))) 
# tdh_df = tdh_df.filter("GlobalSiteId = '10024022'")
TDH_DATA_FILTER_COUNT = tdh_df.count()
print(TDH_DATA_FILTER_COUNT)

# COMMAND ----------

# 1. Add ±10 min buffer columns in tdh_df
tdh_df_New = tdh_df.withColumn("Transaction_plus_15", F.expr("EVENT_START_DATE + interval 15 minutes")) \
    .withColumn("Transaction_minus_15", F.expr("EVENT_START_DATE - interval 15 minutes"))


# 2. Perform join with buffer condition
joined_df = Merge_Trans_Siteinfo_df_New.alias("m").join(
    tdh_df_New.alias("t"),
    (
        (F.col("m.PartitionKey") == F.col("t.PartitionKey")) &
        (F.col("m.GlobalSiteId") == F.col("t.GlobalSiteId")) &
        (F.col("m.SALES_QUANTITY") == F.col("t.SALES_QUANTITY")) &
        (F.col("m.transactionEndTime").between(F.col("t.Transaction_minus_15"), F.col("t.Transaction_plus_15")))
    ),
    "inner"
)

MERGE_DF_COUNT = joined_df.count()

# COMMAND ----------


t_unique_cols = [
    F.col("t.PartitionKey"),
    F.col("t.GlobalSiteId"),
    F.col("m.RowKey"),
    F.col("t.SALES_QUANTITY"),
    F.col('m.pumpNumber'),
    F.col("m.nozzleNumber")
]

# Rank m candidates for each t record by "most recent" m.transactionEndTime
w = Window.partitionBy(*t_unique_cols).orderBy(F.col("m.transactionEndTime").desc())

result_df = (
    joined_df
    .withColumn("m_rank", F.row_number().over(w))
    .filter(F.col("m_rank") == 1)     # keep only the most recent m per t
    .drop("m_rank")
)
MERGE_DF_COUNT_FINAL = result_df.count()

# COMMAND ----------

print(f"WSMA DATA COUNT : {Merge_Trans_Siteinfo_df_wodu_count} AND TDH DATA COUNT : {TDH_DATA_FILTER_COUNT}, AND MERGED DATA COUNT : {MERGE_DF_COUNT}, COUNT AFTER RMOVING DUPLICATES : {MERGE_DF_COUNT_FINAL}")

# COMMAND ----------

Final_Merge_df = result_df.select(['m.PartitionKey', 'm.RowKey', 'm.RequestDateTime', 'm.SiteId', 'm.deviceID', 'm.additionalProductInfo', 'm.productCode', 'm.pumpNumber', 'm.nozzleNumber', 'm.counterReading', 'm.transactionStartTime', 'm.transactionEndTime', 't.EVENT_START_DATE', 'm.deliveryType', 'm.lubricantQuantity', 'm.transactionNumber', 'm.transactionStatus', 'm.Cluster', 'm.GlobalSiteId', 'm.IsWSMASupportedSite', 'm.SiteSimulation', 'm.SiteStatus', 'm.siteName', 'm.City', 'm.START_DATE', 'm.SALES_QUANTITY', 'm.TRANSACTION_HOUR', 'm.TRANSACTION_MINUTE']) # ,'m.TERRITORY_NAME','m.OPERATING_PLATFORM_NAME'

# COMMAND ----------

# Final_Merge_df.select(['PartitionKey','SiteId','transactionStartTime', 'transactionEndTime', 'EVENT_START_DATE']).withColumn("minutes_diff",(F.col("EVENT_START_DATE").cast("long") - F.col("transactionStartTime").cast("long")) / 60
# ).orderBy(F.col('minutes_diff').desc()).display()

# COMMAND ----------

Final_Merge_df = Final_Merge_df.drop(F.col('transactionEndTime')) \
    .withColumnRenamed('EVENT_START_DATE', 'transactionEndTime') \
    .withColumn("transactionStartTime", F.expr("transactionStartTime - interval 1 minutes"))

# COMMAND ----------

# DBTITLE 1,Finding Unmatched WSMA Records Missing in TDH Dataset
from pyspark.sql.functions import col

m = Merge_Trans_Siteinfo_df_New.alias("m")
t = tdh_df_New.alias("t")

join_cond = (
    (F.col("m.PartitionKey") == F.col("t.PartitionKey")) &
    (F.col("m.GlobalSiteId") == F.col("t.GlobalSiteId")) &
    (F.col("m.SALES_QUANTITY") == F.col("t.SALES_QUANTITY")) &
    (F.col("m.transactionEndTime").between(F.col("t.Transaction_minus_15"), F.col("t.Transaction_plus_15")))
)

# Finding Missing records from WSMA that did not match anything in TDH
wsma_missing_in_tdh = m.join(t, join_cond, "left_anti")
wsma_missing_in_tdh = wsma_missing_in_tdh.select(['m.PartitionKey', 'm.RowKey', 'm.RequestDateTime', 'm.SiteId', 'm.deviceID', 'm.additionalProductInfo', 'm.productCode', 'm.pumpNumber', 'm.nozzleNumber', 'm.counterReading', 'm.transactionStartTime', 'm.transactionEndTime', 'm.deliveryType', 'm.lubricantQuantity', 'm.transactionNumber', 'm.transactionStatus', 'm.Cluster', 'm.GlobalSiteId', 'm.IsWSMASupportedSite', 'm.SiteSimulation', 'm.SiteStatus', 'm.siteName', 'm.City', 'm.START_DATE', 'm.SALES_QUANTITY', 'm.TRANSACTION_HOUR', 'm.TRANSACTION_MINUTE']) #,'m.TERRITORY_NAME','m.OPERATING_PLATFORM_NAME'

wsma_missing_in_tdh = wsma_missing_in_tdh.withColumn("transactionEndTime", F.expr("transactionEndTime + interval 3 minutes")) \
    .withColumn("transactionStartTime", F.expr("transactionStartTime - interval 1 minutes"))

print(wsma_missing_in_tdh.count())

# COMMAND ----------

Final_wsma_tdh_merge_df = Final_Merge_df.union(wsma_missing_in_tdh)
Final_wsma_tdh_merge_df_Final = Final_wsma_tdh_merge_df.filter("GlobalSiteId is not null")
print(f"Count of DF with Nulls in GlobalSiteId Field : {Final_wsma_tdh_merge_df.count()} AND Count of DF without Nulls in GlobalSiteId Field : {Final_wsma_tdh_merge_df_Final.count()}")

# COMMAND ----------

Final_wsma_tdh_merge_df_Final.write.format("delta").mode("append").saveAsTable(Target_Table)

# COMMAND ----------

# DBTITLE 1,Updating Log Table
'''
1. if we processed files then update or Insert record into log table otherwise not
2. Creating Log df with Notebook Name and Current Date
3. Merging log df with Delta Log Table
'''
Current_date = datetime.datetime.today().date()
if len(date_lis)>0 and len(df_List_Path)>0:
    log_data = [(Notebook_Name,'',Current_date)]
    log_df = spark.createDataFrame(log_data, ["Notebook_Name","Country","Run_Date"])
    log_df.display()
    delta_table = DeltaTable.forPath(spark,Log_Folder)
    delta_table.alias("trgt").merge(log_df.alias("src"),"trgt.Notebook_Name = src.Notebook_Name").whenNotMatchedInsertAll()\
        .whenMatchedUpdateAll()\
            .execute()