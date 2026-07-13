# Databricks notebook source
# DBTITLE 1,Importing required modules

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, ArrayType, MapType, DataType
from pyspark.sql.functions import col,lit
import datetime
from delta.tables import DeltaTable
import re

# COMMAND ----------

# DBTITLE 1,Creating Widgets
dbutils.widgets.text("Root_Folder","",label="Root Folder")
dbutils.widgets.text("Site_Info_Table","",label="Site Info Table Name")
dbutils.widgets.text("Transaction_Table","",label="Transaction Table Name")
dbutils.widgets.text("Target_Foler","",label="Target Folder Path")
Root_Folder = dbutils.widgets.get("Root_Folder")
Site_Info_Table = dbutils.widgets.get("Site_Info_Table")
Transaction_Table = dbutils.widgets.get("Transaction_Table")
Target_Foler = dbutils.widgets.get("Target_Foler")

# COMMAND ----------

# DBTITLE 1,creating generic variables
Root_Path = f"/mnt/ADLS1/{Root_Folder}"
Site_Info_File_Path = f"{Root_Path}{Site_Info_Table}/{Site_Info_Table}.parquet"
Current_date = datetime.datetime.now().strftime('%Y-%m-%d')
Target_Path = f"/mnt/ADLS2/{Target_Foler}"
print(Target_Path, Site_Info_File_Path)

# COMMAND ----------


try:
    Site_Info_df = spark.read.parquet(Site_Info_File_Path)
    Site_Info_df_run='Y'
    display(Site_Info_df)
except Exception as e:
    Site_Info_df_run='N'
    print("Path Doesn't Exists")
    # raise e

# COMMAND ----------

if Site_Info_df_run=='Y':
    # Define the schema for the JSON string
    current_site_data_schema = StructType([
        StructField("siteName", StringType(), True),
        StructField("city", StringType(), True)
    ])

    # Parse the JSON string into a struct
    Site_Info_df_Site = Site_Info_df.withColumn(
        "CurrentSiteData",
        F.from_json(col("CurrentSiteData"), current_site_data_schema)
    )

    # Now you can select the fields
    Site_Info_df_Site = Site_Info_df_Site.select('*',
        col("CurrentSiteData.siteName").alias("siteName"),
        col("CurrentSiteData.city").alias("City")
    ).drop("CurrentSiteData")
    display(Site_Info_df_Site)


# COMMAND ----------

if Site_Info_df_run=='Y':
    # Define the schema for the JSON string
    current_pump_data_schema = ArrayType(
    StructType([
        StructField("pumpNumber", IntegerType(), True),
        StructField("forecourtControllerType", StringType(), True),
        StructField("nozzleNumber", IntegerType(), True),
        StructField("nozzleOrderNumber", IntegerType(), True),
        StructField("tankReferenceId", IntegerType(), True)
    ])
    )
    # Parse the JSON string into a struct
    Site_Info_df_pump_Parsed = Site_Info_df_Site.withColumn("CurrentPumpNozzle_Parsed",F.from_json(col("CurrentPumpNozzle"), current_pump_data_schema))
    Site_Info_df_pump_explode = Site_Info_df_pump_Parsed.withColumn('CurrentPumpNozzle_Parsed', F.explode(F.col('CurrentPumpNozzle_Parsed')))

    # Now you can select the fields
    Site_Info_df_pump_explode_Final = Site_Info_df_pump_explode.select('*',
        col("CurrentPumpNozzle_Parsed.pumpNumber").alias("pumpNumber"),
        col("CurrentPumpNozzle_Parsed.forecourtControllerType").alias("forecourtControllerType"),
        col("CurrentPumpNozzle_Parsed.nozzleNumber").alias("nozzleNumber"),
        col("CurrentPumpNozzle_Parsed.nozzleOrderNumber").alias("nozzleOrderNumber"),
        col("CurrentPumpNozzle_Parsed.pumpNumber").alias("tankReferenceId")
    ).drop("Address","AlertConfig","Comment","ConfigMessageId","Contact","CountryCode",'CurrentPumpNozzle','CurrentTankNozzleProduct',"ForceUpdate","ITLConfig","IsRKSite","LastPumpNozzle","LastSiteData","LastTankNozzleProduct","Latitude","Longitude","Market","NozzleInfo","OBN","OperatingHours","ReceivedMessageId","Region","SLOC","SiteToAiut","Source","TankInfo","CurrentPumpNozzle_Parsed")

    Target_Path_Final_Site_Info = f"{Target_Path}/{Site_Info_Table}/{Site_Info_Table}.parquet"
    Site_Info_df_pump_explode_Final.write.mode("overwrite").parquet(Target_Path_Final_Site_Info)

    display(Site_Info_df_pump_explode_Final)

# COMMAND ----------

# DBTITLE 1,creating delta log table
Log_Folder = Target_Path + "LOG/Log_Table"
try:
    dbutils.fs.ls(Log_Folder)
    print("Table Exists")
except Exception as e:
    print("Delta table Doesn't exist")
    Log_Schema = StructType([
        StructField("Notebook_Name",StringType(),True),
        StructField("Country",StringType(),True),
        StructField("Run_Date",StringType(),True)
    ])
    dbutils.fs.mkdirs(Log_Folder)
    spark.createDataFrame([],Log_Schema).write.format("delta").mode("overwrite").save(Log_Folder)
    print("Table Has been Created")
    # raise e

# COMMAND ----------

Notebook_Name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(Notebook_Name)

# COMMAND ----------

Log_Table = spark.read.format("delta").load(Log_Folder).filter(col("Notebook_Name")==Notebook_Name)
display(Log_Table)

# COMMAND ----------

'''
1. Read the list of parquet files,
2. check if the df has Transactions Field
3. if yes, define schema and explode the field
4. if no, return the dataframe as it is
'''
def tranformation_logic(dataframe_paths):
    Site_Trans_df = spark.read.parquet(dataframe_paths)
    if 'Transactions' in Site_Trans_df.columns:
        Transaction_Schema = ArrayType(
        StructType([        
            StructField("deviceID", IntegerType(), True),
            StructField("additionalProductInfo", StringType(), True),
            StructField("productCode", StringType(), True),
            StructField("pumpNumber", IntegerType(), True),
            StructField("nozzleNumber", IntegerType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("counterReading", DoubleType(), True),
            StructField("transactionStartTime", StringType(), True),
            StructField("transactionEndTime", StringType(), True),
            StructField("deliveryType", StringType(), True),
            StructField("lubricantQuantity", DoubleType(), True),
            StructField("transactionNumber", IntegerType(), True),
            StructField("transactionStatus", StringType(), True)
        ])
        )
        Site_Trans_df_parsed = Site_Trans_df.withColumn('Transactions_Parsed', F.from_json(F.col('Transactions'), Transaction_Schema))
        Site_Trans_df_explode = Site_Trans_df_parsed.withColumn('Transactions_Parsed', F.explode(F.col('Transactions_Parsed')))
        Site_Trans_df_Final = Site_Trans_df_explode.select("PartitionKey","RowKey","RequestDateTime","SiteId",
                                                        F.col("Transactions_Parsed.deviceID").alias("deviceID"),
                                                        F.col("Transactions_Parsed.additionalProductInfo").alias("additionalProductInfo"),
                                                        F.col("Transactions_Parsed.productCode").alias("productCode"),
                                                        F.col("Transactions_Parsed.pumpNumber").alias("pumpNumber"),
                                                        F.col("Transactions_Parsed.nozzleNumber").alias("nozzleNumber"),
                                                        F.col("Transactions_Parsed.quantity").alias("quantity"),
                                                        F.col("Transactions_Parsed.counterReading").alias("counterReading"),
                                                        F.col("Transactions_Parsed.transactionStartTime").alias("transactionStartTime"),
                                                        F.col("Transactions_Parsed.transactionEndTime").alias("transactionEndTime"),
                                                        F.col("Transactions_Parsed.deliveryType").alias("deliveryType"),
                                                        F.col("Transactions_Parsed.lubricantQuantity").alias("lubricantQuantity"),
                                                        F.col("Transactions_Parsed.transactionNumber").alias("transactionNumber"),
                                                        F.col("Transactions_Parsed.transactionStatus").alias("transactionStatus"))
        return Site_Trans_df_Final
    else :
        print(f"We don't have Transactions field in the dataframe for these paths : {dataframe_paths}")
        return Site_Trans_df



# COMMAND ----------


def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False


# COMMAND ----------

# DBTITLE 1,Getting list of countries
Transaction_Path = Root_Path+Transaction_Table
items = dbutils.fs.ls(Transaction_Path)
country_lis = [x.name.replace("/","") for x in items if x.isDir()]
print(country_lis)

# COMMAND ----------

for country_code in country_lis:
    print(f"============================Processing Country : {country_code}===============================")
    log_data_country = Log_Table.filter(col("Country")==country_code)
    last_run_date = log_data_country.select("Run_Date").collect()
    log_count = log_data_country.count()
    date_lis = []
    ##Checking we have record in log table for all countries, if we have record calculating dates suing last load date
    if log_count > 0:
        last_run_date = datetime.datetime.strptime(last_run_date[0].Run_Date, '%Y-%m-%d').date()
        today = datetime.datetime.today().date()
        current_date_one = last_run_date+datetime.timedelta(days=1)
        while current_date_one <= today:
            date_lis.append(current_date_one.strftime("%Y-%m-%d"))
            current_date_one += datetime.timedelta(days=1)
        print(date_lis)
        ## looping over the dates and processing the files
        for dates in date_lis:
            Day_path = f"{Transaction_Path}/{country_code}/{dates.replace('-','/')}" # generate root path til date and check path exists or not
            if path_exists(Day_path):
                print(f"Path exists Processing the Files: {Day_path}")
                ## get the list of files from day path and loop over the all files
                File_path = dbutils.fs.ls(Day_path)
                File_path_Final = [i.path for i in File_path]
                for path in File_path_Final:
                    Target_Paths = f"{Target_Path}{Transaction_Table}/{dates}/{country_code}/{Transaction_Table}_{dates}.parquet"
                    print(f"Source File Path : {path} AND Target File Path : {Target_Paths}")
                    final_df = tranformation_logic(path)
                    print("Writing Final DF to ADLS Path")
                    final_df.write.mode("overwrite").parquet(Target_Paths) # Writing final df to ADLS
                    print("Writing File Done")
                    print("Updating logs")
                    log_data = [(Notebook_Name,country_code,dates)]
                    log_df = spark.createDataFrame(log_data, ["Notebook_Name","Country","Run_Date"])
                    log_df.display()
                    delta_table = DeltaTable.forPath(spark,Log_Folder)
                    delta_table.alias("trgt").merge(log_df.alias("src"),"trgt.Notebook_Name = src.Notebook_Name and trgt.Country = src.Country").whenNotMatchedInsertAll()\
                        .whenMatchedUpdateAll()\
                            .execute()
                    print("Updating logs done")
            else:
                print(f"Path Doesn't exists : {Day_path}")
    else:
        Country_Path = dbutils.fs.ls(f"{Transaction_Path}/{country_code}") # generate root path til country and check path exists or not
        Country_Path_Lis = [i.path for i in Country_Path if i.isDir()] #get the list of year paths inside Country path
        Month_Path = [dbutils.fs.ls(i) for i in Country_Path_Lis] #get the list of month paths inside Year path
        Month_Path_lis = [j.path for i in Month_Path for j in i if j.isDir()]
        Day_Path =  [dbutils.fs.ls(i) for i in Month_Path_lis] #get the list of day paths inside Month path
        Day_Path_lis = [j.path for i in Day_Path for j in i if j.isDir()]
        for Day_Path in Day_Path_lis:
            if path_exists(Day_Path):
                print(f"Pathe exists Processing the Files: {Day_Path}")
                File_path = dbutils.fs.ls(Day_Path)
                File_path_Final = [i.path for i in File_path] #get list of files inside Day path
                for path in File_path_Final:
                    Dates = re.search(r'(\d{4})/(\d{2})/(\d{2})', path) #check whether we have yyyy/mm/dd pattern in path
                    File_date = datetime.datetime.strptime("-".join(Dates.groups()), "%Y-%m-%d").date() # convert above extracted pattern to date
                    Target_Paths = f"{Target_Path}{Transaction_Table}/{File_date}/{country_code}/{Transaction_Table}_{File_date}.parquet"
                    print(f"Source File Path : {path} AND Target File Path : {Target_Paths}")
                    final_df = tranformation_logic(path)
                    print("Writing Final DF to ADLS Path")
                    final_df.write.mode("overwrite").parquet(Target_Paths) # Writing final df to ADLS
                    print("Writing File Done")
                    print("Updating logs")
                    log_data = [(Notebook_Name,country_code,File_date)]
                    log_df = spark.createDataFrame(log_data, ["Notebook_Name","Country","Run_Date"])
                    log_df.display()
                    delta_table = DeltaTable.forPath(spark,Log_Folder)
                    delta_table.alias("trgt").merge(log_df.alias("src"),"trgt.Notebook_Name = src.Notebook_Name and trgt.Country = src.Country").whenNotMatchedInsertAll()\
                        .whenMatchedUpdateAll()\
                            .execute()
                    print("Updating logs done")
            else:
                print(f"Pathe Doesn't exists : {Day_Path}")




# COMMAND ----------

# delta_table.delete("true")
