# Databricks notebook source
# DBTITLE 1,Importing Required Modules
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, ArrayType, MapType, DataType
from pyspark.sql.functions import col,lit
import datetime
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import DataFrame
from pyspark import StorageLevel

# COMMAND ----------

# DBTITLE 1,Creating Widgets
dbutils.widgets.text("Root_Folder","NDH/Sensitive/WSMA/","Root Folder")
dbutils.widgets.text("Source_Table","",label="Source Table Name")
Root_Folder = dbutils.widgets.get("Root_Folder")
Transaction_Table = dbutils.widgets.get("Source_Table")

# COMMAND ----------

Root_Path = f"/mnt/ADLS2/{Root_Folder}"
Log_Folder = f"/mnt/ADLS2/{Root_Folder}/LOG/Log_Table"
print(Root_Path,Log_Folder)

# COMMAND ----------

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

Notebook_Name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(Notebook_Name)

# COMMAND ----------

# DBTITLE 1,Reading WSMA Data
WSMA_Data = spark.table(Transaction_Table)
WSMA_Data_Country_Lis = sorted([row.PartitionKey for row in WSMA_Data.select("PartitionKey").distinct().collect()])
WSMA_Data_Country_Lis

# COMMAND ----------

# WSMA_Data_Year_Country_Temp.select(
#     F.min('transactionStartTime').alias('min_transactionStartTime'),
#     F.max('transactionStartTime').alias('max_transactionStartTime')
# ).display()

# COMMAND ----------

# DeltaTable.forPath(spark, Log_Folder).delete(F.col("Notebook_Name") == 'Notebook_Name')
spark.read.format("delta").load(Log_Folder).filter(F.col("Notebook_Name") == Notebook_Name).display()

# COMMAND ----------

MDM_Data =  spark.read.parquet("/mnt/ADLS1/PREP/1stParty/MDM/NonSensitive/RETAIL_SITE/RETAIL_OB_EXTRACT_RETAIL_SITE.parquet").select("COUNTRY_CODE", "GLOBAL_SITE_ID", "TERRITORY_NAME", "OPERATING_PLATFORM_NAME").filter(F.col("COUNTRY_CODE").isin(WSMA_Data_Country_Lis)).distinct()
MDM_Data = MDM_Data.withColumnRenamed("COUNTRY_CODE","PartitionKey") \
    .withColumnRenamed("GLOBAL_SITE_ID","GlobalSiteId") 
MDM_Data.display()

# COMMAND ----------

for Country_Code in WSMA_Data_Country_Lis:
    '''
    Read log Table and check whether we have record for this notebook with country
    '''
    # log_table = spark.read.format("delta").load(Log_Folder).filter(F.col("Notebook_Name")==Notebook_Name).filter(F.col("Country")==Country_Code)
    # log_len = log_table.count()

    '''
    1. if we have record for this notebook with country then we will take the latest run year
    1.1. if latest run year is current year then we will take only current year data from table, if latest run year is not current year then we will take only data from latest run year to current year-1 (last run date is 2024-01-05 then list of years will be [2024,2025])
    2. if we don't have then we will run the notebook by taking last 2 years of data by ecluding current year
    '''
    current_year = datetime.datetime.now().year
    Years = [current_year - 2, current_year - 1]
    # if log_len == 0:
    #     Years = [current_year - 2, current_year - 1]
    # else:
    #     log_years = [int(row['Run_Date'][:4]) for row in log_table.select("Run_Date").distinct().collect()]
    #     latest_run_year = max(log_years)
    #     if latest_run_year == current_year:
    #         Years = [current_year]
    #     else:
    #         Years = list(range(latest_run_year,current_year))
    '''
    looping over the years
    '''
    for year in Years:
        WSMA_Data_Year_Country = spark.table(Transaction_Table).filter(F.col("PartitionKey") == Country_Code).filter(F.year(col("transactionStartTime")) == year)
        WSMA_Data_Year_Country = WSMA_Data_Year_Country.dropDuplicates(['SiteId','deviceID','additionalProductInfo', 'productCode', 'pumpNumber', 'nozzleNumber','SALES_QUANTITY', 'counterReading', 'transactionStartTime', 'transactionEndTime', 'deliveryType', 'lubricantQuantity', 'transactionNumber', 'transactionStatus']) 
        MDM_Data_Country = MDM_Data.filter(F.col("PartitionKey") == Country_Code)

        Year_Count = WSMA_Data_Year_Country.count()
        print("===================================================================================================")
        print(f"Count of Year {year} for Country {Country_Code} is {Year_Count}")
        if Year_Count > 0:
            '''
            Finding List of Relevant Pumps
            '''
            print(f"Processing Year {year} for Country {Country_Code} Started at {datetime.datetime.now()}")
            grade_pump_mapping = WSMA_Data_Year_Country.groupby('PartitionKey','GlobalSiteId','SiteId','additionalProductInfo','productCode').agg(F.array_sort(F.collect_set(col('pumpNumber'))).alias('relevant_pumps'))
            grade_pump_mapping = grade_pump_mapping.withColumn('number_of_relevant_pumps', F.size(grade_pump_mapping.relevant_pumps))
            # display(grade_pump_mapping)

            '''
            Creating mogas_diesel_non_truck_flag Field
            Flag whether a transaction is a Mogas / Diesel transaction (excl. Truck diesel). 
            First need to check whether the product code is consistent across all sites.
            I have added additionalProductInfo filter using rlike based on my assumtion. Have to check with Akshay once
            '''
            WSMA_Data_Year_Country = WSMA_Data_Year_Country.withColumn('mogas_diesel_non_truck_flag',F.when(col("additionalProductInfo").rlike("(?i)00003|00006|00007|00008|00009|00011|00015|00016|adblue|auto|cng|hvo|h v o|lng|lpg|frei (2)|truck|gnv|gpl|huisbrandoli|klima|l.p.g|mix|hydrogen"),col('additionalProductInfo')).otherwise(lit('Mogas / non-truck diesel')))
            # WSMA_Data_Year_Country.display()

            '''
            Calculating Count of Distinct Pump number based on SiteID and mogas_diesel_non_truck_flag
            Calculate the number of pumps per grade, but assuming mogas / diesel pumps serve
            all grades (excl. Truck diesel, Gas, LPG etc).
            '''
            agg_df_pump_number = WSMA_Data_Year_Country.groupBy('PartitionKey','GlobalSiteId','SiteId', 'mogas_diesel_non_truck_flag').agg(F.countDistinct('pumpNumber').alias('number_of_relevant_pumps_mogas_diesel_non_truck_aggregated')); 
            WSMA_Data_Year_Country = WSMA_Data_Year_Country.join(F.broadcast(agg_df_pump_number), on=['PartitionKey','GlobalSiteId','SiteId', 'mogas_diesel_non_truck_flag'], how='left')
            # WSMA_Data_Year_Country.display()

            '''
            Join Grade Pump Mapping df with actual df
            '''
            data_with_relevant_pumps = WSMA_Data_Year_Country.join(F.broadcast(grade_pump_mapping.select(['PartitionKey','GlobalSiteId','SiteId','productCode', 'relevant_pumps', 'number_of_relevant_pumps'])),on = ['PartitionKey','GlobalSiteId','SiteId','productCode'], how = 'left')
            # data_with_relevant_pumps.display()

            '''
            Create a transaction id column from the index of the table. This is to assign a
            unique id to each transaction. 
            '''
            max_id = 0

            window_spec = Window.orderBy("SiteId")  # or any column for ordering

            data_with_relevant_pumps = data_with_relevant_pumps.withColumn("txn_id", (F.row_number().over(window_spec) + lit(max_id)))
            # data_with_relevant_pumps.display()

            '''
            1. calculate Sum of sales quantity and transaction count on SiteId, ProductCode, relevant_pumps, mogas_diesel_non_truck_flag level
            Optimized: Avoid repeated groupBy/agg by computing all aggregations in a single pass,
            then joining only once. This reduces shuffles and improves performance.
            Apply broadcast join for agg_df if it is small enough.
            '''
            Target_Path_Relevant_Pumps = Root_Path+'OUTPUT FILES/'+Country_Code+'/'+str(year)+'/'+'RELEVANT_PUMPS_SUMMARY'
            agg_df = data_with_relevant_pumps.groupBy(
                'PartitionKey',
                'GlobalSiteId',
                'SiteId',
                'productCode',
                'relevant_pumps',
                'mogas_diesel_non_truck_flag'
            ).agg(
                F.sum('SALES_QUANTITY').alias('quantity_sum'),
                F.count('txn_id').alias('txn_count')
            )

            agg_df = agg_df.withColumn('site_vol', F.sum('quantity_sum').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId'))) \
                .withColumn('site_txn', F.sum('txn_count').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId'))) \
                .withColumn('grade_vol', F.sum('quantity_sum').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'productCode'))) \
                .withColumn('grade_txn', F.sum('txn_count').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'productCode'))) \
                .withColumn('relevant_pumps_vol', F.sum('quantity_sum').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'relevant_pumps'))) \
                .withColumn('relevant_pumps_txn', F.sum('txn_count').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'relevant_pumps'))) \
                .withColumn('relevant_pumps_vol_mogas_diesel_aggregated', F.sum('quantity_sum').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'mogas_diesel_non_truck_flag'))) \
                .withColumn('relevant_pumps_txn_mogas_diesel_aggregated', F.sum('txn_count').over(Window.partitionBy('PartitionKey','GlobalSiteId','SiteId', 'mogas_diesel_non_truck_flag')))

            # Remove duplicates to get one row per original transaction
            join_cols = ['PartitionKey','GlobalSiteId','SiteId', 'productCode', 'relevant_pumps', 'mogas_diesel_non_truck_flag']
            data_with_relevant_pumps = data_with_relevant_pumps.join(F.broadcast(agg_df), on=join_cols, how='left')

            data_with_relevant_pumps.write.format("delta").mode("overwrite") \
                .partitionBy("PartitionKey", "GlobalSiteId", "SiteId") \
                .save(Target_Path_Relevant_Pumps)
            print(f"Relevant Pump Summary DF has been written to ADLS path : {Target_Path_Relevant_Pumps}")
            # data_with_relevant_pumps.display()

            '''
            Summarise the volumes and transactions at a site-pump-grade level. This will
            help with a quick analysis of which pumps are not used much.
            '''
            Target_Path_Summary = Root_Path+'OUTPUT FILES/'+Country_Code+'/'+str(year)+'/'+'SITE_PUMP_LEVEL_SUMMARY'
            pump_level_summary = data_with_relevant_pumps.groupby('PartitionKey', 'GlobalSiteId', 'SiteId', 'siteName', 'City', 'additionalProductInfo', 'productCode', 'pumpNumber').agg(F.sum('SALES_QUANTITY').alias('Transaction_Vol'), F.count('txn_id').alias('Transaction_Count'))
            # print(f"Before Join Count : {pump_level_summary.count()}")

            pump_level_summary_Final = pump_level_summary.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')

            # print(f"After Join Count : {pump_level_summary_Final.count()}")
            pump_level_summary_Final.write.format("delta").mode("overwrite") \
                .partitionBy("PartitionKey", "GlobalSiteId", "SiteId") \
                .save(Target_Path_Summary)
            print(f"Summary Data has been stored in {Target_Path_Summary}")
            # pump_level_summary.display()

            '''
            Calculating txn_ongoing_actual, txn_ongoing_potential, txn_ongoing_mogas_diesel_combined Fields
            '''
            Target_Path_oa_op_md = Root_Path+'OUTPUT FILES/'+Country_Code+'/'+str(year)+'/'+'OA_OP_MOGAS_DIESEL'

            # Set shuffle partitions to a higher value for better parallelism
            spark.conf.set("spark.sql.shuffle.partitions", 4000)
            data_with_relevant_pumps_New = data_with_relevant_pumps.repartition("SiteId", "PartitionKey", "GlobalSiteId", "siteName")
            # Creating df1 and df2 from existing df
            df1 = data_with_relevant_pumps_New.alias("df1")
            df2 = data_with_relevant_pumps_New.alias("df2")
            # defining Join Condition to merge df1 and df2 to findout oungoing transactions
            join_condition = (
                (F.col("df2.PartitionKey") == F.col("df1.PartitionKey")) &
                (F.col("df2.GlobalSiteId") == F.col("df1.GlobalSiteId")) &
                (F.col("df2.transactionStartTime") < F.col("df1.transactionStartTime")) &
                (F.col("df2.transactionEndTime") >= F.col("df1.transactionStartTime"))
            )
            actual_condition = F.array_contains(F.col("df1.relevant_pumps"), F.col("df2.pumpNumber"))
            potential_condition = F.arrays_overlap(F.col("df1.relevant_pumps"), F.col("df2.relevant_pumps"))
            mogas_diesel_condition = (F.col("df1.mogas_diesel_non_truck_flag") == F.col("df2.mogas_diesel_non_truck_flag"))

            # Join and aggregate for all sites at once
            joined = df1.join(df2, join_condition, how="left")

            result = joined.groupBy(
                "df1.SiteId", "df1.PartitionKey", "df1.GlobalSiteId", "df1.siteName", 
                "df1.transactionStartTime", "df1.transactionEndTime", "df1.pumpNumber", "df1.relevant_pumps"
            ).agg(
                F.sum(F.when(actual_condition, 1).otherwise(0)).alias("txn_ongoing_actual"),
                F.sum(F.when(potential_condition, 1).otherwise(0)).alias("txn_ongoing_potential"),
                F.sum(F.when(mogas_diesel_condition, 1).otherwise(0)).alias("txn_ongoing_mogas_diesel_combined")
            ) #'df1.TERRITORY_NAME', 'df1.OPERATING_PLATFORM_NAME',

            final_df = result.selectExpr(
                "SiteId",
                "PartitionKey",
                "GlobalSiteId",
                "siteName",
                "transactionStartTime",
                "transactionEndTime",
                "pumpNumber",
                "relevant_pumps",
                "txn_ongoing_actual",
                "txn_ongoing_potential",
                "txn_ongoing_mogas_diesel_combined"
            )
            print(f"Final DataFrame Count : {final_df.count()}")
            
            #writing data to ADLS
            print(f"Witing Final DF to ADLS of Year {year} for Country {Country_Code}")
            final_df.write.format("delta").mode("overwrite") \
                .partitionBy("PartitionKey", "GlobalSiteId", "SiteId") \
                .save(Target_Path_oa_op_md)
            print(f"Final DF has been written to ADLS path : {Target_Path_oa_op_md}")
            print(f"Processing Year {year} for Country {Country_Code} Completed at {datetime.datetime.now()}")

            Current_date = datetime.datetime.today().date()
            print(f"Updating log Table with Notebook Name : {Notebook_Name} and Country : {Country_Code} and Data : {Current_date}")
            log_data = [(Notebook_Name,Country_Code,Current_date)]
            log_df = spark.createDataFrame(log_data, ["Notebook_Name","Country","Run_Date"])
            delta_table = DeltaTable.forPath(spark,Log_Folder)
            delta_table.alias("trgt").merge(log_df.alias("src"),"trgt.Notebook_Name = src.Notebook_Name and trgt.Country = src.Country").whenNotMatchedInsertAll()\
                .whenMatchedUpdateAll()\
                    .execute()

            print("                                                   ")

        else:
            print(f"Count of Year {year} for Country {Country_Code} is {Year_Count}")
