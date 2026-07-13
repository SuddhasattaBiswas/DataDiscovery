# Databricks notebook source
pip install Office365-REST-Python-Client

# COMMAND ----------

!pip install Office365-REST-Python-Client openpyxl

# COMMAND ----------

# DBTITLE 1,Importing Required Modules
from pyspark.sql import functions as F
import datetime
from pyspark.sql.window import Window
from functools import reduce
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
import pandas as pd
from io import BytesIO
import openpyxl

# COMMAND ----------

dbutils.widgets.text("Root_Folder","NDH/Sensitive/WSMA/","Root Folder")
dbutils.widgets.text("Source_Table","NDH.WSMA_TDH_DATA_MERGE_DELTA",label="Source Table Name")
dbutils.widgets.text("Percentages","99.5, 98, 95, 90, 85, 80, 75",label="Pecentage of Volume") 
Root_Folder = dbutils.widgets.get("Root_Folder")
Transaction_Table = dbutils.widgets.get("Source_Table")
Percentages = dbutils.widgets.get("Percentages").split(",")

# COMMAND ----------

Root_Path = f"/mnt/ADLS2/{Root_Folder}"
Log_Folder = f"/mnt/ADLS2/{Root_Folder}/LOG/Log_Table"
Log_File = Root_Path+'OUTPUT FILES/LOG/'+str(datetime.datetime.now().strftime("%Y-%m-%d"))
print(Root_Path,Log_Folder)

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        return False

# COMMAND ----------

### get the list of country codes in the table
WSMA_Data = spark.table(Transaction_Table)
WSMA_Data_Country_Lis = sorted([row.PartitionKey for row in WSMA_Data.select("PartitionKey").distinct().collect()])
WSMA_Data_Country_Lis

# COMMAND ----------

percentages = [float(i) for i in Percentages]

# Helper to add a column for each percentage threshold

def add_dispenser_needed_flag(df, percent_col, Partition_fields, percent_list,output_prefix):
    w = Window.partitionBy(*Partition_fields.split(',')).orderBy(percent_col)
    result = df
    # result = result.withColumn("Rank",F.row_number().over(w))
    for item in percent_list:
        col_name = f"{output_prefix}_{item}%_vol"
        
        # Rank rows by percent_col
        rank_col = F.row_number().over(w)
        
        # Find rank of first row exceeding threshold
        cutoff_rank = F.min(F.when(F.col(percent_col) >= item, rank_col)).over(Window.partitionBy(*Partition_fields.split(',')))
        # result = result.withColumn(f"cutoff_rank_{item}",cutoff_rank)
        
        # Flag rows where rank <= cutoff_rank
        flag_col = F.when(rank_col <= cutoff_rank, 1).otherwise(0)
        
        result = result.withColumn(col_name, flag_col)
    return result

# COMMAND ----------

# sharepoint_site_url = "https://eu001-sp.shell.com/sites/UGDSNDSelfServe/"
# client_id = "6087adbf-b124-4c67-b81e-8f7b02a3f14a"
# client_secret=dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV", 
#                                   key = "AZ-AS-NDSS-Sharepoint-Clientkey-ADB")
# target_path = "Shared Documents/General/Pump_Utilization/PUMP UTILIZATION V3.0"
# # Authenticate with SharePoint
# credentials = ClientCredential(client_id, client_secret)
# ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
# def create_folder(path):
#     try:
#         target_folder = ctx.web.get_folder_by_server_relative_url(path)
#         ctx.load(target_folder).execute_query()
#         folder_exist = True
#     except Exception as e:
#         folder_exist = False
#         print(f'Folder {path} Not Found, Creating Folder')
#         print(f"Error : {e}")
#     if not folder_exist:
#         try:
#             target_folder = ctx.web.folders.add(path).execute_query()
#             print(f'Folder {path} Created')
#         except Exception as e:
#             print('Parent folder Not Found', e)
# create_folder(target_path)

# COMMAND ----------

sharepoint_site_url = "https://eu001-sp.shell.com/sites/UGDSNDSelfServe/"
client_id = "6087adbf-b124-4c67-b81e-8f7b02a3f14a"
client_secret=dbutils.secrets.get(scope = "KEYVAULT-AZ-AS-AKV-NDH-DEV", 
                                  key = "AZ-AS-NDSS-Sharepoint-Clientkey-ADB")

SharePoint_Folder = "/sites/UGDSNDSelfServe/Shared Documents/General/Pump_Utilization/PUMP UTILIZATION V3.0/"


def upload_file_to_sharepoint(ADLS_Path, SharePoint_Folder, SharePoint_Trailer_Path, Target_File_Name):
    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
    Source_Path = Root_Path+ADLS_Path+'/'+Target_File_Name
    csv_file = [f.path for f in dbutils.fs.ls(Source_Path) if f.path.endswith('.csv')][0]
    csv_file = csv_file.replace("dbfs:/", "/dbfs/")
    path =SharePoint_Folder+SharePoint_Trailer_Path
    try:
        target_folder = ctx.web.get_folder_by_server_relative_url(path)
        ctx.load(target_folder).execute_query()
        folder_exist = True
    except Exception as e:
        folder_exist = False
    if not folder_exist:
        try:
            # target_folder = ctx.web.folders.add(path).execute_query()
            target_folder = ctx.web.ensure_folder_path(path).execute_query()
        except Exception as e:
            raise e

    with open(csv_file, 'rb') as f:
        target_folder.upload_file(f"{Target_File_Name}.csv", f.read()).execute_query()

# COMMAND ----------

MDM_Data =  spark.read.parquet("/mnt/ADLS1/PREP/1stParty/MDM/NonSensitive/RETAIL_SITE/RETAIL_OB_EXTRACT_RETAIL_SITE.parquet").select("COUNTRY_CODE", "GLOBAL_SITE_ID", "TERRITORY_NAME", "OPERATING_PLATFORM_NAME").filter(F.col("COUNTRY_CODE").isin(WSMA_Data_Country_Lis)).distinct()
MDM_Data = MDM_Data.withColumnRenamed("COUNTRY_CODE","PartitionKey") \
    .withColumnRenamed("GLOBAL_SITE_ID","GlobalSiteId") 
MDM_Data.display()

# COMMAND ----------

# DBTITLE 1,Untitled
# WSMA_Data_Country_Lis = ['DE']
current_year = datetime.datetime.now().year
Years = [current_year - 2, current_year - 1]
# Years = [2024]
Log_Text = []
for Country_code in WSMA_Data_Country_Lis:
    for year in Years:
        Ongoing_transactions_Data_store = 0
        OA_OP_MOGAS_DIESEL_PATH = f"{Root_Path}OUTPUT FILES/{Country_code}/{str(year)}/OA_OP_MOGAS_DIESEL"
        RELEVANT_PUMPS_SUMMARY_PATH = f"{Root_Path}OUTPUT FILES/{Country_code}/{str(year)}/RELEVANT_PUMPS_SUMMARY"
        MDM_Data_Country = MDM_Data.filter(F.col("PartitionKey") == Country_code)
        Log_Text.append("===============================================================================================================")
        if path_exists(OA_OP_MOGAS_DIESEL_PATH) and path_exists(RELEVANT_PUMPS_SUMMARY_PATH):
            print(f"Processing Country : {Country_code} for the Year : {year} Started at : {datetime.datetime.now()}")
            Log_Text.append(f"Processing Country : {Country_code} for the Year : {year} Started at : {datetime.datetime.now()}")
            OA_OP_MOGAS_DIESEL_DF = spark.read.format("delta").load(OA_OP_MOGAS_DIESEL_PATH)
            RELEVANT_PUMPS_SUMMARY_DF = spark.read.format("delta").load(RELEVANT_PUMPS_SUMMARY_PATH)
            WSMA_Data_SiteId_Lis = sorted([str(row.SiteId) for row in OA_OP_MOGAS_DIESEL_DF.select("SiteId").distinct().collect()])#[0:6]
            WSMA_Data_SiteId_Lis_len = len(WSMA_Data_SiteId_Lis)
            print(f"Distinct SitedID's we have {WSMA_Data_SiteId_Lis_len} for the Country Code : {Country_code} during the period of : {year}")
            Log_Text.append(f"Distinct SitedID's we have {WSMA_Data_SiteId_Lis_len} for the Country Code : {Country_code} during the period of : {year}")

            batch_size = 100

            for batch_id in range(0, WSMA_Data_SiteId_Lis_len, batch_size):
                batch = WSMA_Data_SiteId_Lis[batch_id:min(batch_id + batch_size, WSMA_Data_SiteId_Lis_len)]

                result_grouped_txn_data_serve_or_wait_long_lis = []
                result_grouped_txn_data_serve_or_wait_long_with_queue_lis = []
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis = []
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis = []
                result_grouped_result_dict_grade_level_lis = []

                summary_result_detail_lis = []
                summary_result_detail_queue_lis = []
                summary_result_m_d_detail_lis = []
                summary_result_m_d_detail_queue_lis = []
                summary_result_grade_level_detail_lis = []

                summary_detail_rollup_lis = []
                summary_result_queue_rollup_lis = []
                summary_result_m_d_rollup_lis = []
                summary_result_m_d_queue_rollup_lis = []
                summary_result_grade_level_rollup_lis = []
                File_Trailer = f"From_{str(batch[0])}_To_{str(batch[-1])}"
                print(f"Processing Sites {File_Trailer} Started at : {datetime.datetime.now()}")
                for Site in batch:
                    Log_Text.append("                        ")
                    # print(f"Started Analysis for Site : {Site} at {datetime.datetime.now()}")
                    Log_Text.append(f"Started Analysis for Site : {Site} at {datetime.datetime.now()}")
                    OA_OP_MOGAS_DIESEL_Site_DF =  OA_OP_MOGAS_DIESEL_DF.filter(F.col("SiteID") == str(Site))
                    RELEVANT_PUMPS_SUMMARY_Site_DF =  RELEVANT_PUMPS_SUMMARY_DF.filter(F.col("SiteID") == str(Site))
                    RELEVANT_PUMPS_SUMMARY_Site_DF_Count = RELEVANT_PUMPS_SUMMARY_Site_DF.count()
                    # print(f"Count is : {RELEVANT_PUMPS_SUMMARY_Site_DF_Count} : {datetime.datetime.now()}")
                    if RELEVANT_PUMPS_SUMMARY_Site_DF_Count > 0:
                        data_with_relevant_pumps_Final = RELEVANT_PUMPS_SUMMARY_Site_DF.join(F.broadcast(OA_OP_MOGAS_DIESEL_Site_DF), on = ["SiteId","PartitionKey","GlobalSiteId", "siteName", "transactionStartTime","transactionEndTime","pumpNumber","relevant_pumps"], how = 'left')

                        max_transactions = (
                            data_with_relevant_pumps_Final
                            .agg(F.max("txn_ongoing_potential").alias("max_txn"))
                            .collect()[0]["max_txn"] + 1
                        )
                        # print(f"Calculation completed to find max_transactions : {datetime.datetime.now()}")

                        txn_already_happening = data_with_relevant_pumps_Final
                        txn_already_happening_with_queue = data_with_relevant_pumps_Final
                        txn_already_happening_m_d_combined = data_with_relevant_pumps_Final
                        txn_already_happening_m_d_combined_with_queue = data_with_relevant_pumps_Final

                        disp_col_list = []
                        dispenser = 1

                        while dispenser <= max_transactions:
                            col_name = str(dispenser)
                            disp_col_list.append(col_name)
                            
                            # When a customer arrives, if the number of transactions already ongoing
                            # is less than the number of dispensers at the site, the customer will
                            # be served immediately. Else, they need to wait.
                            txn_already_happening = txn_already_happening.withColumn(
                                f"Tran_Ogp_{col_name}",
                                F.when(F.col("txn_ongoing_potential") < dispenser, F.lit("immediately_served")).otherwise(F.lit("waiting"))
                            )
                            txn_already_happening_m_d_combined = txn_already_happening_m_d_combined.withColumn(
                                f"Tran_Og_MD_{col_name}",
                                F.when(F.col("txn_ongoing_mogas_diesel_combined") < dispenser, F.lit("immediately_served")).otherwise(F.lit("waiting"))
                            )
                            # Converting the 'immediately_served' and 'waiting' data to queue length.
                            txn_already_happening_with_queue = txn_already_happening_with_queue.withColumn(
                                f"Tran_Ogp_{col_name}",
                                F.when(
                                    (F.col("txn_ongoing_potential") + 1 - dispenser) > 0,
                                    F.col("txn_ongoing_potential") + 1 - dispenser
                                ).otherwise(F.lit(0))
                            )
                            txn_already_happening_m_d_combined_with_queue = txn_already_happening_m_d_combined_with_queue.withColumn(
                                f"Tran_Og_MD_{col_name}",
                                F.when(
                                    (F.col("txn_ongoing_mogas_diesel_combined") + 1 - dispenser) > 0,
                                    F.col("txn_ongoing_mogas_diesel_combined") + 1 - dispenser
                                ).otherwise(F.lit(0))
                            )
                            dispenser += 1
                        

                        if Ongoing_transactions_Data_store < 5:
                            txn_already_happening.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS"+"/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS")

                            # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS", SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS", "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS")

                            txn_already_happening_with_queue.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS"+"/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE")

                            # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS", SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/RELEVANT_PUMPS", "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE")

                            txn_already_happening_m_d_combined.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL"+"/TXN_ALREADY_HAPPENIING_M_D_COMBINED")

                            # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL", SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL", "TXN_ALREADY_HAPPENIING_M_D_COMBINED")

                            txn_already_happening_m_d_combined_with_queue.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL"+"/TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE")

                            # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL", SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES/"+"TXN_ALREADY_HAPPENIING/"+Site+"/MOGAS_DIESEL", "TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE")

                            # print(f"Calculation completed to find immediatley servied or waiting field : {datetime.datetime.now()}")

                        # Melt equivalent in PySpark: stack the Tran_Ogp_* columns into long format
                        melt_expr = F.expr(
                            "stack({0}, {1}) as (`If_the_site_has_x_dispensers`, `immediately_served_or_waiting`)".format(
                                len(disp_col_list),
                                ", ".join([f"{col}, `Tran_Ogp_{col}`" for col in disp_col_list])
                            )
                        )

                        txn_data_serve_or_wait_long = txn_already_happening.select(
                            "PartitionKey", "SiteId", "GlobalSiteId", "siteName", "txn_id", "deviceID", "additionalProductInfo", "productCode", "pumpNumber", "nozzleNumber", "SALES_QUANTITY",
                            "counterReading", "transactionStartTime", "transactionEndTime", "deliveryType", "lubricantQuantity",
                            "transactionNumber", "transactionStatus", "relevant_pumps",
                            "number_of_relevant_pumps", "number_of_relevant_pumps_mogas_diesel_non_truck_aggregated",  "site_vol",
                            "site_txn", "grade_vol", "grade_txn", "relevant_pumps_vol", "relevant_pumps_txn",
                            "relevant_pumps_vol_mogas_diesel_aggregated", "relevant_pumps_txn_mogas_diesel_aggregated",
                            "txn_ongoing_actual", "txn_ongoing_potential",
                            melt_expr
                        ) #"date",

                        #### Calculating the Volume and number of txn based on the number of dispensers and immediately_served_or_waiting
                        grouped_txn_data_serve_or_wait_long = txn_data_serve_or_wait_long.groupBy(
                            "PartitionKey", "SiteId", "GlobalSiteId", "siteName", "relevant_pumps", "number_of_relevant_pumps", "site_vol", "site_txn", "relevant_pumps_vol", "relevant_pumps_txn", "If_the_site_has_x_dispensers", "immediately_served_or_waiting"
                        ).agg(
                            F.sum("SALES_QUANTITY").alias("volume"),
                            F.count("txn_id").alias("number_of_txn")
                        )

                        result_grouped_txn_data_serve_or_wait_long = grouped_txn_data_serve_or_wait_long.withColumn(
                            "If_the_site_has_x_dispensers", F.col("If_the_site_has_x_dispensers").cast("int")
                        ).withColumn(
                            "Perc_of_relevant_vol", F.col("volume") * 100 / F.col("relevant_pumps_vol")
                        ).withColumn(
                            "Perc_of_relevant_txn", F.col("number_of_txn") * 100 / F.col("relevant_pumps_txn")
                        ).withColumn(
                            "Perc_of_site_vol", F.col("volume") * 100 / F.col("site_vol")
                        ).withColumn(
                            "Perc_of_site_txn", F.col("number_of_txn") * 100 / F.col("site_txn")
                        ).orderBy("If_the_site_has_x_dispensers", "immediately_served_or_waiting")

                        result_grouped_txn_data_serve_or_wait_long_lis.append(result_grouped_txn_data_serve_or_wait_long)

                        # Melt equivalent in PySpark: stack the Tran_Ogp_* columns into long format
                        melt_expr = F.expr(
                            "stack({0}, {1}) as (`If_the_site_has_x_dispensers`, `queue_length`)".format(
                                len(disp_col_list),
                                ", ".join([f"{col}, `Tran_Ogp_{col}`" for col in disp_col_list])
                            )
                        )

                        txn_data_serve_or_wait_long_with_queue = txn_already_happening_with_queue.select(
                            "PartitionKey", "SiteId", "GlobalSiteId", "siteName", "txn_id", "deviceID", "additionalProductInfo", "productCode", "pumpNumber", "nozzleNumber", "SALES_QUANTITY",
                            "counterReading", "transactionStartTime", "transactionEndTime", "deliveryType", "lubricantQuantity",
                            "transactionNumber", "transactionStatus", "relevant_pumps",
                            "number_of_relevant_pumps", "number_of_relevant_pumps_mogas_diesel_non_truck_aggregated",  "site_vol",
                            "site_txn", "grade_vol", "grade_txn", "relevant_pumps_vol", "relevant_pumps_txn",
                            "relevant_pumps_vol_mogas_diesel_aggregated", "relevant_pumps_txn_mogas_diesel_aggregated",
                            "txn_ongoing_actual", "txn_ongoing_potential",
                            melt_expr
                        ) #"date",

                        grouped_txn_data_serve_or_wait_long_with_queue = txn_data_serve_or_wait_long_with_queue.groupBy(
                            "PartitionKey", "SiteId", "GlobalSiteId", "siteName", "relevant_pumps", "number_of_relevant_pumps", "site_vol", "site_txn", "relevant_pumps_vol",
                            "relevant_pumps_txn", "If_the_site_has_x_dispensers", "queue_length"
                        ).agg(
                            F.sum("SALES_QUANTITY").alias("volume"),
                            F.count("txn_id").alias("number_of_txn")
                        ) 

                        result_grouped_txn_data_serve_or_wait_long_with_queue = grouped_txn_data_serve_or_wait_long_with_queue.withColumn(
                            "If_the_site_has_x_dispensers", F.col("If_the_site_has_x_dispensers").cast("int")
                        ).withColumn(
                            "Perc_of_relevant_vol", F.col("volume") * 100 / F.col("relevant_pumps_vol")
                        ).withColumn(
                            "Perc_of_relevant_txn", F.col("number_of_txn") * 100 / F.col("relevant_pumps_txn")
                        ).withColumn(
                            "Perc_of_site_vol", F.col("volume") * 100 / F.col("site_vol")
                        ).withColumn(
                            "Perc_of_site_txn", F.col("number_of_txn") * 100 / F.col("site_txn")
                        ).orderBy("If_the_site_has_x_dispensers", "queue_length")

                        w = Window.partitionBy("relevant_pumps","If_the_site_has_x_dispensers").orderBy("queue_length").rowsBetween(Window.unboundedPreceding, 0) ## If_the_site_has_x_dispensers
                        result_grouped_txn_data_serve_or_wait_long_with_queue = result_grouped_txn_data_serve_or_wait_long_with_queue.withColumn(
                            "cum_vol", F.sum("volume").over(w)
                        ).withColumn(
                            "cum_txn", F.sum("number_of_txn").over(w)
                        ).withColumn(
                            "cum_vol_Perc_of_relevant_vol", F.col("cum_vol") * 100 / F.col("relevant_pumps_vol")
                        ).withColumn(
                            "cum_txn_Perc_of_relevant_txn", F.col("cum_txn") * 100 / F.col("relevant_pumps_txn")
                        )

                        result_grouped_txn_data_serve_or_wait_long_with_queue_lis.append(result_grouped_txn_data_serve_or_wait_long_with_queue)


                        # Melt equivalent in PySpark: stack the Tran_Ogp_* columns into long format
                        melt_expr = F.expr(
                            "stack({0}, {1}) as (`If_the_site_has_x_dispensers`, `immediately_served_or_waiting`)".format(
                                len(disp_col_list),
                                ", ".join([f"{col}, `Tran_Og_MD_{col}`" for col in disp_col_list])
                            )
                        )

                        txn_data_serve_or_wait_m_d_combined_long = txn_already_happening_m_d_combined.select(
                            "PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'txn_id', 'deviceID', 'additionalProductInfo', 'productCode', 'pumpNumber', 'nozzleNumber', 'SALES_QUANTITY', 'counterReading', 'transactionStartTime', 'transactionEndTime', 'deliveryType', 'lubricantQuantity', 'transactionNumber', 'transactionStatus', 'mogas_diesel_non_truck_flag', 'relevant_pumps', 'number_of_relevant_pumps', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated', 'site_vol', 'site_txn', 'grade_vol', 'grade_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', 'txn_ongoing_actual', 'txn_ongoing_mogas_diesel_combined',
                            melt_expr
                        ) #"date", 

                        grouped_txn_data_serve_or_wait_m_d_combined_long = txn_data_serve_or_wait_m_d_combined_long.groupBy(
                            "PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'mogas_diesel_non_truck_flag', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated','site_vol', 'site_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', 'If_the_site_has_x_dispensers', 'immediately_served_or_waiting'
                        ).agg(
                            F.sum("SALES_QUANTITY").alias("volume"),
                            F.count("txn_id").alias("number_of_txn")
                        ) 

                        result_grouped_txn_data_serve_or_wait_m_d_combined_long = grouped_txn_data_serve_or_wait_m_d_combined_long.withColumn(
                            "If_the_site_has_x_dispensers", F.col("If_the_site_has_x_dispensers").cast("int")
                        ).withColumn(
                            "Perc_of_relevant_vol", F.col("volume") * 100 / F.col("relevant_pumps_vol_mogas_diesel_aggregated")
                        ).withColumn(
                            "Perc_of_relevant_txn", F.col("number_of_txn") * 100 / F.col("relevant_pumps_txn_mogas_diesel_aggregated")
                        ).withColumn(
                            "Perc_of_site_vol", F.col("volume") * 100 / F.col("site_vol")
                        ).withColumn(
                            "Perc_of_site_txn", F.col("number_of_txn") * 100 / F.col("site_txn")
                        ).orderBy("If_the_site_has_x_dispensers", "immediately_served_or_waiting")

                        result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis.append(result_grouped_txn_data_serve_or_wait_m_d_combined_long)


                        # Melt equivalent in PySpark: stack the Tran_Ogp_* columns into long format
                        melt_expr = F.expr(
                            "stack({0}, {1}) as (`If_the_site_has_x_dispensers`, `queue_length`)".format(
                                len(disp_col_list),
                                ", ".join([f"{col}, `Tran_Og_MD_{col}`" for col in disp_col_list])
                            )
                        )

                        txn_data_serve_or_wait_m_d_combined_long_with_queue = txn_already_happening_m_d_combined_with_queue.select(
                            "PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'txn_id', 'deviceID', 'additionalProductInfo', 'productCode', 'pumpNumber', 'nozzleNumber', 'SALES_QUANTITY', 'counterReading', 'transactionStartTime', 'transactionEndTime', 'deliveryType', 'lubricantQuantity', 'transactionNumber', 'transactionStatus', 'mogas_diesel_non_truck_flag', 'relevant_pumps', 'number_of_relevant_pumps', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated', 'site_vol', 'site_txn', 'grade_vol', 'grade_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', 'txn_ongoing_actual', 'txn_ongoing_mogas_diesel_combined',
                            melt_expr
                        ) #"date",

                        grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue = txn_data_serve_or_wait_m_d_combined_long_with_queue.groupBy(
                            "PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'mogas_diesel_non_truck_flag', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated','site_vol', 'site_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', 'If_the_site_has_x_dispensers', 'queue_length'
                        ).agg(
                            F.sum("SALES_QUANTITY").alias("volume"),
                            F.count("txn_id").alias("number_of_txn")
                        ) 

                        result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue = grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue.withColumn(
                            "If_the_site_has_x_dispensers", F.col("If_the_site_has_x_dispensers").cast("int")
                        ).withColumn(
                            "Perc_of_relevant_vol", F.col("volume") * 100 / F.col("relevant_pumps_vol_mogas_diesel_aggregated")
                        ).withColumn(
                            "Perc_of_relevant_txn", F.col("number_of_txn") * 100 / F.col("relevant_pumps_txn_mogas_diesel_aggregated")
                        ).withColumn(
                            "Perc_of_site_vol", F.col("volume") * 100 / F.col("site_vol")
                        ).withColumn(
                            "Perc_of_site_txn", F.col("number_of_txn") * 100 / F.col("site_txn")
                        ).orderBy("If_the_site_has_x_dispensers", "queue_length")

                        w = Window.partitionBy("relevant_pumps_vol_mogas_diesel_aggregated", "If_the_site_has_x_dispensers").orderBy("queue_length").rowsBetween(Window.unboundedPreceding, 0) #If_the_site_has_x_dispensers
                        result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue = result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue.withColumn(
                            "cum_vol", F.sum("volume").over(w)
                        ).withColumn(
                            "cum_txn", F.sum("number_of_txn").over(w)
                        ).withColumn(
                            "cum_vol_Perc_of_relevant_vol", F.col("cum_vol") * 100 / F.col("relevant_pumps_vol_mogas_diesel_aggregated")
                        ).withColumn(
                            "cum_txn_Perc_of_relevant_txn", F.col("cum_txn") * 100 / F.col("relevant_pumps_txn_mogas_diesel_aggregated")
                        )

                        result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis.append(result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue)



                        grouped_result_dict_grade_level = txn_data_serve_or_wait_long.groupBy(
                            "PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'relevant_pumps', 'number_of_relevant_pumps','If_the_site_has_x_dispensers', 'immediately_served_or_waiting','additionalProductInfo', 'productCode', 'site_vol', 'site_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'grade_vol', 'grade_txn'
                        ).agg(
                            F.sum("SALES_QUANTITY").alias("volume"),
                            F.count("txn_id").alias("number_of_txn")
                        ) 

                        result_grouped_result_dict_grade_level = grouped_result_dict_grade_level.withColumn(
                            "If_the_site_has_x_dispensers", F.col("If_the_site_has_x_dispensers").cast("int")
                        ).withColumn(
                            "Perc_of_grade_vol", F.col("volume") * 100 / F.col("grade_vol")
                        ).withColumn(
                            "Perc_of_grade_txn", F.col("number_of_txn") * 100 / F.col("grade_txn")
                        ).withColumn(
                            "Perc_of_site_vol", F.col("volume") * 100 / F.col("site_vol")
                        ).withColumn(
                            "Perc_of_site_txn", F.col("number_of_txn") * 100 / F.col("site_txn")
                        ).orderBy("If_the_site_has_x_dispensers", "immediately_served_or_waiting")

                        result_grouped_result_dict_grade_level_lis.append(result_grouped_result_dict_grade_level)
                        # print(f"Calculation completed to calculate cum vol : {datetime.datetime.now()}")


                        pump_util_result_potential_imm_served = result_grouped_txn_data_serve_or_wait_long.filter("immediately_served_or_waiting == 'immediately_served'")
                        pump_util_result_m_d_combined_imm_served = result_grouped_txn_data_serve_or_wait_m_d_combined_long.filter("immediately_served_or_waiting == 'immediately_served'")
                        pump_util_result_grade_level_imm_served = result_grouped_result_dict_grade_level.filter("immediately_served_or_waiting == 'immediately_served'")

                        summary_result_detail = pump_util_result_potential_imm_served
                        summary_result_detail_queue = result_grouped_txn_data_serve_or_wait_long_with_queue.select(["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'relevant_pumps', 'number_of_relevant_pumps', 'site_vol', 'site_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'If_the_site_has_x_dispensers', 'queue_length', 'cum_vol', 'cum_txn', 'cum_vol_Perc_of_relevant_vol', 'cum_txn_Perc_of_relevant_txn']) ## , 'volume', 'number_of_txn', 'Perc_of_relevant_vol', 'Perc_of_relevant_txn', 'Perc_of_site_vol', 'Perc_of_site_txn',
                        summary_result_m_d_detail = pump_util_result_m_d_combined_imm_served
                        summary_result_m_d_detail_queue = result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue.select(["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'mogas_diesel_non_truck_flag', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated', 'site_vol', 'site_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', 'If_the_site_has_x_dispensers', 'queue_length', 'cum_vol', 'cum_txn', 'cum_vol_Perc_of_relevant_vol', 'cum_txn_Perc_of_relevant_txn']) #'volume', 'number_of_txn', 'Perc_of_relevant_vol', 'Perc_of_relevant_txn', 'Perc_of_site_vol', 'Perc_of_site_txn',
                        summary_result_grade_level_detail = pump_util_result_grade_level_imm_served

                        
                        # For potential immediately served
                        summary_result_detail = add_dispenser_needed_flag(
                            pump_util_result_potential_imm_served,
                            "Perc_of_relevant_vol",
                            "SiteId,relevant_pumps",
                            percentages,
                            "Dispenser_needed_for"
                        )
                        summary_result_detail_lis.append(summary_result_detail)


                        # For queue (cumulative)
                        summary_result_detail_queue = add_dispenser_needed_flag(
                            summary_result_detail_queue,
                            "cum_vol_Perc_of_relevant_vol",
                            "SiteId,relevant_pumps,queue_length", #`If_the_site_has_x_dispensers`,queue_length
                            percentages,
                            "Dispenser_needed_for"
                        )
                        summary_result_detail_queue_lis.append(summary_result_detail_queue)

                        # For mogas/diesel combined immediately served
                        summary_result_m_d_detail = add_dispenser_needed_flag(
                            pump_util_result_m_d_combined_imm_served,
                            "Perc_of_relevant_vol",
                            "SiteId,mogas_diesel_non_truck_flag",
                            percentages,
                            "Dispenser_needed_for"
                        )
                        summary_result_m_d_detail_lis.append(summary_result_m_d_detail)

                        summary_result_m_d_detail_queue = add_dispenser_needed_flag(
                            summary_result_m_d_detail_queue,
                            "cum_vol_Perc_of_relevant_vol",
                            "SiteId,mogas_diesel_non_truck_flag,queue_length", #`If_the_site_has_x_dispensers`, queue_length
                            percentages,
                            "Dispenser_needed_for"
                        )
                        summary_result_m_d_detail_queue_lis.append(summary_result_m_d_detail_queue)

                        # For grade level
                        summary_result_grade_level_detail = add_dispenser_needed_flag(
                            pump_util_result_grade_level_imm_served,
                            "Perc_of_grade_vol",
                            "SiteId,additionalProductInfo,productCode",
                            percentages,
                            "Dispenser_needed_for"
                        )
                        summary_result_grade_level_detail_lis.append(summary_result_grade_level_detail)

                        # Define groupby columns for each rollup
                        summary_detail_rollup_cols = ["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'relevant_pumps', 'number_of_relevant_pumps', 'site_vol', 'site_txn', 'relevant_pumps_vol', 'relevant_pumps_txn']
                        summary_result_m_d_rollup_cols = ["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'mogas_diesel_non_truck_flag', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated', 'site_vol', 'site_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated']
                        summary_result_queue_rollup_cols = ["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'relevant_pumps', 'number_of_relevant_pumps', 'site_vol', 'site_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'queue_length'] #"`If_the_site_has_x_dispensers`", queue_length
                        summary_result_m_d_queue_rollup_cols = ["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'mogas_diesel_non_truck_flag', 'number_of_relevant_pumps_mogas_diesel_non_truck_aggregated',  'site_vol', 'site_txn', 'relevant_pumps_vol_mogas_diesel_aggregated', 'relevant_pumps_txn_mogas_diesel_aggregated', "queue_length"] #"`If_the_site_has_x_dispensers`", queue_length
                        summary_result_grade_level_rollup_cols = ["PartitionKey", 'SiteId', "GlobalSiteId", "siteName", 'additionalProductInfo', 'productCode', 'relevant_pumps', 'number_of_relevant_pumps', 'site_vol', 'site_txn', 'relevant_pumps_vol', 'relevant_pumps_txn', 'grade_vol', 'grade_txn']

                        # List of percentage columns
                        percentage_cols = [f"Dispenser_needed_for_{i}%_vol" for i in percentages]

                        # Use F.col() for all columns with special characters
                        summary_detail_rollup = summary_result_detail.groupBy(summary_detail_rollup_cols).agg(
                            *[F.sum(F.col(f"`{col}`")).alias(col) for col in percentage_cols]
                        )
                        summary_result_m_d_rollup = summary_result_m_d_detail.groupBy(summary_result_m_d_rollup_cols).agg(
                            *[F.sum(F.col(f"`{col}`")).alias(col) for col in percentage_cols]
                        )
                        summary_result_queue_rollup = summary_result_detail_queue.groupBy(summary_result_queue_rollup_cols).agg(
                            *[F.sum(F.col(f"`{col}`")).alias(col) for col in percentage_cols]
                        )
                        summary_result_m_d_queue_rollup = summary_result_m_d_detail_queue.groupBy(summary_result_m_d_queue_rollup_cols).agg(
                            *[F.sum(F.col(f"`{col}`")).alias(col) for col in percentage_cols]
                        )
                        summary_result_grade_level_rollup = summary_result_grade_level_detail.groupBy(summary_result_grade_level_rollup_cols).agg(
                            *[F.sum(F.col(f"`{col}`")).alias(col) for col in percentage_cols]
                        )

                        # Add freeable dispenser columns for each percentage
                        for item in percentages:
                            col_name = f"Dispenser_needed_for_{item}%_vol"
                            freeable_disp_col_name = f"Freeable_dispensers_if_we_serve_{item}%_vol"
                            summary_detail_rollup = summary_detail_rollup.withColumn(
                                freeable_disp_col_name, F.col("number_of_relevant_pumps") - F.col(f"`{col_name}`")
                            )
                            summary_result_m_d_rollup = summary_result_m_d_rollup.withColumn(
                                freeable_disp_col_name, F.col("number_of_relevant_pumps_mogas_diesel_non_truck_aggregated") - F.col(f"`{col_name}`")
                            )
                            summary_result_queue_rollup = summary_result_queue_rollup.withColumn(
                                freeable_disp_col_name, F.col("number_of_relevant_pumps") - F.col(f"`{col_name}`")
                            )
                            summary_result_m_d_queue_rollup = summary_result_m_d_queue_rollup.withColumn(
                                freeable_disp_col_name, F.col("number_of_relevant_pumps_mogas_diesel_non_truck_aggregated") - F.col(f"`{col_name}`")
                            )
                            summary_result_grade_level_rollup = summary_result_grade_level_rollup.withColumn(
                                freeable_disp_col_name, F.col("number_of_relevant_pumps") - F.col(f"`{col_name}`")
                            )

                        summary_detail_rollup_lis.append(summary_detail_rollup)
                        summary_result_queue_rollup_lis.append(summary_result_queue_rollup)
                        summary_result_m_d_rollup_lis.append(summary_result_m_d_rollup)
                        summary_result_m_d_queue_rollup_lis.append(summary_result_m_d_queue_rollup)
                        summary_result_grade_level_rollup_lis.append(summary_result_grade_level_rollup)

                        # print(f"Calculation completed to calculate Summary rollup : {datetime.datetime.now()}")
                        

                        print(f"Completed Analysis for Site : {Site} at {datetime.datetime.now()}")
                        Log_Text.append(f"Completed Analysis for Site : {Site} at {datetime.datetime.now()}")
                        Log_Text.append("-------------------------------------------------------")
                        Ongoing_transactions_Data_store+=1

                    else:
                        print(f"We have 0 records for Country : {Country_code} and Site : {Site} and Year : {year}")
                        Log_Text.append(f"We have 0 records for Country : {Country_code} and Site : {Site} and Year : {year}")
                        Log_Text.append("-------------------------------------------------------")
                

                result_grouped_txn_data_serve_or_wait_long_lis_merged = reduce(lambda a, b: a.unionByName(b), result_grouped_txn_data_serve_or_wait_long_lis)
                result_grouped_txn_data_serve_or_wait_long_lis_merged = result_grouped_txn_data_serve_or_wait_long_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                result_grouped_txn_data_serve_or_wait_long_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_%VOL_%TXN_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_%VOL_%TXN_"+File_Trailer)

                result_grouped_txn_data_serve_or_wait_long_with_queue_lis_merged = reduce(lambda a, b: a.unionByName(b), result_grouped_txn_data_serve_or_wait_long_with_queue_lis)
                result_grouped_txn_data_serve_or_wait_long_with_queue_lis_merged = result_grouped_txn_data_serve_or_wait_long_with_queue_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                result_grouped_txn_data_serve_or_wait_long_with_queue_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_%VOL_%TXN_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_%VOL_%TXN_"+File_Trailer)

                result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis_merged = reduce(lambda a, b: a.unionByName(b), result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis)
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis_merged = result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_%VOL_%TXN_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_%VOL_%TXN_"+File_Trailer)

                result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis_merged = reduce(lambda a, b: a.unionByName(b), result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis)
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis_merged = result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                result_grouped_txn_data_serve_or_wait_m_d_combined_long_with_queue_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_%VOL_%TXN_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_%VOL_%TXN_"+File_Trailer)

                result_grouped_result_dict_grade_level_lis_merged = reduce(lambda a, b: a.unionByName(b), result_grouped_result_dict_grade_level_lis)
                result_grouped_result_dict_grade_level_lis_merged = result_grouped_result_dict_grade_level_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                result_grouped_result_dict_grade_level_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_GRADE_LEVEL_%VOL_%TXN_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_GRADE_LEVEL_%VOL_%TXN_"+File_Trailer)

                summary_result_detail_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_detail_lis)
                summary_result_detail_lis_merged = summary_result_detail_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_detail_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_"+File_Trailer)

                summary_result_detail_queue_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_detail_queue_lis)
                summary_result_detail_queue_lis_merged = summary_result_detail_queue_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_detail_queue_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_"+File_Trailer)

                summary_result_m_d_detail_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_m_d_detail_lis)
                summary_result_m_d_detail_lis_merged = summary_result_m_d_detail_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_m_d_detail_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_"+File_Trailer)

                summary_result_m_d_detail_queue_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_m_d_detail_queue_lis)
                summary_result_m_d_detail_queue_lis_merged = summary_result_m_d_detail_queue_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_m_d_detail_queue_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_"+File_Trailer)

                summary_result_grade_level_detail_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_grade_level_detail_lis)
                summary_result_grade_level_detail_lis_merged = summary_result_grade_level_detail_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_grade_level_detail_lis_merged.coalesce(1).withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+"/INTERMEDIATE_SUMMARIES/TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+'/INTERMEDIATE_SUMMARIES', "TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_"+File_Trailer)

                summary_detail_rollup_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_detail_rollup_lis)
                summary_detail_rollup_lis_merged = summary_detail_rollup_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_queue_rollup_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_queue_rollup_lis)
                summary_result_queue_rollup_lis_merged = summary_result_queue_rollup_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_m_d_rollup_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_m_d_rollup_lis)
                summary_result_m_d_rollup_lis_merged = summary_result_m_d_rollup_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_m_d_queue_rollup_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_m_d_queue_rollup_lis)
                summary_result_m_d_queue_rollup_lis_merged = summary_result_m_d_queue_rollup_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')
                summary_result_grade_level_rollup_lis_merged = reduce(lambda a, b: a.unionByName(b), summary_result_grade_level_rollup_lis)
                summary_result_grade_level_rollup_lis_merged = summary_result_grade_level_rollup_lis_merged.join(F.broadcast(MDM_Data_Country), on=['PartitionKey', 'GlobalSiteId'], how='left')

                summary_detail_rollup_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/ROLLUP_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_ROLLUP_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/ROLLUP_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_ROLLUP_"+File_Trailer)

                summary_result_queue_rollup_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+"/ROLLUP_SUMMARIES/TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_ROLLUP_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/RELEVANT_PUMPS/"+File_Trailer+'/ROLLUP_SUMMARIES', "TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_ROLLUP_"+File_Trailer)

                summary_result_m_d_rollup_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/ROLLUP_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_ROLLUP_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/ROLLUP_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_ROLLUP_"+File_Trailer)

                summary_result_m_d_queue_rollup_lis_merged.coalesce(1).write.format("csv").mode("overwrite").option('header','true').save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+"/ROLLUP_SUMMARIES/TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_ROLLUP_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/MOGAS_DIESEL/"+File_Trailer+'/ROLLUP_SUMMARIES', "TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_ROLLUP_"+File_Trailer)

                summary_result_grade_level_rollup_lis_merged.withColumn("relevant_pumps",F.col("relevant_pumps").cast("string")).coalesce(1).write.format("csv").option('header','true').mode("overwrite").save(Root_Path+'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+"/ROLLUP_SUMMARIES/TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_ROLLUP_"+File_Trailer)

                # upload_file_to_sharepoint('OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer, SharePoint_Folder, 'OUTPUT FILES/'+Country_code+'/'+str(year)+"/FINAL SUMMARY FILES"+"/GRADE_LEVEL/"+File_Trailer+'/ROLLUP_SUMMARIES', "TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_ROLLUP_"+File_Trailer)
                
                print(f"Processing Sites {File_Trailer} Completed at : {datetime.datetime.now()}")

            print(f"Processing Country : {Country_code} for the Year : {year} Completed at : {datetime.datetime.now()}")
            Log_Text.append(f"Processing Country : {Country_code} for the Year : {year} Completed at : {datetime.datetime.now()}")
            Log_Text.append("===============================================================================================================")
            Log_Text.append("      ")
        else:
            Log_Text.append(f"Path does not exists : {OA_OP_MOGAS_DIESEL_PATH}")
            Log_Text.append("===============================================================================================================")
            Log_Text.append("      ")
            print(f"Path does not exists : {OA_OP_MOGAS_DIESEL_PATH} or {RELEVANT_PUMPS_SUMMARY_PATH}")

# COMMAND ----------

# DBTITLE 1,Untitled
if path_exists(Log_File):
    print(f"Log File already exists : {Log_File}")
else:
    print(f"Log File does not exists : {Log_File}")
    dbutils.fs.mkdirs(Log_File)
dbutils.fs.put(Log_File+'/Log.txt', '\n'.join(Log_Text), overwrite=True)

# COMMAND ----------

def create_and_upload_excel_to_sharepoint(country_code, year, excel_type='intermediate'):
    """
    Creates an Excel file with multiple sheets and uploads to SharePoint
    
    """
    
    print(f"\n{'='*80}")
    print(f"Creating {excel_type.upper()} Excel for Country: {country_code}, Year: {year}")
    print(f"{'='*80}\n")
    
    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
    
    # Determine folder type and filename
    folder_type = "INTERMEDIATE_SUMMARIES" if excel_type == 'intermediate' else "ROLLUP_SUMMARIES"
    excel_filename = f"PUMP_UTILIZATION_{country_code}_{year}_{folder_type}.xlsx"
    
    # Create BytesIO object to hold Excel file in memory
    excel_buffer = BytesIO()
    
    # Create Excel writer
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        
        # Base path for this country and year
        base_path = f"{Root_Path}OUTPUT FILES/{country_code}/{year}/FINAL SUMMARY FILES"
        
        # Dictionary to store all dataframes by sheet name
        if excel_type == 'intermediate':
            # 10 sheets for intermediate summaries
            sheets_config = {
                # Grade Level sheets (2)
                'Grade_Level_Summary': {
                    'category': 'GRADE_LEVEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_'
                },
                'Grade_Level_Detail': {
                    'category': 'GRADE_LEVEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_GRADE_LEVEL_%VOL_%TXN_'
                },
                
                # Mogas Diesel sheets (4)
                'Mogas_Diesel_Summary': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_'
                },
                'Mogas_Diesel_Queue_Summary': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_'
                },
                'Mogas_Diesel_Detail': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_%VOL_%TXN_'
                },
                'Mogas_Diesel_Queue_Detail': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_%VOL_%TXN_'
                },
                
                # Relevant Pumps sheets (4)
                'Relevant_Pumps_Summary': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_'
                },
                'Relevant_Pumps_Queue_Summary': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_'
                },
                'Relevant_Pumps_Detail': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_%VOL_%TXN_'
                },
                'Relevant_Pumps_Queue_Detail': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_%VOL_%TXN_'
                }
            }
        else:  # rollup
            # 5 sheets for rollup summaries
            sheets_config = {
                'Grade_Level_Rollup': {
                    'category': 'GRADE_LEVEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_GRADE_LEVEL_SUMMARY_ROLLUP_'
                },
                'Mogas_Diesel_Rollup': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_SUMMARY_ROLLUP_'
                },
                'Mogas_Diesel_Queue_Rollup': {
                    'category': 'MOGAS_DIESEL',
                    'filename': 'TXN_ALREADY_HAPPENIING_M_D_COMBINED_WITH_QUEUE_SUMMARY_ROLLUP_'
                },
                'Relevant_Pumps_Rollup': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_SUMMARY_ROLLUP_'
                },
                'Relevant_Pumps_Queue_Rollup': {
                    'category': 'RELEVANT_PUMPS',
                    'filename': 'TXN_ALREADY_HAPPENIING_RELEVANT_PUMPS_WITH_QUEUE_SUMMARY_ROLLUP_'
                }
            }
        
        # Initialize storage for dataframes
        sheets_data = {sheet_name: [] for sheet_name in sheets_config.keys()}
        
        # Collect data from all batch folders
        try:
            # Get all batch folders from RELEVANT_PUMPS (as it contains all batches)
            relevant_pumps_path = f"{base_path}/RELEVANT_PUMPS"
            
            if not path_exists(relevant_pumps_path):
                print(f"Warning: Path does not exist: {relevant_pumps_path}")
                return None
            
            all_folders = dbutils.fs.ls(relevant_pumps_path)
            batch_folders = [f.name.rstrip('/') for f in all_folders if f.name.startswith('From_')]
            
            print(f"Found {len(batch_folders)} batch folders: {batch_folders}")
            
            for batch_folder in batch_folders:
                print(f"  Processing batch: {batch_folder}")
                
                # Process each sheet configuration
                for sheet_name, config in sheets_config.items():
                    category = config['category']
                    filename = config['filename']
                    
                    # Construct the full path
                    csv_path = f"{base_path}/{category}/{batch_folder}/{folder_type}/{filename}{batch_folder}"
                    
                    try:
                        if path_exists(csv_path):
                            # Find the CSV file in the folder
                            csv_files = [f.path for f in dbutils.fs.ls(csv_path) if f.path.endswith('.csv')]
                            
                            if csv_files:
                                csv_file = csv_files[0].replace("dbfs:/", "/dbfs/")
                                df = pd.read_csv(csv_file)
                                sheets_data[sheet_name].append(df)
                                print(f"    ✓ {sheet_name}: {len(df)} rows from {batch_folder}")
                            else:
                                print(f"    ⚠ {sheet_name}: No CSV file found in {csv_path}")
                        else:
                            print(f"    ⚠ {sheet_name}: Path does not exist: {csv_path}")
                    except Exception as e:
                        print(f"    ✗ Error reading {sheet_name} from {batch_folder}: {e}")
        
        except Exception as e:
            print(f"Error collecting CSV files: {e}")
            import traceback
            traceback.print_exc()
        
        # Combine all dataframes for each sheet and write to Excel
        sheets_written = 0
        for sheet_name, df_list in sheets_data.items():
            if df_list:
                # Concatenate all dataframes for this sheet
                combined_df = pd.concat(df_list, ignore_index=True)
                
                # Truncate sheet name to 31 characters (Excel limit)
                sheet_name_truncated = sheet_name[:31]
                
                # Write to Excel
                combined_df.to_excel(writer, sheet_name=sheet_name_truncated, index=False)
                sheets_written += 1
                print(f"\n✓ Sheet '{sheet_name_truncated}': {len(combined_df)} total rows (from {len(df_list)} batches)")
            else:
                print(f"\n✗ Warning: No data found for sheet '{sheet_name}'")
        
        if sheets_written == 0:
            print(f"\n✗✗✗ ERROR: No sheets were written to Excel file! ✗✗✗")
            return None
    
    # Save Excel to buffer
    excel_buffer.seek(0)
    
    # Upload to SharePoint
    sharepoint_path = f"{SharePoint_Folder}OUTPUT FILES/{country_code}/{year}"
    
    try:
        print(f"\nUploading to SharePoint: {sharepoint_path}")
        
        # Check if folder exists, create if not
        try:
            target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_path)
            ctx.load(target_folder).execute_query()
            print(f"  Folder exists: {sharepoint_path}")
        except Exception as e:
            print(f"  Creating folder: {sharepoint_path}")
            target_folder = ctx.web.ensure_folder_path(sharepoint_path).execute_query()
            print(f"  Folder created successfully")
        
        # Upload the Excel file
        excel_bytes = excel_buffer.read()
        target_folder.upload_file(excel_filename, excel_bytes).execute_query()
        
        print(f"\n{'='*80}")
        print(f"✓✓✓ SUCCESS: Uploaded {excel_filename} to SharePoint ✓✓✓")
        print(f"  Location: {sharepoint_path}/{excel_filename}")
        print(f"  File size: {len(excel_bytes) / 1024 / 1024:.2f} MB")
        print(f"  Sheets: {sheets_written}")
        print(f"{'='*80}\n")
        
        return excel_filename
        
    except Exception as e:
        print(f"\n✗✗✗ ERROR uploading to SharePoint: {e} ✗✗✗")
        import traceback
        traceback.print_exc()
        return None


def process_excel_files_for_all_countries():
    """
    Process and upload Excel files for all countries and years
    """
    
    print("\n" + "="*100)
    print("STARTING EXCEL FILE GENERATION AND SHAREPOINT UPLOAD")
    print("="*100 + "\n")
    
    # Use the same country list and years from the main processing
    current_year = datetime.datetime.now().year
    Years = [current_year - 2, current_year - 1]
    
    results = []
    
    for Country_code in WSMA_Data_Country_Lis:
        for year in Years:
            
            # Check if the required paths exist before processing
            OA_OP_MOGAS_DIESEL_PATH = f"{Root_Path}OUTPUT FILES/{Country_code}/{str(year)}/OA_OP_MOGAS_DIESEL"
            RELEVANT_PUMPS_SUMMARY_PATH = f"{Root_Path}OUTPUT FILES/{Country_code}/{str(year)}/RELEVANT_PUMPS_SUMMARY"
            
            if path_exists(OA_OP_MOGAS_DIESEL_PATH) and path_exists(RELEVANT_PUMPS_SUMMARY_PATH):
                
                print(f"\n{'#'*100}")
                print(f"Processing Country: {Country_code}, Year: {year}")
                print(f"{'#'*100}\n")
                
                # Create and upload Intermediate Summaries Excel
                try:
                    intermediate_excel = create_and_upload_excel_to_sharepoint(Country_code, year, 'intermediate')
                    if intermediate_excel:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Type': 'Intermediate',
                            'Status': 'Success',
                            'Filename': intermediate_excel
                        })
                    else:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Type': 'Intermediate',
                            'Status': 'Failed',
                            'Filename': None
                        })
                except Exception as e:
                    print(f"✗ Error creating intermediate Excel for {Country_code} {year}: {e}")
                    import traceback
                    traceback.print_exc()
                    results.append({
                        'Country': Country_code,
                        'Year': year,
                        'Type': 'Intermediate',
                        'Status': 'Error',
                        'Filename': None
                    })
                
                # Create and upload Rollup Summaries Excel
                try:
                    rollup_excel = create_and_upload_excel_to_sharepoint(Country_code, year, 'rollup')
                    if rollup_excel:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Type': 'Rollup',
                            'Status': 'Success',
                            'Filename': rollup_excel
                        })
                    else:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Type': 'Rollup',
                            'Status': 'Failed',
                            'Filename': None
                        })
                except Exception as e:
                    print(f"✗ Error creating rollup Excel for {Country_code} {year}: {e}")
                    import traceback
                    traceback.print_exc()
                    results.append({
                        'Country': Country_code,
                        'Year': year,
                        'Type': 'Rollup',
                        'Status': 'Error',
                        'Filename': None
                    })
                
            else:
                print(f"\nSkipping {Country_code} {year} - Required paths do not exist")
                print(f"  {OA_OP_MOGAS_DIESEL_PATH}: {path_exists(OA_OP_MOGAS_DIESEL_PATH)}")
                print(f"  {RELEVANT_PUMPS_SUMMARY_PATH}: {path_exists(RELEVANT_PUMPS_SUMMARY_PATH)}")
    
    # Print summary
    print("\n" + "="*100)
    print("EXCEL FILE GENERATION SUMMARY")
    print("="*100 + "\n")
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    # Count successes and failures
    total = len(results)
    success = len([r for r in results if r['Status'] == 'Success'])
    failed = len([r for r in results if r['Status'] == 'Failed'])
    errors = len([r for r in results if r['Status'] == 'Error'])
    
    print(f"\n{'='*100}")
    print(f"Total files processed: {total}")
    print(f"  ✓ Successful: {success}")
    print(f"  ✗ Failed: {failed}")
    print(f"  ✗ Errors: {errors}")
    print(f"{'='*100}\n")
    
    return results_df


# Execute the Excel file generation and upload
excel_results = process_excel_files_for_all_countries()

# COMMAND ----------

# ============================================================================
# CODE TO COMBINE SITE PUMP LEVEL SUMMARY DATA AND UPLOAD TO SHAREPOINT
# Add this code at the end of the notebook
# ============================================================================

def combine_site_pump_summary_to_excel(country_code, year):
    """
    Reads all delta/parquet files from SITE_PUMP_LEVEL_SUMMARY folder,
    combines them into an Excel file and uploads to SharePoint
    
    ADLS Source Path: /mnt/ADLS2/{Root_Folder}/OUTPUT FILES/{country_code}/{year}/SITE_PUMP_LEVEL_SUMMARY
    SharePoint Destination: {SharePoint_Folder}/OUTPUT FILES/{country_code}/{year}/Site_Pump_Level_Summary_{country_code}_{year}.xlsx
    
    Parameters:
    -----------
    country_code : str
        The country code (e.g., 'AT', 'US')
    year : int
        The year for which to process data
    
    Returns:
    --------
    str : The filename of the uploaded Excel file, or None if failed
    """
    
    print(f"\n{'='*80}")
    print(f"Creating Site Pump Level Summary Excel for Country: {country_code}, Year: {year}")
    print(f"{'='*80}\n")
    
    # Initialize SharePoint connection
    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(sharepoint_site_url).with_credentials(credentials)
    
    # Excel filename
    excel_filename = f"Site_Pump_Level_Summary_{country_code}_{year}.xlsx"
    
    # Path to SITE_PUMP_LEVEL_SUMMARY folder in ADLS
    site_pump_summary_path = f"{Root_Path}OUTPUT FILES/{country_code}/{year}/SITE_PUMP_LEVEL_SUMMARY"
    
    print(f"Looking for data in: {site_pump_summary_path}")
    
    # Check if path exists
    if not path_exists(site_pump_summary_path):
        print(f"✗ Path does not exist: {site_pump_summary_path}")
        return None
    
    # Create BytesIO object to hold Excel file in memory
    excel_buffer = BytesIO()
    
    # Create Excel writer
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        
        sheets_written = 0
        
        try:
            # List all files/folders in SITE_PUMP_LEVEL_SUMMARY
            all_items = dbutils.fs.ls(site_pump_summary_path)
            
            print(f"Found {len(all_items)} items in {site_pump_summary_path}")
            
            # Check if it's a delta table directly (look for _delta_log folder)
            delta_files = [item for item in all_items if '_delta_log' in item.name]
            
            if delta_files:
                # It's a delta table - read directly
                print(f"  Reading delta table from {site_pump_summary_path}...")
                
                df_spark = spark.read.format("delta").load(site_pump_summary_path)
                df_pandas = df_spark.toPandas()
                
                sheet_name = "Site_Pump_Level_Summary"[:31]
                df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                sheets_written += 1
                
                print(f"  ✓ Sheet '{sheet_name}': {len(df_pandas)} rows")
                
            else:
                # Check for parquet files or subfolders
                parquet_files = [item for item in all_items if item.name.endswith('.parquet') or item.name.endswith('.parquet/')]
                
                if parquet_files:
                    # Read parquet files
                    for idx, parquet_file in enumerate(parquet_files):
                        parquet_path = parquet_file.path
                        print(f"  Reading parquet file from {parquet_path}...")
                        
                        df_spark = spark.read.parquet(parquet_path)
                        df_pandas = df_spark.toPandas()
                        
                        # Create sheet name from file name (limit to 31 chars)
                        file_name = parquet_file.name.replace('.parquet', '').replace('/', '')
                        sheet_name = file_name[:31] if file_name else f"Sheet_{idx+1}"
                        
                        df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                        sheets_written += 1
                        
                        print(f"  ✓ Sheet '{sheet_name}': {len(df_pandas)} rows")
                
                else:
                    # Look for subfolders that might be delta tables or contain data
                    subfolders = [item for item in all_items if item.name.endswith('/')]
                    
                    if not subfolders:
                        print(f"  No delta tables, parquet files, or subfolders found")
                        return None
                    
                    for subfolder in subfolders:
                        subfolder_path = subfolder.path
                        subfolder_name = subfolder.name.rstrip('/')
                        
                        try:
                            # Check if subfolder is a delta table
                            subfolder_items = dbutils.fs.ls(subfolder_path)
                            is_delta = any('_delta_log' in item.name for item in subfolder_items)
                            
                            if is_delta:
                                print(f"  Reading delta table from {subfolder_path}...")
                                df_spark = spark.read.format("delta").load(subfolder_path)
                            else:
                                # Try reading as parquet
                                print(f"  Reading parquet from {subfolder_path}...")
                                df_spark = spark.read.parquet(subfolder_path)
                            
                            df_pandas = df_spark.toPandas()
                            
                            # Create sheet name (limit to 31 chars)
                            sheet_name = subfolder_name[:31]
                            
                            df_pandas.to_excel(writer, sheet_name=sheet_name, index=False)
                            sheets_written += 1
                            
                            print(f"  ✓ Sheet '{sheet_name}': {len(df_pandas)} rows")
                            
                        except Exception as e:
                            print(f"  ⚠ Could not read {subfolder_path}: {e}")
            
            if sheets_written == 0:
                print(f"\n✗✗✗ ERROR: No data found in {site_pump_summary_path} ✗✗✗")
                return None
                
        except Exception as e:
            print(f"✗ Error reading data from {site_pump_summary_path}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # Prepare for upload
    excel_buffer.seek(0)
    
    # Upload to SharePoint
    sharepoint_path = f"{SharePoint_Folder}OUTPUT FILES/{country_code}/{year}"
    
    try:
        print(f"\n  Uploading to SharePoint: {sharepoint_path}")
        
        # Ensure folder exists
        try:
            target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_path)
            ctx.load(target_folder).execute_query()
            print(f"  Folder exists: {sharepoint_path}")
        except Exception as e:
            print(f"  Creating folder: {sharepoint_path}")
            target_folder = ctx.web.ensure_folder_path(sharepoint_path).execute_query()
            print(f"  Folder created successfully")
        
        # Upload the Excel file
        excel_bytes = excel_buffer.read()
        target_folder.upload_file(excel_filename, excel_bytes).execute_query()
        
        print(f"\n{'='*80}")
        print(f"✓✓✓ SUCCESS: Uploaded {excel_filename} to SharePoint ✓✓✓")
        print(f"  Location: {sharepoint_path}/{excel_filename}")
        print(f"  File size: {len(excel_bytes) / 1024 / 1024:.2f} MB")
        print(f"  Sheets: {sheets_written}")
        print(f"{'='*80}\n")
        
        return excel_filename
        
    except Exception as e:
        print(f"\n✗✗✗ ERROR uploading to SharePoint: {e} ✗✗✗")
        import traceback
        traceback.print_exc()
        return None


def process_site_pump_summary_for_all_countries():
    """
    Process and upload Site Pump Level Summary Excel files for all countries and years
    """
    
    print("\n" + "="*100)
    print("STARTING SITE PUMP LEVEL SUMMARY EXCEL GENERATION AND SHAREPOINT UPLOAD")
    print("="*100 + "\n")
    
    # Use the same country list and years from the main processing
    current_year = datetime.datetime.now().year
    Years = [current_year - 2, current_year - 1]
    
    results = []
    
    for Country_code in WSMA_Data_Country_Lis:
        for year in Years:
            
            # Check if the SITE_PUMP_LEVEL_SUMMARY path exists
            site_pump_summary_path = f"{Root_Path}OUTPUT FILES/{Country_code}/{str(year)}/SITE_PUMP_LEVEL_SUMMARY"
            
            if path_exists(site_pump_summary_path):
                
                print(f"\n{'#'*100}")
                print(f"Processing Country: {Country_code}, Year: {year}")
                print(f"{'#'*100}\n")
                
                # Create and upload Site Pump Summary Excel
                try:
                    excel_file = combine_site_pump_summary_to_excel(Country_code, year)
                    if excel_file:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Status': 'Success',
                            'Filename': excel_file
                        })
                    else:
                        results.append({
                            'Country': Country_code,
                            'Year': year,
                            'Status': 'Failed',
                            'Filename': None
                        })
                except Exception as e:
                    print(f"✗ Error creating Excel for {Country_code} {year}: {e}")
                    import traceback
                    traceback.print_exc()
                    results.append({
                        'Country': Country_code,
                        'Year': year,
                        'Status': 'Error',
                        'Filename': None
                    })
                
            else:
                print(f"\nℹ Skipping {Country_code} {year} - SITE_PUMP_LEVEL_SUMMARY folder does not exist")
    
    # Print summary
    print("\n" + "="*100)
    print("SITE PUMP LEVEL SUMMARY EXCEL FILE GENERATION SUMMARY")
    print("="*100 + "\n")
    
    if results:
        results_df = pd.DataFrame(results)
        print(results_df.to_string(index=False))
        
        # Count successes and failures
        total = len(results)
        success = len([r for r in results if r['Status'] == 'Success'])
        failed = len([r for r in results if r['Status'] == 'Failed'])
        errors = len([r for r in results if r['Status'] == 'Error'])
        
        print(f"\n{'='*100}")
        print(f"Total files processed: {total}")
        print(f"  ✓ Successful: {success}")
        print(f"  ✗ Failed: {failed}")
        print(f"  ✗ Errors: {errors}")
        print(f"{'='*100}\n")
        
        return results_df
    else:
        print("ℹ No SITE_PUMP_LEVEL_SUMMARY folders found for any country/year combination.")
        return None


# Execute the Site Pump Summary Excel file generation and upload
site_pump_summary_results = process_site_pump_summary_for_all_countries()