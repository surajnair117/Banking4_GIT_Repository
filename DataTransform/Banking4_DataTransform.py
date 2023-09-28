from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
  builder. \
  config('spark.shuffle.useOldFetchProtocol','true'). \
  config('spark.dynamicAllocation.enabled','true'). \
  config('spark.executor.instances','2'). \
  config('spark.executor.cores','2'). \
  config('spark.executor.memory','2g'). \
  config('spark.ui.port', '0'). \
  config("spark.sql.warehouse.dir", "/user/{username}/warehouse"). \
  enableHiveSupport(). \
  master('yarn'). \
  getOrCreate()


###################################################### Import the functions ######################################################

from pyspark.sql.types import *
from pyspark.sql.functions import *
import os


################################################# Import CONFIG file ###############################################################


Annual_Income_Limit1 = int(os.environ.get('Annual_Income_Limit1'))
Annual_Income_Limit2 = int(os.environ.get('Annual_Income_Limit2'))
Annual_Income_Limit3 = int(os.environ.get('Annual_Income_Limit3'))

Annual_Income_Score1 = int(os.environ.get('Annual_Income_Score1'))
Annual_Income_Score2 = int(os.environ.get('Annual_Income_Score2'))
Annual_Income_Score3 = int(os.environ.get('Annual_Income_Score3'))
Annual_Income_Score4 = int(os.environ.get('Annual_Income_Score4'))
Credit_Limit_Multiplier = int(os.environ.get('Credit_Limit_Multiplier'))
Month_EMI_Percentage_3 = int(os.environ.get('Month_EMI_Percentage_3'))
Month_EMI_Percentage_6 = int(os.environ.get('Month_EMI_Percentage_6'))
Month_EMI_Percentage_9 = int(os.environ.get('Month_EMI_Percentage_9'))
FileInputPath = os.environ.get('FileInputPath')
TempFilePath = os.environ.get('TempFilePath')
OutputFilePath = os.environ.get('OutputFilePath')
RejectFilePath = os.environ.get('RejectFilePath')
Inter_Table_Path = os.environ.get('Inter_Table_Path')

tbl_path = "{}/CUST_PAR/".format(Inter_Table_Path)
DatabaseName = os.environ.get('DatabaseName')



###################################################### Import the dataframes from other PY files ######################################################

customer_data_sanctioned_country_clear_for_table = spark.sql("select * from {}.CUST_CIF_STG_PAR".format(DatabaseName))

cust_cnt = customer_data_sanctioned_country_clear_for_table.count()
print("Count of records of Cust detail: {}".format(cust_cnt))
customer_data_sanctioned_country_clear_for_table.cache()



cust_acct_dtl_df = spark.sql("select * from {}.CUSTOMER_ACCOUNT_STG".format(DatabaseName))

acct_cust_cnt = cust_acct_dtl_df.count()
print("Count of records of Cust Acct detail: {}".format(acct_cust_cnt))
cust_acct_dtl_df.cache()


trans_dtl_df = spark.sql("select * from {}.TRANSACTION_STG".format(DatabaseName))

trans_dtl_cnt = trans_dtl_df.count()
print("Count of records of Transaction detail: {}".format(trans_dtl_cnt))
trans_dtl_df.cache()


#################################################### Decode EMI formula ###########################################################

monthly_rate_3 = Month_EMI_Percentage_3/1200
monthly_rate_6 = Month_EMI_Percentage_6/1200
monthly_rate_9 = Month_EMI_Percentage_9/1200


###################################################### Temporary part till all dataframes are created ######################################################



################################################################################################################################################################


####################################################### JOIN 3 Dataframes to create base transformation dataframe ##############################################################

joined_df_pre_validation = trans_dtl_df.join(cust_acct_dtl_df, ["Cust_Id", "Account_Num","Card_Num"], "left") \
.join(broadcast(customer_data_sanctioned_country_clear_for_table),col("Cust_Id") == col("CUSTOMER_ID"),"left")


joined_df_pre_validation.show()

account_null_check = ["Acct_open_dt","FIRST_NAME"]
joined_df_pre_validation_wo_null = joined_df_pre_validation.dropna(how="any", subset=account_null_check)


print("Record with valid account details:")
joined_df_pre_validation_wo_null.show()

acount_null_rej_reason = "Transaction made through an account/card which is not registered or an Invalid transaction due to customer data not available"

joined_df_account_nulls = joined_df_pre_validation.subtract(joined_df_pre_validation_wo_null)

joined_df_account_nulls = joined_df_account_nulls.withColumn("Rejection_Reason",lit(acount_null_rej_reason))

print("Record rejected due to account or a card which is not registered or due to customer not being found in valid customer entries")
joined_df_account_nulls.show()

jnd_nulls = joined_df_account_nulls.count()

print("Count of records with incorrect card: {}".format(jnd_nulls))

################################################### Transform each field #############################################################


joined_df_direct_maps = joined_df_pre_validation_wo_null.select("Cust_Id", "Account_Num", "Card_Num" ,"Transaction_Dt", "FIRST_NAME", "LAST_NAME", "Acct_open_dt", "Acct_close_dt", "Annual_Income", "Defaulted", "Transaction_Amount", "Transaction_Purpose", "Transaction_Place")


joined_df_direct_maps = joined_df_pre_validation_wo_null \
.withColumn("Account_Current_Status",when(col("Defaulted") >= 1, "Y").otherwise("N")) \
.withColumn("Credit_Limit",col("Annual_Income")*Credit_Limit_Multiplier) \
.withColumn("CS_Metric_1", when(datediff(current_date(),col("Acct_open_dt")) < 365, 200) \
            .when((datediff(current_date(), col("Acct_open_dt")) >= 365) & (datediff(current_date(), col("Acct_open_dt")) < 720), 400) \
            .when(datediff(current_date(), col("Acct_open_dt")) >= 720, 500) \
           ) \
.withColumn("CS_Metric_2", when(col("Defaulted") == 0, 100) \
           .when((col("Defaulted") >= 1) & (col("Defaulted") < 3), 50) \
           .when(col("Defaulted") >= 3, 0) \
           .otherwise(0)) \
.withColumn("CS_Metric_3", when(col("Annual_Income") < Annual_Income_Limit1, Annual_Income_Score1) \
            .when((col("Annual_Income") >= Annual_Income_Limit1) & (col("Annual_Income") < Annual_Income_Limit2), Annual_Income_Score2) \
            .when((col("Annual_Income") >= Annual_Income_Limit2) & (col("Annual_Income") < Annual_Income_Limit3), Annual_Income_Score3) \
            .otherwise(Annual_Income_Score4)) \
.withColumn("Credit_Score",col("CS_Metric_3") + col("CS_Metric_2") + col("CS_Metric_1")) \
.withColumn("Cust_credit_limit_remaining",col("Credit_Limit") - col("Transaction_Amount")) \
.withColumn("Cust_credit_limit_used",col("Cust_credit_limit_remaining") + col("Transaction_Amount")) \
.withColumn("Customer_Since", when(col("Acct_close_dt").isNull(), datediff(current_date(),col("Acct_open_dt"))) ) \
.withColumn("Acct_Active",when(col("Acct_close_dt").isNull(),"Y").otherwise("N")) \
.withColumn("DIff_Trans_Limit",(col("Cust_credit_limit_remaining") - col("Transaction_Amount"))) \
.withColumn("EMI_Eligible", when((col("Opt_EMI") == 'Y') & (col("Credit_Score") >= 500) & (col("Customer_Since") >= 720) & (col("Transaction_Amount") > 56000) & (col("Cust_credit_limit_remaining") - col("Transaction_Amount") > 0), "Y").otherwise("N")) \
.withColumn("EMI_3_month_check1", col("Transaction_Amount") * lit(monthly_rate_3) * (pow(1 + lit(monthly_rate_3),3)) ) \
.withColumn("EMI_3_month_check2", (pow(1 + lit(monthly_rate_3),3)) - 1) \
.withColumn("EMI_3_month", col("EMI_3_month_check1") / col("EMI_3_month_check2")) \
.withColumn("EMI_6_month_check1", col("Transaction_Amount") * lit(monthly_rate_6) * (pow(1 + lit(monthly_rate_6),6)) ) \
.withColumn("EMI_6_month_check2", (pow(1 + lit(monthly_rate_6),6)) - 1) \
.withColumn("EMI_6_month", col("EMI_6_month_check1") / col("EMI_6_month_check2")) \
.withColumn("EMI_9_month_check1", col("Transaction_Amount") * lit(monthly_rate_9) * (pow(1 + lit(monthly_rate_9),9)) ) \
.withColumn("EMI_9_month_check2", (pow(1 + lit(monthly_rate_9),9)) - 1) \
.withColumn("EMI_9_month", col("EMI_9_month_check1") / col("EMI_9_month_check2"))




joined_df_direct_maps = joined_df_direct_maps.filter(col("Cust_credit_limit_remaining") >= 0)


print("Joined dataset i.e. direct mapping:")
joined_df_direct_maps.show()

cnt = joined_df_direct_maps.count()

print("The count of records are: {}".format(cnt))

##################################################################### Create Intermediate Table ############################################################################



joined_df_intermediate_tbl_df = joined_df_direct_maps.select(

col("Cust_Id").cast("Integer"),
col("Account_Num").cast("Integer"),
col("Card_Num").cast("Long"),
col("Transaction_Dt").cast("Date"),
col("Transaction_Amount").cast("float"),
col("Transaction_Purpose").cast("String"),
col("Transaction_Place").cast("String"),
col("Opt_EMI").cast("String"),
col("Acct_open_dt").cast("Date"),
col("Acct_close_dt").cast("Date"),
col("Acct_Cycle_dt").cast("Integer"),
col("Defaulted").cast("String"),
col("FIRST_NAME").cast("String"),
col("MIDDLE_NAME").cast("String"),
col("LAST_NAME").cast("String"),
col("DOB").cast("Date"),
col("ADDRESS1").cast("String"),
col("ADDRESS2").cast("String"),
col("STATE").cast("String"),
col("CITY").cast("String"),
col("COUNTRY").cast("String"),
col("PIN_CODE").cast("String"),
col("CUST_PEP_EXPOSED").cast("String"),
col("ANNUAL_INCOME").cast("float"),
col("YEAR").cast("Integer"),
col("MONTH").cast("Integer"),
col("Account_Current_Status").cast("String"),
col("Credit_Limit").cast("float"),
col("CS_Metric_1").cast("Integer"),
col("CS_Metric_2").cast("Integer"),
col("CS_Metric_3").cast("Integer"),
col("Credit_Score").cast("Integer"),
col("Cust_credit_limit_used").cast("float"),
col("Cust_credit_limit_remaining").cast("float"),
col("Customer_Since").cast("Integer"),
col("Acct_Active").cast("String"),
col("DIff_Trans_Limit").cast("float"),
col("EMI_Eligible").cast("String"),
col("EMI_3_month_check1").cast("float"),
col("EMI_3_month_check2").cast("float"),
col("EMI_3_month").cast("float"),
col("EMI_6_month_check1").cast("float"),
col("EMI_6_month_check2").cast("float"),
col("EMI_6_month").cast("float"),
col("EMI_9_month_check1").cast("float"),
col("EMI_9_month_check2").cast("float"),
col("EMI_9_month").cast("float")

)


joined_df_intermediate_tbl_df.write \
.format("parquet") \
.mode("overwrite") \
.partitionBy("YEAR", "COUNTRY", "STATE") \
.bucketBy(4, "Cust_Id") \
.sortBy("Cust_Id") \
.option("path","{}/CUST_PAR/".format(Inter_Table_Path)) \
.option("basepath","{}/CUST_PAR/".format(Inter_Table_Path)) \
.saveAsTable("{}.SIMPLE_PAY_INTERMEDIATE_PAR".format(DatabaseName))

print("SIMPLE_PAY_INTERMEDIATE_PAR table is created")


spark.sql("CACHE TABLE {}.SIMPLE_PAY_INTERMEDIATE_PAR".format(DatabaseName))

print("SIMPLE_PAY_INTERMEDIATE_PAR table is cached")



