from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
  builder. \
  config('spark.shuffle.useOldFetchProtocol','true'). \
  config('spark.dynamicAllocation.enabled','false'). \
  config('spark.executor.instances','2'). \
  config('spark.executor.cores','2'). \
  config('spark.executor.memory','2g'). \
  config('spark.ui.port', '0'). \
  config("spark.sql.warehouse.dir", "/user/{username}/warehouse"). \
  enableHiveSupport(). \
  master('yarn'). \
  getOrCreate()


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import subprocess, os

################################################# Import CONFIG file variables ###############################################################


FileInputPath = os.environ.get('FileInputPath')
TempFilePath = os.environ.get('TempFilePath')
OutputFilePath = os.environ.get('OutputFilePath')
RejectFilePath = os.environ.get('RejectFilePath')
DatabaseName = os.environ.get('DatabaseName')


cust_acc_schema = "cust_id String, Account_num String, card_num String, acct_open_dt String, acct_close_dt String, acct_cycle_dt String, defaulted String"


cust_acc_df = spark.read \
.format("csv") \
.schema(cust_acc_schema) \
.option("header","true") \
.option("mode","permissive") \
.option("dateFormat","dd-MMM-yy") \
.load("{}/CUST_ACCT_DTL_Sample.csv".format(FileInputPath)) \
.distinct()

cust_acc_df = cust_acc_df.sort("Cust_Id","Account_Num").dropDuplicates(["Cust_Id","Account_Num"])
cust_acc_df.show()

acct_cust_cnt = cust_acc_df.count()
print("Count of records of Cust Acct detail: {}".format(acct_cust_cnt))



### Null check

columns_null_check = ["cust_id", "Account_Num", "card_num", "acct_open_dt", "acct_cycle_dt", "defaulted"]
cust_acc_no_null_df = cust_acc_df.dropna(how = "any", subset = columns_null_check)
cust_acc_no_null_df.show()
cust_acc_null_df = cust_acc_df.subtract(cust_acc_no_null_df)
null_reason = "Record is rejected due to NULL value in the column"
cust_acc_null_df = cust_acc_null_df.withColumn("Rejection_reason", lit(null_reason))
print("Records with null in cust_acc data are -")
cust_acc_null_df.show()


## Duplicate records drop

cust_acc_no_duplicate_df = cust_acc_no_null_df.dropDuplicates(["cust_id", "Account_num"])
cust_acc_no_duplicate_df.count()
duplicate_rejection = "Record is rejected due to duplicate Customer Id"
cust_acc_duplicate_df = cust_acc_no_null_df.subtract(cust_acc_no_duplicate_df)
cust_acc_duplicate_df = cust_acc_duplicate_df.withColumn("Rejection_reason", lit(duplicate_rejection))
print("Duplicate records are - ")
cust_acc_duplicate_df.show()


## Droping closed accounts

cust_acc_no_closed_acc_df = cust_acc_no_duplicate_df.filter(~(cust_acc_no_duplicate_df["acct_close_dt"].isNotNull()))
cust_acc_no_closed_acc_df.show()
cust_acc_no_closed_acc_df.count()
account_closed_rejection = "Account for this customer has already bee closed"
cust_acc_closed_df = cust_acc_no_duplicate_df.filter(cust_acc_no_duplicate_df["acct_close_dt"].isNotNull())
cust_acc_closed_df = cust_acc_closed_df.withColumn("Rejection_reason", lit(account_closed_rejection))
print("accounts which are closed are - ")
cust_acc_closed_df.show()


## Changing the date format to yyyy-mm-dd

cust_acc_no_closed_acc_df = cust_acc_no_closed_acc_df.withColumn("acct_open_dt", to_date("acct_open_dt", "dd-MMM-yy"))

## Special character check

regex_pattern_int_check = r'[a-zA-Z-!@#$%^&*()_+={}\[\]:;<>,.?~\\/ ]'
cust_acc_no_special_char_df = cust_acc_no_closed_acc_df.filter(~(col("cust_id").rlike(regex_pattern_int_check) | col("Account_num").rlike(regex_pattern_int_check) | col("card_num").rlike(regex_pattern_int_check) | col("acct_cycle_dt").rlike(regex_pattern_int_check) | col("defaulted").rlike(regex_pattern_int_check)))
cust_acc_no_special_char_df.show()
cust_acc_no_special_char_df.count()
special_char_rejection = "special character present in one of the column"
cust_acc_special_char_df = cust_acc_no_closed_acc_df.subtract(cust_acc_no_special_char_df)
cust_acc_special_char_df = cust_acc_special_char_df.withColumn("Rejection_reason", lit(special_char_rejection))
print("records with special character are - ")
cust_acc_special_char_df.show()

## Card length check

cust_acc_filtered_wrong_card_df = cust_acc_no_special_char_df.filter(~(length("card_num") != 15))
cust_acc_filtered_wrong_card_df.show()
cust_acc_filtered_wrong_card_df.count()
card_invalid_rejection = "card nummber is not valid"
cust_acc_card_invalid_df = cust_acc_no_special_char_df.filter(length("card_num") != 15)
cust_acc_card_invalid_df = cust_acc_card_invalid_df.withColumn("Rejection_reason", lit(card_invalid_rejection))
print("invalid cards are -  ")
cust_acc_card_invalid_df.show()

print("final filtered results")
cust_acc_filtered_wrong_card_df.show()

## Aggregated invalid records

master_cust_reject_df = cust_acc_null_df.union(cust_acc_duplicate_df).union(cust_acc_closed_df).union(cust_acc_special_char_df).union(cust_acc_card_invalid_df)
print("aggregated invalid records are - ")
master_cust_reject_df.show(truncate= False)
cust_acc_filtered_wrong_card_df = cust_acc_filtered_wrong_card_df.withColumn("cust_id", col("cust_id").cast(IntegerType())) \
.withColumn("Account_num", col("Account_num").cast(LongType())) \
.withColumn("card_num", col("card_num").cast(LongType())) \
.withColumn("acct_cycle_dt", col("acct_cycle_dt").cast(IntegerType())) \
.withColumn("defaulted", col("defaulted").cast(IntegerType()))

cust_acc_filtered_wrong_card_df.write.format("parquet").mode("overwrite").option("path","{}".format(TempFilePath)).saveAsTable("{}.CUSTOMER_ACCOUNT_STG".format(DatabaseName))

print("Final customer account data fetched from saved table - ")
spark.sql("select * from {}.CUSTOMER_ACCOUNT_STG".format(DatabaseName)).show()