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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from functools import reduce
import os


################################################# Import CONFIG file variables ###############################################################


FileInputPath = os.environ.get('FileInputPath')
TempFilePath = os.environ.get('TempFilePath')
OutputFilePath = os.environ.get('OutputFilePath')
RejectFilePath = os.environ.get('RejectFilePath')
TransactionOPPath = os.environ.get('TransactionOPPath')
DatabaseName = os.environ.get('DatabaseName')
Date_Format_trn = os.environ.get('Date_Format_trn')


################################################# Read the file ###############################################################

trans_dtl_schema = "Cust_Id Integer, Account_Num Integer, Card_Num long, Transaction_Dt Date, Transaction_Amount double, Transaction_Purpose String, Transaction_Place String, Opt_EMI String"


Transaction_data_df = spark.read \
.format("csv") \
.schema(trans_dtl_schema) \
.option("header","true") \
.option("mode","permissive") \
.option("dateFormat","dd-MMM-yy") \
.load("{}/TRAN_DTL_STG_SampleFile.csv".format(FileInputPath)) \
.distinct()

Transaction_data_df = Transaction_data_df.sort("Cust_Id","Account_Num").dropDuplicates(["Cust_Id","Account_Num"])
Transaction_data_df.show()

trans_acct_cnt = Transaction_data_df.count()
print("Count of records of Transaction detail: {}".format(trans_acct_cnt))


######################################### Null Checks #################################################################################

null_check_subsets= ['Cust_Id', 'Account_Num', 'Card_Num', 'Transaction_Dt', 'Transaction_Amount', 'Transaction_Purpose', 'Transaction_Place', 'Opt_EMI']

Transaction_with_no_null_data_df=Transaction_data_df.dropna("any",subset=null_check_subsets)

Transaction_with_null_data_df=Transaction_data_df.subtract(Transaction_with_no_null_data_df)

Null_Rejection='Record is rejected due to presence of null values'

Transaction_with_null_data_final_df=Transaction_with_null_data_df.withColumn("Rejection_Reason",lit(Null_Rejection))

print('below dataset is removed due to null')

Transaction_with_null_data_final_df.show()


########################### Duplicates Checks ################################################################################

Trasaction_with_no_duplicate_data_df=Transaction_with_no_null_data_df.dropDuplicates(["cust_id", "Account_num"])

Trasaction_with_duplicate_data_df=Transaction_with_no_null_data_df.subtract(Trasaction_with_no_duplicate_data_df)

Duplicate_Rejection="Record is rejected due to presence of duplicate transaction"

Trasaction_with_duplicate_data_df= Trasaction_with_duplicate_data_df.withColumn("Rejection_Reason", lit(Duplicate_Rejection))


print('below dataset is removed due to duplicate value')

Trasaction_with_duplicate_data_df.show()




############################### Alphabet checks ########################################################################################

regex_pattern_alphabet_check = r'[0-9!@#$%^&*()_+={}\[\]:;<>,.?~\\/]'


Transaction_with_invalid_alpha_df = Trasaction_with_no_duplicate_data_df \
.filter(col("Transaction_Purpose").rlike(regex_pattern_alphabet_check) |
        col("Transaction_Place").rlike(regex_pattern_alphabet_check))


Transaction_with_valid_alpha_df = Trasaction_with_no_duplicate_data_df.subtract(Transaction_with_invalid_alpha_df)

Alpha_Rejection='Record is rejected as integer and special character found in fields which should only have Alphabets'

Transaction_with_invalid_alpha_df=Transaction_with_invalid_alpha_df.withColumn("Rejection_Reason", lit(Alpha_Rejection))

print('Below records are rejected as integer and special character found in fields which should only have Alphabets ')

Transaction_with_invalid_alpha_df.show()



######################################### FinalDataframe ####################################################################################################


Transaction_Final_DF=Transaction_with_valid_alpha_df

print('Final Transaction to be evaluated')

Transaction_Final_DF.show()

Transaction_Final_DF.coalesce(1).write \
.format('parquet') \
.mode('overwrite') \
.option("header", "true") \
.option('path',"{}".format(TransactionOPPath)) \
.saveAsTable("{}.TRANSACTION_STG".format(DatabaseName))

print("Final Transaction data fetched from saved table - ")
spark.sql("select * from {}.TRANSACTION_STG".format(DatabaseName)).show()

