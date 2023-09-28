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
DatabaseName = os.environ.get('DatabaseName')



###################################################### Define Schema ######################################################

customer_data_schema = "CUSTOMER_ID String, FIRST_NAME String, MIDDLE_NAME String, LAST_NAME String, DOB String, ADDRESS1 String, ADDRESS2 String, STATE String, CITY String, COUNTRY String, PIN_CODE String, CUST_PEP_EXPOSED String, ANNUAL_INCOME Float"


###################################################### Read the file ######################################################

### PERMISSIVE

customer_data_df = spark.read \
.format("csv") \
.schema(customer_data_schema) \
.option("mode","permissive") \
.option("header","true") \
.load("{}/Banking4_CustomerDetails_20230912.csv".format(FileInputPath))

print("Current dataset is:")

customer_data_df.show()

### FAILFAST

try:
    customer_data_df_fail_fast = spark.read \
        .format("csv") \
        .option("mode","failFast") \
        .schema(customer_data_schema) \
        .option("Header","True") \
        .load("{}/Banking4_CustomerDetails_20230912.csv".format(FileInputPath))
except Exception as e:
    print("Exception occurred:", e)


### CAPTURE RECORDS DROPPED


dropped_records = customer_data_df_fail_fast.subtract(customer_data_df)

print("Dropped records are:")
dropped_records.show()

############################## Transform the date file in correct YYYY-MM-DD format ####################################################################


customer_data_base_df = customer_data_df \
.withColumn("DOB", to_date("DOB","dd-MM-yyyy")) \
.withColumn("YEAR", year("DOB")) \
.withColumn("MONTH", month("DOB")) \
.withColumn("CUST_PEP_EXPOSED",upper(col("CUST_PEP_EXPOSED"))) \
.withColumn("COUNTRY",upper(col("COUNTRY")))  


############################## CUSTOMER_ID VALIDATIONS ####################################################################

### Remove Duplicate based on Customer Id

customer_data_base_df_no_dup = customer_data_base_df.dropDuplicates(["CUSTOMER_ID"])
print("Dataset without duplicate records is:")
customer_data_base_df_no_dup.show()

Customer_data_duplicates_df = customer_data_base_df.subtract(customer_data_base_df_no_dup)

Duplicate_rejection = "Record is rejected due to duplicate Customer Id"
Customer_data_duplicates_df = Customer_data_duplicates_df.withColumn("Rejection_Reason",lit(Duplicate_rejection))

print("Duplicate records are:")
Customer_data_duplicates_df.show()


############################################################ Null Check ############################################################################


columns_null_check = ["CUSTOMER_ID","FIRST_NAME","LAST_NAME","DOB","ADDRESS2","STATE","CITY","COUNTRY","PIN_CODE","CUST_PEP_EXPOSED","ANNUAL_INCOME"]
customer_data_no_null = customer_data_base_df_no_dup.dropna(how="any", subset=columns_null_check)


customer_null_data = customer_data_base_df_no_dup.subtract(customer_data_no_null)

null_rejection = "Record is rejected due to NULL value in the column"
customer_null_data = customer_null_data.withColumn("Rejection_Reason",lit(null_rejection))

print("Records with NULL in their columns are:")
customer_null_data.show()


################################################################################### Alpha character check ################################################



regex_pattern_int_check = r'[a-zA-Z!@#$%^&*()_+={}\[\]:;<>,.?~\\/ ]'


customer_data_no_special_chars = customer_data_no_null \
.filter(~col("CUSTOMER_ID").rlike(regex_pattern_int_check) &
        ~col("DOB").rlike(regex_pattern_int_check))

print("Records with no special chars:")
customer_data_no_special_chars.show()


print("Records with special characters or alphabets in customer_id or DOB fields:")

customer_data_special_chars = customer_data_no_null.subtract(customer_data_no_special_chars)

Alpha_rejection = "Record is rejected due to presence of alpha or special characters in CUSTOMER_ID/DOB fields"
customer_data_special_chars = customer_data_special_chars.withColumn("Rejection_Reason",lit(Alpha_rejection))


print("Records with special characters in Customer_Id or DOB fields:")
customer_data_special_chars.show()



################################################### Validate fields which should only have alphabets #######################################################################


regex_pattern_string_check = r'[0-9!@#$%^&*()_+={}\[\]:;<>,.?~\\/]'


customer_data_num_check_rejects = customer_data_no_special_chars \
.filter(col("STATE").rlike(regex_pattern_string_check) |
        col("CITY").rlike(regex_pattern_string_check) |
        col("COUNTRY").rlike(regex_pattern_string_check) |
        col("FIRST_NAME").rlike(regex_pattern_string_check) |
        col("MIDDLE_NAME").rlike(regex_pattern_string_check) |
        col("LAST_NAME").rlike(regex_pattern_string_check)
        )


customer_data_string_values = customer_data_no_special_chars.subtract(customer_data_num_check_rejects)

print("Records with proper Name, State, City, Country:")
customer_data_string_values.show()

Number_rejection = "Record is rejected as integer and special character found in fields which should only have Alphabets"
customer_data_num_check_rejects = customer_data_num_check_rejects.withColumn("Rejection_Reason",lit(Number_rejection))

print("Rejections due to special chars or numbers in Name, State, City, Country fields:")
customer_data_num_check_rejects.show()


################################################### PEP EXPOSED #############################################################################

PEP_EXPOSED = ['TRUE']

customer_data_pep_exposed = customer_data_string_values.filter(col("CUST_PEP_EXPOSED").isin(PEP_EXPOSED))

print("Data which has correct PEP Exposed value")
customer_data_pep_exposed.show()


customer_data_pep_not_exposed  = customer_data_string_values.subtract(customer_data_pep_exposed)

print("Records with PEP Exposed as FALSE:")

PEP_EXP_Rejection = "The customer does not satisfy PEP exposed condition"
customer_data_pep_not_exposed = customer_data_pep_not_exposed.withColumn("Rejection_Reason",lit(PEP_EXP_Rejection))

customer_data_pep_not_exposed.show()


################################################### SANCTIONED COUNTRY Check ######################################################################

sanctioned_countries_df = spark.read \
.format("csv") \
.option("header","true") \
.load("{}/Sanctioned_Countries.csv".format(FileInputPath)) \
.distinct()


customer_data_sanctioned_country_clear = customer_data_pep_exposed \
    .join(broadcast(sanctioned_countries_df), 
          (customer_data_pep_exposed.COUNTRY) == (sanctioned_countries_df.COUNTRY),
          "left_anti"
         )


customer_data_sanctioned_country_rejects = customer_data_pep_exposed.subtract(customer_data_sanctioned_country_clear)

print("Records with sanction clearence are:")
customer_data_sanctioned_country_clear.show()

sanction_rejection = "Customer is from country which comes under sanctioned category"
customer_data_sanctioned_country_rejects = customer_data_sanctioned_country_rejects.withColumn("Rejection_Reason",lit(sanction_rejection))

print("Records with country which is in sanctioned list are:")
customer_data_sanctioned_country_rejects.show()

#################################################  Check for Negative Annual Income #########################################################

customer_data_negative_df_rejects = customer_data_sanctioned_country_clear.filter(col("ANNUAL_INCOME") < 0)


customer_data_sanctioned_country_clear = customer_data_sanctioned_country_clear.subtract(customer_data_negative_df_rejects)

customer_data_sanctioned_country_clear.count()

customer_data_sanctioned_country_clear.show()

amount_rejection = "The annual income mentioned is negative"
customer_data_negative_df_rejects = customer_data_negative_df_rejects.withColumn("Rejection_Reason",lit(amount_rejection))

print("Records with negative annual income are:")
customer_data_negative_df_rejects.show()

#####customer_data_sanctioned_country_clear.write.mode("overwrite").parquet("{}/customer_data_sanctioned_country_clear.parquet".format(TempFilePath))

#################################################  MASTER UNION REJECTION DF's #########################################################


dfs_to_union = [
    Customer_data_duplicates_df,
    customer_null_data,
    customer_data_special_chars,
    customer_data_num_check_rejects,
    customer_data_pep_not_exposed,
    customer_data_sanctioned_country_rejects,
    customer_data_negative_df_rejects
]

master_rejection_df = reduce(lambda df1, df2: df1.union(df2), dfs_to_union)


print("Master reject records are:")

master_customer_rejection_df_order = ['Rejection_Reason','CUSTOMER_ID', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'DOB', 'ADDRESS1', 'ADDRESS2', 'STATE', 'CITY', 'COUNTRY', 'PIN_CODE', 'CUST_PEP_EXPOSED', 'ANNUAL_INCOME']
FileName = "Customer Information"

master_customer_rejection_df = master_rejection_df.select(master_customer_rejection_df_order)
master_customer_rejection_df = master_customer_rejection_df.withColumn("FileName",lit(FileName))
master_customer_rejection_df.show(truncate=False)


################################################ CREATE SPARK SQL Table for CUST_CIF_STG data ##########################################################################

customer_data_sanctioned_country_clear_for_table = (
    customer_data_sanctioned_country_clear
    .withColumn("CUSTOMER_ID", col("CUSTOMER_ID").cast("Integer"))
    .withColumn("FIRST_NAME", col("FIRST_NAME").cast("String"))
    .withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast("String"))
    .withColumn("LAST_NAME", col("LAST_NAME").cast("String"))
    .withColumn("DOB", col("DOB").cast("Date"))
    .withColumn("ADDRESS1", col("ADDRESS1").cast("String"))
    .withColumn("ADDRESS2", col("ADDRESS2").cast("String"))
    .withColumn("STATE", col("STATE").cast("String"))
    .withColumn("CITY", col("CITY").cast("String"))
    .withColumn("COUNTRY", col("COUNTRY").cast("String"))
    .withColumn("PIN_CODE", col("PIN_CODE").cast("String"))
    .withColumn("CUST_PEP_EXPOSED", col("CUST_PEP_EXPOSED").cast("String"))
    .withColumn("ANNUAL_INCOME", col("ANNUAL_INCOME").cast("Float"))
    .withColumn("YEAR", col("YEAR").cast("Integer"))
    .withColumn("MONTH", col("MONTH").cast("Integer"))
)





customer_data_sanctioned_country_clear_for_table.write \
.format("parquet") \
.option("compression", "snappy") \
.mode("overwrite") \
.partitionBy("COUNTRY","STATE") \
.bucketBy(4, "CUSTOMER_ID") \
.sortBy("CUSTOMER_ID") \
.option("path","{}/CUST_PAR/".format(OutputFilePath)) \
.option("basepath","{}/CUST_PAR/".format(OutputFilePath)) \
.saveAsTable("{}.CUST_CIF_STG_PAR".format(DatabaseName))

###customer_data_sanctioned_country_clear_for_table.write.mode("overwrite").parquet("{}/tbl/customer_data_sanctioned_country_clear_for_table.parquet".format(TempFilePath))

###customer_data_sanctioned_country_clear_for_table.show()

print("CUST_CIF_STG_PAR table is created")


################################################ CREATE SPARK SQL Table for CUST_CIF metadata rejects ##########################################################################


master_customer_rejection_df.write \
.format("parquet") \
.option("compression", "snappy") \
.mode("overwrite") \
.partitionBy("Rejection_Reason") \
.option("path","{}".format(RejectFilePath)) \
.option("basepath","{}".format(RejectFilePath)) \
.saveAsTable("{}.CUST_Information_Metadata_Rejects".format(DatabaseName))

print("CUST_CIF_Metadata_Rejects table is created")
