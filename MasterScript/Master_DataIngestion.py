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


###################################################### Import the functions and config parameters ######################################################

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import subprocess, os

config_df = spark.read \
.format("csv") \
.option("header","true") \
.option("mode","permissive") \
.load("/user/itv006343/Banking4/InputData/Config.csv")

FileInputPath = config_df.filter(config_df.Parameter == "InputFilePath").select("Value").collect()[0][0]
TempFilePath = config_df.filter(config_df.Parameter == "TempFilePath").select("Value").collect()[0][0]
OutputFilePath = config_df.filter(config_df.Parameter == "OutputFilePath").select("Value").collect()[0][0]
RejectFilePath = config_df.filter(config_df.Parameter == "RejectFilePath").select("Value").collect()[0][0]
TransactionOPPath = config_df.filter(config_df.Parameter == "TransactionOPPath").select("Value").collect()[0][0]
Annual_Income_Limit1 = int(config_df.filter(config_df.Parameter == "Annual_Income_Limit1").select("Value").collect()[0][0])
Annual_Income_Limit2 = int(config_df.filter(config_df.Parameter == "Annual_Income_Limit2").select("Value").collect()[0][0])
Annual_Income_Limit3 = int(config_df.filter(config_df.Parameter == "Annual_Income_Limit3").select("Value").collect()[0][0])
Annual_Income_Score1 = int(config_df.filter(config_df.Parameter == "Annual_Income_Score1").select("Value").collect()[0][0])
Annual_Income_Score2 = int(config_df.filter(config_df.Parameter == "Annual_Income_Score2").select("Value").collect()[0][0])
Annual_Income_Score3 = int(config_df.filter(config_df.Parameter == "Annual_Income_Score3").select("Value").collect()[0][0])
Annual_Income_Score4 = int(config_df.filter(config_df.Parameter == "Annual_Income_Score4").select("Value").collect()[0][0])
Credit_Limit_Multiplier = int(config_df.filter(config_df.Parameter == "Credit_Limit_Multiplier").select("Value").collect()[0][0])
Month_EMI_Percentage_3 = config_df.filter(config_df.Parameter == "Month_EMI_Percentage_3").select("Value").collect()[0][0]
Month_EMI_Percentage_6 = config_df.filter(config_df.Parameter == "Month_EMI_Percentage_6").select("Value").collect()[0][0]
Month_EMI_Percentage_9 = config_df.filter(config_df.Parameter == "Month_EMI_Percentage_9").select("Value").collect()[0][0]
Inter_Table_Path = config_df.filter(config_df.Parameter == "Inter_Table_Path").select("Value").collect()[0][0]
DatabaseName = config_df.filter(config_df.Parameter == "DatabaseName").select("Value").collect()[0][0]
Date_Format_trn = config_df.filter(config_df.Parameter == "Date_Format_trn").select("Value").collect()[0][0]
Cred_Notify_Threshold = config_df.filter(config_df.Parameter == "Cred_Notify_Threshold").select("Value").collect()[0][0]


os.environ['FileInputPath'] = FileInputPath
os.environ['TempFilePath'] = TempFilePath
os.environ['OutputFilePath'] = OutputFilePath
os.environ['RejectFilePath'] = RejectFilePath
os.environ['TransactionOPPath'] = TransactionOPPath
os.environ['Annual_Income_Limit1'] = str(Annual_Income_Limit1)
os.environ['Annual_Income_Limit2'] = str(Annual_Income_Limit2)
os.environ['Annual_Income_Limit3'] = str(Annual_Income_Limit3)
os.environ['Annual_Income_Score1'] = str(Annual_Income_Score1)
os.environ['Annual_Income_Score2'] = str(Annual_Income_Score2)
os.environ['Annual_Income_Score3'] = str(Annual_Income_Score3)
os.environ['Annual_Income_Score4'] = str(Annual_Income_Score4)
os.environ['Credit_Limit_Multiplier'] = str(Credit_Limit_Multiplier)
os.environ['Month_EMI_Percentage_3'] = Month_EMI_Percentage_3
os.environ['Month_EMI_Percentage_6'] = Month_EMI_Percentage_6
os.environ['Month_EMI_Percentage_9'] = Month_EMI_Percentage_9
os.environ['Inter_Table_Path'] = Inter_Table_Path
os.environ['DatabaseName'] = DatabaseName
os.environ['Date_Format_trn'] = Date_Format_trn
os.environ['Cred_Notify_Threshold'] = Cred_Notify_Threshold



spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '/user/itv006343/'".format(DatabaseName))

print("Database created successfully")


############################################# Execute Customer Information Data Ingestion #############################################


return_code_cust = subprocess.call(["spark-submit", "Banking4_DataIngestion_CustomerDetail.py"])

if return_code_cust != 0:
    print("Error running Customer Ingestion and Validation script. Return code:{}".format(return_code_cust))
else:
    print("Customer Ingestion and Validation Script executed successfully")



############################################# Execute Account Information Data Ingestion ##############################################

if return_code_cust == 0:
    

    return_code_acct = subprocess.call(["spark-submit", "Banking4_DataIngestion_AccountDetail.py"])

    if return_code_acct != 0:
        print("Error running Account Ingestion and Validation script. Return code:{}".format(return_code_acct))
    else:
        print("Account Ingestion and Validation Script executed successfully")



############################################# Execute Transaction Information Data Ingestion #############################################

if return_code_acct == 0:

    return_code_trans = subprocess.call(["spark-submit", "Banking4_DataIngestion_TransactionDetail.py"])

    if return_code_trans != 0:
        print("Error running Transaction Ingestion and Validation script. Return code:{}".format(return_code_trans))
    else:
        print("Transaction Ingestion and Validation Script executed successfully")



############################################# Execute Data Transform process #############################################

if return_code_cust == 0 and return_code_acct == 0 and return_code_trans == 0:
    

    return_code_transform = subprocess.call(["spark-submit", "Banking4_DataTransform.py"])

    if return_code_transform != 0:
        print("Error running Data Transformation script. Return code:{}".format(return_code_transform))
    else:
        print("Data Transformation Script executed successfully")


############################################# Execute Data Reporting process #############################################


if return_code_transform == 0:
    
    return_code_report = subprocess.call(["spark-submit", "Banking4_DataReporting.py"])

    if return_code_report != 0:
        print("Error running Data Reporting script. Return code:{}".format(return_code_report))
    else:
        print("Data Reporting Script executed successfully")
