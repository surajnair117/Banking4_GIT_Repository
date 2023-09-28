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
from pyspark.sql.functions import upper
from pyspark.sql.window import Window
import os


################################################# Import CONFIG file ###############################################################


FileInputPath = os.environ.get('FileInputPath')
TempFilePath = os.environ.get('TempFilePath')
OutputFilePath = os.environ.get('OutputFilePath')
RejectFilePath = os.environ.get('RejectFilePath')
Inter_Table_Path = os.environ.get('Inter_Table_Path')
DatabaseName = os.environ.get('DatabaseName')
Cred_Notify_Threshold = os.environ.get('Cred_Notify_Threshold')


############################################ Geography Summary View ################################################################

spark.sql("""
create or replace view {}.Geography_Summary_View as
select 
country, 
state, 
count(Cust_Id), 
avg(Annual_Income), 
max(Credit_Score), 
min(Credit_Score) 
from {}.SIMPLE_PAY_INTERMEDIATE_PAR 
group by country, state 
order by country, state
""".format(DatabaseName,DatabaseName))


############################################ EMI Offer Eligible ################################################################


spark.sql("""
create or replace view {}.SimplePay_EMI_Notify as
select 
FIRST_NAME, LAST_NAME, Cust_Id, Account_Num,
Card_Num, Transaction_Dt, Transaction_amount, Transaction_Purpose, 
EMI_3_month, (EMI_3_month * 3) as PayAfter3month,
EMI_6_month, (EMI_6_month * 6) as PayAfter6month,
EMI_9_month, (EMI_9_month * 9) as PayAfter9month
from {}.SIMPLE_PAY_INTERMEDIATE_PAR 
where EMI_Eligible = 'Y'
""".format(DatabaseName,DatabaseName))


print("Users who should be notified with EMI options and interest rates:")
spark.sql("select * from {}.SimplePay_EMI_Notify".format(DatabaseName)).show()


############################################ Previous Transaction ################################################################

spark.sql("""
create or replace view {}.Previous_Transaction_View as

WITH Aggregated AS (
    SELECT Cust_Id, Card_Num, YEAR, MONTH, FIRST_NAME, LAST_NAME,
           SUM(Transaction_amount) as Latest_Transaction
    FROM {}.SIMPLE_PAY_INTERMEDIATE_PAR
    GROUP BY Cust_Id, Card_Num, YEAR, MONTH, FIRST_NAME, LAST_NAME
)

select Cust_Id,YEAR, MONTH,FIRST_NAME, LAST_NAME, 
Latest_Transaction,
lag(Latest_Transaction) OVER (PARTITION BY Cust_Id, YEAR,MONTH order By YEAR, MONTH desc) as PREVIOUS_TRANSACTION
from Aggregated
order By YEAR, Cust_Id

""".format(DatabaseName,DatabaseName))


############################################ Credit Limit Threshold Notification ################################################################

spark.sql("""
create or replace view {}.Credit_Limit_Threshold_Notify as

with cred_limit_expire as (

select FIRST_NAME, LAST_NAME, COUNTRY,Cust_credit_limit_remaining,Credit_Limit,
(Cust_credit_limit_remaining/Credit_Limit) * 100 as Ratio

FROM {}.SIMPLE_PAY_INTERMEDIATE_PAR 

)

select FIRST_NAME, LAST_NAME, COUNTRY,Cust_credit_limit_remaining, Credit_Limit as Total_Credit_Limit, Ratio
from cred_limit_expire
where Ratio <= {}
""".format(DatabaseName,DatabaseName,Cred_Notify_Threshold))

print("Users whose credit limit threshold is nearing completion: ")
spark.sql("select * from {}.Credit_Limit_Threshold_Notify".format(DatabaseName)).show()

print("Data Report View generation is complete!!")