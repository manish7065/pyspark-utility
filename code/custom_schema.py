from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()


#Custom Schema---------------------------------------------------------
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


user_schema = StructType([
StructField("user_id", IntegerType()),
StructField("age", IntegerType()),
StructField("gender", StringType()),
StructField("designation", StringType()),
StructField("salary", IntegerType()),
])


# user_df = spark.read.csv(path=f"/user/{os.environ['USER']}/u1.user", sep="|", header=True, schema=user_schema)
user_df = spark.read.csv(path = f"../data/u2.user", sep="|", header=False,  schema=user_schema)


result_df = user_df.filter(col('designation')=='other')
result_df.show()
result_df.write.csv(
path=f"../data/output_21_02", 
header=True,
mode="overwrite",sep="|")
# path=f"/user/{os.environ['USER']}/output_21_02", 