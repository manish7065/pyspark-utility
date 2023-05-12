from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext


import os
from pyspark.sql.functions import *
# user_df = spark.read.csv(path=f"/user/{os.environ['USER']}/u1.user", sep="|", header=True, inferSchema=True)
user_df = spark.read.csv(path=f"../data/u1.user", sep="|", header=True, inferSchema=True)


result_df = user_df.withColumn("category",lit('Default Value'))
result_df.show()


result_df = user_df.withColumn("category", when(col('salary') < 50000, "exploited"))
result_df.show()


result_df = user_df.withColumn("category", when(col('salary') < 50000, "Low Salary").when((col('salary') >= 50000) & (col('salary') < 100000), "Good Salary").otherwise("High Salary"))
result_df.show()


result_df.groupBy(col("category")).count().show()   