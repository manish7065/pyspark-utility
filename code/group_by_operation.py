from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext

import os
from pyspark.sql.functions import *

# Creating DataFrame

# user_df = spark.read.csv(path="../data/u1.user", sep="|", inferSchema=True)
user_df = spark.read.csv(path=f"../data/u1.user", sep="|", header=True, inferSchema=True)


result_df = user_df.withColumn("category", when(col("salary") < 50000, "low salary").when((col("salary") >= 50000) & (col("salary") < 100000), "good salary").otherwise("high salary"))
# result_df = user_df.withColumn("category", when(col('salary') < 50000, "Low Salary").when((col('salary') >= 50000) & (col('salary') < 100000), "Good Salary").otherwise("High Salary"))

result_df.show()
result_df.printSchema()

# Group By-----------------------------------------------------------------
result_df.groupBy(col("category")).agg(
min('salary').alias('minimum_salary'),
max('salary').alias('maximum_salary'),
avg('salary').alias('average_salary'),
).show()

result_df.groupBy(col("category")).agg(
min('salary').alias('minimum_salary'),
max('salary').alias('maximum_salary'),
avg('salary').alias('average_salary'),
).sort(col('maximum_salary'), ascending=False).show()

# * 50000 > salary ⇒ Low Salary
# * 50000 <= salary < 100000 ⇒ Good Salary
# * 100000 < salary ⇒ High Salary
