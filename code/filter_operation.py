from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext


# Basic Operations

import os


# user_df = spark.read.csv(path=f"/user/{os.environ['USER']}/u1.user", sep="|", header=True)

user_df = spark.read.csv(path=f"../data/u1.user", sep="|", header=True)

user_df.show(n=60, truncate=False)

user_df.select("id","designation").show()

from pyspark.sql.functions import *  #Importing all the functions 
user_df.select(col("id"), col("designation").alias("desig"))
df1 = user_df.filter(col("designation") == 'technician')
df1.show()


df2 = user_df.filter(col("designation") != 'technician')
df2.show()


df3 = user_df.filter(col("designation").isin(['technician','executive']))
df3.show()

