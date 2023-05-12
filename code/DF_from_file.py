from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext


# Create DataFrame from File
import os

dfr = spark.read
# user_df = dfr.csv(path=f"/user/{os.environ['USER']}/u1.user", sep="|", header=True)
user_df = dfr.csv(path=f"../data/u1.user", sep="|", header=True)

user_df.show(n=60, truncate=False)

# Convert RDD to DataFrame
employee_records = [(1,"Naveen"), (2,"Nikshay"), (3,"Kishore")]
rdd = spark.sparkContext.parallelize(employee_records)


df1 = rdd.toDF()


columns = ["id", "name"]
df2 = rdd.toDF(columns)
df2.show()

