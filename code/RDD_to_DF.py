from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext

# Convert RDD to DataFrame
employee_records = [(1,"Naveen"), (2,"Nikshay"), (3,"Kishore")]
rdd = spark.sparkContext.parallelize(employee_records)


df1 = rdd.toDF()


columns = ["id", "name"]
df2 = rdd.toDF(columns)
df2.show()
df2.printSchema()

df3 = spark.createDataFrame(rdd, schema=columns)
df3.show()
df3.printSchema()
