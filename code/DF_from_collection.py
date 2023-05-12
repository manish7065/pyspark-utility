from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext

#    Create DataFrame from Collection

employee_records = [(1,"Naveen"), (2,"Nikshay"), (3,"Kishore")]
columns = ["id", "name"]
employee_df = spark.createDataFrame(employee_records).toDF(*columns)
employee_df.show()
employee_df.printSchema()


