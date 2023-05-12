from pyspark.sql import SparkSession


if __name__ == '__main__':
   # Step 01 - Create Spark Session
   spark = SparkSession.builder.appName("Spark Demo").master("local").getOrCreate()
   # Step 02 : Create Spark Context
   sc = spark.sparkContext
   # Step 03 : Create RDD by loading the file
#    user_rdd = sc.textFile("../dataset/user.csv")
   user_rdd = sc.textFile("../data/user.csv")



   # Print Top 5 records
   for record in user_rdd.take(50):
       print(record)