from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("app").master("local").getOrCreate()
    spark.read.csv(path = "..data/employee-missing-records.dat", header=True, inferSchema=True)

    df.printSchema()
    df.show()
    df.na.drop().show()

    df.na.drop(subset=['col_name']).show()


    df.na.fill("NULL IN SOURCE")
