from pyspark.sql import SparkSession


if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("app").master("local").getOrCreate()

    print("------PERMISSIVE MODE-----")
    df = spark.read.option("columnNameOfCorruptRecord","Rejectedrecords").json(path=f"../data/access_logs.json")
    df.show(truncate=False)

    print("--------DROPMALFORMED MODE---------")
    df = spark.read.json(path = "../data/access_logs.json", mode="DROPMALFORMED")
    df.show(truncate=False)

    print("---------FALFAST MODE--------------")
    df = spark.read.json(path = "../data/access_logs.json", mode="FATLAST")
    df.show(truncate=False)
