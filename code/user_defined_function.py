from pyspark.sql import SparkSession
from pyspark.functions import udf,col
from pyspark.sql.types import StringType

def convert_case(str):
    return str.title()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("app").master("local").getOrCreate()
    data = [(1,'manish kumar'),(2, "avnish yadav"),(3, "mohit chaudhary")]

    df = spark.createDataFrame(data=data,schema=['id','name'])
    df.show()