from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("test").getOrCreate()

print("=================================Testing==============================")
time.sleep(60)

spark.close()
