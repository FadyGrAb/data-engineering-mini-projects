from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from time import sleep

spark = (
    SparkSession.builder.appName("demo")
    .config("spark.eventLog.enabled", True)
    .getOrCreate()
)

df = spark.createDataFrame(
    [("fady", 38), ("rasha", 38), ("maria", 12)], ["first_name", "age"]
)

df.select(avg("age")).show()
sleep(5)
