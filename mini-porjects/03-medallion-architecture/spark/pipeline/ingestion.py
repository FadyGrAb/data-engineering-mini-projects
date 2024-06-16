from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import os

spark = (
    SparkSession.builder.appName("Ingestion")
    .config("spark.jars", "/spark/jars/postgresql-jdbc.jar")
    .getOrCreate()
)

db_username = os.environ.get("DB_USERNAME")
db_password = os.environ.get("DB_PASSWORD")

jdbc_url = "jdbc:postgresql://postgres:5432/retail"

connection_params = {
    "user": db_username,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

orders_df = (
    spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_params)
    .withColumn("update", to_date(col("ordertime")))
    .write.mode("overwrite")
    .partitionBy("update")
    .parquet("hdfs://hadoop-namenode:8020/data/bronze/orders")
)

orders_history_df = (
    spark.read.jdbc(url=jdbc_url, table="ordershistory", properties=connection_params)
    .withColumn("update", to_date(col("updatedat")))
    .write.mode("overwrite")
    .partitionBy("update")
    .parquet("hdfs://hadoop-namenode:8020/data/bronze/ordershistory")
)

spark.stop()
