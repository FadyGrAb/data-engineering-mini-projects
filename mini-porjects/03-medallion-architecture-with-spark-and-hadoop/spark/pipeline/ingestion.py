import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max as col_max


def get_bookmark(stage: str) -> str:
    bookmark_path = f"{hdfs_url}/data/{stage}/bookmark/"
    try:
        df = spark.read.json(bookmark_path)
        json_str = df.toJSON().first()
        return json.loads(json_str)["lastUpdate"]
    except Exception as e:
        print(e)
        return ""


def set_bookmark(stage: str, last_update: str) -> str:
    bookmark_path = f"{hdfs_url}/data/{stage}/bookmark/"
    json_str = json.dumps({"lastUpdate": last_update})
    json_df = spark.read.json(spark.sparkContext.parallelize([json_str]))
    json_df.write.mode("overwrite").json(bookmark_path)


spark = (
    SparkSession.builder.appName("Ingestion")
    .config("spark.jars", "/spark/jars/postgresql-jdbc.jar")
    .getOrCreate()
)

db_username = os.environ.get("DB_USERNAME")
db_password = os.environ.get("DB_PASSWORD")

jdbc_url = "jdbc:postgresql://postgres:5432/retail"
hdfs_url = "hdfs://hadoop-namenode:8020"

connection_params = {
    "user": db_username,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

# Get bookmark
bookmark = get_bookmark("bronze")

if bookmark:
    orders_query = f"(SELECT * FROM orders WHERE ordertime > '{bookmark}') AS orders"
    orders_hist_query = (
        f"(SELECT * FROM ordershistory WHERE updatedat > '{bookmark}') AS ordershistory"
    )
else:
    orders_query = "orders"
    orders_hist_query = "ordershistory"


# orders table
(
    spark.read.jdbc(url=jdbc_url, table="orders", properties=connection_params)
    .withColumn("update", to_date(col("ordertime")))
    .write.mode("append")
    .partitionBy("update")
    .parquet("hdfs://hadoop-namenode:8020/data/bronze/orders")
)

# ordershistory table
ordershistory_df = spark.read.jdbc(
    url=jdbc_url, table=orders_hist_query, properties=connection_params
)
(
    ordershistory_df.withColumn("update", to_date(col("updatedat")))
    .write.mode("append")
    .partitionBy("update")
    .parquet("hdfs://hadoop-namenode:8020/data/bronze/ordershistory")
)

# Set bookmark
last_update = ordershistory_df.select(col_max("updatedat")).first()[0]
if last_update:
    last_update = last_update.strftime("%Y-%m-%d %H:%M:%S")
    set_bookmark("bronze", last_update)

spark.stop()
