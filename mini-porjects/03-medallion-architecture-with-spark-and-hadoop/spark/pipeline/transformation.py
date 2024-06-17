import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as col_max

spark = SparkSession.builder.appName("Transformation").getOrCreate()

hdfs_url = "hdfs://hadoop-namenode:8020/data"

orders_df = spark.read.parquet(f"{hdfs_url}/bronze/orders").createOrReplaceTempView(
    "orders"
)
ordershistory_df = spark.read.parquet(
    f"{hdfs_url}/bronze/ordershistory"
).createOrReplaceTempView("ordershistory")

joined_df = spark.sql(
    """
        SELECT
            o.orderid,
            ordertime,
            branch,
            historyid,
            status,
            updatedat,
            oh.update
        FROM orders AS o
        INNER JOIN ordershistory AS oh
            ON o.orderid = oh.orderid
        WHERE
            status != "INTRANSIT"
    """
)

(
    joined_df.write.mode("overwrite")
    .partitionBy("update")
    .parquet(f"{hdfs_url}/silver/allorders")
)

spark.stop()
