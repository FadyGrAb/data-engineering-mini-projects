from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp

spark = SparkSession.builder.appName("Serving").getOrCreate()

hdfs_url = "hdfs://hadoop-namenode:8020/data"

spark.read.parquet(f"{hdfs_url}/silver/allorders").createOrReplaceTempView("allorders")

spark.sql(
    """
    SELECT
        t1.orderid,
        t1.historyid,
        t1.branch,
        t1.ordertime AS start,
        t2.updatedat AS end,
        (unix_timestamp(end) - unix_timestamp(start)) / 3600 AS deliverytime
    FROM (SELECT * FROM allorders WHERE status = 'NEW') AS t1
    INNER JOIN (SELECT * FROM allorders WHERE status = 'DELIVERED') AS t2
    ON t1.orderid = t2.orderid
    """
).createOrReplaceTempView("allorders_pivot")

spark.sql(
    """
    SELECT branch, AVG(deliverytime) AS avg_deliverytime
    FROM allorders_pivot
    GROUP BY branch
    ORDER BY 2 ASC
    """
).write.mode("overwrite").csv(f"{hdfs_url}/gold/deliveriesavg", header=True)


spark.sql(
    """
    SELECT branch, COUNT(*) AS deliveries_count
    FROM allorders_pivot
    GROUP BY branch
    ORDER BY 2 DESC
    """
).write.mode("overwrite").csv(f"{hdfs_url}/gold/deliveriescount", header=True)

spark.stop()
