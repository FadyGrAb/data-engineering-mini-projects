from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark HDFS Example").getOrCreate()

hdfs_file = "hdfs://hadoop-namenode:8020/user/hadoop/hello.txt"

df = spark.read.text(hdfs_file)

df.show()
