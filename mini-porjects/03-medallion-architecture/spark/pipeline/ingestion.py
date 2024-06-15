from pyspark.sql import SparkSession

spark = SparkSession \
            .builder \
            .appName("Ingestion") \
            .config("spark.jars", "/spark/jars/postgresql-jdbc.jar") \
            .getOrCreate()

# Postgres JDBC connection string
postgres_jdbc = "jdbc:postgresql://postgres:5432/retail"