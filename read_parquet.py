from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()
df = spark.read.parquet("/opt/spark/data/avg_temp")
print("Data from parquet file:")
df.show()
spark.stop()
