from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Hello-world").master("local[*]").getOrCreate()

print(spark.version)