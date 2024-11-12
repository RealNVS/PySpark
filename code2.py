from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Hello-world").master("local[*]").getOrCreate()

df = spark.read.option("header", True).csv("data/AAPL.csv")
df.show()
df.printSchema()

df.close()