import os
os.environ['SPARK_HOME'] = "/Users/user/Documents/spark_home"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExampleSparkApp") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

numbers = [10, 20, 30, 40, 50]
rdd = spark.sparkContext.parallelize(numbers)

a = rdd.collect()
print("Elements in numbers RDD:", a)
data = [("Alice", 22), ("Bob", 28), ("Charlie", 34), ("David", 42)]
rdd = spark.sparkContext.parallelize(data)

print("All elements of the data RDD:", rdd.collect())

count = rdd.count()
print("The total number of elements in data RDD:", count)

first_element = rdd.first()
print("First element of data RDD:", first_element)

rdd.foreach(lambda x: print(x))

spark.stop()
