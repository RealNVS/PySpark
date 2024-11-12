from pathlib import Path
import os
from pyspark.sql import SparkSession

# Set environment variables for Spark
os.environ['SPARK_HOME'] = str(Path("C:/spark-3.5.3-bin-hadoop3"))
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'lab'
os.environ['PYSPARK_PYTHON'] = 'python'

spark=''
if 'spark' not in locals():
    spark = SparkSession.builder \
        .appName("MySparkApplication") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

else:
    print("Spark session already exists.")
print(spark)
spark.stop()
