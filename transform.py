import sys
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark Warc Parser") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.26.0.jar") \
    .getOrCreate()

if len(sys.argv) == 1:
    print("Please provide a dataset name.")

warcFile=sys.argv[1]

path = f"gs://raw-common-crawl-upmx/WARC-2024/{warcFile}"

warcDataFrame = spark.read.text(path,lineSep="\n")

for record in warcDataFrame:
    print(record)
    break


