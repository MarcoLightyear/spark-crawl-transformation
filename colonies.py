# 1. Vertices table
# id    |   value(targetURI)
# 2. Edges table
# id    |   vertice |   link(hrefValue)
import sys
import re
import time
import csv
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from warcio.archiveiterator import ArchiveIterator
from google.cloud import logging

logging_client = logging.Client()
logger = logging_client.logger('colonies-py')
start = time.time()
logger.log_text("Initiating script...", severity="DEBUG")

if len(sys.argv) == 1:
    print("Please provide the input paths file.")

input_file = sys.argv[1]

spark = SparkSession.builder \
    .appName("colonies-app") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

verticesSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("targetURI", StringType(), True)
])

edgesSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("targetURIId", StringType(), True),
    StructField("edge", StringType(), True)
])

verticesId = 1
edgesId = 1
spark.sparkContext.addFile(input_file)
with open(SparkFiles.get('input_warc_paths.csv'), mode='r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        vertices = []
        edges = []
        path = row['path']
        filename = row['filename']
        logger.log_text(f"Adding file...{filename}", severity="DEBUG")
        spark.sparkContext.addFile(path)
        logger.log_text("File added...", severity="DEBUG")
        with open(SparkFiles.get(filename), 'rb') as stream:
            pattern = re.compile(b'href="((?:http|https)://[^/"]+)')
            targetURIPattern = re.compile(r'(https?:\/\/[^\/\?\#]+)')
            previousTargetURI = ''
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    targetURI = record.rec_headers.get_header(
                        'WARC-Target-URI')
                    if(previousTargetURI != targetURIPattern.search(targetURI).group(1)):
                        vertices.append((verticesId, targetURIPattern.search(targetURI).group(1)))
                        previousTargetURI = targetURIPattern.search(targetURI).group(1)
                    htmlContent = record.content_stream().read()
                    foundEdges = pattern.findall(htmlContent)
                    for edge in foundEdges:
                        if (edge.decode('iso-8859-1') not in targetURI):
                            edges.append(
                                ((edgesId, verticesId, edge.decode('iso-8859-1'))))
                            edgesId += 1
                    verticesId += 1

        end = time.time()
        logger.log_text(f"File processed in: {end-start}", severity="DEBUG")

        verticesDataframe = spark.createDataFrame(
            vertices, verticesSchema).write \
            .format('bigquery') \
            .option("writeMethod", "direct") \
            .option('table', 'data-analysis-upmx-holding:clean_common_crawl_upmx.vertices_v3') \
            .mode('append') \
            .save()

        edgesDataframe = spark.createDataFrame(
            edges, edgesSchema).write \
            .format('bigquery') \
            .option("writeMethod", "direct") \
            .option('table', 'data-analysis-upmx-holding:clean_common_crawl_upmx.edges_v3') \
            .mode('append') \
            .save()

        end = time.time()
        logger.log_text(f"Job for file: {filename} ended in: {end-start}", severity="DEBUG")
