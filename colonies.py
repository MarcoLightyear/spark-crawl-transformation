# 1. Vertices table
# id    |   value(targetURI)
# 2. Edges table
# id    |   vertice |   link(hrefValue)
import sys
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from warcio.archiveiterator import ArchiveIterator


if len(sys.argv) == 1:
    print("Please provide the input path of the WARC file.")

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

vertices = []
edges = []

spark.sparkContext.addFile(input_file)

with open(SparkFiles.get('CC-MAIN-20240220211055-20240221001055-00000.warc'), 'rb') as stream:
    verticesId = 1
    edgesId = 1
    pattern = re.compile(b'href="([^"]+)"')
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            targetURI = record.rec_headers.get_header('WARC-Target-URI')
            vertices.append((verticesId, targetURI))
            htmlContent = record.content_stream().read()
            foundEdges = pattern.findall(htmlContent)
            for edge in foundEdges:
                edges.append(((edgesId, verticesId, edge.decode('iso-8859-1'))))
                edgesId += 1
            verticesId += 1

verticesDataframe = spark.createDataFrame(
    vertices, verticesSchema).write \
         .format('bigquery') \
            .option("writeMethod", "direct") \
            .option('table', 'data-analysis-upmx-holding:clean_common_crawl_upmx.vertices') \
            .save()  # TODO: save this into BigQuery

edgesDataframe = spark.createDataFrame(
    edges, edgesSchema).write \
         .format('bigquery') \
            .option("writeMethod", "direct") \
            .option('table', 'data-analysis-upmx-holding:clean_common_crawl_upmx.edges') \
            .save()