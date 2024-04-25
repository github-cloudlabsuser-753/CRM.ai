from pyspark.sql import SparkSession
from datetime import datetimefrom
from ingestor import CosmosDBIngestor


spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .getOrCreate()

ingestor = CosmosDBIngestor()


if __name__ == "__main__":
    df = spark.read.csv("<path>", header=True)

    df.foreach(ingestor.ingest_call_transcript)

    spark.stop()



