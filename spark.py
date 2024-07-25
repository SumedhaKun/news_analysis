from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf
import text_processing

seniment_udf = udf(text_processing.get_sentiment, StringType())

# Create SparkSession
spark = SparkSession.builder \
    .appName("NewsStreaming") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "npr"
kafka_group_id = "group2"

# Define schema for the incoming JSON data
schema = StructType() \
    .add("source", StringType()) \
    .add("article", StringType()) \
    .add("category",StringType()) \
    .add("content", StringType())

# Read from Kafka using Spark Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("group.id", kafka_group_id) \
    .load()

# Convert the value column from binary to string
df = df.withColumn("value", df["value"].cast("string"))

# Parse JSON data from 'value' column into a DataFrame
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment", seniment_udf(col("content")))

# Output the parsed DataFrame to a Spark SQL table
parsed_df \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()



# Start the streaming query
spark.streams.awaitAnyTermination()
parsed_df.show(10)
