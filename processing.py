from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import udf
import text_processing
from dotenv import load_dotenv
import os
from pyspark.sql.functions import monotonically_increasing_id
load_dotenv()

while True:
    sentiment_udf = udf(text_processing.get_sentiment, StringType())
    snowflake_options = {
        "sfURL": "https://"+os.getenv("SNOW_ACCOUNT")+".snowflakecomputing.com",
        "sfUser": "SNOW_USER",
        "sfPassword": os.getenv("SNOW_PSW"),
        "sfDatabase": "NEWSDB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "SNOW_WH",
        "sfRole":"USER_ROLE"
    }
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("NewsStreaming") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0–10_2.12:3.4.3') \
        .config("spark.jars", "spark-sql-kafka-0-10_2.12-3.4.3.jar") \
        .config("spark.snowflake.url", snowflake_options["sfURL"]) \
        .config("spark.snowflake.user", snowflake_options["sfUser"]) \
        .config("spark.snowflake.password", snowflake_options["sfPassword"]) \
        .config("spark.snowflake.database", snowflake_options["sfDatabase"]) \
        .config("spark.snowflake.schema", snowflake_options["sfSchema"]) \
        .config("spark.snowflake.warehouse", snowflake_options["sfWarehouse"]) \
        .config("spark.snowflake.role", snowflake_options["sfRole"]) \
        .getOrCreate()

    # Kafka parameters
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "guardian,npr,wpost,bbc,nbc,cnn"
    kafka_group_id = "group2"



    # schema for the incoming JSON data
    schema = StructType() \
        .add("source", StringType()) \
        .add("article", StringType()) \
        .add("category",StringType()) \
        .add("content", StringType())



    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("group.id", kafka_group_id) \
        .load()


    df = df.withColumn("value", df["value"].cast("string"))


    parsed_df = df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("sentiment", sentiment_udf(col("content"))) \

    parsed_df_with_id=parsed_df.withColumn("ID", monotonically_increasing_id())
    parsed_df=parsed_df_with_id.select("ID", *parsed_df.columns)

    parsed_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "NEWS") \
        .mode("append") \
        .save()

