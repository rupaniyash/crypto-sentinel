from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
INPUT_TOPIC = "crypto_trades"
OUTPUT_PATH = "/opt/spark_data/storage"
CHECKPOINT_PATH = "/opt/spark_data/checkpoint"

spark = SparkSession.builder \
    .appName("CryptoSentinelLake") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("side", StringType()) \
    .add("timestamp", TimestampType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

processed_df = parsed_df.withColumn("processing_time", current_timestamp())

query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime='10 seconds') \
    .start()

print(f"🚀 Data Lake Active! Writing Parquet to {OUTPUT_PATH}")
query.awaitTermination()