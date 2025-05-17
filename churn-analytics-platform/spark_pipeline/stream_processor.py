import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"  # Ensure this is set (even if winutils is only used for Windows permissions)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Define schema for incoming Kafka messages
schema = StructType() \
    .add("customer_id", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", DoubleType())

spark = (
    SparkSession.builder
    .appName("KafkaSparkStructuredStreaming")
    .config("spark.sql.streaming.checkpointLocation", "file:///C:/temp/spark-checkpoint")
    .config("spark.hadoop.io.nativeio.enabled", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
    .getOrCreate()
)


# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "customer_events") \
    .load()

# Parse JSON from Kafka value
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Example: basic transformation - count events per customer (can be expanded)
df_features = df_parsed.groupBy("customer_id", "event_type").count()

# Output to console for now
query = df_features.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "file:///C:/temp/spark-checkpoint") \
    .start()


query.awaitTermination()
