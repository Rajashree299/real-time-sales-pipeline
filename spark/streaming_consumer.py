"""
Spark Structured Streaming Consumer for Sales Events
=====================================================

Consumes, validates, and writes streaming sales data to S3 (Bronze layer).
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    # Configuration (In production, load from config.py)
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = "sales_events"
    s3_path = "s3a://sales-data-lake/bronze/sales_events/"
    checkpoint_path = "s3a://sales-data-lake/checkpoints/sales_streaming/"

    spark = SparkSession.builder \
        .appName("SalesStreamingConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema Enforcement
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("country", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_source", StringType(), True)
    ])

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Data Validation
    valid_df = parsed_df.filter(
        (col("price") > 0) & 
        (col("quantity") > 0) & 
        (col("order_id").isNotNull()) &
        (col("customer_id").isNotNull())
    )

    # Add Ingestion Metadata & Partitioning Columns
    final_df = valid_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("order_ts", to_timestamp(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("year", year(col("order_ts"))) \
        .withColumn("month", month(col("order_ts"))) \
        .withColumn("day", dayofmonth(col("order_ts")))

    # Write to Bronze Layer (Parquet)
    query = final_df.writeStream \
        .format("parquet") \
        .option("path", s3_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
