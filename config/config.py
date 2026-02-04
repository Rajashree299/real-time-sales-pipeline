"""
Configuration Module for Real-Time Sales Data Pipeline
======================================================

This module centralizes all configuration settings for the data pipeline,
including Kafka, S3, Redshift, and Spark configurations.

Author: Data Engineering Team
Version: 1.0.0
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """
    Kafka configuration settings.
    
    In production, these values should be loaded from environment variables
    or a secrets manager like AWS Secrets Manager.
    """
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name: str = os.getenv("KAFKA_TOPIC", "sales_events")
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP", "sales_consumer_group")
    
    # Producer settings
    producer_acks: str = "all"  # Wait for all replicas to acknowledge
    producer_retries: int = 3
    producer_batch_size: int = 16384  # 16KB batch size
    producer_linger_ms: int = 10  # Wait up to 10ms for batching
    
    # Consumer settings
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False  # Manual commit for exactly-once semantics
    
    # Security (for production)
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism: Optional[str] = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = os.getenv("KAFKA_SASL_PASSWORD")


@dataclass
class S3Config:
    """
    AWS S3 configuration for data lake layers.
    
    Data Lake Architecture:
    - Bronze: Raw data as ingested (immutable)
    - Silver: Cleaned and validated data
    - Gold: Aggregated, business-ready data
    """
    bucket_name: str = os.getenv("S3_BUCKET", "sales-data-lake")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    
    # Layer paths following medallion architecture
    bronze_path: str = "bronze/sales_events/"
    silver_path: str = "silver/cleaned_transactions/"
    gold_path: str = "gold/aggregations/"
    
    # Dead letter queue for malformed records
    dlq_path: str = "dlq/malformed_records/"
    
    # Checkpoint location for Spark Streaming
    checkpoint_path: str = "checkpoints/streaming/"
    
    @property
    def bronze_s3_path(self) -> str:
        return f"s3://{self.bucket_name}/{self.bronze_path}"
    
    @property
    def silver_s3_path(self) -> str:
        return f"s3://{self.bucket_name}/{self.silver_path}"
    
    @property
    def gold_s3_path(self) -> str:
        return f"s3://{self.bucket_name}/{self.gold_path}"
    
    @property
    def dlq_s3_path(self) -> str:
        return f"s3://{self.bucket_name}/{self.dlq_path}"
    
    @property
    def checkpoint_s3_path(self) -> str:
        return f"s3://{self.bucket_name}/{self.checkpoint_path}"


@dataclass
class RedshiftConfig:
    """
    Amazon Redshift configuration for data warehouse.
    
    Connection details should always be loaded from environment variables
    or AWS Secrets Manager in production.
    """
    host: str = os.getenv("REDSHIFT_HOST", "your-cluster.region.redshift.amazonaws.com")
    port: int = int(os.getenv("REDSHIFT_PORT", "5439"))
    database: str = os.getenv("REDSHIFT_DATABASE", "sales_dw")
    username: str = os.getenv("REDSHIFT_USERNAME", "admin")
    password: str = os.getenv("REDSHIFT_PASSWORD", "")
    
    # IAM role for COPY command
    iam_role: str = os.getenv("REDSHIFT_IAM_ROLE", "arn:aws:iam::ACCOUNT_ID:role/RedshiftS3Role")
    
    # Schema names
    schema_name: str = "sales"
    staging_schema: str = "staging"
    
    @property
    def connection_string(self) -> str:
        """Generate JDBC connection string for Spark."""
        return f"jdbc:redshift://{self.host}:{self.port}/{self.database}"
    
    @property
    def psycopg2_connection_params(self) -> dict:
        """Generate connection parameters for psycopg2."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.username,
            "password": self.password
        }


@dataclass
class SparkConfig:
    """
    Apache Spark configuration for streaming and batch processing.
    """
    app_name: str = "SalesDataPipeline"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    
    # Streaming settings
    trigger_interval: str = "30 seconds"  # Micro-batch interval
    watermark_delay: str = "1 hour"  # Late data handling
    
    # Performance tuning
    shuffle_partitions: int = 200
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    executor_cores: int = 4
    
    # Kafka integration
    kafka_starting_offsets: str = "earliest"
    kafka_max_offsets_per_trigger: int = 100000
    
    # Output settings
    output_mode: str = "append"
    output_format: str = "parquet"
    compression: str = "snappy"


@dataclass
class DataQualityConfig:
    """
    Data quality thresholds and settings.
    
    These thresholds define acceptable data quality levels.
    Violations should trigger alerts in production.
    """
    # Maximum allowed null percentage for critical columns
    max_null_percentage: float = 0.01  # 1%
    
    # Maximum allowed duplicate percentage
    max_duplicate_percentage: float = 0.001  # 0.1%
    
    # Row count variance tolerance between layers
    row_count_tolerance: float = 0.05  # 5% variance allowed
    
    # Data freshness threshold (in hours)
    max_data_age_hours: int = 24
    
    # Critical columns that must not have nulls
    critical_columns: tuple = (
        "order_id",
        "customer_id",
        "product_id",
        "price",
        "quantity",
        "order_timestamp"
    )
    
    # Columns with numeric constraints
    positive_value_columns: tuple = ("price", "quantity")


# Singleton instances for easy import
kafka_config = KafkaConfig()
s3_config = S3Config()
redshift_config = RedshiftConfig()
spark_config = SparkConfig()
data_quality_config = DataQualityConfig()


def get_spark_session():
    """
    Create and return a configured Spark session.
    
    Returns:
        SparkSession: Configured Spark session with necessary packages
    """
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder
        .appName(spark_config.app_name)
        .master(spark_config.master)
        .config("spark.sql.shuffle.partitions", spark_config.shuffle_partitions)
        .config("spark.executor.memory", spark_config.executor_memory)
        .config("spark.driver.memory", spark_config.driver_memory)
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.compression.codec", spark_config.compression)
        .getOrCreate())
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
