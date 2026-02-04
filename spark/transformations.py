"""
Spark Transformations Library
============================

Reusable logic for Bronze -> Silver -> Gold ETL.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, to_date, round, row_number, sum, count, desc

def deduplicate_data(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Removes duplicates keeping the latest record."""
    window = Window.partitionBy(key_col).orderBy(col(order_col).desc())
    return df.withColumn("row_num", row_number().over(window)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")

def calculate_revenue(df: DataFrame) -> DataFrame:
    """Calculates revenue per order."""
    return df.withColumn("revenue", round(col("price") * col("quantity"), 2))

def get_daily_revenue(df: DataFrame) -> DataFrame:
    """Aggregates revenue by day."""
    return df.groupBy("order_date").agg(
        round(sum("revenue"), 2).alias("total_daily_revenue"),
        count("order_id").alias("total_orders")
    ).orderBy("order_date")

def get_category_metrics(df: DataFrame) -> DataFrame:
    """Aggregates metrics by category."""
    return df.groupBy("category").agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    ).orderBy(desc("total_revenue"))

def get_top_products(df: DataFrame, n: int = 10) -> DataFrame:
    """Gets top N products by revenue."""
    return df.groupBy("product_id", "product_name").agg(
        round(sum("revenue"), 2).alias("total_revenue")
    ).orderBy(desc("total_revenue")).limit(n)
