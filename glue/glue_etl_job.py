"""
AWS Glue ETL Job: Bronze to Silver to Gold
==========================================

This job performs batch ETL on the data lake:
1. Reads Bronze data (raw Parquet)
2. Transforms to Silver (Cleaned & Deduplicated)
3. Aggregates to Gold (Business-level metrics)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

# Import custom transformations
# Note: In a real Glue environment, you'd upload transformations.py to S3 
# and include it in the job's --extra-py-files
from transformations import deduplicate_data, calculate_revenue, get_daily_revenue, get_category_metrics

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # 1. READ BRONZE LAYER
    bronze_path = "s3://sales-data-lake/bronze/sales_events/"
    bronze_df = spark.read.parquet(bronze_path)

    # 2. TRANSFORM TO SILVER (CLEANING & DEDUPLICATION)
    # Deduplicate based on order_id, keeping latest ingestion_timestamp
    deduped_df = deduplicate_data(bronze_df, "order_id", "ingestion_timestamp")
    
    # Add revenue and basic date columns
    silver_df = calculate_revenue(deduped_df) \
        .withColumn("order_date", to_date(col("order_ts")))

    # Write Silver Layer
    silver_path = "s3://sales-data-lake/silver/sales_transactions/"
    silver_df.write.mode("overwrite").parquet(silver_path)

    # 3. TRANSFORM TO GOLD (AGGREGATIONS)
    # A. Daily Revenue
    daily_rev_df = get_daily_revenue(silver_df)
    daily_rev_df.write.mode("overwrite").parquet("s3://sales-data-lake/gold/daily_revenue/")

    # B. Category Performance
    cat_metrics_df = get_category_metrics(silver_df)
    cat_metrics_df.write.mode("overwrite").parquet("s3://sales-data-lake/gold/category_metrics/")

    job.commit()

if __name__ == "__main__":
    main()
