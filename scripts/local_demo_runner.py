"""
Local Simulation Runner: Real-Time Sales Data Pipeline
======================================================
This script simulates the entire data engineering pipeline locally using Pandas.
It mimics the Medallion Architecture (Bronze -> Silver -> Gold) and runs 
the logic equivalent to the Spark/Glue jobs.

Flow:
1. Producer Simulation: Reads from sample_data.csv.
2. Bronze Ingestion: Saves raw 'streamed' data to local lake.
3. Silver Transformation: Cleans, deduplicates, and validates data.
4. Gold Aggregation: Performs business aggregations (Revenue by Category/Date).
5. Data Quality: Runs checks on the processed data.
"""

import os
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime
import shutil

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LocalDemoRunner")

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_DATA = os.path.join(BASE_DIR, "data", "sample_data.csv")
LAKE_PATH = os.path.join(BASE_DIR, "data", "local_lake")

BRONZE_PATH = os.path.join(LAKE_PATH, "bronze")
SILVER_PATH = os.path.join(LAKE_PATH, "silver")
GOLD_PATH = os.path.join(LAKE_PATH, "gold")

def setup_lake():
    """Create local lake directory structure."""
    if os.path.exists(LAKE_PATH):
        logger.info("Cleaning old lake data...")
        shutil.rmtree(LAKE_PATH)
    
    os.makedirs(BRONZE_PATH, exist_ok=True)
    os.makedirs(SILVER_PATH, exist_ok=True)
    os.makedirs(GOLD_PATH, exist_ok=True)
    logger.info(f"Initialized local lake at {LAKE_PATH}")

def run_bronze_ingestion():
    """Simulate Kafka -> S3 Bronze ingestion."""
    logger.info(">>> STEP 1: BRONZE INGESTION (Simulation)")
    df = pd.read_csv(SAMPLE_DATA)
    
    # Add metadata fields as Spark Streaming does
    df['ingestion_timestamp'] = datetime.now().isoformat()
    df['event_source'] = "local_simulation_producer"
    
    # Partition by Date simulation (simplified for local)
    df['order_date'] = pd.to_datetime(df['order_timestamp']).dt.date
    
    # Save to Bronze
    df.to_parquet(os.path.join(BRONZE_PATH, "raw_sales.parquet"), index=False)
    logger.info(f"Ingested {len(df)} records into Bronze Layer.")
    return df

def run_silver_transformation(bronze_df):
    """Simulate Glue ETL: Bronze -> Silver (Cleaning & Deduping)."""
    logger.info(">>> STEP 2: SILVER TRANSFORMATION (Cleaning)")
    
    # 1. Deduplication (Same logic as Spark transformations.py)
    silver_df = bronze_df.drop_duplicates(subset=["order_id"])
    
    # 2. Validation (Check for price/quantity > 0)
    silver_df = silver_df[(silver_df['price'] > 0) & (silver_df['quantity'] > 0)]
    
    # 3. Revenue Calculation
    silver_df['revenue'] = silver_df['price'] * silver_df['quantity']
    
    # Save to Silver
    silver_df.to_parquet(os.path.join(SILVER_PATH, "cleaned_sales.parquet"), index=False)
    logger.info(f"Silver Transformation Complete. {len(silver_df)} clean records saved.")
    return silver_df

def run_gold_aggregation(silver_df):
    """Simulate Glue ETL: Silver -> Gold (Aggregations)."""
    logger.info(">>> STEP 3: GOLD AGGREGATION (Business Metrics)")
    
    # A. Daily Revenue
    daily_revenue = silver_df.groupby('order_date')['revenue'].sum().reset_index()
    daily_revenue.to_parquet(os.path.join(GOLD_PATH, "daily_revenue.parquet"), index=False)
    
    # B. Category Metrics
    category_metrics = silver_df.groupby('category').agg({
        'revenue': 'sum',
        'order_id': 'count'
    }).rename(columns={'order_id': 'order_count'}).reset_index()
    category_metrics.to_parquet(os.path.join(GOLD_PATH, "category_metrics.parquet"), index=False)
    
    logger.info("Gold Layer created: 'daily_revenue.parquet' and 'category_metrics.parquet'.")
    return daily_revenue, category_metrics

def run_quality_checks(bronze_df, silver_df):
    """Simulate quality_checks.py logic."""
    logger.info(">>> STEP 4: DATA QUALITY CHECKS")
    
    # 1. Row Count Check
    bronze_count = len(bronze_df)
    silver_count = len(silver_df)
    drop_pct = ((bronze_count - silver_count) / bronze_count) * 100
    logger.info(f"Row Count: Bronze={bronze_count}, Silver={silver_count} (Drop: {drop_pct:.2f}%)")
    
    # 2. Null Checks
    null_counts = silver_df[['order_id', 'customer_id', 'revenue']].isnull().sum().sum()
    logger.info(f"Null Checks: {null_counts} nulls found in critical columns.")
    
    if drop_pct <= 5 and null_counts == 0:
        logger.info("SUMMARY: Data Quality Checks PASSED! ✅")
    else:
        logger.warning("SUMMARY: Data Quality Checks FAILED! ❌")

def main():
    logger.info("Starting Pipeline Simulation...")
    try:
        setup_lake()
        
        # Bronze Layer
        bronze_df = run_bronze_ingestion()
        
        # Silver Layer
        silver_df = run_silver_transformation(bronze_df)
        
        # Gold Layer
        run_gold_aggregation(silver_df)
        
        # Quality Validation
        run_quality_checks(bronze_df, silver_df)
        
        logger.info("\nPipeline Simulation Successful! You can explore the data in 'data/local_lake'")
        
    except Exception as e:
        logger.error(f"Pipeline crashed: {e}")

if __name__ == "__main__":
    main()
