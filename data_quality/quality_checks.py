"""
Data Quality & Validation Framework
====================================

Implements checks for Row Count, Nulls, and Duplicates.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_row_counts(source_df: DataFrame, target_df: DataFrame, layer_name: str):
    """
    Validates that data is mostly preserved between layers.
    A small drop is expected if validation removes malformed records.
    """
    source_count = source_df.count()
    target_count = target_df.count()
    drop_pct = ((source_count - target_count) / source_count) * 100
    
    logger.info(f"DQ CHECK: {layer_name} Row Count Validation")
    logger.info(f"Source Rows: {source_count}, Target Rows: {target_count}")
    logger.info(f"Data Drop: {drop_pct:.2f}%")
    
    if drop_pct > 5:
        logger.warning(f"HIGH DATA DROP DETECTED: {drop_pct:.2f}%")
    return drop_pct <= 5

def check_nulls(df: DataFrame, critical_cols: list):
    """
    Ensures that critical columns have 0% null values.
    Important for join keys and financial metrics.
    """
    logger.info("DQ CHECK: Null Percentage Validation")
    total_rows = df.count()
    passed = True
    
    for c in critical_cols:
        null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        null_pct = (null_count / total_rows) * 100
        logger.info(f"Column {c}: {null_pct:.2f}% nulls")
        if null_pct > 0:
            logger.error(f"CRITICAL DQ FAILURE: {c} contains null values!")
            passed = False
    return passed

def check_duplicate_orders(df: DataFrame):
    """
    Checks for duplicate order_ids in Silver/Gold layers.
    Duplicate orders lead to incorrect revenue reporting.
    """
    dup_count = df.groupBy("order_id").count().filter("count > 1").count()
    logger.info(f"DQ CHECK: Duplicate Detection")
    logger.info(f"Duplicate order_ids found: {dup_count}")
    
    if dup_count > 0:
        logger.error("DQ FAILURE: Duplicates detected in a deduplicated layer!")
        return False
    return True

def run_all_checks(bronze_df, silver_df):
    """Entry point for DQ pipeline."""
    results = {
        "count_check": check_row_counts(bronze_df, silver_df, "Bronze -> Silver"),
        "null_check": check_nulls(silver_df, ["order_id", "customer_id", "revenue"]),
        "dup_check": check_duplicate_orders(silver_df)
    }
    
    if all(results.values()):
        logger.info("SUMMARY: All Data Quality checks Passed.")
    else:
        logger.error(f"SUMMARY: Data Quality Failures detected: {results}")
    return results
