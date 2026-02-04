-- =============================================================================
-- REDSHIFT STAR SCHEMA DESIGN
-- =============================================================================

-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS sales;

-- 2. Product Dimension
-- Diststyle ALL: Small table, useful to join with facts on any node
CREATE TABLE IF NOT EXISTS sales.dim_product (
    product_key INT IDENTITY(1,1),
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    PRIMARY KEY (product_key)
) DISTSTYLE ALL;

-- 3. Customer Dimension
CREATE TABLE IF NOT EXISTS sales.dim_customer (
    customer_key INT IDENTITY(1,1),
    customer_id VARCHAR(50) NOT NULL,
    country VARCHAR(100),
    PRIMARY KEY (customer_key)
) DISTSTYLE ALL;

-- 4. Date Dimension
CREATE TABLE IF NOT EXISTS sales.dim_date (
    date_key INT NOT NULL,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    is_weekend BOOLEAN,
    PRIMARY KEY (date_key)
) DISTSTYLE ALL;

-- 5. Sales Fact Table
-- Distkey customer_id: Colocates customer data for performance
-- Sortkey order_ts: Optimizes time-range queries
CREATE TABLE IF NOT EXISTS sales.fact_sales (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    price DECIMAL(10,2),
    quantity INT,
    revenue DECIMAL(12,2),
    order_ts TIMESTAMP,
    order_date DATE,
    PRIMARY KEY (order_id)
)
DISTKEY (customer_id)
SORTKEY (order_ts);

-- =============================================================================
-- COPY COMMAND EXAMPLE
-- =============================================================================

-- COPY sales.fact_sales
-- FROM 's3://sales-data-lake/silver/sales_transactions/'
-- IAM_ROLE 'arn:aws:iam::your-account-id:role/RedshiftS3Role'
-- FORMAT AS PARQUET;
