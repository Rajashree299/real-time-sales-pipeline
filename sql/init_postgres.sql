-- Enable logical replication
ALTER SYSTEM SET wal_level = 'logical';

-- Create sample sales tables
CREATE TABLE IF NOT EXISTS public.sales_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    price DECIMAL(10, 2),
    order_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    region VARCHAR(50)
);

-- Add some initial data
INSERT INTO public.sales_orders (order_id, customer_id, product_id, quantity, price, region)
VALUES 
('ORD001', 'CUST001', 'PROD001', 2, 500.00, 'North'),
('ORD002', 'CUST002', 'PROD002', 1, 1200.00, 'South');

-- Create a publication for Debezium (optional but good practice)
-- CREATE PUBLICATION dbz_publication FOR TABLE public.sales_orders;
