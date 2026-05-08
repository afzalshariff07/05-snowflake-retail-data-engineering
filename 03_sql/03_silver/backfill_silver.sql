-- Switch to INSERT + SELECT to bypass the stream entirely
-- and load directly from Bronze into Silver

USE DATABASE pacificretail_db;
USE SCHEMA silver;

-- Direct INSERT for customer (applying all DQ rules manually)
INSERT INTO silver.customer (
    customer_id, name, email, country, customer_type,
    registration_date, age, gender, total_purchases, last_updated_timestamp
)
SELECT
    customer_id,
    name,
    email,
    country,
    CASE
        WHEN TRIM(UPPER(customer_type)) IN ('REGULAR','REG','R') THEN 'Regular'
        WHEN TRIM(UPPER(customer_type)) IN ('PREMIUM','PREM','P') THEN 'Premium'
        ELSE 'Unknown'
    END,
    registration_date,
    CASE WHEN age BETWEEN 18 AND 120 THEN age ELSE NULL END,
    CASE
        WHEN TRIM(UPPER(gender)) IN ('M','MALE')   THEN 'Male'
        WHEN TRIM(UPPER(gender)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'Other'
    END,
    CASE WHEN total_purchases >= 0 THEN total_purchases ELSE 0 END,
    CURRENT_TIMESTAMP()
FROM pacificretail_db.bronze.raw_customer
WHERE customer_id IS NOT NULL AND email IS NOT NULL;

-- Direct INSERT for product
INSERT INTO silver.product (
    product_id, name, category, price, brand,
    stock_quantity, rating, is_active, last_updated_timestamp
)
SELECT
    product_id, name, category,
    CASE WHEN price < 0 THEN 0 ELSE price END,
    brand,
    CASE WHEN stock_quantity >= 0 THEN stock_quantity ELSE 0 END,
    CASE WHEN rating BETWEEN 0 AND 5 THEN rating ELSE 0 END,
    is_active,
    CURRENT_TIMESTAMP()
FROM pacificretail_db.bronze.raw_product;

-- Direct INSERT for orders
INSERT INTO silver.orders (
    transaction_id, customer_id, product_id, quantity,
    store_type, total_amount, transaction_date,
    payment_method, last_updated_timestamp
)
SELECT
    transaction_id, customer_id, product_id, quantity,
    store_type, total_amount, transaction_date,
    payment_method, CURRENT_TIMESTAMP()
FROM pacificretail_db.bronze.raw_order
WHERE transaction_id IS NOT NULL AND total_amount > 0;

-- ── Verify Silver is now populated ───────────────────────────────────────────
SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM silver.customer
UNION ALL
SELECT 'product',                COUNT(*)               FROM silver.product
UNION ALL
SELECT 'orders',                 COUNT(*)               FROM silver.orders;