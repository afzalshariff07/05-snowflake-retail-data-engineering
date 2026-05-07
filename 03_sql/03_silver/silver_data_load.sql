-- ============================================================
-- File        : silver_data_load.sql
-- Folder      : 03_sql/03_silver/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the three Silver layer tables that will hold
--   cleaned, validated, and conformed data processed from the Bronze layer.
--
--   Silver is the single source of truth for all downstream analytics.
--   Data here has been quality-checked and standardised — it is trusted,
--   consistent, and ready for joining across entities.
--
--   Three tables are created in this script:
--     1. SILVER.CUSTOMER  → Cleaned customer master data
--     2. SILVER.PRODUCT   → Cleaned product catalog data
--     3. SILVER.ORDERS    → Validated transaction records
--
-- Key differences from Bronze tables:
--   → No source metadata columns (source_file_name, source_file_row_number)
--      — lineage tracking is the responsibility of the Bronze layer
--   → No ingestion_timestamp — replaced by last_updated_timestamp
--      which reflects when the row was last MERGED (inserted or updated)
--   → Data types are intentional — same as Bronze, but values are guaranteed
--      clean by the time they land here via Stored Procedure MERGE logic
--   → Supports UPSERT pattern: INSERT new rows, UPDATE existing rows
--      using the MERGE statement driven by Snowflake Streams (CDC)
--
-- Load mechanism (set up in subsequent scripts):
--   Bronze Streams (CDC) → Stored Procedures → MERGE INTO Silver tables
--
-- Prerequisites:
--   → create_db_and_schemas.sql    must have been run (SILVER schema exists)
--   → All Bronze tables must be populated before Silver MERGE tasks run
--
-- Idempotent   : Yes — all tables use CREATE IF NOT EXISTS; safe to re-run.
-- Run Before   : 03_sql/03_silver/stream_creation.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- Ensure all objects are created inside PACIFICRETAIL_DB.SILVER schema.

USE DATABASE pacificretail_db;
USE SCHEMA silver;


-- ── STEP 2: Create SILVER.CUSTOMER Table ──────────────────────────────────────
-- Stores cleaned and standardised customer records.
-- Populated via MERGE from bronze.raw_customer using customer_changes_stream.
--
-- Data quality rules applied before rows land here (via Stored Procedure):
--   → Rows with NULL customer_id or NULL email are excluded entirely
--   → customer_type is standardised: REG/R/REGULAR → 'Regular'
--                                    PREM/P/PREMIUM → 'Premium'
--                                    anything else  → 'Unknown'
--   → age is validated: must be between 18 and 120; out-of-range set to NULL
--   → gender is standardised: M/MALE   → 'Male'
--                              F/FEMALE → 'Female'
--                              else     → 'Other'
--   → total_purchases: negative values set to 0
--
-- MERGE key     : customer_id (matched to detect INSERT vs UPDATE)
-- last_updated_timestamp refreshes on every MERGE (both inserts and updates)

CREATE TABLE IF NOT EXISTS silver.customer (
    customer_id             INT,            -- unique customer identifier (MERGE key)
    name                    STRING,         -- full name of the customer
    email                   STRING,         -- email address (must not be NULL)
    country                 STRING,         -- country of residence
    customer_type           STRING,         -- standardised: Regular | Premium | Unknown
    registration_date       DATE,           -- date customer registered on the platform
    age                     INT,            -- validated age (18–120); NULL if out of range
    gender                  STRING,         -- standardised: Male | Female | Other
    total_purchases         INT,            -- cumulative purchases (floor: 0)
    last_updated_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                                            -- timestamp of last MERGE operation
);


-- ── STEP 3: Create SILVER.PRODUCT Table ───────────────────────────────────────
-- Stores cleaned and validated product catalog records.
-- Populated via MERGE from bronze.raw_product using product_changes_stream.
--
-- Data quality rules applied before rows land here (via Stored Procedure):
--   → price         : negative values set to 0
--   → stock_quantity: negative values set to 0
--   → rating        : must be between 0.0 and 5.0; out-of-range set to 0
--
-- MERGE key     : product_id (matched to detect INSERT vs UPDATE)
-- last_updated_timestamp refreshes on every MERGE (both inserts and updates)

CREATE TABLE IF NOT EXISTS silver.product (
    product_id              INT,            -- unique product identifier (MERGE key)
    name                    STRING,         -- product display name
    category                STRING,         -- product category e.g. Electronics, Clothing
    brand                   STRING,         -- brand or manufacturer name
    price                   FLOAT,          -- listed price (floor: 0)
    stock_quantity          INT,            -- inventory count (floor: 0)
    rating                  FLOAT,          -- customer rating (valid range: 0.0–5.0)
    is_active               BOOLEAN,        -- TRUE if product is live on the platform
    last_updated_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                                            -- timestamp of last MERGE operation
);


-- ── STEP 4: Create SILVER.ORDERS Table ────────────────────────────────────────
-- Stores validated transaction records from the E-Commerce Platform.
-- Populated via MERGE from bronze.raw_order using order_changes_stream.
--
-- Data quality rules applied before rows land here (via Stored Procedure):
--   → Rows with NULL transaction_id are excluded entirely
--   → Rows with total_amount <= 0 are excluded entirely
--      (zero or negative order values indicate cancelled/erroneous transactions)
--
-- MERGE key     : transaction_id (matched to detect INSERT vs UPDATE)
-- last_updated_timestamp refreshes on every MERGE (both inserts and updates)
--
-- Note: Unlike customer and product, orders data is mostly INSERT-heavy
-- (new transactions) with rare UPDATEs (e.g. returns, adjustments).
-- The MERGE pattern handles both cases uniformly.

CREATE TABLE IF NOT EXISTS silver.orders (
    transaction_id          STRING,         -- unique transaction ID (MERGE key, must not be NULL)
    customer_id             INT,            -- foreign key → silver.customer
    product_id              INT,            -- foreign key → silver.product
    quantity                INT,            -- units purchased in this transaction
    store_type              STRING,         -- sales channel e.g. Online, In-Store, Mobile App
    total_amount            DOUBLE,         -- transaction value (must be > 0)
    transaction_date        DATE,           -- date the transaction occurred
    payment_method          STRING,         -- payment type e.g. Credit Card, PayPal
    last_updated_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                                            -- timestamp of last MERGE operation
);


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm all Silver tables were created correctly.

-- 1. Confirm all three Silver tables exist
SHOW TABLES IN SCHEMA pacificretail_db.silver;

-- 2. Verify structure of each table
DESCRIBE TABLE silver.customer;
DESCRIBE TABLE silver.product;
DESCRIBE TABLE silver.orders;

-- 3. Confirm tables are empty at this stage (data arrives after Streams + Tasks run)
SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM silver.customer
UNION ALL
SELECT 'product',                COUNT(*)               FROM silver.product
UNION ALL
SELECT 'orders',                 COUNT(*)               FROM silver.orders;

-- ============================================================
-- END OF SCRIPT — Silver tables created
-- Next step → Run: 03_sql/03_silver/stream_creation.sql
-- ============================================================