-- ============================================================
-- File        : gold_view1_daily_sales.sql
-- Folder      : 03_sql/04_gold/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the first Gold layer analytical view:
--   VW_DAILY_SALES_ANALYSIS
--
--   This view answers the core business question:
--   "What was sold, by whom, at what value — grouped by day?"
--
--   It joins all three Silver tables (orders, product, customer) and
--   aggregates transaction data to the day-product-customer grain,
--   producing a denormalised, analytics-ready dataset for:
--     → Daily revenue reporting dashboards (Power BI / Tableau)
--     → Product performance analysis by category
--     → Customer type revenue segmentation (Regular vs Premium)
--     → Pricing analysis (avg price per unit vs avg transaction value)
--     → Sales operations and executive reporting
--
-- View grain (level of detail):
--   One row per unique combination of:
--   transaction_date + product_id + customer_id + customer_type
--
-- Source tables (all from Silver — clean, validated data):
--   silver.orders   → transaction facts (quantity, amount, date)
--   silver.product  → product dimension (name, category)
--   silver.customer → customer dimension (customer_type)
--
-- Metrics computed:
--   total_quantity        → SUM of units sold for this combination
--   total_sales           → SUM of revenue generated
--   num_transactions      → COUNT of distinct transaction IDs
--   avg_price_per_unit    → total_sales / total_quantity
--   avg_transaction_value → total_sales / num_transactions
--
-- NULLIF usage:
--   Both division metrics use NULLIF(..., 0) to prevent division-by-zero errors.
--   If total_quantity or num_transactions is 0, the metric returns NULL
--   instead of throwing a runtime error.
--
-- View behaviour:
--   → No data is stored — the view queries Silver tables live on each access
--   → Always reflects the latest Silver data (updated by Silver merge tasks)
--   → Can be directly connected to Power BI via Snowflake connector
--
-- Prerequisites:
--   → silver_data_load.sql        must have been run (Silver tables exist)
--   → All Silver merge tasks      must have run (Silver tables have data)
--   → Gold schema                 must exist (created in gold_layer.sql)
--
-- Idempotent   : Yes — uses CREATE OR REPLACE; safe to re-run.
-- Run Before   : 03_sql/04_gold/gold_view2_customer_affinity.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- View is created in the GOLD schema.
-- Source tables are referenced with full SILVER schema prefix.

USE DATABASE pacificretail_db;
USE SCHEMA gold;


-- ── STEP 2: Create Gold View — VW_DAILY_SALES_ANALYSIS ────────────────────────
-- Aggregates Silver order transactions to a daily product-customer grain.
-- Joins orders → product → customer to enrich transaction facts with
-- descriptive dimensions for business-friendly reporting.
--
-- JOIN logic:
--   orders JOIN product  ON product_id  → enriches each sale with product details
--   orders JOIN customer ON customer_id → enriches each sale with customer segment
--
-- Both JOINs are INNER JOINs — only transactions where both the product
-- and customer exist in Silver are included. This ensures referential
-- integrity in the Gold layer output.

CREATE OR REPLACE VIEW vw_daily_sales_analysis AS
SELECT

    -- ── Dimension columns (GROUP BY keys) ─────────────────────────────────────

    o.transaction_date,             -- date of sale (daily grain for time-series analysis)
    p.product_id,                   -- product identifier (for product-level drill-down)
    p.name           AS product_name,      -- product display name
    p.category       AS product_category,  -- category for category-level aggregation
    c.customer_id,                  -- customer identifier (for customer-level drill-down)
    c.customer_type,                -- Regular | Premium (for segment comparison)

    -- ── Metric columns (aggregated measures) ──────────────────────────────────

    -- Total units sold for this product by this customer on this date
    SUM(o.quantity)                                                     AS total_quantity,

    -- Total revenue generated for this product by this customer on this date
    SUM(o.total_amount)                                                 AS total_sales,

    -- Number of distinct transactions (orders) contributing to the above totals
    -- Uses COUNT(DISTINCT) to avoid double-counting if the same transaction_id
    -- appears multiple times due to upstream data anomalies
    COUNT(DISTINCT o.transaction_id)                                    AS num_transactions,

    -- Average revenue per unit sold
    -- NULLIF prevents division-by-zero if total_quantity aggregates to 0
    -- Formula: total revenue ÷ total units = effective price per unit
    SUM(o.total_amount) / NULLIF(SUM(o.quantity), 0)                   AS avg_price_per_unit,

    -- Average value per individual transaction (basket size metric)
    -- NULLIF prevents division-by-zero if no transactions exist for this group
    -- Formula: total revenue ÷ number of transactions = avg order value
    SUM(o.total_amount) / NULLIF(COUNT(DISTINCT o.transaction_id), 0)  AS avg_transaction_value

FROM silver.orders o

-- Enrich transaction with product details (name, category)
-- INNER JOIN: excludes orphaned orders with no matching product in Silver
JOIN silver.product  p ON o.product_id  = p.product_id

-- Enrich transaction with customer segment details (customer_type)
-- INNER JOIN: excludes orphaned orders with no matching customer in Silver
JOIN silver.customer c ON o.customer_id = c.customer_id

-- ── GROUP BY all non-aggregated columns ───────────────────────────────────────
-- Every SELECT column that is not wrapped in an aggregate function
-- must appear in the GROUP BY clause.
-- The combination of these 6 columns defines the view's grain:
--   one row per day × product × customer segment
GROUP BY
    o.transaction_date,
    p.product_id,
    p.name,
    p.category,
    c.customer_id,
    c.customer_type;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the view was created and returns valid data.

-- 1. Confirm the view exists in the Gold schema
SHOW VIEWS LIKE 'vw_daily_sales_analysis' IN SCHEMA pacificretail_db.gold;

-- 2. Preview the view output — inspect all computed metrics
SELECT * FROM vw_daily_sales_analysis LIMIT 20;

-- 3. Top 10 products by total revenue — product performance ranking
SELECT
    product_name,
    product_category,
    SUM(total_sales)       AS total_revenue,
    SUM(total_quantity)    AS total_units_sold,
    SUM(num_transactions)  AS total_transactions
FROM vw_daily_sales_analysis
GROUP BY product_name, product_category
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Revenue by customer type — Regular vs Premium segmentation
SELECT
    customer_type,
    SUM(total_sales)          AS total_revenue,
    SUM(num_transactions)     AS total_transactions,
    AVG(avg_transaction_value) AS avg_basket_size
FROM vw_daily_sales_analysis
GROUP BY customer_type
ORDER BY total_revenue DESC;

-- 5. Daily revenue trend — time series for dashboard
SELECT
    transaction_date,
    SUM(total_sales)       AS daily_revenue,
    SUM(total_quantity)    AS daily_units_sold,
    SUM(num_transactions)  AS daily_transactions
FROM vw_daily_sales_analysis
GROUP BY transaction_date
ORDER BY transaction_date DESC
LIMIT 30;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/04_gold/gold_view2_customer_affinity.sql
-- ============================================================