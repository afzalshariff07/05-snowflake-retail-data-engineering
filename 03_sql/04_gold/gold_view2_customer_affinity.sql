-- ============================================================
-- File        : gold_view2_customer_affinity.sql
-- Folder      : 03_sql/04_gold/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the second Gold layer analytical view:
--   VW_CUSTOMER_PRODUCT_AFFINITY
--
--   This view answers the business question:
--   "Which customers buy which products, how often, and how much
--    do they spend — broken down by month?"
--
--   It joins all three Silver tables (customer, orders, product) and
--   aggregates purchase behaviour to the customer-product-month grain,
--   producing an analytics-ready dataset for:
--     → Customer segmentation and loyalty analysis
--     → Product affinity and cross-sell/upsell opportunity identification
--     → Personalised marketing and targeted promotions
--     → Monthly purchasing behaviour trends per customer
--     → ML feature engineering (recommendation models, churn prediction,
--       demand forecasting, customer lifetime value estimation)
--
-- View grain (level of detail):
--   One row per unique combination of:
--   customer_id + product_id + purchase_month (month-truncated transaction_date)
--
-- Source tables (all from Silver — clean, validated data):
--   silver.customer → customer dimension (customer_type, segment)
--   silver.orders   → transaction facts (quantity, amount, date)
--   silver.product  → product dimension (name, category)
--
-- Metrics computed:
--   purchase_count                    → number of distinct transactions for this
--                                       customer-product-month combination
--   total_quantity                    → total units purchased
--   total_spent                       → total amount spent
--   avg_purchase_amount               → average spend per transaction
--   days_between_first_last_purchase  → purchase frequency indicator within the month
--                                       (0 = single purchase; >0 = repeat buyer)
--
-- Key functions used:
--   DATE_TRUNC('MONTH', ...)  → groups all transactions in the same calendar month
--   DATEDIFF('DAY', MIN, MAX) → measures the spread of purchases within the month
--                               (a proxy for purchase frequency/loyalty signal)
--   AVG(total_amount)         → average spend per order for this combination
--
-- View behaviour:
--   → No data is stored — queries Silver tables live on each access
--   → Always reflects the latest Silver data after each merge task run
--   → Can be connected directly to Power BI, Tableau, or ML pipelines
--   → Well-suited as a feature store input for recommendation engines
--
-- Prerequisites:
--   → silver_data_load.sql        must have been run (Silver tables exist)
--   → All Silver merge tasks      must have run (Silver tables have data)
--   → Gold schema                 must exist (created in gold_layer.sql)
--   → gold_view1_daily_sales.sql  recommended to run first (maintains order)
--
-- Idempotent   : Yes — uses CREATE OR REPLACE; safe to re-run.
-- Run After    : 03_sql/04_gold/gold_view1_daily_sales.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- View is created in the GOLD schema.
-- Source tables are referenced with full SILVER schema prefix.

USE DATABASE pacificretail_db;
USE SCHEMA gold;


-- ── STEP 2: Create Gold View — VW_CUSTOMER_PRODUCT_AFFINITY ───────────────────
-- Aggregates Silver purchase data to a monthly customer-product grain.
-- Leads with the customer dimension (vs View 1 which leads with the date)
-- because the analytical focus here is on WHO is buying WHAT, not WHEN.
--
-- JOIN logic:
--   customer JOIN orders  ON customer_id → links customer to their transactions
--   orders   JOIN product ON product_id  → enriches each transaction with product info
--
-- Both JOINs are INNER JOINs — only customers who have placed at least one order,
-- and only orders that reference a valid product in Silver, are included.
-- This ensures clean referential integrity in the affinity output.

CREATE OR REPLACE VIEW vw_customer_product_affinity AS
SELECT

    -- ── Dimension columns (GROUP BY keys) ─────────────────────────────────────

    c.customer_id,                  -- customer identifier
    c.customer_type,                -- Regular | Premium (for segment-level analysis)
    p.product_id,                   -- product identifier
    p.name           AS product_name,      -- product display name
    p.category       AS product_category,  -- product category

    -- Truncates transaction_date to the first day of the month
    -- e.g. 2024-03-15 → 2024-03-01
    -- Groups all purchases within the same calendar month for monthly trending
    DATE_TRUNC('MONTH', o.transaction_date) AS purchase_month,

    -- ── Metric columns (aggregated measures) ──────────────────────────────────

    -- Number of distinct orders placed by this customer for this product in this month
    -- COUNT(DISTINCT) prevents inflated counts if a transaction_id appears multiple times
    COUNT(DISTINCT o.transaction_id)                                    AS purchase_count,

    -- Total units of this product purchased by this customer in this month
    SUM(o.quantity)                                                     AS total_quantity,

    -- Total amount spent by this customer on this product in this month
    SUM(o.total_amount)                                                 AS total_spent,

    -- Average spend per individual order for this customer-product-month combination
    -- Useful for identifying high-value transactions vs bulk/discount purchases
    AVG(o.total_amount)                                                 AS avg_purchase_amount,

    -- Number of days between the customer's first and last purchase of this product
    -- within the month. Used as a purchase frequency / loyalty signal:
    --   0  = single purchase event (bought once in the month)
    --   >0 = repeat buyer (bought multiple times across different days)
    --   Higher value = purchases spread across more of the month → habitual buyer
    -- This metric is a valuable feature for ML recommendation and churn models.
    DATEDIFF('DAY',
        MIN(o.transaction_date),    -- earliest purchase of this product in the month
        MAX(o.transaction_date)     -- latest purchase of this product in the month
    )                                                                   AS days_between_first_last_purchase

FROM silver.customer c

-- Join customer to their order transactions
-- INNER JOIN: only customers with at least one order appear in this view
JOIN silver.orders  o ON c.customer_id = o.customer_id

-- Enrich each order with product details (name, category)
-- INNER JOIN: only orders with a valid matching product in Silver are included
JOIN silver.product p ON o.product_id  = p.product_id

-- ── GROUP BY all non-aggregated columns ───────────────────────────────────────
-- Every SELECT column not wrapped in an aggregate function must appear here.
-- The combination of these 6 columns defines the view's grain:
--   one row per customer × product × calendar month
GROUP BY
    c.customer_id,
    c.customer_type,
    p.product_id,
    p.name,
    p.category,
    DATE_TRUNC('MONTH', o.transaction_date);


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the view was created and returns valid data.

-- 1. Confirm the view exists in the Gold schema
SHOW VIEWS LIKE 'vw_customer_product_affinity' IN SCHEMA pacificretail_db.gold;

-- 2. Preview the full view output
SELECT * FROM vw_customer_product_affinity LIMIT 20;

-- 3. Top 10 most loyal customers by total spend across all products
SELECT
    customer_id,
    customer_type,
    COUNT(DISTINCT product_id)  AS distinct_products_bought,
    SUM(purchase_count)         AS total_orders,
    SUM(total_spent)            AS lifetime_spend,
    AVG(avg_purchase_amount)    AS avg_order_value
FROM vw_customer_product_affinity
GROUP BY customer_id, customer_type
ORDER BY lifetime_spend DESC
LIMIT 10;

-- 4. Most purchased product categories by customer segment
SELECT
    customer_type,
    product_category,
    SUM(purchase_count)   AS total_orders,
    SUM(total_quantity)   AS total_units,
    SUM(total_spent)      AS total_revenue
FROM vw_customer_product_affinity
GROUP BY customer_type, product_category
ORDER BY customer_type, total_revenue DESC;

-- 5. Repeat buyers — customers who purchased the same product more than once in a month
--    (purchase_count > 1 within the same month = strong affinity signal for ML models)
SELECT
    customer_id,
    product_name,
    product_category,
    purchase_month,
    purchase_count,
    total_spent,
    days_between_first_last_purchase
FROM vw_customer_product_affinity
WHERE purchase_count > 1
ORDER BY purchase_count DESC, total_spent DESC
LIMIT 20;

-- 6. Monthly revenue trend across all customers and products
SELECT
    purchase_month,
    COUNT(DISTINCT customer_id)  AS active_customers,
    COUNT(DISTINCT product_id)   AS products_purchased,
    SUM(total_spent)             AS monthly_revenue,
    AVG(avg_purchase_amount)     AS avg_order_value
FROM vw_customer_product_affinity
GROUP BY purchase_month
ORDER BY purchase_month DESC;

-- ============================================================
-- END OF SCRIPT — Gold layer complete
-- Both analytical views are now available:
--   VW_DAILY_SALES_ANALYSIS        → daily revenue by product and customer type
--   VW_CUSTOMER_PRODUCT_AFFINITY   → monthly purchase behaviour per customer
--
-- The full PacificRetail Medallion Architecture pipeline is now live:
--   ADLS → Bronze (raw) → Silver (clean) → Gold (analytical) → BI / ML
-- ============================================================
