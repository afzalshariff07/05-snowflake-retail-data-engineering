-- ============================================================
-- File        : product_transform.sql
-- Folder      : 03_sql/03_silver/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the Silver layer transformation pipeline for
--   product catalog data — a Stored Procedure and a scheduled Task that
--   together move clean, validated product records from Bronze to Silver.
--
--   Two objects are created:
--     1. STORED PROCEDURE  → merge_product_to_silver()
--                            Reads from product_changes_stream, applies
--                            data quality rules, and MERGEs into silver.product
--     2. TASK              → product_silver_merge_task
--                            Calls the stored procedure every 4 hours
--                            (offset by 15 minutes from the customer task)
--
-- How it works (end-to-end):
--   bronze.raw_product (new rows)
--      ↓  captured by
--   bronze.product_changes_stream
--      ↓  read and transformed by
--   merge_product_to_silver() Stored Procedure
--      ↓  applies DQ rules + MERGE
--   silver.product
--      ↓  (INSERT new products / UPDATE existing products)
--
-- Data Quality Rules applied inside the Stored Procedure:
--   ┌──────────────────┬──────────────────────────────────────────────────────┐
--   │ Column           │ Rule                                                 │
--   ├──────────────────┼──────────────────────────────────────────────────────┤
--   │ price            │ negative values → set to 0 (floor at 0)             │
--   │ stock_quantity   │ negative values → set to 0 (floor at 0)             │
--   │ rating           │ valid range 0–5 → kept; out of range → set to 0     │
--   └──────────────────┴──────────────────────────────────────────────────────┘
--
--   Note: Unlike customer records, there are NO null-exclusion filters here.
--   All product rows from the stream are processed — quality issues are
--   corrected in-place (floor/cap) rather than rejecting the entire row.
--
-- MERGE logic:
--   WHEN MATCHED     → UPDATE all fields (product already exists in Silver)
--   WHEN NOT MATCHED → INSERT new row (first time this product_id is seen)
--   Match key        → product_id
--
-- Schedule  : Every 4 hours at :15 past the hour (America/New_York)
--             Offset by 15 minutes from silver_customer_merge_task (:00)
--             to avoid simultaneous warehouse contention on compute_wh.
-- Warehouse : compute_wh
--
-- Prerequisites:
--   → silver_data_load.sql   must have been run (silver.product table exists)
--   → stream_creation.sql    must have been run (product_changes_stream exists)
--   → customer_transform.sql recommended to run first (maintains order)
--
-- Idempotent   : Yes — uses CREATE OR REPLACE for both objects; safe to re-run.
-- Run Before   : 03_sql/03_silver/orders_transform.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- All objects created in SILVER schema; stream referenced from BRONZE schema.

USE DATABASE pacificretail_db;
USE SCHEMA silver;


-- ── STEP 2: Create Stored Procedure — merge_product_to_silver() ───────────────
-- This procedure encapsulates the full Bronze → Silver transformation logic
-- for product catalog data. It is called by the Task defined in Step 3.
--
-- Procedure signature:
--   RETURNS STRING   → Returns a confirmation message on successful execution
--   LANGUAGE SQL     → Written in Snowflake SQL scripting (not JavaScript/Python)
--
-- Internal variables:
--   rows_inserted    → Placeholder for tracking inserted rows (extensible for logging)
--   rows_updated     → Placeholder for tracking updated rows (extensible for logging)

CREATE OR REPLACE PROCEDURE merge_product_to_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT;  -- tracks number of new product rows inserted into Silver
    rows_updated  INT;  -- tracks number of existing product rows updated in Silver

BEGIN

    -- ── MERGE: Bronze Stream → Silver Product ──────────────────────────────────
    -- Reads all pending rows from product_changes_stream (new rows appended to
    -- raw_product since the last successful run), applies data quality rules
    -- inline, and upserts the results into silver.product.
    --
    -- Key difference from customer MERGE:
    --   No WHERE filter on NULL product_id — all rows are processed.
    --   Quality issues are corrected (price → 0, rating → 0) rather than
    --   excluding the row, because even a product with a bad rating value
    --   is still a valid catalog entry that downstream teams need.
    --
    -- SOURCE: product_changes_stream (transformed inline)
    -- TARGET: silver.product
    -- MATCH KEY: product_id

    MERGE INTO silver.product AS target
    USING (

        SELECT
            product_id,
            name,
            category,

            -- ── DQ Rule: Price Floor Validation ───────────────────────────────
            -- Negative prices are data entry errors from the Inventory system.
            -- Floor is set to 0 — a product cannot have a negative listed price.
            -- Products with price = 0 are typically free samples or promotional items.
            CASE
                WHEN price < 0 THEN 0
                ELSE price
            END AS price,

            brand,

            -- ── DQ Rule: Stock Quantity Floor Validation ───────────────────────
            -- Negative stock counts are system errors (e.g. failed inventory sync).
            -- Floor is set to 0 — physical stock cannot be negative.
            -- A stock_quantity of 0 correctly indicates out-of-stock status.
            CASE
                WHEN stock_quantity >= 0 THEN stock_quantity
                ELSE 0
            END AS stock_quantity,

            -- ── DQ Rule: Rating Range Validation ──────────────────────────────
            -- Customer ratings must fall within the 0–5 star scale.
            -- Out-of-range values (e.g. -1, 6, 99) are data errors — set to 0.
            -- A rating of 0 indicates either an unrated product or a corrected
            -- invalid value; downstream analytics should treat 0 as "unrated".
            -- Note: unlike age (where out-of-range → NULL), rating defaults to 0
            -- because NULL ratings complicate aggregation queries in the Gold layer.
            CASE
                WHEN rating BETWEEN 0 AND 5 THEN rating
                ELSE 0
            END AS rating,

            is_active,

            CURRENT_TIMESTAMP() AS last_updated_timestamp  -- MERGE execution time

        FROM bronze.product_changes_stream
        -- No NULL filter here — all product rows are processed regardless of values.
        -- Data quality is enforced by the CASE expressions above, not by exclusion.

    ) AS source

    -- Match condition: if product_id already exists in Silver → UPDATE
    --                  if product_id is new                   → INSERT
    ON target.product_id = source.product_id

    -- ── WHEN MATCHED: Update existing product record ───────────────────────────
    -- Product already exists in silver.product — update all fields to reflect
    -- latest values from the Inventory system (e.g. price change, stock update,
    -- product deactivation via is_active = FALSE).
    -- last_updated_timestamp refreshes to record when the update occurred.
    WHEN MATCHED THEN
        UPDATE SET
            name                   = source.name,
            category               = source.category,
            price                  = source.price,
            brand                  = source.brand,
            stock_quantity         = source.stock_quantity,
            rating                 = source.rating,
            is_active              = source.is_active,
            last_updated_timestamp = source.last_updated_timestamp

    -- ── WHEN NOT MATCHED: Insert new product record ────────────────────────────
    -- product_id not found in silver.product — this is a new catalog entry.
    -- All fields including the validated/corrected values are inserted.
    WHEN NOT MATCHED THEN
        INSERT (
            product_id, name, category, price, brand,
            stock_quantity, rating, is_active, last_updated_timestamp
        )
        VALUES (
            source.product_id, source.name, source.category, source.price, source.brand,
            source.stock_quantity, source.rating, source.is_active, source.last_updated_timestamp
        );

    -- Return a confirmation message on successful completion
    RETURN 'Products processed successfully';

END;
$$;


-- ── STEP 3: Create Scheduled Task — product_silver_merge_task ─────────────────
-- A Snowflake Task that calls merge_product_to_silver() on a schedule.
-- Using CALL inside a Task allows the full procedure logic (DECLARE, BEGIN, MERGE)
-- to run as a single atomic unit on each execution.
--
-- Task configuration:
--   WAREHOUSE = compute_wh                 → Virtual warehouse for execution
--   SCHEDULE = 'USING CRON 15 */4 * * *'  → Every 4 hours at :15 past the hour
--   America/New_York                       → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   15 */4 * * *
--   │   │  │ │ └── Day of week  : every day (*)
--   │   │  │ └──── Month        : every month (*)
--   │   │  └────── Day of month : every day (*)
--   │   └────────── Hour        : every 4th hour (*/4 = 00, 04, 08, 12, 16, 20)
--   └────────────── Minute      : 15
--
-- Runs at: 00:15, 04:15, 08:15, 12:15, 16:15, 20:15 (6 times per day)
--
-- Why :15 offset?
--   silver_customer_merge_task runs at :00 on the same compute_wh.
--   Staggering by 15 minutes ensures both tasks don't compete for the
--   same warehouse resources simultaneously, avoiding queue delays.

CREATE OR REPLACE TASK product_silver_merge_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 15 */4 * * * America/New_York'
AS
    CALL merge_product_to_silver();


-- ── STEP 4: Activate the Task ─────────────────────────────────────────────────
-- Tasks are created in SUSPENDED state by default.
-- RESUME activates the task to start running on its defined CRON schedule.

ALTER TASK product_silver_merge_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the procedure and task are set up correctly.

-- 1. Confirm the stored procedure was created
SHOW PROCEDURES LIKE 'merge_product_to_silver' IN SCHEMA pacificretail_db.silver;

-- 2. Confirm the task is in STARTED state and scheduled correctly
SHOW TASKS LIKE 'product_silver_merge_task' IN SCHEMA pacificretail_db.silver;

-- 3. Manually execute the procedure to test the Bronze → Silver pipeline
--    (run this after Bronze raw_product has data loaded)
CALL merge_product_to_silver();

-- 4. Confirm rows landed in silver.product
SELECT COUNT(*) AS total_products FROM silver.product;

-- 5. Preview Silver product data — verify DQ rules were applied correctly
--    Check: price should have no negatives
--           stock_quantity should have no negatives
--           rating should be between 0 and 5 only
SELECT
    COUNT(*)                              AS total_rows,
    SUM(CASE WHEN price < 0 THEN 1 END)  AS negative_prices,
    SUM(CASE WHEN stock_quantity < 0 THEN 1 END) AS negative_stock,
    SUM(CASE WHEN rating < 0 OR rating > 5 THEN 1 END) AS invalid_ratings,
    MIN(price)                            AS min_price,
    MAX(rating)                           AS max_rating,
    COUNT(CASE WHEN is_active THEN 1 END) AS active_products
FROM silver.product;

-- 6. Confirm the stream has been consumed (should return 0 after procedure runs)
SELECT COUNT(*) AS pending_rows FROM bronze.product_changes_stream;

-- 7. Check all Silver tasks are active and staggered correctly
SHOW TASKS IN SCHEMA pacificretail_db.silver;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/03_silver/orders_transform.sql
-- ============================================================