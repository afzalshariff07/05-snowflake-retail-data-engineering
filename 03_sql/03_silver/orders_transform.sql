-- ============================================================
-- File        : orders_transform.sql
-- Folder      : 03_sql/03_silver/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the Silver layer transformation pipeline for
--   order/transaction data — a Stored Procedure and a scheduled Task that
--   together move validated transaction records from Bronze to Silver.
--
--   Two objects are created:
--     1. STORED PROCEDURE  → merge_order_to_silver()
--                            Reads from order_changes_stream, applies
--                            data quality filters, and MERGEs into silver.orders
--     2. TASK              → order_silver_merge_task
--                            Calls the stored procedure every 2 hours
--                            (more frequent than customer/product — orders are
--                            the highest-volume, most time-sensitive data)
--
-- How it works (end-to-end):
--   bronze.raw_order (new rows)
--      ↓  captured by
--   bronze.order_changes_stream
--      ↓  read and filtered by
--   merge_order_to_silver() Stored Procedure
--      ↓  applies DQ filters + MERGE
--   silver.orders
--      ↓  (INSERT new transactions / UPDATE existing transactions)
--
-- Data Quality Rules applied inside the Stored Procedure:
--   ┌────────────────────┬────────────────────────────────────────────────────┐
--   │ Column             │ Rule                                               │
--   ├────────────────────┼────────────────────────────────────────────────────┤
--   │ transaction_id     │ NULL → entire row excluded from Silver             │
--   │ total_amount       │ <= 0 → entire row excluded from Silver             │
--   └────────────────────┴────────────────────────────────────────────────────┘
--
-- Design note — exclusion vs correction:
--   Orders use row-exclusion (WHERE filter) rather than value-correction (CASE).
--   A transaction with no ID cannot be tracked, referenced, or joined — it has
--   no business value and must be discarded. A transaction with zero or negative
--   total_amount represents a cancelled, reversed, or erroneous order — not a
--   legitimate sale. Both are hard failures with no safe default to substitute.
--   Compare: customer age out-of-range → NULL (row still useful)
--            product rating out-of-range → 0  (row still useful)
--            order with no transaction_id → discard (row has no identity)
--
-- MERGE logic:
--   WHEN MATCHED     → UPDATE all fields (handles order amendments/corrections)
--   WHEN NOT MATCHED → INSERT new row (new transaction not yet in Silver)
--   Match key        → transaction_id
--
-- Schedule  : Every 2 hours at :30 past the hour (America/New_York)
--             More frequent than customer (:00 */4) and product (:15 */4)
--             because order volume is the highest of all three streams.
--             :30 offset avoids overlap with the other two Silver tasks.
-- Warehouse : compute_wh
--
-- Prerequisites:
--   → silver_data_load.sql    must have been run (silver.orders table exists)
--   → stream_creation.sql     must have been run (order_changes_stream exists)
--   → customer_transform.sql  must have been run before this
--   → product_transform.sql   must have been run before this
--
-- Idempotent   : Yes — uses CREATE OR REPLACE for both objects; safe to re-run.
-- Run Before   : 03_sql/04_gold/gold_view1_daily_sales.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- All objects created in SILVER schema; stream referenced from BRONZE schema.

USE DATABASE pacificretail_db;
USE SCHEMA silver;


-- ── STEP 2: Create Stored Procedure — merge_order_to_silver() ─────────────────
-- This procedure encapsulates the full Bronze → Silver transformation logic
-- for transaction data. It is called by the Task defined in Step 3.
--
-- Procedure signature:
--   RETURNS STRING   → Returns a confirmation message on successful execution
--   LANGUAGE SQL     → Written in Snowflake SQL scripting (not JavaScript/Python)
--
-- Internal variables:
--   rows_inserted    → Placeholder for tracking inserted rows (extensible for logging)
--   rows_updated     → Placeholder for tracking updated rows (extensible for logging)

CREATE OR REPLACE PROCEDURE merge_order_to_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT;  -- tracks number of new order rows inserted into Silver
    rows_updated  INT;  -- tracks number of existing order rows updated in Silver

BEGIN

    -- ── MERGE: Bronze Stream → Silver Orders ───────────────────────────────────
    -- Reads all pending rows from order_changes_stream (new rows appended to
    -- raw_order since the last successful run), applies data quality filters,
    -- and upserts valid transactions into silver.orders.
    --
    -- Unlike customer and product, orders have NO value-correction CASE logic.
    -- The only transformations are hard-exclusion WHERE filters — bad rows
    -- are simply not loaded into Silver rather than having values adjusted.
    --
    -- SOURCE: order_changes_stream (filtered by WHERE clause)
    -- TARGET: silver.orders
    -- MATCH KEY: transaction_id

    MERGE INTO silver.orders AS target
    USING (

        SELECT
            transaction_id,     -- unique transaction identifier (MERGE key)
            customer_id,        -- foreign key → silver.customer
            product_id,         -- foreign key → silver.product
            quantity,           -- units purchased
            store_type,         -- sales channel: Online | In-Store | Mobile App
            total_amount,       -- transaction value (guaranteed > 0 by WHERE filter)
            transaction_date,   -- date of transaction
            payment_method,     -- payment type: Credit Card | PayPal | Bank Transfer

            CURRENT_TIMESTAMP() AS last_updated_timestamp  -- MERGE execution time

        FROM bronze.order_changes_stream

        -- ── DQ Filter 1: Exclude rows with no transaction ID ──────────────────
        -- A transaction without an ID cannot be uniquely identified, tracked,
        -- or joined to customer/product tables in the Gold layer.
        -- These rows are irreversibly invalid — no safe default exists.
        WHERE transaction_id IS NOT NULL

        -- ── DQ Filter 2: Exclude zero and negative transaction amounts ─────────
        -- total_amount <= 0 indicates one of:
        --   → Cancelled order (should not appear as a sale)
        --   → Reversed/refunded transaction (handled separately by finance)
        --   → Data error from the E-Commerce Platform
        -- None of these represent legitimate sales and must not inflate revenue
        -- figures in the Gold layer Daily Sales Analysis view.
        AND total_amount > 0

    ) AS source

    -- Match condition: if transaction_id already exists in Silver → UPDATE
    --                  if transaction_id is new                   → INSERT
    ON target.transaction_id = source.transaction_id

    -- ── WHEN MATCHED: Update existing transaction record ──────────────────────
    -- Transaction already exists in silver.orders — update all fields.
    -- This handles legitimate post-transaction amendments such as:
    --   → Quantity corrections (warehouse adjustment after dispatch)
    --   → Store type reclassification (platform migration edge cases)
    --   → Payment method updates (payment retry with different method)
    -- last_updated_timestamp refreshes to record when the update occurred.
    WHEN MATCHED THEN
        UPDATE SET
            customer_id            = source.customer_id,
            product_id             = source.product_id,
            quantity               = source.quantity,
            store_type             = source.store_type,
            total_amount           = source.total_amount,
            transaction_date       = source.transaction_date,
            payment_method         = source.payment_method,
            last_updated_timestamp = source.last_updated_timestamp

    -- ── WHEN NOT MATCHED: Insert new transaction record ───────────────────────
    -- transaction_id not found in silver.orders — this is a new sale.
    -- All fields from the validated source CTE are inserted.
    WHEN NOT MATCHED THEN
        INSERT (
            transaction_id, customer_id, product_id, quantity,
            store_type, total_amount, transaction_date,
            payment_method, last_updated_timestamp
        )
        VALUES (
            source.transaction_id, source.customer_id, source.product_id, source.quantity,
            source.store_type, source.total_amount, source.transaction_date,
            source.payment_method, source.last_updated_timestamp
        );

    -- Return a confirmation message on successful completion
    RETURN 'Orders processed successfully';

END;
$$;


-- ── STEP 3: Create Scheduled Task — order_silver_merge_task ───────────────────
-- A Snowflake Task that calls merge_order_to_silver() on a schedule.
-- Using CALL inside a Task allows the full procedure logic (DECLARE, BEGIN, MERGE)
-- to run as a single atomic unit on each execution.
--
-- Task configuration:
--   WAREHOUSE = compute_wh                 → Virtual warehouse for execution
--   SCHEDULE = 'USING CRON 30 */2 * * *'  → Every 2 hours at :30 past the hour
--   America/New_York                       → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   30 */2 * * *
--   │   │  │ │ └── Day of week  : every day (*)
--   │   │  │ └──── Month        : every month (*)
--   │   │  └────── Day of month : every day (*)
--   │   └────────── Hour        : every 2nd hour (*/2 = 00,02,04,06,08,10,12...)
--   └────────────── Minute      : 30
--
-- Runs at: 00:30, 02:30, 04:30, 06:30, 08:30, 10:30, 12:30 ... (12 times per day)
--
-- Why every 2 hours (vs 4 hours for customer and product)?
--   Orders are the highest-volume, most time-sensitive data stream.
--   PacificRetail processes transactions continuously across 15 countries
--   and multiple time zones. Refreshing Silver orders every 2 hours ensures
--   the Gold Daily Sales view stays no more than ~2 hours behind reality,
--   meeting the business SLA for near-real-time sales reporting.
--
-- Why :30 offset?
--   customer task  → :00 every 4 hours (compute_wh)
--   product task   → :15 every 4 hours (compute_wh)
--   orders task    → :30 every 2 hours (compute_wh)
--   All three share compute_wh. Staggering by 15-minute increments prevents
--   simultaneous warehouse queue contention across all Silver merge tasks.

CREATE OR REPLACE TASK order_silver_merge_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 30 */2 * * * America/New_York'
AS
    CALL merge_order_to_silver();


-- ── STEP 4: Activate the Task ─────────────────────────────────────────────────
-- Tasks are created in SUSPENDED state by default.
-- RESUME activates the task to start running on its defined CRON schedule.

ALTER TASK order_silver_merge_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the procedure and task are set up correctly.

-- 1. Confirm the stored procedure was created
SHOW PROCEDURES LIKE 'merge_order_to_silver' IN SCHEMA pacificretail_db.silver;

-- 2. Confirm the task is in STARTED state and scheduled correctly
SHOW TASKS LIKE 'order_silver_merge_task' IN SCHEMA pacificretail_db.silver;

-- 3. Manually execute the procedure to test the Bronze → Silver pipeline
--    (run this after Bronze raw_order has data loaded)
CALL merge_order_to_silver();

-- 4. Confirm rows landed in silver.orders
SELECT COUNT(*) AS total_orders FROM silver.orders;

-- 5. Preview Silver orders data — verify DQ filters were applied correctly
--    Check: no NULL transaction_ids, no zero/negative total_amounts
SELECT
    COUNT(*)                                          AS total_rows,
    SUM(CASE WHEN transaction_id IS NULL THEN 1 END)  AS null_transaction_ids,
    SUM(CASE WHEN total_amount <= 0 THEN 1 END)       AS invalid_amounts,
    MIN(total_amount)                                 AS min_amount,
    MAX(total_amount)                                 AS max_amount,
    MIN(transaction_date)                             AS earliest_date,
    MAX(transaction_date)                             AS latest_date,
    COUNT(DISTINCT store_type)                        AS distinct_store_types,
    COUNT(DISTINCT payment_method)                    AS distinct_payment_methods
FROM silver.orders;

-- 6. Confirm the stream has been consumed (should return 0 after procedure runs)
SELECT COUNT(*) AS pending_rows FROM bronze.order_changes_stream;

-- 7. Confirm all three Silver tasks are active and correctly staggered
SHOW TASKS IN SCHEMA pacificretail_db.silver;

-- ============================================================
-- END OF SCRIPT — Silver layer complete
-- All three Silver merge procedures and tasks are now active:
--   silver_customer_merge_task → :00 every 4 hours
--   product_silver_merge_task  → :15 every 4 hours
--   order_silver_merge_task    → :30 every 2 hours
-- Next step → Run: 03_sql/04_gold/gold_view1_daily_sales.sql
-- ============================================================