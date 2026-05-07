-- ============================================================
-- File        : customer_transform.sql
-- Folder      : 03_sql/03_silver/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates the Silver layer transformation pipeline for
--   customer data — a Stored Procedure and a scheduled Task that together
--   move clean, validated customer records from Bronze to Silver.
--
--   Two objects are created:
--     1. STORED PROCEDURE  → process_customer_changes()
--                            Reads from customer_changes_stream, applies
--                            data quality rules, and MERGEs into silver.customer
--     2. TASK              → silver_customer_merge_task
--                            Calls the stored procedure every 4 hours
--
-- How it works (end-to-end):
--   bronze.raw_customer (new rows)
--      ↓  captured by
--   bronze.customer_changes_stream
--      ↓  read and transformed by
--   process_customer_changes() Stored Procedure
--      ↓  applies DQ rules + MERGE
--   silver.customer
--      ↓  (INSERT new customers / UPDATE existing customers)
--
-- Data Quality Rules applied inside the Stored Procedure:
--   ┌─────────────────────┬────────────────────────────────────────────────────┐
--   │ Column              │ Rule                                               │
--   ├─────────────────────┼────────────────────────────────────────────────────┤
--   │ customer_id         │ NULL → row excluded from Silver entirely           │
--   │ email               │ NULL → row excluded from Silver entirely           │
--   │ customer_type       │ REG/R/REGULAR  → 'Regular'                        │
--   │                     │ PREM/P/PREMIUM → 'Premium'                        │
--   │                     │ anything else  → 'Unknown'                        │
--   │ age                 │ valid range 18–120 → kept; out of range → NULL    │
--   │ gender              │ M/MALE   → 'Male'                                 │
--   │                     │ F/FEMALE → 'Female'                               │
--   │                     │ else     → 'Other'                                │
--   │ total_purchases     │ negative → set to 0                               │
--   └─────────────────────┴────────────────────────────────────────────────────┘
--
-- MERGE logic:
--   WHEN MATCHED     → UPDATE all fields (customer record already exists in Silver)
--   WHEN NOT MATCHED → INSERT new row (first time this customer_id is seen)
--   Match key        → customer_id
--
-- Schedule  : Every 4 hours (America/New_York)
-- Warehouse : compute_wh
--
-- Prerequisites:
--   → silver_data_load.sql   must have been run (silver.customer table exists)
--   → stream_creation.sql    must have been run (customer_changes_stream exists)
--
-- Idempotent   : Yes — uses CREATE OR REPLACE for both objects; safe to re-run.
-- Run Before   : 03_sql/03_silver/product_transform.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- All objects created in SILVER schema; stream referenced from BRONZE schema.

USE DATABASE pacificretail_db;
USE SCHEMA silver;


-- ── STEP 2: Create Stored Procedure — process_customer_changes() ───────────────
-- This procedure encapsulates the full Bronze → Silver transformation logic
-- for customer data. It is called by the Task defined in Step 3.
--
-- Procedure signature:
--   RETURNS STRING   → Returns a confirmation message on successful execution
--   LANGUAGE SQL     → Written in Snowflake SQL scripting (not JavaScript/Python)
--
-- Internal variables:
--   rows_inserted    → Placeholder for tracking inserted rows (extensible for logging)
--   rows_updated     → Placeholder for tracking updated rows (extensible for logging)

CREATE OR REPLACE PROCEDURE process_customer_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT;  -- tracks number of new customer rows inserted into Silver
    rows_updated  INT;  -- tracks number of existing customer rows updated in Silver

BEGIN

    -- ── MERGE: Bronze Stream → Silver Customer ─────────────────────────────────
    -- The MERGE statement is the core of this procedure. It reads all pending
    -- rows from customer_changes_stream (new rows appended to raw_customer since
    -- the last successful run), applies data quality transformations inline,
    -- and upserts the results into silver.customer.
    --
    -- SOURCE: customer_changes_stream (filtered and transformed inline)
    -- TARGET: silver.customer
    -- MATCH KEY: customer_id

    MERGE INTO silver.customer AS target
    USING (

        SELECT
            customer_id,
            name,
            email,
            country,

            -- ── DQ Rule: Customer Type Standardisation ─────────────────────────
            -- Raw values from CRM are inconsistent across regions and entry methods.
            -- TRIM removes leading/trailing spaces; UPPER normalises case before matching.
            -- Mapped values:
            --   'REGULAR', 'REG', 'R'    → 'Regular'
            --   'PREMIUM', 'PREM', 'P'   → 'Premium'
            --   anything else (incl NULL) → 'Unknown'
            CASE
                WHEN TRIM(UPPER(customer_type)) IN ('REGULAR', 'REG', 'R')    THEN 'Regular'
                WHEN TRIM(UPPER(customer_type)) IN ('PREMIUM', 'PREM', 'P')   THEN 'Premium'
                ELSE 'Unknown'
            END AS customer_type,

            registration_date,

            -- ── DQ Rule: Age Validation ────────────────────────────────────────
            -- Valid customer age range is 18 (minimum legal age) to 120 (data cap).
            -- Out-of-range values (e.g. 0, 999, negative) are set to NULL rather
            -- than dropping the row — the customer record is still valid overall.
            CASE
                WHEN age BETWEEN 18 AND 120 THEN age
                ELSE NULL
            END AS age,

            -- ── DQ Rule: Gender Standardisation ───────────────────────────────
            -- Raw values from CRM use various abbreviations and formats globally.
            -- TRIM and UPPER normalise before matching to avoid case/space mismatches.
            -- Mapped values:
            --   'M', 'MALE'    → 'Male'
            --   'F', 'FEMALE'  → 'Female'
            --   anything else  → 'Other'  (includes NULL, blank, non-binary entries)
            CASE
                WHEN TRIM(UPPER(gender)) IN ('M', 'MALE')   THEN 'Male'
                WHEN TRIM(UPPER(gender)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'Other'
            END AS gender,

            -- ── DQ Rule: Total Purchases Floor ────────────────────────────────
            -- Negative purchase counts are data errors from the CRM system.
            -- Floor is set to 0 — a customer cannot have fewer than zero purchases.
            CASE
                WHEN total_purchases >= 0 THEN total_purchases
                ELSE 0
            END AS total_purchases,

            CURRENT_TIMESTAMP() AS last_updated_timestamp  -- MERGE execution time

        FROM bronze.customer_changes_stream
        WHERE customer_id IS NOT NULL   -- DQ Rule: exclude rows with no customer ID
          AND email IS NOT NULL         -- DQ Rule: exclude rows with no email address

    ) AS source

    -- Match condition: if customer_id already exists in Silver → UPDATE
    --                  if customer_id is new                   → INSERT
    ON target.customer_id = source.customer_id

    -- ── WHEN MATCHED: Update existing customer record ──────────────────────────
    -- Customer already exists in silver.customer — update all fields to reflect
    -- the latest values from the CRM system (e.g. email change, country update).
    -- last_updated_timestamp is refreshed to record when the update occurred.
    WHEN MATCHED THEN
        UPDATE SET
            name                   = source.name,
            email                  = source.email,
            country                = source.country,
            customer_type          = source.customer_type,
            registration_date      = source.registration_date,
            age                    = source.age,
            gender                 = source.gender,
            total_purchases        = source.total_purchases,
            last_updated_timestamp = source.last_updated_timestamp

    -- ── WHEN NOT MATCHED: Insert new customer record ───────────────────────────
    -- customer_id not found in silver.customer — this is a new customer.
    -- All fields including the transformed/validated values are inserted.
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id, name, email, country, customer_type,
            registration_date, age, gender, total_purchases, last_updated_timestamp
        )
        VALUES (
            source.customer_id, source.name, source.email, source.country, source.customer_type,
            source.registration_date, source.age, source.gender, source.total_purchases, source.last_updated_timestamp
        );

    -- Return a confirmation message on successful completion
    RETURN 'Customers processed successfully';

END;
$$;


-- ── STEP 3: Create Scheduled Task — silver_customer_merge_task ────────────────
-- A Snowflake Task that calls process_customer_changes() on a schedule.
-- Using CALL inside a Task allows the full procedure logic (DECLARE, BEGIN, MERGE)
-- to run as a single atomic unit on each execution.
--
-- Task configuration:
--   WAREHOUSE = compute_wh                → Virtual warehouse for execution
--   SCHEDULE = 'USING CRON 0 */4 * * *'  → Every 4 hours
--   America/New_York                      → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   0 */4 * * *
--   │  │   │ │ └── Day of week  : every day (*)
--   │  │   │ └──── Month        : every month (*)
--   │  │   └────── Day of month : every day (*)
--   │  └────────── Hour         : every 4th hour (*/4 = 00:00, 04:00, 08:00 ...)
--   └──────────── Minute        : 00
--
-- Runs at: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 (6 times per day)
-- This ensures Silver customer data is at most 4 hours behind Bronze.

CREATE OR REPLACE TASK silver_customer_merge_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 0 */4 * * * America/New_York'
AS
    CALL process_customer_changes();


-- ── STEP 4: Activate the Task ─────────────────────────────────────────────────
-- Tasks are created in SUSPENDED state by default.
-- RESUME activates the task to start running on its defined CRON schedule.

ALTER TASK silver_customer_merge_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the procedure and task are set up correctly.

-- 1. Confirm the stored procedure was created
SHOW PROCEDURES LIKE 'process_customer_changes' IN SCHEMA pacificretail_db.silver;

-- 2. Confirm the task is in STARTED state and scheduled correctly
SHOW TASKS LIKE 'silver_customer_merge_task' IN SCHEMA pacificretail_db.silver;

-- 3. Manually execute the procedure to test the Bronze → Silver pipeline
--    (run this after Bronze raw_customer has data loaded)
CALL process_customer_changes();

-- 4. Confirm rows landed in silver.customer
SELECT COUNT(*) AS total_customers FROM silver.customer;

-- 5. Preview Silver customer data — verify DQ rules were applied correctly
--    Check: customer_type should be Regular/Premium/Unknown only
--           gender should be Male/Female/Other only
--           age should be NULL for any out-of-range values
SELECT
    customer_type,
    gender,
    COUNT(*)          AS row_count,
    MIN(age)          AS min_age,
    MAX(age)          AS max_age,
    MIN(total_purchases) AS min_purchases
FROM silver.customer
GROUP BY customer_type, gender
ORDER BY customer_type, gender;

-- 6. Confirm the stream has been consumed (should return 0 after procedure runs)
SELECT COUNT(*) AS pending_rows FROM bronze.customer_changes_stream;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/03_silver/product_transform.sql
-- ============================================================