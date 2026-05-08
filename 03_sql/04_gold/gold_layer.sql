-- ============================================================
-- File        : gold_layer.sql
-- Folder      : 03_sql/04_gold/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script serves two purposes:
--     1. GOLD SCHEMA CREATION  → Creates the Gold schema if it does not exist
--     2. PIPELINE ORCHESTRATION & TESTING → Manually triggers the full
--        Bronze → Silver pipeline end-to-end and validates data at each layer
--
--   Use this script when:
--     → Running the pipeline for the first time after setup
--     → Testing a fresh data load without waiting for scheduled Tasks
--     → Validating data quality and row counts across all three layers
--     → Debugging pipeline issues by inspecting layer-by-layer output
--
-- Full pipeline execution order (manual trigger):
--
--   STEP 1 : Create Gold schema
--   STEP 2 : Switch to Bronze → execute all three Bronze load tasks
--   STEP 3 : Verify Bronze tables (raw_customer, raw_product, raw_order)
--   STEP 4 : Inspect CDC Streams — confirm rows are pending for Silver
--   STEP 5 : Switch to Silver → execute all three Silver merge tasks
--   STEP 6 : Verify Silver tables (customer, product, orders)
--
-- Note:
--   In production, this manual execution is replaced by the scheduled Tasks.
--   This script is purely for development, testing, and demonstration purposes.
--   Gold layer VIEW creation is handled in separate scripts:
--     → gold_view1_daily_sales.sql
--     → gold_view2_customer_affinity.sql
--
-- Prerequisites:
--   → All Bronze scripts (01_setup + 02_bronze) must have been run
--   → All Silver scripts (03_silver) must have been run
--   → ADLS stage must be connected and files must exist in landing container
--
-- Idempotent   : Yes — Gold schema uses CREATE IF NOT EXISTS; safe to re-run.
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- PHASE 1 — GOLD SCHEMA SETUP
-- ════════════════════════════════════════════════════════════

-- ── STEP 1: Create Gold Schema ────────────────────────────────────────────────
-- The Gold schema is the business-ready analytical layer.
-- It holds the final views that Power BI, Tableau, and ML pipelines consume.
-- Created here if not already created by create_db_and_schemas.sql.
-- IF NOT EXISTS ensures this is safe to re-run without errors.

CREATE SCHEMA IF NOT EXISTS pacificretail_db.gold;


-- ════════════════════════════════════════════════════════════
-- PHASE 2 — BRONZE LAYER: TRIGGER + VERIFY
-- ════════════════════════════════════════════════════════════

-- ── STEP 2: Switch to Bronze schema context ───────────────────────────────────
USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 3: Manually trigger Bronze load tasks ────────────────────────────────
-- Normally these tasks run on their scheduled CRON (02:00, 03:00, 04:00 AM).
-- EXECUTE TASK forces an immediate run without waiting for the schedule —
-- useful for first-time setup, testing, and ad-hoc data refreshes.
--
-- Execution order matters:
--   Customer first  → most stable master data (changes least frequently)
--   Product second  → catalog data (changes moderately)
--   Orders last     → transactional data (highest volume, most recent)

EXECUTE TASK load_customer_data_task;   -- COPY CSV  → raw_customer  (scheduled: 02:00 AM)
EXECUTE TASK load_product_data_task;    -- COPY JSON → raw_product   (scheduled: 03:00 AM)
EXECUTE TASK load_order_data_task;      -- COPY Parquet → raw_order  (scheduled: 04:00 AM)


-- ── STEP 4: Verify Bronze layer data ──────────────────────────────────────────
-- After triggering the tasks, inspect each Bronze table to confirm:
--   → Rows were loaded from ADLS files
--   → Metadata columns (source_file_name, source_file_row_number) are populated
--   → Data looks correct before the Silver merge runs

-- Inspect raw customer records (CSV source — CRM system)
SELECT * FROM raw_customer LIMIT 20;

-- Inspect raw product records (JSON source — Inventory Management)
SELECT * FROM raw_product LIMIT 20;

-- Inspect raw order records (Parquet source — E-Commerce Platform)
SELECT * FROM raw_order LIMIT 20;


-- ════════════════════════════════════════════════════════════
-- PHASE 3 — STREAM INSPECTION
-- ════════════════════════════════════════════════════════════

-- ── STEP 5: Inspect CDC Streams ───────────────────────────────────────────────
-- After Bronze tasks load data, the Streams should capture those new rows.
-- SHOW STREAMS confirms all three streams exist and are APPEND_ONLY.
-- SELECT from each stream previews the pending rows waiting for Silver MERGE.
-- Row count should match what was loaded in the Bronze tasks above.

-- List all streams and confirm status (STALE = FALSE means stream is healthy)
SHOW STREAMS IN SCHEMA pacificretail_db.bronze;

-- Preview rows pending in the customer stream
-- (These rows will be consumed and cleared once Silver customer task runs)
SELECT * FROM customer_changes_stream LIMIT 20;


-- ════════════════════════════════════════════════════════════
-- PHASE 4 — SILVER LAYER: TRIGGER + VERIFY
-- ════════════════════════════════════════════════════════════

-- ── STEP 6: Switch to Silver schema context ───────────────────────────────────
USE DATABASE pacificretail_db;
USE SCHEMA silver;


-- ── STEP 7: Confirm Silver tasks exist and are active ─────────────────────────
-- Before executing manually, confirm all three Silver tasks are in STARTED state.
-- STATE should show 'started' — if 'suspended', run ALTER TASK ... RESUME first.

SHOW TASKS IN SCHEMA pacificretail_db.silver;


-- ── STEP 8: Manually trigger Silver merge tasks ───────────────────────────────
-- Normally these tasks run every 2–4 hours on their CRON schedules.
-- EXECUTE TASK forces an immediate run — each task calls its Stored Procedure
-- which reads the Bronze stream, applies DQ rules, and MERGEs into Silver.
--
-- Execution order:
--   Orders first   → highest volume, most time-sensitive (runs every 2 hrs in prod)
--   Product second → catalog updates (every 4 hrs in prod)
--   Customer last  → master data, lowest churn rate (every 4 hrs in prod)

EXECUTE TASK order_silver_merge_task;    -- calls merge_order_to_silver()
EXECUTE TASK product_silver_merge_task;  -- calls merge_product_to_silver()
EXECUTE TASK silver_customer_merge_task; -- calls process_customer_changes()


-- ── STEP 9: Verify Silver layer data ──────────────────────────────────────────
-- After triggering the Silver tasks, inspect each Silver table to confirm:
--   → Rows were MERGEd from Bronze streams
--   → DQ rules were applied (standardised customer_type, valid rating, etc.)
--   → Metadata columns (source_file_name, source_file_row_number) are gone
--   → last_updated_timestamp is populated with the MERGE execution time

-- Inspect clean customer records — check customer_type, gender, age validation
SELECT * FROM silver.customer LIMIT 20;

-- Inspect clean product records — check price, stock_quantity, rating validation
SELECT * FROM silver.product LIMIT 20;

-- Inspect validated order records — check total_amount > 0, transaction_id not null
SELECT * FROM silver.orders LIMIT 20;


-- ── STEP 10: Cross-layer row count summary ────────────────────────────────────
-- Compare row counts across Bronze and Silver to understand:
--   → How many rows were loaded from ADLS (Bronze count)
--   → How many rows passed DQ filters and landed in Silver (Silver count)
--   → The difference = rows rejected by DQ rules (investigate if high)

SELECT 'Bronze' AS layer, 'raw_customer' AS table_name, COUNT(*) AS row_count FROM raw_customer
UNION ALL
SELECT 'Bronze', 'raw_product',  COUNT(*) FROM raw_product
UNION ALL
SELECT 'Bronze', 'raw_order',    COUNT(*) FROM raw_order
UNION ALL
SELECT 'Silver', 'customer',     COUNT(*) FROM silver.customer
UNION ALL
SELECT 'Silver', 'product',      COUNT(*) FROM silver.product
UNION ALL
SELECT 'Silver', 'orders',       COUNT(*) FROM silver.orders
ORDER BY layer DESC, table_name;

-- ============================================================
-- END OF SCRIPT — Full pipeline triggered and verified
-- Bronze and Silver layers are loaded and ready.
-- Next step → Run: 03_sql/04_gold/gold_view1_daily_sales.sql
-- ============================================================