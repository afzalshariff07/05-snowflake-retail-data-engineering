-- ============================================================
-- File        : stream_creation.sql
-- Folder      : 03_sql/03_silver/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script creates three Snowflake Streams — one on each Bronze table.
--   Streams are the Change Data Capture (CDC) mechanism that powers the
--   incremental Bronze → Silver pipeline.
--
--   A Stream monitors a table for new rows and maintains an offset pointer.
--   When a Stored Procedure reads from the stream and commits the transaction,
--   the offset advances — ensuring each new row is processed exactly once.
--
--   Three streams are created:
--     1. customer_changes_stream  → monitors bronze.raw_customer
--     2. product_changes_stream   → monitors bronze.raw_product
--     3. order_changes_stream     → monitors bronze.raw_order
--
-- How Streams fit in the pipeline:
--
--   ADLS Files
--      ↓  (COPY INTO via Bronze Tasks)
--   Bronze Tables  (raw_customer | raw_product | raw_order)
--      ↓  (CDC captured by Streams)
--   Streams        (customer_changes | product_changes | order_changes)
--      ↓  (consumed by Stored Procedures via Silver Tasks)
--   Silver Tables  (customer | product | orders)
--
-- Stream type used: APPEND_ONLY = TRUE
--   → Captures only INSERT operations (new rows appended by COPY INTO)
--   → Does not track UPDATE or DELETE operations on Bronze tables
--   → More efficient than the default stream type for Bronze layer use case
--      since Bronze tables are append-only by design (no updates/deletes)
--   → Reduces stream storage overhead vs. full CDC streams
--
-- Key Stream behaviour:
--   → Each stream maintains an independent offset pointer per consumer
--   → Once a Stored Procedure successfully reads and COMMITs from the stream,
--      those rows are marked as consumed and will not appear again
--   → If no new rows exist in the stream, the Silver Task will find it empty
--      and the MERGE will process zero rows (safe no-op behaviour)
--   → Streams are schema-aware — if the Bronze table schema changes,
--      the stream must be recreated
--
-- Prerequisites:
--   → silver_data_load.sql    must have been run (Silver tables must exist)
--   → Bronze tables must exist: raw_customer, raw_product, raw_order
--
-- Idempotent   : Yes — uses CREATE OR REPLACE; safe to re-run.
--                Note: Re-creating a stream resets its offset pointer,
--                meaning previously consumed rows may be re-processed.
-- Run Before   : 03_sql/03_silver/customer_transform.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- Streams are created in the BRONZE schema because they monitor Bronze tables.
-- The Silver Stored Procedures will reference these streams by their full path:
--   pacificretail_db.bronze.customer_changes_stream

USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 2: Create Stream on RAW_CUSTOMER ─────────────────────────────────────
-- Monitors bronze.raw_customer for newly appended rows.
-- Consumed by: process_customer_changes() Stored Procedure
-- Drives     : silver.customer table via MERGE (INSERT new / UPDATE existing)
--
-- APPEND_ONLY = TRUE:
--   Captures only rows added by the nightly COPY INTO customer task.
--   Since raw_customer is never directly updated or deleted, full CDC
--   tracking would add overhead with no benefit.

CREATE OR REPLACE STREAM customer_changes_stream
    ON TABLE raw_customer
    APPEND_ONLY = TRUE;


-- ── STEP 3: Create Stream on RAW_PRODUCT ──────────────────────────────────────
-- Monitors bronze.raw_product for newly appended rows.
-- Consumed by: merge_product_to_silver() Stored Procedure
-- Drives     : silver.product table via MERGE (INSERT new / UPDATE existing)
--
-- Product catalog updates (price changes, stock updates, new products)
-- are captured as new appended rows in raw_product and surfaced here.

CREATE OR REPLACE STREAM product_changes_stream
    ON TABLE raw_product
    APPEND_ONLY = TRUE;


-- ── STEP 4: Create Stream on RAW_ORDER ────────────────────────────────────────
-- Monitors bronze.raw_order for newly appended rows.
-- Consumed by: merge_order_to_silver() Stored Procedure
-- Drives     : silver.orders table via MERGE (INSERT new / UPDATE existing)
--
-- Orders are the highest-volume stream — the E-Commerce Platform generates
-- transactions continuously. The Silver task for orders runs every 2 hours
-- (more frequently than customer and product) to keep Silver near real-time.

CREATE OR REPLACE STREAM order_changes_stream
    ON TABLE raw_order
    APPEND_ONLY = TRUE;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm all three streams were created correctly.

-- 1. List all streams in the Bronze schema — all three should appear
SHOW STREAMS IN SCHEMA pacificretail_db.bronze;

-- 2. Check stream details — confirm APPEND_ONLY mode and source table for each
SELECT
    system$stream_get_table_timestamp('customer_changes_stream') AS customer_stream_offset,
    system$stream_get_table_timestamp('product_changes_stream')  AS product_stream_offset,
    system$stream_get_table_timestamp('order_changes_stream')    AS order_stream_offset;

-- 3. Preview rows currently visible in each stream
--    (will show rows if Bronze tables already have data loaded)
SELECT COUNT(*) AS pending_customer_rows FROM customer_changes_stream;
SELECT COUNT(*) AS pending_product_rows  FROM product_changes_stream;
SELECT COUNT(*) AS pending_order_rows    FROM order_changes_stream;

-- ============================================================
-- END OF SCRIPT — CDC Streams created on all Bronze tables
-- Next step → Run: 03_sql/03_silver/customer_transform.sql
-- ============================================================