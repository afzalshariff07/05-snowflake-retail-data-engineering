-- ============================================================
-- File        : product_load.sql
-- Folder      : 03_sql/02_bronze/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script sets up the complete Bronze layer ingestion pipeline
--   for PacificRetail's product catalog sourced from the Inventory
--   Management System.
--
--   It creates three objects in sequence:
--     1. FILE FORMAT  → Tells Snowflake how to parse incoming JSON files
--     2. RAW_PRODUCT  → Bronze table that stores raw product records as-is
--     3. TASK         → Scheduled job that runs COPY INTO every day at 03:00 AM
--
-- Source System  : Inventory Management System
-- Source Format  : JSON (array of product objects, UTF-8 encoded)
-- Source Path    : @pacificretail_stage/Product/
-- Target Table   : PACIFICRETAIL_DB.BRONZE.RAW_PRODUCT
-- Load Strategy  : Full file load — append only, no deduplication at Bronze
-- Schedule       : Daily at 03:00 AM (America/New_York)
--                  (1 hour after customer load to avoid warehouse contention)
--
-- Key difference from CSV load:
--   JSON files require extracting fields by KEY NAME using $1:field_name syntax
--   rather than by positional reference ($1, $2 ...) used in CSV loads.
--
-- Metadata columns captured for lineage:
--   source_file_name        → ADLS file path the row was loaded from
--   source_file_row_number  → Row position within the source file
--   ingestion_timestamp     → Timestamp when the row landed in Snowflake
--
-- Prerequisites:
--   → create_db_and_schemas.sql       must have been run
--   → external_stage_creation.sql     must have been run (pacificretail_stage (ADLS) must exist)
--   → customer_load.sql               recommended to run first (maintains order)
--   → ACCOUNTADMIN or SYSADMIN role   required to create Tasks
--
-- Idempotent   : Partially — FILE FORMAT and TASK use CREATE OR REPLACE.
--                RAW_PRODUCT uses CREATE IF NOT EXISTS (data preserved on re-run).
-- Run Before   : 03_sql/02_bronze/orders_load.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- Ensure all objects are created in the correct database and schema.

USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 2: Create JSON File Format ───────────────────────────────────────────
-- A File Format object tells Snowflake how to interpret the raw bytes
-- of incoming JSON files before loading them into a table.
--
-- Key parameters explained:
--   TYPE = JSON               → Source files are JSON format
--   STRIP_OUTER_ARRAY = TRUE  → The JSON file contains a top-level array [ {...}, {...} ]
--                               This setting strips the outer brackets so each object
--                               inside the array is treated as a separate row.
--                               Without this, the entire file loads as a single VARIANT row.
--   IGNORE_UTF8_ERRORS = TRUE → Skips characters that are not valid UTF-8 instead of
--                               aborting the load — important for product names that may
--                               contain special characters from international markets.

CREATE OR REPLACE FILE FORMAT json_file_format
    TYPE               = JSON
    STRIP_OUTER_ARRAY  = TRUE
    IGNORE_UTF8_ERRORS = TRUE;


-- ── STEP 3: Preview raw JSON data from ADLS stage ─────────────────────────────
-- Before creating the table, query the stage directly to verify:
--   a) The stage connection to ADLS is working for the Product folder
--   b) JSON structure is as expected (object keys visible in $1)
--   c) STRIP_OUTER_ARRAY is working — each row should show one product object
--
-- In JSON loads, $1 represents the entire JSON object for that row.
-- Fields are accessed using dot notation: $1:field_name
-- Example output: { "product_id": 101, "name": "Laptop Pro", "category": "Electronics", ... }

SELECT
    $1
FROM @pacificretail_stage/Product/
    (FILE_FORMAT => json_file_format)
LIMIT 10;


-- ── STEP 4: Create Bronze RAW_PRODUCT Table ────────────────────────────────────
-- This is the landing table for all raw product catalog data from the
-- Inventory Management System.
--
-- Design principles for Bronze:
--   → No transformations — data stored exactly as received from source
--   → Permissive data types used where possible to avoid load rejections
--   → Three metadata columns appended for full data lineage tracking
--   → Append-only — existing rows are never updated or deleted at this layer
--
-- Column definitions:
--   product_id          → Unique identifier for each product (from Inventory system)
--   name                → Product display name (may contain special characters)
--   category            → Product category e.g. Electronics, Clothing, Home & Garden
--   brand               → Brand or manufacturer name
--   price               → Listed price — validated in Silver (must not be negative)
--   stock_quantity      → Current inventory count — validated in Silver (must not be negative)
--   rating              → Customer rating score — validated in Silver (must be 0.0–5.0)
--   is_active           → Flag indicating if product is live on the platform (TRUE/FALSE)
--   source_file_name    → [METADATA] Full ADLS path of the source JSON file
--   source_file_row_number → [METADATA] Object position within the JSON array
--   ingestion_timestamp → [METADATA] Auto-populated UTC timestamp of load time

CREATE TABLE IF NOT EXISTS raw_product (
    product_id              INT,
    name                    STRING,
    category                STRING,
    brand                   STRING,
    price                   FLOAT,
    stock_quantity          INT,
    rating                  FLOAT,
    is_active               BOOLEAN,
    source_file_name        STRING,
    source_file_row_number  INT,
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ── STEP 5: Create Scheduled Task — load_product_data_task ────────────────────
-- A Snowflake Task is a scheduler object that runs a SQL statement
-- on a defined schedule using a virtual warehouse.
--
-- Task configuration:
--   WAREHOUSE = compute_wh              → Virtual warehouse used to execute the load
--   SCHEDULE = 'USING CRON 0 3 * * *'  → Runs every day at 03:00 AM
--   America/New_York                    → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   0 3 * * *
--   │ │ │ │ └── Day of week  : every day (*)
--   │ │ │ └──── Month        : every month (*)
--   │ │ └────── Day of month : every day (*)
--   │ └──────── Hour         : 03 (3 AM) — 1 hour after customer load (02:00 AM)
--   └────────── Minute       : 00
--
-- JSON-specific COPY INTO behaviour:
--   Unlike CSV ($1, $2, $3 ...), JSON fields are extracted by KEY NAME
--   using the syntax: $1:field_name::DATATYPE
--   $1 represents the full JSON object for each row (one product per row
--   after STRIP_OUTER_ARRAY removes the enclosing array brackets).
--
--   ON_ERROR = 'CONTINUE' — skips malformed JSON objects, logs them, continues
--   PATTERN = '.*[.]json' — only processes .json files, ignores other file types

CREATE OR REPLACE TASK load_product_data_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 0 3 * * * America/New_York'
AS
    COPY INTO raw_product (
        product_id,
        name,
        category,
        brand,
        price,
        stock_quantity,
        rating,
        is_active,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1:product_id::INT,         -- unique product identifier
            $1:name::STRING,            -- product display name
            $1:category::STRING,        -- product category
            $1:brand::STRING,           -- brand / manufacturer
            $1:price::FLOAT,            -- listed price (validated in Silver)
            $1:stock_quantity::INT,     -- inventory count (validated in Silver)
            $1:rating::FLOAT,           -- customer rating 0–5 (validated in Silver)
            $1:is_active::BOOLEAN,      -- product live status flag
            metadata$filename,          -- source file path from ADLS (lineage)
            metadata$file_row_number    -- row position in source file (lineage)
        FROM @pacificretail_stage/Product/
    )
    FILE_FORMAT = (FORMAT_NAME = 'json_file_format')
    ON_ERROR    = 'CONTINUE'
    PATTERN     = '.*[.]json';


-- ── STEP 6: Activate the Task ─────────────────────────────────────────────────
-- Snowflake Tasks are created in SUSPENDED state by default.
-- RESUME activates the task so it starts executing on its defined schedule.
-- Without this command, the task exists but will never run automatically.

ALTER TASK load_product_data_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm everything was set up correctly.

-- 1. Confirm file format exists
SHOW FILE FORMATS LIKE 'json_file_format' IN SCHEMA pacificretail_db.bronze;

-- 2. Confirm table was created with correct structure
DESCRIBE TABLE raw_product;

-- 3. Confirm task is scheduled and in STARTED state
SHOW TASKS LIKE 'load_product_data_task' IN SCHEMA pacificretail_db.bronze;

-- 4. Trigger the task manually to test the load without waiting for the schedule
EXECUTE TASK load_product_data_task;

-- 5. Check rows loaded into raw_product
SELECT COUNT(*) AS total_rows FROM raw_product;

-- 6. Preview the loaded data including metadata columns
SELECT * FROM raw_product LIMIT 10;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/02_bronze/orders_load.sql
-- ============================================================