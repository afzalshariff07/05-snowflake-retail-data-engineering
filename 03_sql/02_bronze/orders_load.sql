-- ============================================================
-- File        : orders_load.sql
-- Folder      : 03_sql/02_bronze/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script sets up the complete Bronze layer ingestion pipeline
--   for PacificRetail's order/transaction data sourced from the
--   E-Commerce Platform.
--
--   It creates three objects in sequence:
--     1. FILE FORMAT  → Tells Snowflake how to parse incoming Parquet files
--     2. RAW_ORDER    → Bronze table that stores raw transaction records as-is
--     3. TASK         → Scheduled job that runs COPY INTO every day at 04:00 AM
--
-- Source System  : E-Commerce Platform
-- Source Format  : Parquet (Snappy-compressed columnar format)
-- Source Path    : @pacificretail_stage/Order/
-- Target Table   : PACIFICRETAIL_DB.BRONZE.RAW_ORDER
-- Load Strategy  : Full file load — append only, no deduplication at Bronze
-- Schedule       : Daily at 04:00 AM (America/New_York)
--                  (Staggered — 1 hour after product load at 03:00 AM)
--
-- Key difference from CSV and JSON loads:
--   Parquet is a binary columnar format — fields are self-describing with
--   embedded schema (column names and data types stored inside the file).
--   Like JSON, fields are accessed using $1:field_name syntax, but no
--   STRIP_OUTER_ARRAY is needed as Parquet files are inherently row-based.
--   Snowflake reads Parquet natively without requiring format conversion.
--
-- Metadata columns captured for lineage:
--   source_file_name        → ADLS file path the row was loaded from
--   source_file_row_number  → Row position within the source file
--   ingestion_timestamp     → Timestamp when the row landed in Snowflake
--
-- Prerequisites:
--   → create_db_and_schemas.sql       must have been run
--   → external_stage_creation.sql     must have been run (adls_stage i.e., pacificretail_stage must exist)
--   → customer_load.sql               must have been run before this
--   → product_load.sql                must have been run before this
--   → ACCOUNTADMIN or SYSADMIN role   required to create Tasks
--
-- Idempotent   : No — RAW_ORDER uses CREATE OR REPLACE (existing data is dropped).
--                Re-running this script will truncate and recreate the table.
-- Run Before   : 03_sql/03_silver/silver_data_load.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- Ensure all objects are created in the correct database and schema.

USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 2: Create Parquet File Format ────────────────────────────────────────
-- A File Format object tells Snowflake how to interpret incoming Parquet files.
--
-- Key parameters explained:
--   TYPE = PARQUET            → Source files are in Apache Parquet binary format
--   COMPRESSION = AUTO        → Snowflake auto-detects compression codec
--                               (Snappy is the most common; also supports GZIP, LZO)
--                               PacificRetail order files use Snappy compression.
--   BINARY_AS_TEXT = FALSE    → Binary columns are kept as BINARY type, not
--                               interpreted as text strings. Important for data
--                               integrity — prevents silent data corruption on
--                               binary fields like checksums or encoded IDs.
--   TRIM_SPACE = FALSE        → Whitespace in string fields is preserved as-is.
--                               Trimming is deferred to the Silver layer where
--                               deliberate data cleaning takes place.

CREATE OR REPLACE FILE FORMAT parquet_file_format
    TYPE           = PARQUET
    COMPRESSION    = AUTO
    BINARY_AS_TEXT = FALSE
    TRIM_SPACE     = FALSE;


-- ── STEP 3: Preview raw Parquet data from ADLS stage ──────────────────────────
-- Before creating the table, query the stage directly to verify:
--   a) The stage connection to ADLS is working for the Order folder
--   b) Parquet schema is as expected — SELECT * returns all embedded columns
--   c) Data types look correct (Parquet preserves types from the source system)
--
-- Unlike CSV (positional) and JSON (key-name extraction), Parquet supports
-- SELECT * because column names and types are embedded in the file format itself.
-- This makes the preview richer — you see actual column names, not $1, $2 etc.

SELECT
    *
FROM @pacificretail_stage/Order/
    (FILE_FORMAT => parquet_file_format)
LIMIT 10;


-- ── STEP 4: Create Bronze RAW_ORDER Table ─────────────────────────────────────
-- This is the landing table for all raw transaction data from the
-- E-Commerce Platform. Note: CREATE OR REPLACE is used here (not IF NOT EXISTS)
-- meaning re-running this script will DROP and recreate the table.
--
-- Design principles for Bronze:
--   → No transformations — data stored exactly as received from the platform
--   → Column order reflects business entity grouping (IDs → attributes → amounts → dates)
--   → Three metadata columns appended for full data lineage tracking
--   → Append-only intent — new files add rows, existing rows not modified
--
-- Column definitions:
--   transaction_id      → Unique identifier for each order transaction (key for Silver MERGE)
--                         NULL transactions are filtered out in Silver
--   customer_id         → Foreign key linking to raw_customer / silver.customer
--   product_id          → Foreign key linking to raw_product / silver.product
--   quantity            → Number of units purchased in this transaction
--   store_type          → Sales channel e.g. 'Online', 'In-Store', 'Mobile App'
--   total_amount        → Total transaction value — Silver filters out negatives and zeros
--   transaction_date    → Date the transaction occurred
--   payment_method      → Payment type e.g. 'Credit Card', 'PayPal', 'Bank Transfer'
--   source_file_name    → [METADATA] Full ADLS path of the source Parquet file
--   source_file_row_number → [METADATA] Row position within the Parquet file
--   ingestion_timestamp → [METADATA] Auto-populated UTC timestamp of load time

CREATE OR REPLACE TABLE raw_order (
    transaction_id          STRING,
    customer_id             INT,
    product_id              INT,
    quantity                INT,
    store_type              STRING,
    total_amount            DOUBLE,
    transaction_date        DATE,
    payment_method          STRING,
    source_file_name        STRING,
    source_file_row_number  INT,
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ── STEP 5: Create Scheduled Task — load_order_data_task ──────────────────────
-- A Snowflake Task is a scheduler object that runs a SQL statement
-- on a defined schedule using a virtual warehouse.
--
-- Task configuration:
--   WAREHOUSE = compute_wh              → Virtual warehouse used to execute the load
--   SCHEDULE = 'USING CRON 0 4 * * *'  → Runs every day at 04:00 AM
--   America/New_York                    → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   0 4 * * *
--   │ │ │ │ └── Day of week  : every day (*)
--   │ │ │ └──── Month        : every month (*)
--   │ │ └────── Day of month : every day (*)
--   │ └──────── Hour         : 04 (4 AM) — staggered after customer (02 AM) and product (03 AM)
--   └────────── Minute       : 00
--
-- Parquet-specific COPY INTO behaviour:
--   Like JSON, Parquet fields are extracted by KEY NAME using $1:field_name syntax.
--   However, since Parquet embeds its own schema, the types are already known —
--   the explicit ::DATATYPE casts below override or confirm the Parquet-native types
--   to ensure Snowflake stores them exactly as intended in the target table.
--
--   ON_ERROR = 'CONTINUE'   → Skips rows with extraction errors, logs them, continues
--   PATTERN = '.*[.]parquet' → Only processes .parquet files, ignores other file types
--                              including staging/temp files that may land in the folder

CREATE OR REPLACE TASK load_order_data_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 0 4 * * * America/New_York'
AS
    COPY INTO raw_order (
        customer_id,
        payment_method,
        product_id,
        quantity,
        store_type,
        total_amount,
        transaction_date,
        transaction_id,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1:customer_id::INT,            -- foreign key → silver.customer
            $1:payment_method::STRING,      -- payment type e.g. Credit Card, PayPal
            $1:product_id::INT,             -- foreign key → silver.product
            $1:quantity::INT,               -- units purchased in this transaction
            $1:store_type::STRING,          -- sales channel e.g. Online, In-Store
            $1:total_amount::DOUBLE,        -- transaction value (Silver filters negatives/zeros)
            $1:transaction_date::DATE,      -- date of transaction
            $1:transaction_id::STRING,      -- unique transaction ID (key for Silver MERGE)
            METADATA$FILENAME,              -- source file path from ADLS (lineage)
            METADATA$FILE_ROW_NUMBER        -- row position in source file (lineage)
        FROM @pacificretail_stage/Order/
    )
    FILE_FORMAT = (FORMAT_NAME = 'parquet_file_format')
    ON_ERROR    = 'CONTINUE'
    PATTERN     = '.*[.]parquet';


-- ── STEP 6: Activate the Task ─────────────────────────────────────────────────
-- Snowflake Tasks are created in SUSPENDED state by default.
-- RESUME activates the task so it starts executing on its defined schedule.
-- Without this command, the task exists but will never run automatically.

ALTER TASK load_order_data_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm everything was set up correctly.

-- 1. Confirm file format exists
SHOW FILE FORMATS LIKE 'parquet_file_format' IN SCHEMA pacificretail_db.bronze;

-- 2. Confirm table was created with correct structure
DESCRIBE TABLE raw_order;

-- 3. Confirm task is scheduled and in STARTED state
SHOW TASKS LIKE 'load_order_data_task' IN SCHEMA pacificretail_db.bronze;

-- 4. Trigger the task manually to test the load without waiting for the schedule
EXECUTE TASK load_order_data_task;

-- 5. Check rows loaded into raw_order
SELECT COUNT(*) AS total_rows FROM raw_order;

-- 6. Preview the loaded data including metadata columns
SELECT * FROM raw_order LIMIT 10;

-- 7. Confirm all three Bronze tasks are active and scheduled correctly
SHOW TASKS IN SCHEMA pacificretail_db.bronze;

-- ============================================================
-- END OF SCRIPT — Bronze layer complete
-- All three raw tables are now loaded and scheduled:
--   raw_customer  → 02:00 AM daily
--   raw_product   → 03:00 AM daily
--   raw_order     → 04:00 AM daily
-- Next step → Run: 03_sql/03_silver/silver_data_load.sql
-- ============================================================