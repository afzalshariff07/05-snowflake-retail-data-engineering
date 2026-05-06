-- ============================================================
-- File        : customer_load.sql
-- Folder      : 03_sql/02_bronze/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script sets up the complete Bronze layer ingestion pipeline
--   for PacificRetail's customer data sourced from the CRM system.
--
--   It creates three objects in sequence:
--     1. FILE FORMAT   → Tells Snowflake how to parse the incoming CSV files
--     2. RAW_CUSTOMER  → Bronze table that stores raw customer records as-is
--     3. TASK          → Scheduled job that runs COPY INTO every night at 02:00 AM
--
-- Source System  : CRM System
-- Source Format  : CSV (comma-delimited, with header row)
-- Source Path    : @adls_stage/Customer/
-- Target Table   : PACIFICRETAIL_DB.BRONZE.RAW_CUSTOMER
-- Load Strategy  : Full file load — append only, no deduplication at Bronze
-- Schedule       : Daily at 02:00 AM (America/New_York)
--
-- Metadata columns captured for lineage:
--   source_file_name        → ADLS file path the row was loaded from
--   source_file_row_number  → Row position within the source file
--   ingestion_timestamp     → Timestamp when the row landed in Snowflake
--
-- Prerequisites:
--   → create_db_and_schemas.sql       must have been run
--   → external_stage_creation.sql    must have been run (adls_stage must exist)
--   → ACCOUNTADMIN or SYSADMIN role  required to create Tasks
--
-- Idempotent   : Partially — FILE FORMAT and TASK use CREATE OR REPLACE.
--                RAW_CUSTOMER uses CREATE IF NOT EXISTS (data preserved on re-run).
-- Run Before   : 03_sql/03_silver/silver_data_load.sql
-- ============================================================


-- ── STEP 1: Set context ────────────────────────────────────────────────────────
-- Ensure all objects are created in the correct database and schema.

USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 2: Create CSV File Format ────────────────────────────────────────────
-- A File Format object tells Snowflake exactly how to interpret the raw bytes
-- of incoming files before loading them into a table.
--
-- Key parameters explained:
--   TYPE = CSV                  → Source files are comma-separated values
--   FIELD_DELIMITER = ','       → Columns are separated by commas
--   SKIP_HEADER = 1             → First row is a header — skip it during load
--   NULL_IF = ('NULL','null','') → Treat these string values as SQL NULL
--   EMPTY_FIELD_AS_NULL = TRUE  → Empty fields (,,) are stored as NULL, not ''
--   COMPRESSION = AUTO          → Snowflake auto-detects if file is gzip/bzip2/etc.

CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE                = CSV
    FIELD_DELIMITER     = ','
    SKIP_HEADER         = 1
    NULL_IF             = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION         = AUTO;


-- ── STEP 3: Preview raw CSV data from ADLS stage ──────────────────────────────
-- Before creating the table, query the stage directly to verify:
--   a) The stage connection to ADLS is working
--   b) Column positions ($1, $2 ...) match the expected CSV layout
--   c) Data looks correct before committing to a table structure
--
-- Column mapping from CSV:
--   $1 → customer_id        $2 → name             $3 → email
--   $4 → country            $5 → customer_type     $6 → registration_date
--   $7 → age                $8 → gender            $9 → total_purchases

SELECT
    $1 AS customer_id,
    $2 AS name,
    $3 AS email,
    $4 AS country,
    $5 AS customer_type,
    $6 AS registration_date
FROM @pacificretail_stage/Customer
    (FILE_FORMAT => csv_file_format)
LIMIT 10;


-- ── STEP 4: Create Bronze RAW_CUSTOMER Table ───────────────────────────────────
-- This is the landing table for all raw customer data from the CRM system.
-- Design principles for Bronze:
--   → No transformations — data is stored exactly as received
--   → Permissive data types (STRING over ENUM) to avoid load rejections
--   → Three metadata columns appended for full data lineage tracking
--   → Append-only — existing rows are never updated or deleted at this layer
--
-- Column definitions:
--   customer_id         → Unique identifier for each customer (from CRM)
--   name                → Full name of the customer
--   email               → Customer email address (used as key in Silver MERGE)
--   country             → Country of residence
--   customer_type       → Raw value e.g. 'REG', 'PREM', 'R', 'P' — standardised in Silver
--   registration_date   → Date customer registered on the platform
--   age                 → Customer age — validated in Silver (must be 18–120)
--   gender              → Raw value e.g. 'M', 'F', 'MALE' — standardised in Silver
--   total_purchases     → Cumulative number of purchases made
--   source_file_name    → [METADATA] Full ADLS path of the source file
--   source_file_row_number → [METADATA] Row number within the source file
--   ingestion_timestamp → [METADATA] Auto-populated UTC timestamp of load time

CREATE TABLE IF NOT EXISTS raw_customer (
    customer_id             INT,
    name                    STRING,
    email                   STRING,
    country                 STRING,
    customer_type           STRING,
    registration_date       DATE,
    age                     INT,
    gender                  STRING,
    total_purchases         INT,
    source_file_name        STRING,
    source_file_row_number  INT,
    ingestion_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ── STEP 5: Create Scheduled Task — load_customer_data_task ───────────────────
-- A Snowflake Task is a scheduler object that runs a single SQL statement
-- (or a Stored Procedure call) on a defined schedule using a virtual warehouse.
--
-- Task configuration:
--   WAREHOUSE = compute_wh              → Virtual warehouse used to execute the load
--   SCHEDULE = 'USING CRON 0 2 * * *'  → Runs every day at 02:00 AM
--   America/New_York                    → Timezone for the CRON expression
--
-- CRON expression breakdown:
--   0 2 * * *
--   │ │ │ │ └── Day of week  : every day (*)
--   │ │ │ └──── Month        : every month (*)
--   │ │ └────── Day of month : every day (*)
--   │ └──────── Hour         : 02 (2 AM)
--   └────────── Minute       : 00
--
-- The task body is a COPY INTO statement that:
--   → Reads all .csv files from @adls_stage/Customer/
--   → Extracts columns by positional reference ($1 through $9)
--   → Casts $6 explicitly to DATE (registration_date)
--   → Captures ADLS metadata: filename and row number for lineage
--   → ON_ERROR = 'CONTINUE' — skips bad rows and logs them instead of aborting
--   → PATTERN = '.*[.]csv' — only processes .csv files, ignores other file types

CREATE OR REPLACE TASK load_customer_data_task
    WAREHOUSE = compute_wh
    SCHEDULE  = 'USING CRON 0 2 * * * America/New_York'
AS
    COPY INTO raw_customer (
        customer_id,
        name,
        email,
        country,
        customer_type,
        registration_date,
        age,
        gender,
        total_purchases,
        source_file_name,
        source_file_row_number
    )
    FROM (
        SELECT
            $1,                       -- customer_id
            $2,                       -- name
            $3,                       -- email
            $4,                       -- country
            $5,                       -- customer_type
            $6::DATE,                 -- registration_date (explicit cast required)
            $7,                       -- age
            $8,                       -- gender
            $9,                       -- total_purchases
            metadata$filename,        -- source file path from ADLS (lineage)
            metadata$file_row_number  -- row position in source file (lineage)
        FROM @pacificretail_stage/Customer/
    )
    FILE_FORMAT = (FORMAT_NAME = 'csv_file_format')
    ON_ERROR    = 'CONTINUE'
    PATTERN     = '.*[.]csv';


-- ── STEP 6: Activate the Task ─────────────────────────────────────────────────
-- Snowflake Tasks are created in SUSPENDED state by default.
-- RESUME activates the task so it starts executing on its defined schedule.
-- Without this command, the task exists but will never run automatically.

ALTER TASK load_customer_data_task RESUME;


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm everything was set up correctly.

-- 1. Confirm file format exists
SHOW FILE FORMATS LIKE 'csv_file_format' IN SCHEMA pacificretail_db.bronze;

-- 2. Confirm table was created with correct structure
DESCRIBE TABLE raw_customer;

-- 3. Confirm task is scheduled and in STARTED state
SHOW TASKS LIKE 'load_customer_data_task' IN SCHEMA pacificretail_db.bronze;

-- 4. Trigger the task manually to test the load without waiting for the schedule
EXECUTE TASK load_customer_data_task;

-- 5. Check rows loaded into raw_customer
SELECT COUNT(*) AS total_rows FROM raw_customer;

-- 6. Preview the loaded data including metadata columns
SELECT * FROM raw_customer LIMIT 10;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/02_bronze/product_load.sql
-- ============================================================