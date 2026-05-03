-- ============================================================
-- File        : create_db_and_schemas.sql
-- Folder      : 03_sql/01_setup/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This is the FIRST script to run in the pipeline setup.
--   It creates the top-level Snowflake database and the three
--   schemas that form the Medallion Architecture:
--
--     BRONZE  → Raw ingestion layer  (untransformed source data)
--     SILVER  → Conformed layer      (cleaned, validated, merged)
--     GOLD    → Analytical layer     (business-ready views for BI & ML)
--
-- Architecture:
--   PACIFICRETAIL_DB
--   ├── BRONZE   ← raw_customer | raw_product | raw_order
--   ├── SILVER   ← customer     | product     | orders
--   └── GOLD     ← VW_DAILY_SALES_ANALYSIS | VW_CUSTOMER_PRODUCT_AFFINITY
--
-- Prerequisites : None — this is the foundational setup script.
-- Run Before   : external_stage_creation.sql
-- Idempotent   : Yes — uses IF NOT EXISTS; safe to re-run.
-- ============================================================


-- ── STEP 1: Create the Database ────────────────────────────────────────────────
-- Creates the top-level database for the entire PacificRetail project.
-- IF NOT EXISTS ensures this script is safe to re-run without errors.

CREATE DATABASE IF NOT EXISTS pacificretail_db;


-- ── STEP 2: Switch context to the new database ─────────────────────────────────
-- All subsequent schema creation commands will execute inside PACIFICRETAIL_DB.

USE DATABASE pacificretail_db;


-- ── STEP 3: Create BRONZE Schema ───────────────────────────────────────────────
-- The Bronze schema is the raw ingestion layer.
-- Data lands here exactly as received from source systems — no transformations.
-- Supported source formats: CSV (customers), JSON (products), Parquet (orders).
-- Tables in this schema carry metadata columns for lineage tracking:
--   source_file_name, source_file_row_number, ingestion_timestamp.

CREATE SCHEMA IF NOT EXISTS bronze;


-- ── STEP 4: Create SILVER Schema ───────────────────────────────────────────────
-- The Silver schema is the conformed and cleaned layer.
-- Data is loaded here incrementally from Bronze using Snowflake Streams (CDC)
-- and Stored Procedures that apply data quality rules and MERGE logic.
-- Key transformations: null filtering, type standardisation, range validation.

CREATE SCHEMA IF NOT EXISTS silver;


-- ── STEP 5: Create GOLD Schema ─────────────────────────────────────────────────
-- The Gold schema is the business-ready analytical layer.
-- Contains views built on top of Silver tables, optimised for:
--   → BI tools       : Power BI, Tableau, Snowsight
--   → Self-service   : business analysts and operations teams
--   → ML pipelines   : feature engineering for recommendation and forecasting models

CREATE SCHEMA IF NOT EXISTS gold;


-- ── VERIFICATION ───────────────────────────────────────────────────────────────
-- Run the queries below to confirm all objects were created successfully.

SHOW DATABASES LIKE 'pacificretail_db';
SHOW SCHEMAS IN DATABASE pacificretail_db;

-- Expected output: 3 schemas visible → BRONZE | SILVER | GOLD
-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/01_setup/external_stage_creation.sql
-- ============================================================


