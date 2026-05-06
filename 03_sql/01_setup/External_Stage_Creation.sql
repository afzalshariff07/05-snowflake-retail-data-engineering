-- ============================================================
-- File        : external_stage_creation.sql
-- Folder      : 03_sql/01_setup/
-- Project     : PacificRetail — End-to-End Snowflake Data Engineering
-- Author      : Mohammed Afzal Shariff
-- LinkedIn    : https://www.linkedin.com/in/mohammed-afzal-shariff/
-- GitHub      : https://github.com/afzalshariff07
-- ============================================================
-- Purpose:
--   This script establishes a secure, authenticated connection between
--   Snowflake and Azure Data Lake Storage (ADLS Gen2) using a
--   Storage Integration object.
--
--   A Storage Integration is the recommended approach by Snowflake
--   for connecting to cloud storage — it avoids embedding credentials
--   (SAS tokens or access keys) directly in the stage definition,
--   making it more secure and easier to manage at an enterprise level.
--
-- What this script creates:
--   1. STORAGE INTEGRATION  → Trusted link between Snowflake & Azure ADLS
--   2. EXTERNAL STAGE       → Named pointer to the ADLS container path
--                             used by COPY INTO commands in Bronze layer
--
-- Architecture position:
--   ADLS Gen2 (landing/)  ←──[this script]──→  Snowflake Bronze Schema
--
-- Prerequisites:
--   → create_db_and_schemas.sql must have been run first
--   → Azure Storage Account must already exist
--   → You must have Owner / Contributor rights in Azure to assign IAM roles
--
-- Post-run Manual Step (REQUIRED):
--   After running STEP 1 below, execute:
--     DESC INTEGRATION azure_pacificretail_integration;
--   Copy the value of AZURE_MULTI_TENANT_APP_NAME and go to Azure Portal:
--     → Storage Account → IAM → Add Role Assignment
--     → Role: Storage Blob Data Contributor
--     → Assign to: the Snowflake multi-tenant app (copied above)
--   Without this IAM step, Snowflake cannot read files from ADLS.
--
-- Placeholders to replace before running:
--   'Tenant_ID'        → Your Azure Active Directory (Entra ID) Tenant ID
--   'pacificretailstg' → Your Azure Storage Account name
--   '<container_name>' → Your ADLS container name (e.g., landing)
--
-- Idempotent   : Yes — uses CREATE OR REPLACE; safe to re-run.
-- Run Before   : 03_sql/02_bronze/customer_load.sql
-- ============================================================
-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Confirm you are now on ACCOUNTADMIN
SELECT CURRENT_ROLE();

-- ── STEP 1: Set database and schema context ────────────────────────────────────
-- All objects will be created inside PACIFICRETAIL_DB under the BRONZE schema.

USE DATABASE pacificretail_db;
USE SCHEMA bronze;


-- ── STEP 2: Create Storage Integration ────────────────────────────────────────
-- A Storage Integration creates a trust relationship between your Snowflake
-- account and your Azure ADLS Gen2 storage account using Azure Entra ID (AAD).
--
-- Key parameters:
--   TYPE = EXTERNAL_STAGE          → Used for staging files (not Snowpipe direct)
--   STORAGE_PROVIDER = AZURE       → Cloud provider
--   ENABLED = TRUE                 → Activates the integration immediately
--   AZURE_TENANT_ID                → Your organisation's Azure Directory (Tenant) ID
--   STORAGE_ALLOWED_LOCATIONS      → Restricts access to only this container path
--                                    for security — Snowflake cannot access anything
--                                    outside this path even if credentials allow it.
--
-- ⚠️  Replace the placeholder values below before executing:
--   'Tenant_ID'        → e.g., 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
--   'pacificretailstgac' → your actual storage account name
--   '<container_name>' → e.g., 'landing'

CREATE OR REPLACE STORAGE INTEGRATION azure_pacificretail_integration
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = AZURE
  ENABLED                   = TRUE
  AZURE_TENANT_ID           = '8573f768-ac84-4c3d-8bf3-f38dc39528af'
  STORAGE_ALLOWED_LOCATIONS = ('azure://pacificretailstgac.blob.core.windows.net/landing/');
-- 'Tenant ID'

-- ── STEP 3: Retrieve Integration Details for Azure IAM Setup ──────────────────
-- After creating the integration, run this command to get the
-- AZURE_MULTI_TENANT_APP_NAME — the Snowflake-managed Azure service principal
-- that needs to be granted the 'Storage Blob Data Contributor' role in Azure IAM.
--
-- Steps to complete in Azure Portal after copying the app name:
--   1. Go to your Storage Account → Access Control (IAM)
--   2. Click Add → Add Role Assignment
--   3. Role: Storage Blob Data Contributor
--   4. Assign access to: Azure AD user, group, or service principal
--   5. Search for and select the AZURE_MULTI_TENANT_APP_NAME value
--   6. Save — Snowflake can now read from the ADLS container

DESC INTEGRATION azure_pacificretail_integration;


-- ── STEP 4: Create External Stage ─────────────────────────────────────────────
-- An External Stage is a named reference to a specific path in ADLS.
-- The Bronze layer COPY INTO commands reference this stage by name
-- to load CSV, JSON, and Parquet files into their respective raw tables.
--
-- STORAGE_INTEGRATION links the stage to the trusted integration created above,
-- so no credentials (SAS token / access key) need to be stored in the stage.
--
-- ⚠️  Replace the placeholder values to match your environment:
--   'pacificretailstg' → your actual storage account name
--   '<container_name>' → e.g., 'landing'

CREATE OR REPLACE STAGE pacificretail_stage
  STORAGE_INTEGRATION = azure_pacificretail_integration
  URL                 = 'azure://pacificretailstgac.blob.core.windows.net/landing/';


-- ── VERIFICATION ──────────────────────────────────────────────────────────────
-- Run these queries to confirm the integration and stage were created correctly.

-- 1. Check integration status — ENABLED should be TRUE
SHOW INTEGRATIONS LIKE 'azure_pacificretail_integration';

-- 2. List all stages in the Bronze schema — pacificretail_stage should appear
SHOW STAGES IN SCHEMA pacificretail_db.bronze;

-- 3. List files visible through the stage (run after IAM role is assigned)
--    This confirms Snowflake can successfully read from ADLS
LIST @pacificretail_stage;

-- OR

ls @pacificretail_stage;

-- ============================================================
-- END OF SCRIPT
-- Next step → Run: 03_sql/02_bronze/customer_load.sql
-- ============================================================


