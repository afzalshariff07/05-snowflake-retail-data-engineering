-- 🥇 STEP 1: Create API Integration
CREATE OR REPLACE API INTEGRATION github_integration
API_PROVIDER = git_https_api
API_ALLOWED_PREFIXES = ('https://github.com/afzalshariff07')
ALLOWED_AUTHENTICATION_SECRETS = ALL
ENABLED = TRUE;

-- 🥈 STEP 2: Create Database and Schema
CREATE DATABASE IF NOT EXISTS PACIFICRETAIL_DB;
USE DATABASE PACIFICRETAIL_DB;

CREATE SCHEMA IF NOT EXISTS ADMIN;
USE SCHEMA ADMIN;

-- 🥉 STEP 3: Create Secret
CREATE OR REPLACE SECRET github_pat_secret
TYPE = PASSWORD
USERNAME = 'afzalshariff07'
PASSWORD ='YOUR_NEW_GITHUB_PAT_TOKEN';

-- 🏅 STEP 4: Grant Access
GRANT USAGE ON INTEGRATION github_integration TO ROLE SYSADMIN;
GRANT USAGE ON SECRET github_pat_secret TO ROLE SYSADMIN;


-- 🏗️ STEP 5: Create Git Repository Object in Snowflake
CREATE OR REPLACE GIT REPOSITORY "05-snowflake-retail-data-engineering"
API_INTEGRATION = github_integration
GIT_CREDENTIALS = github_pat_secret
ORIGIN = 'https://github.com/afzalshariff07/05-snowflake-retail-data-engineering.git';

-- 📋 STEP 6: Verify Git Repository Creation
SHOW GIT REPOSITORIES;

-- 🔄 STEP 7: Fetch Latest Repository Content from GitHub
ALTER GIT REPOSITORY "05-snowflake-retail-data-engineering" FETCH;

-- 🌿 STEP 8: Validate Available Git Branches
SHOW GIT BRANCHES IN GIT REPOSITORY "05-snowflake-retail-data-engineering";

-- 📂 STEP 9: Validate Repository Files in Main Branch
LS @"05-snowflake-retail-data-engineering"/branches/main;

