# 🔗 Azure ADLS Gen2 — Snowflake Storage Integration Setup Guide

> **PacificRetail — End-to-End Snowflake Data Engineering**
> A step-by-step walkthrough for connecting Snowflake to Azure Data Lake Storage Gen2
> using a Storage Integration object — including troubleshooting real errors encountered during setup.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture Position](#-architecture-position)
- [Prerequisites](#-prerequisites)
- [Step-by-Step Setup](#-step-by-step-setup)
  - [Step 1 — Create the Storage Integration in Snowflake](#step-1--create-the-storage-integration-in-snowflake)
  - [Step 2 — Retrieve Integration Details](#step-2--retrieve-integration-details)
  - [Step 3 — Grant Azure Consent](#step-3--grant-azure-consent)
  - [Step 4 — Verify Azure Tenant ID](#step-4--verify-azure-tenant-id)
  - [Step 5 — Assign IAM Role in Azure Portal](#step-5--assign-iam-role-in-azure-portal)
  - [Step 6 — Create the External Stage in Snowflake](#step-6--create-the-external-stage-in-snowflake)
  - [Step 7 — Verify the Connection](#step-7--verify-the-connection)
- [Troubleshooting Reference](#-troubleshooting-reference)
- [Completion Checklist](#-completion-checklist)
- [Next Steps](#-next-steps)

---

## 🔍 Overview

This guide captures the end-to-end setup process for connecting **Snowflake** to **Azure Data
Lake Storage Gen2 (ADLS Gen2)** using a Snowflake **Storage Integration** object. This is a
one-time configuration step that establishes a secure, credential-free trust relationship
between both platforms — a prerequisite for the Bronze layer `COPY INTO` operations in the
PacificRetail Medallion Architecture project.

### Why Storage Integration?

The Storage Integration approach is Snowflake's **recommended method** for cloud storage
connectivity. Unlike SAS tokens or access keys, it uses **Azure Entra ID (formerly AAD)
service principals**, making it enterprise-grade, auditable, and credential-free.

| Approach | Security | Maintainability |
|----------|----------|-----------------|
| SAS Token / Access Key | ❌ Credentials embedded in stage | ❌ Must rotate manually |
| **Storage Integration** | ✅ Azure Entra ID service principal | ✅ Managed by Snowflake |

---

## 🏗️ Architecture Position

This setup sits at the boundary between the raw data landing zone and Snowflake's Bronze schema:

```
┌─────────────────────┐        ┌──────────────────────────────┐        ┌────────────────────┐
│    ADLS Gen2        │        │   Storage Integration        │        │  Snowflake         │
│ (landing/container) │──────► │      +  External Stage       │──────► │  Bronze Schema     │
└─────────────────────┘        └──────────────────────────────┘        └────────────────────┘
```

**Script location:** `03_sql/01_setup/external_stage_creation.sql`
**Run after:** `03_sql/01_setup/create_db_and_schemas.sql`
**Run before:** `03_sql/02_bronze/customer_load.sql`

---

## ✅ Prerequisites

Before running the setup scripts, confirm the following:

- [ ] Snowflake account created with **Azure as the cloud provider**
- [ ] `create_db_and_schemas.sql` has been executed — `PACIFICRETAIL_DB` with `BRONZE`, `SILVER`, `GOLD` schemas exist
- [ ] Azure Storage Account (`pacificretailstgac`) created with a `landing/` container
- [ ] Source files uploaded to the `landing/` container (`/Customer/`, `/Product/`, `/Order/` subfolders)
- [ ] `ACCOUNTADMIN` role access in Snowflake
- [ ] Owner or Contributor rights on the Azure Storage Account for IAM role assignment

> ⚠️ **Account Note:** This project uses two separate accounts — one for Snowflake
> and one for Azure. Ensure you are logged into the **correct account** at each step.

---

## 🚀 Step-by-Step Setup

### Step 1 — Create the Storage Integration in Snowflake

Run the following SQL in your Snowflake worksheet under the `ACCOUNTADMIN` role:

```sql
USE ROLE ACCOUNTADMIN;

USE DATABASE pacificretail_db;
USE SCHEMA bronze;

CREATE OR REPLACE STORAGE INTEGRATION azure_pacificretail_integration
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = AZURE
  ENABLED                   = TRUE
  AZURE_TENANT_ID           = '<your-tenant-id>'
  STORAGE_ALLOWED_LOCATIONS = ('azure://<storage-account>.blob.core.windows.net/<container>/');
```

**Key parameters explained:**

| Parameter | Purpose |
|-----------|---------|
| `TYPE = EXTERNAL_STAGE` | Used for file staging via `COPY INTO` commands |
| `STORAGE_PROVIDER = AZURE` | Specifies Azure as the cloud provider |
| `AZURE_TENANT_ID` | Your Azure Entra ID (formerly AAD) Tenant ID |
| `STORAGE_ALLOWED_LOCATIONS` | Restricts Snowflake access to only this container path — nothing outside it can be accessed even if credentials allow |

---

### Step 2 — Retrieve Integration Details

After creating the integration, run the `DESC` command to get the Snowflake-managed
service principal details needed for Azure IAM:

```sql
DESC INTEGRATION azure_pacificretail_integration;
```

From the output table, note down these two critical values:

| Property | What to copy |
|----------|-------------|
| `AZURE_CONSENT_URL` | A long URL starting with `https://login.microsoftonline.com/...` — used to grant consent in the browser |
| `AZURE_MULTI_TENANT_APP_NAME` | The Snowflake service principal name — needed for IAM role assignment in Azure Portal |

> 📌 **Tip:** Paste both values into a notepad — you will need them in the next steps.

---

### Step 3 — Grant Azure Consent

This step registers the Snowflake managed application inside your Azure tenant as an
**Enterprise Application**. Without this, the service principal will not exist in Azure
and cannot be assigned any IAM roles.

1. Copy the `AZURE_CONSENT_URL` value from Step 2
2. Open it in a browser where you are **logged in as the Azure account owner**
3. A Microsoft **"Permissions requested"** dialog will appear — click **Accept**
4. You will be redirected to the Snowflake homepage after acceptance

> ✅ **The redirect to snowflake.com is expected behaviour.** It is Snowflake's registered
> redirect URI. The consent was successfully granted as long as you saw the permissions
> dialog and clicked Accept before landing on that page.

> ⚠️ **If you do not see the Permissions dialog** and are taken directly to another page,
> try opening the URL in a **private/incognito window** while logged into the correct
> Azure account.

---

### Step 4 — Verify Azure Tenant ID

Since a separate Azure account is used in this project, it is important to confirm the
Tenant ID in the Snowflake script matches the Tenant ID of the Azure account that owns
the storage account.

1. In Azure Portal, search for **"Microsoft Entra ID"** in the top search bar
2. Click it and navigate to the **Overview** page
3. Copy the **Tenant ID** shown and compare it to the value in your SQL script

**Two outcomes:**

- ✅ **They match** → No changes needed, proceed to Step 5
- ❌ **They don't match** → Update the `AZURE_TENANT_ID` value in your script and
  re-run the `CREATE OR REPLACE STORAGE INTEGRATION` block before continuing

> ✅ **In this project:** The Tenant ID was confirmed to match the Azure account
> owning the storage. No script changes were required.

---

### Step 5 — Assign IAM Role in Azure Portal

This is the core authorisation step. The Snowflake service principal needs the
**Storage Blob Data Contributor** role on the storage account to read files from ADLS Gen2.

#### 5a — Navigate to the Storage Account

1. In Azure Portal, search **"Storage accounts"** in the top search bar
2. Click on your **storage account** (e.g., `<your-storage-account>`)
3. In the left menu, click **Access Control (IAM)**
4. Click **+ Add** → **Add role assignment**

#### 5b — Select the Role

1. Under the **Role** tab, search for **Storage Blob Data Contributor**
2. Select it and click **Next**

#### 5c — Assign the Service Principal

1. Under the **Members** tab, keep **Assign access to** as `User, group, or service principal`
2. Click **+ Select members**
3. In the search box, paste the `AZURE_MULTI_TENANT_APP_NAME` value from Step 2

> ⚠️ **Troubleshooting — Service Principal Not Found in Search ("No results"):**
>
> If the search returns no results, the service principal name may not be discoverable
> by display name in this panel. Use the **Object ID** instead:
>
> 1. In Azure Portal, search **"Enterprise applications"**
> 2. Search for the app by the first part of your `AZURE_MULTI_TENANT_APP_NAME` (e.g., the prefix before the underscore)
> 3. Click on the app → copy the **Object ID** from the Overview page
> 4. Return to the **Select members** panel and paste the **Object ID** in the search box
>
> ✅ This resolved the issue in this project.

4. Select the service principal when it appears → click **Select**
5. Click **Review + assign** → **Review + assign** again to confirm

---

### Step 6 — Create the External Stage in Snowflake

With the trust relationship now established, create the External Stage that Bronze layer
`COPY INTO` commands will reference:

```sql
USE DATABASE pacificretail_db;
USE SCHEMA bronze;

CREATE OR REPLACE STAGE pacificretail_stage
  STORAGE_INTEGRATION = azure_pacificretail_integration
  URL                 = 'azure://<your-storage-account>.blob.core.windows.net/<container>/';
```

The `STORAGE_INTEGRATION` parameter links the stage to the trusted integration created
in Step 1 — no credentials (SAS token / access key) need to be stored in the stage definition.

---

### Step 7 — Verify the Connection

Wait **2–3 minutes** for Azure IAM to propagate, then run the following verification queries:

```sql
-- 1. Confirm integration is enabled — ENABLED should show TRUE
SHOW INTEGRATIONS LIKE 'azure_pacificretail_integration';

-- 2. Confirm stage exists in the Bronze schema
SHOW STAGES IN SCHEMA pacificretail_db.bronze;

-- 3. List files accessible through the stage
--    If files appear, the full trust chain is working
LIST @pacificretail_stage;
```

> ✅ **Success indicator:** The `LIST @pacificretail_stage` command returns the CSV,
> JSON, and Parquet files from the ADLS `landing/` container — confirming Snowflake
> can successfully read from Azure ADLS Gen2.

---

## 🛠️ Troubleshooting Reference

| Error / Issue | Root Cause | Resolution |
|---------------|-----------|------------|
| `Error authenticating with Azure` | IAM role not yet assigned to Snowflake service principal | Complete Steps 3–5 — grant consent and assign Storage Blob Data Contributor role |
| Service principal not found in Select members search ("No results") | App registered but not searchable by display name in that panel | Use the Object ID from Enterprise Applications instead of the display name |
| Consent URL redirects without showing the permissions dialog | Consent may have been skipped or already granted in a different browser session | Try in an incognito window with the correct Azure account signed in |
| Tenant ID mismatch | Script has a different Tenant ID than the Azure account owning the storage | Verify Tenant ID in Microsoft Entra ID → Overview, update the SQL script, and recreate the Storage Integration |
| `LIST @stage` returns no files after IAM assignment | IAM propagation delay (can take 2–5 minutes) or files not yet uploaded to landing container | Wait 2–3 minutes and retry; confirm files exist in the ADLS landing container |

---

## ☑️ Completion Checklist

| # | Component | Status |
|---|-----------|--------|
| 1 | Storage Integration created | ✅ `azure_pacificretail_integration` — ENABLED = TRUE |
| 2 | Azure Consent granted | ✅ Enterprise Application registered in Azure tenant |
| 3 | Tenant ID verified | ✅ Tenant ID confirmed to match across Snowflake script and Azure account |
| 4 | IAM Role assigned | ✅ Storage Blob Data Contributor assigned on storage account |
| 5 | External Stage created | ✅ `pacificretail_stage` pointing to `landing/` container |
| 6 | Connection verified | ✅ `LIST @pacificretail_stage` returns ADLS files |

---

## ▶️ Next Steps

With the Storage Integration and External Stage in place, the Bronze layer data loads
can now proceed. Run scripts in this order:

```
03_sql/02_bronze/customer_load.sql   → Loads customer CSV  → bronze.raw_customer
03_sql/02_bronze/product_load.sql    → Loads product JSON  → bronze.raw_product
03_sql/02_bronze/orders_load.sql     → Loads orders Parquet → bronze.raw_order
```

Then proceed to the Silver layer transformations once all Bronze tables are populated.

---

## 👤 Author

**Mohammed Afzal Shariff**
Business Intelligence Associate Manager — Accenture Solutions, Bengaluru
Microsoft Certified: Power Platform Solution Architect Expert | Power BI Data Analyst | Azure Fundamentals

*Expanding expertise in: Snowflake · Azure Data Engineering · Python · Databricks · Machine Learning*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://www.linkedin.com/in/mohammed-afzal-shariff/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/afzalshariff07)

---

*Part of the PacificRetail — End-to-End Snowflake Data Engineering project*
*Based on the LinkedIn Learning course by Deepak Goyal*