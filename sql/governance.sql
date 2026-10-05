-- ==============================================================================
-- DATABRICKS UNITY CATALOG GOVERNANCE & RBAC POLICIES
-- Enterprise 3-Level Namespace: <catalog>.<schema>.<table>
-- ==============================================================================

-- 1. Create Unity Catalog
CREATE CATALOG IF NOT EXISTS formula1_catalog
COMMENT 'Enterprise Lakehouse Catalog for Formula 1 Racing Telemetry and Analytics';

USE CATALOG formula1_catalog;

-- 2. Create Medallion Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion tier with metadata injection (ingestion_date, input_file_name)';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, typed, enriched, and merged Delta Lake tables with SCD-1 and fact upsert logic';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Curated analytical dimensional models and business marts for BI and ML consumption';

-- 3. Role-Based Access Control (RBAC) Grants
-- Create Principals / Groups
-- Groups: f1_data_engineers, f1_data_analysts, f1_data_scientists, external_contractors

-- Data Engineers: Full Administrative & DDL Privileges
GRANT USE CATALOG ON CATALOG formula1_catalog TO `f1_data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `f1_data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA silver TO `f1_data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA gold TO `f1_data_engineers`;

-- Data Analysts: Read-Only Access to Silver & Gold Marts
GRANT USE CATALOG ON CATALOG formula1_catalog TO `f1_data_analysts`;
GRANT USE SCHEMA ON SCHEMA silver TO `f1_data_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO `f1_data_analysts`;
GRANT USE SCHEMA ON SCHEMA gold TO `f1_data_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO `f1_data_analysts`;

-- Data Scientists: Read Access to Silver & Gold + Feature Table Generation
GRANT USE CATALOG ON CATALOG formula1_catalog TO `f1_data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA silver TO `f1_data_scientists`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `f1_data_scientists`;

-- 4. Dynamic Column Masking Policy for PII Protection
-- Protects Driver Date of Birth (DOB) from unauthorized disclosure
CREATE OR REPLACE FUNCTION mask_driver_dob(dob DATE)
RETURN CASE
    WHEN is_account_group_member('f1_data_engineers') THEN dob
    WHEN is_account_group_member('f1_compliance_officers') THEN dob
    ELSE DATE '1900-01-01'
END;

ALTER TABLE silver.drivers ALTER COLUMN dob SET MASK mask_driver_dob;

-- 5. Row-Level Security (Row Filter Policy)
-- Restricts external race contractors to view only current-season (>= 2022) results
CREATE OR REPLACE FUNCTION filter_contractor_race_season(race_year INT)
RETURN CASE
    WHEN is_account_group_member('external_contractors') THEN race_year >= 2022
    ELSE TRUE
END;

ALTER TABLE gold.race_results_gold SET ROW FILTER filter_contractor_race_season ON (race_year);

-- 6. Table & Column Tagging (Data Governance & Catalog Lineage)
ALTER TABLE silver.drivers SET TAGS ('tier' = 'silver', 'confidentiality' = 'restricted', 'contains_pii' = 'true');
ALTER TABLE silver.results SET TAGS ('tier' = 'silver', 'data_domain' = 'telemetry_fact', 'granularity' = 'race_driver_result');
ALTER TABLE gold.driver_standings SET TAGS ('tier' = 'gold', 'certified' = 'true', 'sla' = '2h_post_race');
