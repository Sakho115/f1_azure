-- Provision Databricks Unity Catalog for Formula 1 Lakehouse
CREATE CATALOG IF NOT EXISTS formula1_catalog;
USE CATALOG formula1_catalog;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Provision External Locations for ADLS Gen2 Containers
CREATE EXTERNAL LOCATION IF NOT EXISTS adls_raw
  URL 'abfss://raw@f1lakehousestorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `f1_storage_credential`);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_bronze
  URL 'abfss://bronze@f1lakehousestorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `f1_storage_credential`);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_silver
  URL 'abfss://silver@f1lakehousestorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `f1_storage_credential`);

CREATE EXTERNAL LOCATION IF NOT EXISTS adls_gold
  URL 'abfss://gold@f1lakehousestorage.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `f1_storage_credential`);
