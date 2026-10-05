# Azure Enterprise Deployment Guide

This directory contains production-ready ARM, JSON, SQL, and CLI templates to deploy the Formula 1 Lakehouse Pipeline to Microsoft Azure.

## Deployment Steps
1. **Provision ADLS Gen2 Containers**: Run `adls/setup_containers.sh` to create `raw`, `bronze`, `silver`, and `gold` storage containers.
2. **Setup Databricks & Unity Catalog**: Import `databricks/cluster_config.json` and execute `unity-catalog/setup_catalog.sql`.
3. **Deploy Azure Data Factory Pipelines**: Import `adf/pipeline_formula1.json` and linked services into your ADF instance.
4. **Configure Key Vault Secrets**: Add Service Principal keys to Azure Key Vault and reference them in Databricks Secret Scopes.
