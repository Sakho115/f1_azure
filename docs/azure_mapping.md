# Enterprise Azure Cloud vs Local Open-Source Architecture Mapping

| Component | Local Open-Source Stack | Enterprise Azure Target | Conceptual Mapping & Rationale |
| :--- | :--- | :--- | :--- |
| **Storage Layer** | Local Filesystem / MinIO S3 API | Azure Data Lake Storage Gen2 (ADLS Gen2) | Both implement hierarchical namespace storage (`data/raw`, `data/bronze`, `data/silver`, `data/gold` ↔ `abfss://<container>@<storage>.dfs.core.windows.net/`). |
| **Distributed Compute** | PySpark 3.5.1 + OpenJDK 17 + Python 3.11 | Azure Databricks Multi-Node Cluster | Identical PySpark DataFrame API, Catalyst optimizer, and Spark SQL runtime. |
| **Table Storage Format** | Delta Lake 3.2.0 (`_delta_log`, Parquet) | Delta Lake on Databricks | Full ACID transaction log, Schema enforcement, Time-Travel, and Delta MERGE support. |
| **Pipeline Orchestration**| Python Orchestrator (`pipeline.py`) + Airflow DAG | Azure Data Factory (ADF) Pipelines | DAG task dependencies, retries, conditional branches, post-race Sunday triggers. |
| **Secrets & Identity** | Service Principal simulation + `.env` provider | Microsoft Entra ID + Key Vault / Secret Scopes | OAuth2 Service Principal token acquisition (`fs.azure.account.oauth2.client.id`). |
| **Data Governance** | Local Catalog Simulator + `sql/governance.sql` | Databricks Unity Catalog | 3-level namespace (`catalog.schema.table`), RBAC grants, PII masking, row filtering. |
| **BI & Analytics** | Streamlit + Plotly + DuckDB OLAP engine | Power BI / Databricks SQL Dashboards | Real-time interactive visual dashboards, driver standings, telemetry plots. |
