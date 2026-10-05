# Enterprise Security & Credentials Architecture

## Security Reference Model

```mermaid
flowchart LR
    SP["Microsoft Entra ID (Service Principal)"] -->|OAuth 2.0 Token| ADLS["ADLS Gen2 Storage Account"]
    KV["Azure Key Vault / Databricks Secrets"] -->|Secret Scope| DB["Azure Databricks PySpark Cluster"]
    DB -->|Mount / ABFSS| ADLS
```

## Local Mode Security Equivalents
- **Secrets Management**: Abstracted `SecretsManager` (`src/utils/secrets.py`) reads credentials from environment variables or `.env`.
- **Service Principal Simulation**: Maps `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID` for cloud deployment testing.
- **Data Credential Masking**: Loggers redact secret tokens before writing output.
