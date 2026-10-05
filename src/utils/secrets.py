"""
Secrets and Security Management Module for Formula 1 Lakehouse.
Implements an enterprise-grade credential management interface that seamlessly
supports both Local Development Mode (Environment variables / .env) and Azure Production Mode
(Microsoft Entra ID Service Principal + Databricks Secret Scopes / Azure Key Vault).
"""

import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from src.utils.logger import get_logger

logger = get_logger("SecretsManager")

# Load .env if present
load_dotenv()

class SecretsManager:
    """
    Abstracted Secrets Manager providing credential retrieval and Azure OAuth2
    configuration for PySpark storage accounts.
    """

    def __init__(self, env: Optional[str] = None):
        self.env = env or os.getenv("PIPELINE_ENV", "local").lower()
        self._mock_secret_vault: Dict[str, Dict[str, str]] = {
            "f1_secrets": {
                "storage_account_key": os.getenv("ADLS_STORAGE_ACCOUNT_KEY", "local_mock_key"),
                "service_principal_client_id": os.getenv("AZURE_CLIENT_ID", "mock-sp-client-id"),
                "service_principal_client_secret": os.getenv("AZURE_CLIENT_SECRET", "mock-sp-client-secret"),
                "tenant_id": os.getenv("AZURE_TENANT_ID", "mock-tenant-id"),
                "ergast_api_key": os.getenv("ERGAST_API_KEY", "mock-ergast-key"),
            }
        }

    def get_secret(self, scope: str, key: str) -> str:
        """
        Retrieves a secret from the specified scope, matching Databricks dbutils.secrets.get(scope, key) syntax.
        """
        if scope in self._mock_secret_vault and key in self._mock_secret_vault[scope]:
            val = self._mock_secret_vault[scope][key]
            return val
        
        # Fallback to direct environment lookup
        env_key = f"{scope}_{key}".upper()
        if env_key in os.environ:
            return os.environ[env_key]
        if key.upper() in os.environ:
            return os.environ[key.upper()]
            
        raise KeyError(f"Secret '{key}' not found in scope '{scope}' or environment variables.")

    def get_adls_spark_configs(self, storage_account: Optional[str] = None) -> Dict[str, str]:
        """
        Generates PySpark Hadoop configurations for ADLS Gen2 OAuth 2.0 authentication
        via Microsoft Entra ID Service Principal.
        """
        storage_account = storage_account or os.getenv("ADLS_STORAGE_ACCOUNT", "f1lakehousestorage")
        client_id = self.get_secret("f1_secrets", "service_principal_client_id")
        client_secret = self.get_secret("f1_secrets", "service_principal_client_secret")
        tenant_id = self.get_secret("f1_secrets", "tenant_id")

        if self.env == "local":
            logger.info("Operating in LOCAL mode. Local filesystem / MinIO storage configured.")
            return {}

        logger.info(f"Operating in AZURE mode. Configuring ADLS Gen2 OAuth2 credentials for account: {storage_account}")
        return {
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": client_id,
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": client_secret,
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net":
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }

    @staticmethod
    def mask_secret(secret_value: str) -> str:
        """Masks sensitive credentials for safe logging."""
        if not secret_value or len(secret_value) < 6:
            return "******"
        return f"{secret_value[:2]}****{secret_value[-2:]}"
