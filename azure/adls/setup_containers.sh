#!/usr/bin/env bash
# Azure CLI provisioning script for ADLS Gen2 Storage Containers
set -euo pipefail

RESOURCE_GROUP=${1:-"rg-formula1-lakehouse"}
LOCATION=${2:-"eastus"}
STORAGE_ACCOUNT=${3:-"f1lakehousestorage"}

echo "Creating Azure Resource Group: ${RESOURCE_GROUP}..."
az group create --name "${RESOURCE_GROUP}" --location "${LOCATION}"

echo "Creating ADLS Gen2 Storage Account (HNS Enabled): ${STORAGE_ACCOUNT}..."
az storage account create \
    --name "${STORAGE_ACCOUNT}" \
    --resource-group "${RESOURCE_GROUP}" \
    --location "${LOCATION}" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true

echo "Creating ADLS Gen2 Medallion Storage Containers..."
for CONTAINER in raw bronze silver gold; do
    echo "Creating container: ${CONTAINER}"
    az storage container create \
        --account-name "${STORAGE_ACCOUNT}" \
        --name "${CONTAINER}" \
        --auth-mode login
done

echo "ADLS Gen2 Container Provisioning Complete!"
