#!/bin/bash

set -euo pipefail

# This script tests the run_now functionality.
# It triggers an immediate run of an ingestion source.

# Check if .env file exists
if [ -f ".env" ]; then
    # Load environment variables
    source .env
    echo "✅ Loaded environment variables from .env file"
else
    echo "⚠️ No .env file found, using existing environment variables"
fi

# Check if required variables are set
if [ -z "${DATAHUB_GMS_URL-}" ]; then
    echo "❌ DATAHUB_GMS_URL is not set"
    exit 1
fi

echo "🔄 Testing connection to DataHub server at $DATAHUB_GMS_URL..."

# Test API connection
python -c "from utils.datahub_rest_client import DataHubRestClient; client = DataHubRestClient('$DATAHUB_GMS_URL', '${DATAHUB_TOKEN-}'); print('✅ Connected to DataHub server')" || {
    echo "❌ Failed to connect to DataHub server"
    exit 1
}

# Define test recipe ID
TEST_RECIPE_ID="analytics-database-prod"

echo "🔄 Running ingestion source $TEST_RECIPE_ID immediately"

# Run the run_now script
python scripts/run_now.py --source-id $TEST_RECIPE_ID

# Output success message
echo "✅ Successfully triggered immediate run of ingestion source: $TEST_RECIPE_ID"
echo "✅ Run now test completed" 