#!/bin/bash
# Test to verify recipe pull functionality
# This test will:
# 1. Check for an existing test recipe in DataHub - create one if needed
# 2. Pull the recipe from DataHub using both individual pull and batch pull
# 3. Verify the pulled recipes are accessible

# Get directory where this script is located
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/.." || exit

# Output directory for pulled recipes
PULL_DIR="test/recipes/pulled"
mkdir -p "$PULL_DIR"

# Source ID to use for testing
SOURCE_ID="analytics-database-prod"

echo "=== Testing Recipe Pull Functionality ==="

# Verify environment variables are set
if [ -z "$DATAHUB_GMS_URL" ] && [ -z "$DATAHUB_SERVER" ]; then
    # Try loading from .env file
    if [ -f .env ]; then
        source .env
        echo "Loaded .env file"
    else
        echo "⚠️ DATAHUB_GMS_URL or DATAHUB_SERVER is not set, and no .env file found"
        echo "Set these variables to connect to your DataHub instance"
        exit 1
    fi
fi

# Use DATAHUB_GMS_URL if available, otherwise DATAHUB_SERVER
SERVER_URL="${DATAHUB_GMS_URL:-$DATAHUB_SERVER}"
if [ -z "$SERVER_URL" ]; then
    echo "❌ DataHub server URL not configured. Please set DATAHUB_GMS_URL or DATAHUB_SERVER."
    exit 1
fi

echo "Testing connection to DataHub at $SERVER_URL"

# Check connection to DataHub
CONNECTION_STATUS=$(python3 -c "
import os
import sys
sys.path.append('.')
from utils.datahub_rest_client import DataHubRestClient

server = os.environ.get('DATAHUB_GMS_URL') or os.environ.get('DATAHUB_SERVER')
token = os.environ.get('DATAHUB_TOKEN')

# Create client
client = DataHubRestClient(server, token)

# Test connection
if client.test_connection():
    print('connected')
else:
    print('failed')
")

if [ "$CONNECTION_STATUS" != "connected" ]; then
    echo "❌ Failed to connect to DataHub at $SERVER_URL"
    exit 1
fi

echo "✅ Successfully connected to DataHub"

# Check if our test source exists
echo "Checking if test source $SOURCE_ID exists..."
SOURCE_EXISTS=$(python3 -c "
import os
import sys
sys.path.append('.')
from utils.datahub_rest_client import DataHubRestClient

server = os.environ.get('DATAHUB_GMS_URL') or os.environ.get('DATAHUB_SERVER')
token = os.environ.get('DATAHUB_TOKEN')

try:
    # Create client with or without token
    if token and token != 'your_datahub_pat_token_here':
        client = DataHubRestClient(server_url=server, token=token)
    else:
        client = DataHubRestClient(server_url=server)
    
    # Try to get the source directly
    source = client.get_ingestion_source('$SOURCE_ID')
    if source:
        print('true')
    else:
        print('false')
except Exception as e:
    print('false')
")

# If the source doesn't exist, create it
if [ "$SOURCE_EXISTS" != "true" ]; then
  echo "Test recipe not found. Creating it first..."
  python scripts/push_recipe.py --instance recipes/instances/analytics-db.yml
  if [ $? -ne 0 ]; then
    echo "⚠️ Failed to create test recipe. Aborting test."
    exit 1
  fi
  echo "✅ Created test recipe: $SOURCE_ID"
fi

# Pull recipe directly by ID instead of listing
echo "Pulling recipe directly by ID: $SOURCE_ID"
python scripts/pull_recipe.py --output-dir "$PULL_DIR" --source-id "$SOURCE_ID"
PULL_STATUS=$?

# Check all possible recipe file locations
if [ -f "$PULL_DIR/BATCH-$SOURCE_ID.yml" ] || [ -f "$PULL_DIR/batch-$SOURCE_ID.yml" ] || \
   [ -f "$PULL_DIR/postgres-$SOURCE_ID.yml" ] || [ -f "$PULL_DIR/$SOURCE_ID.yml" ]; then
  echo "✅ Successfully pulled recipe by ID from DataHub!"
  echo "Pulled recipe file:"
  ls -la "$PULL_DIR"
  echo "=== Recipe Pull test complete ==="
  exit 0
fi

# Pull all recipes and check if our test recipe is included
echo "Pulling all recipes from DataHub..."
python scripts/pull_recipe.py --output-dir "$PULL_DIR"

# Check all possible recipe file locations
if [ -f "$PULL_DIR/BATCH-$SOURCE_ID.yml" ] || [ -f "$PULL_DIR/batch-$SOURCE_ID.yml" ] || \
   [ -f "$PULL_DIR/postgres-$SOURCE_ID.yml" ] || [ -f "$PULL_DIR/$SOURCE_ID.yml" ]; then
  echo "✅ Successfully pulled recipe while pulling all recipes from DataHub!"
  echo "Pulled recipe files:"
  ls -la "$PULL_DIR"
  echo "=== Recipe Pull test complete ==="
  exit 0
else
  echo "❌ Failed to find pulled recipe in output directory after multiple attempts"
  echo "Files in pull directory:"
  ls -la "$PULL_DIR"
  echo "=== Recipe Pull test failed ==="
  exit 1
fi 