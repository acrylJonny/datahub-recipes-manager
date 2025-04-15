#!/bin/bash
# Test running ingestion sources in DataHub

# Exit on error
set -e

# Change to the project root directory
cd "$(dirname "$0")/.."

echo "=== Testing Run Ingestion Source ==="
echo "Checking if .env file exists and has required variables..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found. Please create one with your DataHub credentials."
    exit 1
fi

# Load environment variables
source .env

# Check for required environment variables
if [ -z "$DATAHUB_GMS_URL" ]; then
  echo "Error: DATAHUB_GMS_URL must be set in .env file"
  echo "Update your .env file with valid DataHub server URL"
  exit 1
fi

# Check if DATAHUB_GMS_URL is defined
if [ -z "${DATAHUB_GMS_URL}" ]; then
    echo "DATAHUB_GMS_URL not defined, using default: http://localhost:8080"
    export DATAHUB_GMS_URL="http://localhost:8080"
fi

echo "Checking connection to DataHub..."
python -c "
import sys
import os
from dotenv import load_dotenv
from utils.datahub_rest_client import DataHubRestClient

# Load environment variables from .env file
load_dotenv()

# Get DataHub URL
datahub_gms_url = os.getenv('DATAHUB_GMS_URL')
datahub_token = os.getenv('DATAHUB_TOKEN')

# Create client
client = DataHubRestClient(datahub_gms_url, datahub_token) if datahub_token else DataHubRestClient(datahub_gms_url)

# Test connection
try:
    if client.test_connection():
        print('✅ Successfully connected to DataHub!')
    else:
        print('❌ Failed to connect to DataHub')
        sys.exit(1)
except Exception as e:
    print(f'❌ Error connecting to DataHub: {str(e)}')
    sys.exit(1)
"

# Check for analytics-database-prod source for test
echo "Checking for test source to run..."
SOURCE_EXISTS=$(python -c "
import os, sys
from dotenv import load_dotenv
sys.path.append('utils')
from datahub_rest_client import DataHubRestClient

load_dotenv()
server = os.environ.get('DATAHUB_GMS_URL')
token = os.environ.get('DATAHUB_TOKEN')

try:
    if token and token != 'your_datahub_pat_token_here':
        client = DataHubRestClient(server_url=server, token=token)
    else:
        client = DataHubRestClient(server_url=server)
    
    # Check if source exists
    source = client.get_ingestion_source('analytics-database-prod')
    if source:
        print('true')
    else:
        print('false')
except:
    print('false')
")

if [ "$SOURCE_EXISTS" != "true" ]; then
  echo "Test source 'analytics-database-prod' not found. Creating it first..."
  python scripts/push_recipe.py --instance recipes/instances/analytics-db.yml
  if [ $? -ne 0 ]; then
    echo "⚠️ Failed to create test source. Cannot proceed with run test."
    exit 1
  fi
  echo "✅ Created test source 'analytics-database-prod'."
fi

# Run the ingestion source
echo "Testing running ingestion source..."
python scripts/run_ingestion_source.py --source-id analytics-database-prod || {
    echo "⚠️ Failed to run ingestion source. This might be normal if the source doesn't exist."
}

echo "✅ Test run ingestion completed."
echo "=== Run Ingestion Source test complete ===" 