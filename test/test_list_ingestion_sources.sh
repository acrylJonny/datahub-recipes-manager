#!/bin/bash
# Test listing ingestion sources in DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing List Ingestion Sources ==="
echo "Checking if .env file exists and has required variables..."

if [ ! -f .env ]; then
  echo "Error: .env file not found. Run setup_test_env.sh first."
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

# Test connecting to DataHub
echo "Testing DataHub connection..."
python -c "
import os, sys
from dotenv import load_dotenv
sys.path.append('utils')
from datahub_rest_client import DataHubRestClient

load_dotenv()
server = os.environ.get('DATAHUB_GMS_URL')
token = os.environ.get('DATAHUB_TOKEN')

try:
    # Create client with or without token
    if token and token != 'your_datahub_pat_token_here':
        client = DataHubRestClient(server_url=server, token=token)
    else:
        client = DataHubRestClient(server_url=server)
    
    # Test connection
    if client.test_connection():
        print('✅ Successfully connected to DataHub!')
    else:
        print('❌ Failed to connect to DataHub')
        sys.exit(1)
except Exception as e:
    print(f'❌ Error connecting to DataHub: {str(e)}')
    sys.exit(1)
"

# Check if any test source exists, create one if needed
echo "Checking for test sources to list..."
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
    
    # Get all sources
    sources = client.list_ingestion_sources()
    if sources and len(sources) > 0:
        print('true')
    else:
        print('false')
except:
    print('false')
")

if [ "$SOURCE_EXISTS" != "true" ]; then
  echo "No test sources found. Creating one first..."
  python scripts/push_recipe.py --instance recipes/instances/dev/analytics-db.yml
  if [ $? -ne 0 ]; then
    echo "⚠️ Failed to create test source. Cannot proceed with list test."
    exit 1
  fi
  echo "✅ Created test source 'analytics-database-prod'."
fi

# Test listing ingestion sources in text format
echo "Testing listing ingestion sources in text format..."
python scripts/list_ingestion_sources.py
if [ $? -ne 0 ]; then
  echo "⚠️ Failed to list ingestion sources in text format."
  exit 1
fi
echo "✅ Successfully listed ingestion sources in text format."

# Test listing ingestion sources in JSON format
echo "Testing listing ingestion sources in JSON format..."
python scripts/list_ingestion_sources.py --format json > /tmp/sources.json
if [ $? -ne 0 ]; then
  echo "⚠️ Failed to list ingestion sources in JSON format."
  exit 1
fi

# Verify JSON output is valid
if jq empty /tmp/sources.json 2>/dev/null; then
  echo "✅ Successfully listed ingestion sources in valid JSON format."
  rm /tmp/sources.json
else
  echo "⚠️ JSON output is not valid."
  rm /tmp/sources.json
  exit 1
fi

echo "=== List Ingestion Sources test complete ===" 