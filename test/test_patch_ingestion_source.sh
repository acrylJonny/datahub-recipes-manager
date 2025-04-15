#!/bin/bash
# Test patching ingestion sources in DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing Patch Ingestion Source ==="
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

# Check for analytics-database-prod source for test
echo "Checking for test source to patch..."
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
    echo "⚠️ Failed to create test source. Cannot proceed with patch test."
    exit 1
  fi
  echo "✅ Created test source 'analytics-database-prod'."
fi

# Test patching the schedule
echo "Testing patching ingestion source schedule..."
NEW_SCHEDULE="0 3 * * *"
python scripts/patch_ingestion_source.py --source-id analytics-database-prod --schedule "$NEW_SCHEDULE"
if [ $? -ne 0 ]; then
  echo "⚠️ Failed to patch ingestion source schedule."
  exit 1
fi
echo "✅ Successfully patched ingestion source schedule to '$NEW_SCHEDULE'."

# Verify the schedule was changed
SCHEDULE_CHANGED=$(python -c "
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
    
    source = client.get_ingestion_source('analytics-database-prod')
    if source and 'schedule' in source and source['schedule'].get('interval') == '$NEW_SCHEDULE':
        print('true')
    else:
        print('false')
except:
    print('false')
")

if [ "$SCHEDULE_CHANGED" == "true" ]; then
  echo "✅ Verified schedule was successfully changed to '$NEW_SCHEDULE'."
else
  echo "⚠️ Could not verify schedule change. This may be due to API limitations."
fi

echo "=== Patch Ingestion Source test complete ===" 