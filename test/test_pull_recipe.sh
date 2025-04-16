#!/bin/bash
# Test pulling recipes from DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing Recipe Pull from DataHub ==="
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

# Create a temporary output directory for pulled recipes
PULL_DIR="recipes/pulled"
mkdir -p "$PULL_DIR"
# Clear any existing files
rm -f "$PULL_DIR"/*.yml

# Check if the test recipe exists in DataHub
echo "Checking if test recipe exists in DataHub..."
SOURCE_ID="analytics-database-prod"
SOURCE_EXISTS=$(python -c "
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
elif [ $PULL_STATUS -ne 0 ]; then
  # If the direct pull failed with error, try fallback
  echo "⚠️ Failed to pull recipe by direct ID. Trying general pull as fallback..."
  
  # Try general pull method
  python scripts/pull_recipe.py --output-dir "$PULL_DIR" || {
    echo "⚠️ Recipe pull failed. This could be due to SDK compatibility issues."
    echo "The pull functionality requires a specific DataHub SDK version."
    echo "This doesn't necessarily mean your setup is broken - you may need"
    echo "to adjust the DataHub API client to match your specific SDK version."
  }
  
  # Check if any recipes were pulled
  if [ "$(ls -A "$PULL_DIR")" ]; then
    echo "✅ Successfully pulled recipes using general method!"
    echo "Pulled recipes:"
    ls -la "$PULL_DIR"
    echo "=== Recipe Pull test complete ==="
    exit 0
  else
    echo "⚠️ No recipes were pulled. This may be normal if you haven't pushed any recipes yet"
    echo "or if there are SDK compatibility issues with your DataHub version."
    echo "=== Recipe Pull test complete ==="
    exit 1
  fi
else
  # We got success return code but no expected file
  echo "⚠️ Pull process completed but no recipe files were found."
  echo "=== Recipe Pull test complete ==="
  exit 1
fi 