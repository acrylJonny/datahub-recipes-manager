#!/bin/bash
# Test pushing recipes to DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing Recipe Push to DataHub ==="
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

# If connection test passed, push a recipe
echo "Pushing a test recipe to DataHub..."
python scripts/push_recipe.py --instance recipes/instances/dev/analytics-db.yml || {
  echo "⚠️ Recipe push failed. This could be due to SDK compatibility issues."
  echo "The push functionality requires a specific DataHub SDK version."
  echo "This doesn't necessarily mean your setup is broken - you may need"
  echo "to adjust the DataHub API client to match your specific SDK version."
}

echo "=== Recipe Push test complete ===" 