#!/bin/bash
# Test deployment to DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing DataHub deployment ==="
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

if [ -z "$DATAHUB_TOKEN" ]; then
  echo "Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication."
  echo "This will only work if your DataHub instance doesn't require authentication."
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

# If connection test passed, try to push a recipe
echo "Pushing a test recipe to DataHub with secret creation..."
python scripts/push_recipe.py --instance recipes/instances/dev/analytics-db.yml --create-secrets

echo "=== Deployment test complete ==="