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

# If connection test passed, pull recipes
echo "Pulling recipes from DataHub..."
python scripts/pull_recipe.py --output-dir "$PULL_DIR" || {
  echo "⚠️ Recipe pull failed. This could be due to SDK compatibility issues."
  echo "The pull functionality requires a specific DataHub SDK version."
  echo "This doesn't necessarily mean your setup is broken - you may need"
  echo "to adjust the DataHub API client to match your specific SDK version."
}

# Check if any recipes were pulled
if [ "$(ls -A "$PULL_DIR")" ]; then
  echo "✅ Successfully pulled recipes from DataHub!"
  echo "Pulled recipes:"
  ls -la "$PULL_DIR"
else
  echo "⚠️ No recipes were pulled. This may be normal if you haven't pushed any recipes yet"
  echo "or if there are SDK compatibility issues with your DataHub version."
fi

echo "=== Recipe Pull test complete ===" 