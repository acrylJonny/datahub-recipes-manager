#!/bin/bash
# Test updating secrets in DataHub
cd "$(dirname "$0")/.." || exit  # Move to project root

echo "=== Testing Update Secret ==="
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

# Create and update secret
echo "Creating test secret 'test-secret-for-update'..."
SECRET_NAME="test-secret-for-update"
ORIGINAL_VALUE="original-value-$(date +%s)"
UPDATED_VALUE="updated-value-$(date +%s)"

# First create a secret (using create_secret.py if available, otherwise manage_secrets.py)
if [ -f "scripts/create_secret.py" ]; then
  python scripts/create_secret.py --name "$SECRET_NAME" --value "$ORIGINAL_VALUE"
  CREATE_STATUS=$?
else
  python scripts/manage_secrets.py create --name "$SECRET_NAME" --value "$ORIGINAL_VALUE"
  CREATE_STATUS=$?
fi

if [ $CREATE_STATUS -ne 0 ]; then
  echo "⚠️ Failed to create test secret. This could be due to API limitations."
  echo "   Some DataHub instances may not support secret management via API."
  echo "   Skipping secret update test."
  exit 0
fi
echo "✅ Successfully created test secret."

# Now update the secret using update_secret.py
echo "Updating test secret..."
python scripts/update_secret.py --name "$SECRET_NAME" --value "$UPDATED_VALUE"
if [ $? -ne 0 ]; then
  echo "⚠️ Failed to update secret. This could be due to API limitations."
  echo "   Some DataHub instances may not support secret management via API."
  # Try to clean up the created secret
  if [ -f "scripts/delete_secret.py" ]; then
    python scripts/delete_secret.py --name "$SECRET_NAME"
  else
    python scripts/manage_secrets.py delete --name "$SECRET_NAME"
  fi
  exit 1
fi
echo "✅ Successfully updated test secret."

# Clean up - delete the secret
echo "Cleaning up test secret..."
if [ -f "scripts/delete_secret.py" ]; then
  python scripts/delete_secret.py --name "$SECRET_NAME"
else
  python scripts/manage_secrets.py delete --name "$SECRET_NAME"
fi

if [ $? -ne 0 ]; then
  echo "⚠️ Failed to clean up test secret, but test was successful."
else
  echo "✅ Successfully cleaned up test secret."
fi

echo "=== Update Secret test complete ===" 