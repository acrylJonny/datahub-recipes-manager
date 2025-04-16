#!/bin/bash
# Test script for manage_policies.py

set -e

# Change to the root directory
cd "$(dirname "$0")/.."

# Check if .env file exists
if [ ! -f .env ]; then
  echo "Error: .env file not found. Please create it with the required variables."
  exit 1
fi

# Load environment variables from .env file
source .env

# Check if DATAHUB_SERVER or DATAHUB_GMS_URL is set
if [ -z "$DATAHUB_SERVER" ] && [ -z "$DATAHUB_GMS_URL" ]; then
  echo "Error: DATAHUB_SERVER or DATAHUB_GMS_URL environment variable not set"
  exit 1
fi

# Create a temporary policy config file
TEMP_POLICY_FILE=$(mktemp)
cat > "$TEMP_POLICY_FILE" << EOF
{
  "id": "test-policy-$(date +%s)",
  "name": "Test Policy",
  "type": "METADATA",
  "state": "ACTIVE",
  "description": "Test policy created by test_manage_policies.sh",
  "resources": [
    {
      "type": "TAG",
      "resource": "*"
    }
  ],
  "privileges": [
    "EDIT_ENTITY_TAGS"
  ],
  "actors": {
    "allUsers": true
  }
}
EOF

echo "=== Testing DataHub Policy Management ==="

echo -e "\n1. Testing connection to DataHub"
# Test connection to DataHub
if python3 -c "
import os
import sys
sys.path.append('.')
from utils.datahub_rest_client import DataHubRestClient
client = DataHubRestClient(os.getenv('DATAHUB_GMS_URL') or os.getenv('DATAHUB_SERVER'), os.getenv('DATAHUB_TOKEN'))
success = client.test_connection()
sys.exit(0 if success else 1)
"; then
  echo "✅ Connection to DataHub successful"
else
  echo "❌ Failed to connect to DataHub"
  rm "$TEMP_POLICY_FILE"
  exit 1
fi

echo -e "\n2. Listing policies"
python3 scripts/manage_policies.py list --limit 5

echo -e "\n3. Creating test policy"
POLICY_ID=$(grep '"id":' "$TEMP_POLICY_FILE" | cut -d'"' -f4)
echo "Creating policy with ID: $POLICY_ID"
python3 scripts/manage_policies.py create --config-file "$TEMP_POLICY_FILE"

echo -e "\n4. Getting the created policy"
python3 scripts/manage_policies.py get --id "$POLICY_ID"

echo -e "\n5. Updating the test policy"
# Create updated policy config
TEMP_UPDATE_FILE=$(mktemp)
cat > "$TEMP_UPDATE_FILE" << EOF
{
  "name": "Updated Test Policy",
  "description": "This policy was updated by test_manage_policies.sh",
  "state": "ACTIVE"
}
EOF

python3 scripts/manage_policies.py update --id "$POLICY_ID" --config-file "$TEMP_UPDATE_FILE"

echo -e "\n6. Getting the updated policy"
python3 scripts/manage_policies.py get --id "$POLICY_ID"

echo -e "\n7. Deleting the test policy"
python3 scripts/manage_policies.py delete --id "$POLICY_ID"

echo -e "\n8. Verifying deletion"
if python3 scripts/manage_policies.py get --id "$POLICY_ID" 2>&1 | grep -q "not found"; then
  echo "✅ Policy was successfully deleted"
else
  echo "❌ Policy deletion verification failed"
fi

# Clean up temporary files
rm "$TEMP_POLICY_FILE" "$TEMP_UPDATE_FILE"

echo -e "\n=== Policy Management Test Completed ===" 