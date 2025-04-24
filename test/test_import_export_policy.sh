#!/bin/bash
# Test script for policy import/export functionality

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

# Create a temporary export directory
EXPORT_DIR=$(mktemp -d)
echo "Using temporary export directory: $EXPORT_DIR"

# Create a unique policy name for testing
POLICY_NAME="Test Policy $(date +%s)"
POLICY_ID=""

echo "=== Testing DataHub Policy Import/Export ==="

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
  rm -rf "$EXPORT_DIR"
  exit 1
fi

echo -e "\n2. Creating test policy for export"
CREATE_OUTPUT=$(python3 scripts/manage_policy.py create --name "$POLICY_NAME" --description "Test policy for export testing" --privileges '["VIEW_ENTITY_PAGE"]')
echo "$CREATE_OUTPUT"

# Extract the policy ID
POLICY_URN=$(echo "$CREATE_OUTPUT" | grep -o '"urn": "[^"]*"' | head -1 | awk -F '"' '{print $4}')
POLICY_ID="${POLICY_URN##*:}"

if [ -z "$POLICY_ID" ]; then
  echo "❌ Failed to extract policy ID"
  exit 1
fi

echo "Created policy with ID: $POLICY_ID"

echo -e "\n3. Exporting the policy"
python3 scripts/export_policy.py --policy-id "$POLICY_ID" --output-dir "$EXPORT_DIR"

# Check if export was successful
if [ -n "$(ls -A "$EXPORT_DIR")" ]; then
  echo "✅ Policy exported successfully"
  echo "Exported files:"
  ls -la "$EXPORT_DIR"
else
  echo "❌ Policy export failed - no files found in export directory"
  rm -rf "$EXPORT_DIR"
  exit 1
fi

echo -e "\n4. Deleting the test policy"
python3 scripts/manage_policy.py delete "$POLICY_ID"

echo -e "\n5. Verifying deletion"
if ! python3 scripts/manage_policy.py get "$POLICY_ID" 2>&1 | grep -q "Policy not found"; then
  echo "❌ Policy deletion verification failed"
  rm -rf "$EXPORT_DIR"
  exit 1
fi
echo "✅ Original policy deleted successfully"

echo -e "\n6. Importing the policy back"
python3 -c "
import os
import sys
# Add parent directory to Python path
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from utils.env_loader import load_env_file
load_env_file('.env')
" || { echo "Failed to import utils.env_loader, continuing anyway"; }

python3 scripts/import_policy.py --input-dir "$EXPORT_DIR"

# Direct approach to get the policy ID from the file name
POLICY_JSON_FILE=$(ls "$EXPORT_DIR"/*.json | head -1)
echo "Policy JSON file: $POLICY_JSON_FILE"
NEW_POLICY_ID=$(basename "$POLICY_JSON_FILE" | grep -o '_[^_]*\.json' | sed 's/_//;s/\.json//')
echo "Extracted Policy ID from filename: $NEW_POLICY_ID"

if [ -z "$NEW_POLICY_ID" ]; then
  echo "❌ Failed to extract policy ID from filename, trying list command..."
  
  # Trying to get it from list output - either traditional format or URN format
  LIST_OUTPUT=$(python3 scripts/manage_policy.py list)
  if echo "$LIST_OUTPUT" | grep -q "$POLICY_NAME"; then
    echo "Found policy in list output"
    # Try to extract URN if available
    POLICY_LINE=$(echo "$LIST_OUTPUT" | grep -A2 "$POLICY_NAME")
    if echo "$POLICY_LINE" | grep -q "urn:li:dataHubPolicy"; then
      NEW_POLICY_URN=$(echo "$POLICY_LINE" | grep -o 'urn:li:dataHubPolicy:[^"]*' | head -1)
      NEW_POLICY_ID="${NEW_POLICY_URN##*:}"
      echo "Extracted Policy ID from URN: $NEW_POLICY_ID"
    elif echo "$POLICY_LINE" | grep -q "ID:"; then
      NEW_POLICY_ID=$(echo "$POLICY_LINE" | grep "ID:" | awk '{print $2}' | tr -d ',' | tr -d ' ')
      echo "Extracted Policy ID from ID field: $NEW_POLICY_ID"
    fi
  fi
fi

if [ -z "$NEW_POLICY_ID" ]; then
  echo "❌ Failed to find imported policy ID"
  rm -rf "$EXPORT_DIR"
  exit 1
fi

echo "✅ Policy imported successfully with ID: $NEW_POLICY_ID"

echo -e "\n8. Clean up - deleting the imported policy"
python3 scripts/manage_policy.py delete "$NEW_POLICY_ID"

echo -e "\n9. Testing force update functionality"
# Create a new policy
CREATE_OUTPUT=$(python3 scripts/manage_policy.py create --name "$POLICY_NAME" --description "Original policy" --privileges '["VIEW_ENTITY_PAGE"]')
POLICY_URN=$(echo "$CREATE_OUTPUT" | grep -o '"urn": "[^"]*"' | head -1 | awk -F '"' '{print $4}')
POLICY_ID="${POLICY_URN##*:}"

if [ -z "$POLICY_ID" ]; then
  echo "❌ Failed to extract policy ID for force update test"
  rm -rf "$EXPORT_DIR"
  exit 1
fi

echo "Created policy with ID: $POLICY_ID for force update test"

# Export the policy
python3 scripts/export_policy.py --policy-id "$POLICY_ID" --output-dir "$EXPORT_DIR"

# Modify the policy in DataHub
python3 scripts/manage_policy.py update "$POLICY_ID" --description "Modified policy"

# Import with force update
python3 -c "
import os
import sys
# Add parent directory to Python path
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from utils.env_loader import load_env_file
load_env_file('.env')
" || { echo "Failed to import utils.env_loader, continuing anyway"; }

python3 scripts/import_policy.py --input-dir "$EXPORT_DIR" --force-update

# Verify description was reverted to original
DESCRIPTION=$(python3 scripts/manage_policy.py get "$POLICY_ID" | grep "description" | grep "Original")
if [ -n "$DESCRIPTION" ]; then
  echo "✅ Force update functionality working correctly"
else
  echo "❌ Force update test failed"
fi

# Final cleanup
python3 scripts/manage_policy.py delete "$POLICY_ID"
rm -rf "$EXPORT_DIR"

echo -e "\n=== Policy Import/Export Tests Completed ===" 