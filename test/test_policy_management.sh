#!/bin/bash

# Test script for policy management functionality

# Load environment variables from .env file
if [ -f ../.env ]; then
    set -a
    source ../.env
    set +a
    echo "Loaded environment variables from ../.env"
else
    echo "No .env file found"
    exit 1
fi

# Check if DATAHUB_SERVER is set
if [ -z "$DATAHUB_SERVER" ]; then
    echo "DATAHUB_SERVER environment variable is not set"
    exit 1
fi

echo "Testing connection to DataHub at $DATAHUB_SERVER"
PYTHONPATH=.. python -c "
from utils.datahub_rest_client import DataHubRestClient
import os
client = DataHubRestClient(os.environ.get('DATAHUB_SERVER'), os.environ.get('DATAHUB_TOKEN'))
if client.test_connection():
    print('Connection successful')
else:
    print('Connection failed')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "Failed to connect to DataHub"
    exit 1
fi

# Create a unique policy name for testing
POLICY_NAME="Test Policy $(date +%s)"
POLICY_ID=""

echo "===== Testing Policy Management Functions ====="

echo "1. Creating policy: $POLICY_NAME"
CREATE_OUTPUT=$(PYTHONPATH=.. python ../scripts/manage_policy.py create --name "$POLICY_NAME" --description "Test policy created by automated test" --privileges '["VIEW_ENTITY_PAGE"]')
echo "$CREATE_OUTPUT"

# Extract the policy ID from the output using grep and awk
POLICY_ID=$(echo "$CREATE_OUTPUT" | grep -o '"id": "[^"]*"' | head -1 | awk -F '"' '{print $4}')

if [ -z "$POLICY_ID" ]; then
    echo "Failed to extract policy ID from creation output"
    exit 1
fi

echo "Created policy with ID: $POLICY_ID"

echo "2. Getting policy details for: $POLICY_ID"
PYTHONPATH=.. python ../scripts/manage_policy.py get "$POLICY_ID"

echo "3. Updating policy: $POLICY_ID"
PYTHONPATH=.. python ../scripts/manage_policy.py update "$POLICY_ID" --description "Updated description by automated test" --state "ACTIVE"

echo "4. Getting updated policy details for: $POLICY_ID"
PYTHONPATH=.. python ../scripts/manage_policy.py get "$POLICY_ID"

echo "5. Listing all policies"
PYTHONPATH=.. python ../scripts/manage_policy.py list

echo "6. Deleting policy: $POLICY_ID"
PYTHONPATH=.. python ../scripts/manage_policy.py delete "$POLICY_ID"

echo "7. Verifying deletion by attempting to get the policy"
PYTHONPATH=.. python ../scripts/manage_policy.py get "$POLICY_ID" || echo "Policy successfully deleted"

echo "===== Policy Management Tests Completed =====" 