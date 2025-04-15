#!/bin/bash
# Test script for recipe patching functionality

# Exit on error
set -e

# Check if .env file exists
if [ ! -f ../.env ]; then
    echo "Error: .env file not found in parent directory."
    echo "Please create an .env file with DATAHUB_SERVER and optionally DATAHUB_TOKEN variables."
    exit 1
fi

# Load environment variables
source ../.env

# Check if required variables are set
if [ -z "$DATAHUB_SERVER" ]; then
    echo "Error: DATAHUB_SERVER environment variable is not set in .env file."
    exit 1
fi

echo "Testing connection to DataHub..."
# Use the DataHubRestClient for connection testing
python - << EOF
import os
import sys
from dotenv import load_dotenv

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.datahub_rest_client import DataHubRestClient

# Load environment variables
load_dotenv("../.env")

# Get DataHub connection details
server_url = os.environ.get("DATAHUB_SERVER")
token = os.environ.get("DATAHUB_TOKEN")

# Create client based on whether token is available
if token:
    client = DataHubRestClient(server_url=server_url, token=token)
else:
    client = DataHubRestClient(server_url=server_url)
    print("Warning: DATAHUB_TOKEN is not set. Connecting without authentication.")

# Test connection
try:
    if client.test_connection():
        print("Successfully connected to DataHub.")
        sys.exit(0)
    else:
        print("Failed to connect to DataHub.")
        sys.exit(1)
except Exception as e:
    print(f"Error testing connection: {e}")
    sys.exit(1)
EOF

CONNECTION_TEST_RESULT=$?
if [ $CONNECTION_TEST_RESULT -ne 0 ]; then
    echo "Connection test failed. Exiting..."
    exit 1
fi

# Use a known recipe ID and example files for testing
TEST_SOURCE_ID="analytics-database-prod"
TEST_RECIPE="../recipes/instances/analytics-db.yml"
TEST_SCHEDULE="0 4 * * *"  # Change this to a different schedule for testing

echo "Testing the patch recipe script..."
echo "Example usage:"
echo "  python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --recipe ${TEST_RECIPE} # Update recipe file only"
echo "  python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --schedule '${TEST_SCHEDULE}' # Update schedule only"
echo "  python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --recipe ${TEST_RECIPE} --schedule '${TEST_SCHEDULE}' # Update both"
echo "  python ../scripts/patch_recipe.py --help # Show help for all options"
echo ""

# Uncomment one of the following lines to actually patch a recipe during testing
# python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --recipe ${TEST_RECIPE}
# python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --schedule "${TEST_SCHEDULE}"
# python ../scripts/patch_recipe.py --id ${TEST_SOURCE_ID} --recipe ${TEST_RECIPE} --schedule "${TEST_SCHEDULE}"

echo "Patch recipe test completed." 