#!/bin/bash
# Test script for recipe run functionality

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

# Use a known recipe ID for testing
# This test is commented out by default to avoid unintended executions
TEST_SOURCE_ID="analytics-database-prod"

echo "Testing the run recipe script..."
echo "To run a recipe, use: python ../scripts/run_recipe.py --id <recipe_id>"
echo ""
echo "Run recipe test commands available:"
echo "  python ../scripts/run_recipe.py --id ${TEST_SOURCE_ID} # Run a specific recipe"
echo "  python ../scripts/run_recipe.py --help # Show help for all options"
echo ""

# Uncomment the following line to actually run a recipe during testing
# python ../scripts/run_recipe.py --id ${TEST_SOURCE_ID}

echo "Run recipe test completed." 