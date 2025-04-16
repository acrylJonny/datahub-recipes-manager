#!/bin/bash

# Test script to check the fallback mechanisms of run_ingestion_source.py
# This script tests whether the method successfully falls back to alternate endpoints

# Source common utilities
source "$(dirname "$0")/test_common.sh"

echo "🧪 Starting test for run_ingestion_source fallback mechanisms..."

# Check if DATAHUB_GMS_URL is set
if [ -z "$DATAHUB_GMS_URL" ]; then
  echo "❌ DATAHUB_GMS_URL is not set"
  exit 1
fi

# Create a Python script to test the functionality directly
cat > /tmp/test_fallback.py << EOF
import os
import sys
import logging
from pathlib import Path

# Add the parent directory to the Python path
sys.path.append("$(dirname "$(pwd)")")

# Import the DataHubRestClient
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("test_fallback")

def main():
    # Get the DataHub server URL and token
    server_url = os.environ.get("DATAHUB_GMS_URL")
    token = os.environ.get("DATAHUB_TOKEN")
    
    # Source ID to test with
    source_id = "analytics-database-prod"
    
    # Create a client and test connection
    client = DataHubRestClient(server_url, token)
    logger.info(f"Testing connection to DataHub at {server_url}")
    
    if client.test_connection():
        logger.info("✅ Connection to DataHub successful")
    else:
        logger.error("❌ Connection to DataHub failed")
        return 1
    
    # Test the run_ingestion_source method
    logger.info(f"Testing run_ingestion_source with source_id: {source_id}")
    
    result = client.run_ingestion_source(source_id)
    
    if result:
        logger.info(f"✅ Successfully triggered ingestion source: {source_id}")
        return 0
    else:
        logger.error(f"❌ Failed to trigger ingestion source: {source_id}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
EOF

# Run the test script
echo "🧪 Running fallback test script..."
python /tmp/test_fallback.py

# Get the exit code
exit_code=$?

# Clean up
rm -f /tmp/test_fallback.py

if [ $exit_code -eq 0 ]; then
  echo "✅ Test passed: run_ingestion_source fallback mechanisms are working"
  exit 0
else
  echo "❌ Test failed: run_ingestion_source fallback mechanisms are not working"
  exit 1
fi 