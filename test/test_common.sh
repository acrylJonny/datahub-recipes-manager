#!/bin/bash

# Common utility functions and setup for test scripts

# Check if .env file exists and load it
if [ -f ".env" ]; then
    # Load environment variables
    source .env
    echo "✅ Loaded environment variables from .env file"
else
    echo "⚠️ No .env file found in test directory, using existing environment variables"
fi

# Check parent directory for .env file if not in test directory
if [ -f "../.env" ] && [ -z "${DATAHUB_GMS_URL-}" ]; then
    # Load environment variables
    source ../.env
    echo "✅ Loaded environment variables from parent directory .env file"
fi

# Add color output support
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test connection to DataHub utility function
test_datahub_connection() {
    echo "🔄 Testing connection to DataHub at ${DATAHUB_GMS_URL}..."
    
    python -c "import sys; sys.path.append('..'); from utils.datahub_rest_client import DataHubRestClient; client = DataHubRestClient('${DATAHUB_GMS_URL}', '${DATAHUB_TOKEN-}'); print('✅ Connected to DataHub') if client.test_connection() else sys.exit(1)" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Connection to DataHub successful${NC}"
        return 0
    else
        echo -e "${RED}❌ Connection to DataHub failed${NC}"
        return 1
    fi
} 