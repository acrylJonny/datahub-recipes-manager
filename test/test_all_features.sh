#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
cd "$SCRIPT_DIR"

# Set testing environment variables
export IS_TESTING=true
export TESTING_ENVIRONMENT=true

echo -e "${YELLOW}=== DataHub Recipe Manager - Testing All Features ===${NC}"

# Check for .env file
if [ -f ../.env ]; then
    source ../.env
    echo -e "${GREEN}✅ Found .env file with required variables${NC}"
else
    echo -e "${YELLOW}⚠️ No .env file found. Creating a sample one for testing${NC}"
    cat > ../.env << EOL
# DataHub connection settings
DATAHUB_GMS_URL=http://localhost:8080
#DATAHUB_TOKEN=your_token_here

# Database settings for testing
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=postgres
DB_NAME=postgres
EOL
    echo -e "${GREEN}✅ Created sample .env file for testing${NC}"
fi

echo -e "${BLUE}===== TESTING CONNECTION TO DATAHUB =====${NC}"
python ../scripts/test_connection.py
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to connect to DataHub. Check your DATAHUB_GMS_URL and DATAHUB_TOKEN in .env${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Successfully connected to DataHub${NC}"

echo -e "${BLUE}===== SETTING UP POSTGRESQL TEST DATA =====${NC}"
./setup_postgres_testdata.sh
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to set up PostgreSQL test data${NC}"
    echo -e "${YELLOW}Please make sure DataHub's PostgreSQL container is running${NC}"
    exit 1
fi
echo -e "${GREEN}✅ PostgreSQL test data set up successfully${NC}"

echo -e "${BLUE}===== TESTING PUSH RECIPE =====${NC}"
python ../scripts/push_recipe.py --instance ../recipes/instances/dev/analytics-db.yml
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Recipe push failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Recipe push successful${NC}"

echo -e "${BLUE}===== TESTING PULL RECIPES =====${NC}"
python ../scripts/pull_recipe.py --output-dir ./pulled_recipes
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Recipe pull failed${NC}"
    exit 1
fi

# Verify we got at least one recipe
RECIPE_COUNT=$(ls -l ./pulled_recipes | grep -v ^total | wc -l)
if [ $RECIPE_COUNT -lt 1 ]; then
    echo -e "${RED}❌ No recipes were pulled${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Recipe pull successful - Retrieved $RECIPE_COUNT recipes${NC}"

echo -e "${BLUE}===== TESTING RUN RECIPE =====${NC}"
python ../scripts/run_now.py --source-id analytics-database-prod
if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Run recipe failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Run recipe successful${NC}"

echo -e "${BLUE}===== TESTING PATCH RECIPE =====${NC}"
# Back up the original file
cp ../recipes/instances/dev/analytics-db.yml ../recipes/instances/dev/analytics-db.yml.bak

# Modify cron schedule
echo "Updating cron schedule in recipe..."
sed -i '' 's/cron: "0 2 \* \* \*"/cron: "0 3 \* \* \*"/' ../recipes/instances/dev/analytics-db.yml || sed -i 's/cron: "0 2 \* \* \*"/cron: "0 3 \* \* \*"/' ../recipes/instances/dev/analytics-db.yml

# Push the modified recipe
python ../scripts/patch_recipe.py --id analytics-database-prod --schedule "0 3 * * *"
PATCH_STATUS=$?
# Clean up - restore original
mv ../recipes/instances/dev/analytics-db.yml.bak ../recipes/instances/dev/analytics-db.yml
if [ $PATCH_STATUS -ne 0 ]; then
    echo -e "${RED}❌ Recipe patch failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Recipe patch successful${NC}"

# Test creating a secret (if secret value provided)
if [ -n "$DATAHUB_TEST_SECRET_VALUE" ]; then
    echo -e "${BLUE}===== TESTING SECRET MANAGEMENT =====${NC}"
    python ../scripts/manage_secret.py --name test-secret --value "$DATAHUB_TEST_SECRET_VALUE" --action create
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Secret creation failed${NC}"
    else
        echo -e "${GREEN}✅ Secret creation successful${NC}"
        
        # Test patching a secret
        python ../scripts/manage_secret.py --name test-secret --value "updated-$DATAHUB_TEST_SECRET_VALUE" --action patch
        if [ $? -ne 0 ]; then
            echo -e "${RED}❌ Secret patching failed${NC}"
        else
            echo -e "${GREEN}✅ Secret patching successful${NC}"
            
            # Test deleting the secret
            python ../scripts/manage_secret.py --name test-secret --action delete
            if [ $? -ne 0 ]; then
                echo -e "${RED}❌ Secret deletion failed${NC}"
            else
                echo -e "${GREEN}✅ Secret deletion successful${NC}"
            fi
        fi
    fi
else
    echo -e "${YELLOW}Skipping secret management tests (DATAHUB_TEST_SECRET_VALUE not set)${NC}"
fi

# Clean up
echo -e "${BLUE}===== CLEANUP =====${NC}"
rm -rf ./pulled_recipes
echo -e "${GREEN}✅ Test artifacts cleaned up${NC}"

echo -e "${GREEN}=== All Tests Completed Successfully ✅ ===${NC}"

# Create directory for test environment
mkdir -p recipes/instances/dev

cd "$(dirname "$0")" || exit

# Step 1: Test recipe validation
echo "=== Step 1: Testing Recipe Validation ==="
echo "Checking template validation..."
python ../scripts/validate_recipe.py --templates ../recipes/templates/*.yml
echo "Checking instance validation..."
python ../scripts/validate_recipe.py --instances ../recipes/instances/dev/*.yml 

# Step 2: Push Recipe
echo "=== Step 2: Testing Recipe Push ==="
echo "Pushing recipe to DataHub..."
python ../scripts/push_recipe.py --instance ../recipes/instances/dev/analytics-db.yml 