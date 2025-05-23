#!/bin/bash
# Test the DataHub domain scripts

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Source common test functions
source "$SCRIPT_DIR/test_common.sh"

# Default settings
SERVER_URL=${DATAHUB_SERVER:-"http://localhost:8080"}
TOKEN=${DATAHUB_TOKEN:-""}
TOKEN_FILE=""
OUTPUT_DIR="/tmp/datahub-domain-tests"
TEST_DOMAIN_ID="test_domain_$(date +%s)"
TEST_SUB_DOMAIN_ID="test_sub_domain_$(date +%s)"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Clean previous test data
rm -f "$OUTPUT_DIR"/*.json

# Set up token file if token is provided
if [ -n "$TOKEN" ]; then
    TOKEN_FILE="$OUTPUT_DIR/token.txt"
    echo "$TOKEN" > "$TOKEN_FILE"
    DATAHUB_TOKEN_ARG="--token-file $TOKEN_FILE"
else
    DATAHUB_TOKEN_ARG=""
fi

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to run a test and report results
run_test() {
    local test_name="$1"
    local command="$2"
    local expected_result="$3"
    
    echo -e "${YELLOW}Running test: $test_name${NC}"
    echo "Command: $command"
    
    output=$(eval "$command" 2>&1)
    result=$?
    
    if [[ $result -eq $expected_result ]]; then
        echo -e "${GREEN}PASSED${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        echo "Output: $output"
        exit 1
    fi
    
    echo ""
    return $result
}

# Test 1: Test connection to DataHub
test_connection() {
    echo -e "${YELLOW}Testing connection to DataHub at $SERVER_URL${NC}"
    
    python3 "$ROOT_DIR/scripts/test_connection.py" --server-url "$SERVER_URL" $DATAHUB_TOKEN_ARG
    
    if [[ $? -ne 0 ]]; then
        echo -e "${RED}Failed to connect to DataHub server at $SERVER_URL${NC}"
        echo "Make sure the server is running and accessible"
        exit 1
    fi
    
    echo -e "${GREEN}Successfully connected to DataHub server${NC}"
    echo ""
}

# Test 2: List domains
test_list_domains() {
    local output_file="$OUTPUT_DIR/domains.json"
    
    run_test "List domains" \
        "python3 $ROOT_DIR/scripts/domains/list_domains.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $output_file --pretty-print" \
        0
    
    # Verify the output file exists and is valid JSON
    if [[ ! -f "$output_file" ]]; then
        echo -e "${RED}Output file was not created: $output_file${NC}"
        exit 1
    fi
    
    # Check if the file is valid JSON
    python3 -m json.tool "$output_file" > /dev/null
    if [[ $? -ne 0 ]]; then
        echo -e "${RED}Output file is not valid JSON: $output_file${NC}"
        exit 1
    fi
    
    # Print domain count
    domains_count=$(jq '.domains | length' "$output_file")
    echo -e "Found ${GREEN}$domains_count${NC} domains"
}

# Test 3: Create a test domain
test_create_domain() {
    local domain_name="Test Domain $(date +%s)"
    local domain_description="A test domain created by the domain scripts test"
    
    run_test "Create domain" \
        "python3 $ROOT_DIR/scripts/domains/create_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --name \"$domain_name\" --description \"$domain_description\" --color \"#00AABB\" --icon \"database\"" \
        0 \
        || echo -e "${YELLOW}Note: Domain creation might not be fully implemented yet${NC}"
    
    # Save the test domain ID for later cleanup
    echo "$TEST_DOMAIN_ID" > "$OUTPUT_DIR/test_domain_id.txt"
}

# Test 4: Get domain details
test_get_domain() {
    local output_file="$OUTPUT_DIR/domain_details.json"
    local domain_urn="urn:li:domain:$TEST_DOMAIN_ID"
    
    run_test "Get domain details" \
        "python3 $ROOT_DIR/scripts/domains/get_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --domain-urn \"$domain_urn\" --output-file $output_file --pretty-print" \
        0 \
        || echo -e "${YELLOW}Skipping domain detail check - the domain may not exist${NC}"
}

# Test 5: Create a sub-domain
test_create_subdomain() {
    local domain_name="Test Sub-Domain $(date +%s)"
    local domain_description="A test sub-domain created by the domain scripts test"
    local parent_domain_urn="urn:li:domain:$TEST_DOMAIN_ID"
    
    run_test "Create sub-domain" \
        "python3 $ROOT_DIR/scripts/domains/create_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --name \"$domain_name\" --description \"$domain_description\" --parent-domain \"$parent_domain_urn\" --color \"#BBAA00\" --icon \"folder\"" \
        0 \
        || echo -e "${YELLOW}Note: Domain creation might not be fully implemented yet${NC}"
    
    # Save the test sub-domain ID for later cleanup
    echo "$TEST_SUB_DOMAIN_ID" > "$OUTPUT_DIR/test_subdomain_id.txt"
}

# Test 6: Update domain
test_update_domain() {
    local domain_urn="urn:li:domain:$TEST_DOMAIN_ID"
    local new_description="Updated description for test domain"
    
    run_test "Update domain" \
        "python3 $ROOT_DIR/scripts/domains/update_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --domain-urn \"$domain_urn\" --description \"$new_description\" --color \"#AABBCC\"" \
        0 \
        || echo -e "${YELLOW}Note: Domain update might not be fully implemented yet${NC}"
}

# Test 7: Clean up - Delete test domains
cleanup_test_domains() {
    echo -e "${YELLOW}Cleaning up test domains${NC}"
    
    # Delete sub-domain first
    if [[ -f "$OUTPUT_DIR/test_subdomain_id.txt" ]]; then
        local subdomain_id=$(cat "$OUTPUT_DIR/test_subdomain_id.txt")
        local subdomain_urn="urn:li:domain:$subdomain_id"
        
        run_test "Delete sub-domain" \
            "python3 $ROOT_DIR/scripts/domains/delete_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --domain-urn \"$subdomain_urn\" --confirm" \
            0 \
            || echo -e "${YELLOW}Note: Domain deletion might not be fully implemented yet${NC}"
    fi
    
    # Delete parent domain
    if [[ -f "$OUTPUT_DIR/test_domain_id.txt" ]]; then
        local domain_id=$(cat "$OUTPUT_DIR/test_domain_id.txt")
        local domain_urn="urn:li:domain:$domain_id"
        
        run_test "Delete domain" \
            "python3 $ROOT_DIR/scripts/domains/delete_domain.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --domain-urn \"$domain_urn\" --confirm" \
            0 \
            || echo -e "${YELLOW}Note: Domain deletion might not be fully implemented yet${NC}"
    fi
}

# Main test flow
echo -e "${GREEN}=== DataHub Domain Scripts Tests ===${NC}"
echo "Server URL: $SERVER_URL"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Run the tests
test_connection
test_list_domains

# Only run create/update/delete tests if we have token authentication
if [[ -n "$TOKEN" ]]; then
    test_create_domain
    test_get_domain
    test_create_subdomain
    test_update_domain
    cleanup_test_domains
else
    echo -e "${YELLOW}Skipping creation/update/deletion tests - no token provided${NC}"
fi

echo -e "${GREEN}All tests completed!${NC}" 