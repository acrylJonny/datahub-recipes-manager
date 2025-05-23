#!/bin/bash
# Test the DataHub metadata client functionality for exporting and importing metadata

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Source common test functions
source "$SCRIPT_DIR/test_common.sh"

# Default settings
SERVER_URL=${DATAHUB_SERVER:-"http://localhost:8080"}
TOKEN=${DATAHUB_TOKEN:-""}
TOKEN_FILE=""
OUTPUT_DIR="/tmp/datahub-metadata-tests"
TEST_ALL_TYPES=true

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

# Test 2: Export all metadata
test_export_all() {
    local output_file="$OUTPUT_DIR/all_metadata.json"
    
    run_test "Export all metadata" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $output_file --export-type all --pretty-print" \
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
    
    echo -e "${GREEN}Successfully exported all metadata to $output_file${NC}"
    
    # Count and display the number of items in each section
    domains_count=$(jq '.domains | length' "$output_file")
    glossary_nodes_count=$(jq '.glossary.nodes | length' "$output_file")
    glossary_terms_count=$(jq '.glossary.terms | length' "$output_file")
    tags_count=$(jq '.tags | length' "$output_file")
    properties_count=$(jq '.structured_properties | length' "$output_file")
    tests_count=$(jq '.tests | length' "$output_file")
    
    echo "Exported:"
    echo " - $domains_count domains"
    echo " - $glossary_nodes_count glossary nodes"
    echo " - $glossary_terms_count glossary terms"
    echo " - $tags_count tags"
    echo " - $properties_count structured properties"
    echo " - $tests_count tests"
    echo ""
}

# Test 3: Export specific metadata types
test_export_specific_types() {
    # Test domains export
    local domains_file="$OUTPUT_DIR/domains.json"
    run_test "Export domains" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $domains_file --export-type domains --pretty-print" \
        0
    
    # Test glossary export
    local glossary_file="$OUTPUT_DIR/glossary.json"
    run_test "Export glossary" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $glossary_file --export-type glossary --pretty-print" \
        0
    
    # Test tags export
    local tags_file="$OUTPUT_DIR/tags.json"
    run_test "Export tags" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $tags_file --export-type tags --pretty-print" \
        0
    
    # Test properties export
    local properties_file="$OUTPUT_DIR/properties.json"
    run_test "Export structured properties" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $properties_file --export-type properties --pretty-print" \
        0
    
    # Test tests export
    local tests_file="$OUTPUT_DIR/tests.json"
    run_test "Export metadata tests" \
        "python3 $ROOT_DIR/scripts/export_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --output-file $tests_file --export-type tests --pretty-print" \
        0
}

# Test 4: Dry run import
test_dry_run_import() {
    local input_file="$OUTPUT_DIR/all_metadata.json"
    
    # Check if the input file exists
    if [[ ! -f "$input_file" ]]; then
        echo -e "${YELLOW}Skipping import test because export file doesn't exist: $input_file${NC}"
        return
    fi
    
    run_test "Dry run import of all metadata" \
        "python3 $ROOT_DIR/scripts/import_metadata.py --server-url $SERVER_URL $DATAHUB_TOKEN_ARG --input-file $input_file --import-type all --dry-run" \
        0
}

# Main test flow
echo -e "${GREEN}=== DataHub Metadata Client Tests ===${NC}"
echo "Server URL: $SERVER_URL"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Run the tests
test_connection

# Only run all tests if TEST_ALL_TYPES is set to true
if [[ "$TEST_ALL_TYPES" == "true" ]]; then
    test_export_all
    test_export_specific_types
    test_dry_run_import
else
    # Run only the main export test
    test_export_all
fi

echo -e "${GREEN}All tests completed successfully!${NC}" 