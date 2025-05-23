#!/bin/bash
set -e  # Exit on first error

cd "$(dirname "$0")/.." || exit  # Move to project root
echo "=== Running all tests from $(pwd) ==="

# Check for required dependencies
echo "=== Checking for dependencies ==="
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is required but not installed. Please install Python 3."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Docker is required but not installed. Please install Docker."
    exit 1
fi

# Install dependencies if needed
echo "=== Installing dependencies ==="
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing required packages..."
pip install -r requirements.txt

# Run environment setup
echo "=== Setting up test environment ==="
bash test/setup_test_env.sh

# Wait for database to be fully ready
echo "Waiting for database to be fully ready..."
sleep 5

# Run validation tests
echo "=== Running validation tests ==="
bash test/test_validate.sh

# Run rendering tests
echo "=== Running rendering tests ==="
bash test/test_render.sh

# Run deploy tests - only if .env has DataHub credentials
echo "=== Running deployment tests ==="
set +e  # Allow commands to fail without stopping the script
bash test/test_deploy.sh

# Run recipe push test 
echo "=== Running recipe push test ==="
bash test/test_push_recipe.sh
  
# Run recipe pull test
echo "=== Running recipe pull test ==="
bash test/test_pull_recipe.sh

# Run patch ingestion source test
echo "=== Running patch ingestion source test ==="
bash test/test_patch_ingestion_source.sh

# Run ingestion source test
echo "=== Running run ingestion source test ==="
bash test/test_run_now.sh

# Run list ingestion sources test
echo "=== Running list ingestion sources test ==="
bash test/test_list_ingestion_sources.sh

# Run update secret test
echo "=== Running update secret test ==="
bash test/test_update_secret.sh

# Run policy management test
echo "=== Running policy management test ==="
bash test/test_policy_management.sh

# Run import/export policy test
echo "=== Running import/export policy test ==="
bash test/test_import_export_policy.sh

# Run all features test
echo "=== Running all features test ==="
bash test/test_all_features.sh

# Add the metadata_sync test to the list of tests
tests=(
    # Existing tests...
    
    # Add the metadata_sync test
    "test_metadata_sync.sh"
)

set -e  # Restore exit on error behavior

echo "=== All tests completed! ==="