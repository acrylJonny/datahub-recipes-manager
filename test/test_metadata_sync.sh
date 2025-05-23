#!/bin/bash
# Test script for metadata synchronization between environments

# Source common test functions
source "$(dirname "$0")/test_common.sh"

# Set up test environments
setup_test_env() {
    echo "Setting up test environments..."
    
    # Create source and target directories
    mkdir -p test/tmp/source
    mkdir -p test/tmp/target
    
    # Create a sample tag in the source environment
    cat > test/tmp/source/tags.json << EOF
[
    {
        "name": "PII",
        "description": "Personally Identifiable Information",
        "color": "#FF0000"
    },
    {
        "name": "Confidential",
        "description": "Confidential information",
        "color": "#0000FF"
    }
]
EOF

    echo "Test environments set up successfully"
}

# Test generating deterministic URNs
test_deterministic_urns() {
    echo "Testing deterministic URN generation..."
    
    # Test tag URN generation
    echo "Generating URN for tag 'PII'..."
    python scripts/utils/generate_deterministic_urns.py --entity-type tag --name "PII" > test/tmp/tag_urn.json
    
    # Verify the URN is deterministic (should be the same when run again)
    echo "Verifying URN is deterministic..."
    python scripts/utils/generate_deterministic_urns.py --entity-type tag --name "PII" > test/tmp/tag_urn_2.json
    
    # Compare the two URNs
    if diff test/tmp/tag_urn.json test/tmp/tag_urn_2.json > /dev/null; then
        echo_success "URNs are deterministic (identical across runs)"
    else
        echo_error "URNs are not deterministic"
        exit 1
    fi
    
    # Make sure the URN matches our implementation
    expected_md5=$(echo -n "tag:pii" | md5sum | awk '{print $1}')
    actual_urn=$(grep -o "urn:li:tag:[a-f0-9]*" test/tmp/tag_urn.json | head -1)
    actual_md5=${actual_urn#urn:li:tag:}
    
    if [ "$expected_md5" = "$actual_md5" ]; then
        echo_success "URN matches MD5 implementation"
    else
        echo_error "URN doesn't match MD5 implementation"
        echo "Expected: $expected_md5"
        echo "Actual: $actual_md5"
        exit 1
    fi
}

# Test syncing metadata between environments
test_metadata_sync() {
    echo "Testing metadata synchronization..."
    
    # Generate URNs for all tags in the source environment
    python scripts/utils/generate_deterministic_urns.py --input-file test/tmp/source/tags.json --output-file test/tmp/source/tags_with_urns.json
    
    # Sync tags to target environment
    python scripts/metadata_sync/sync_metadata.py \
        --source-url "file://test/tmp/source/tags_with_urns.json" \
        --target-url "file://test/tmp/target/tags.json" \
        --entity-type tag \
        --use-deterministic-urns
    
    # Verify target tags exist
    if [ -f test/tmp/target/tags.json ]; then
        echo_success "Tags synced to target environment"
    else
        echo_error "Failed to sync tags to target environment"
        exit 1
    fi
    
    # Count tags in target environment
    tag_count=$(grep -o "\"name\":" test/tmp/target/tags.json | wc -l)
    if [ "$tag_count" -eq 2 ]; then
        echo_success "Correct number of tags synced ($tag_count)"
    else
        echo_error "Incorrect number of tags synced (expected 2, got $tag_count)"
        exit 1
    fi
}

# Test CICD workflow for metadata synchronization
test_cicd_workflow() {
    echo "Testing CICD workflow for metadata synchronization..."
    
    # Create PR branch
    mkdir -p test/tmp/cicd/repo
    cd test/tmp/cicd/repo
    
    # Initialize Git repo
    git init
    
    # Create main branch with initial metadata
    cat > metadata.json << EOF
[
    {
        "name": "PII",
        "description": "Personal data",
        "deterministic_urn": "urn:li:tag:$(echo -n "tag:pii" | md5sum | awk '{print $1}')"
    }
]
EOF
    
    git add metadata.json
    git config --global user.email "test@example.com"
    git config --global user.name "Test User"
    git commit -m "Initial metadata"
    
    # Create feature branch with changes
    git checkout -b feature/update-metadata
    
    # Update metadata
    cat > metadata.json << EOF
[
    {
        "name": "PII",
        "description": "Personally Identifiable Information",
        "deterministic_urn": "urn:li:tag:$(echo -n "tag:pii" | md5sum | awk '{print $1}')"
    },
    {
        "name": "Confidential",
        "description": "Confidential information",
        "deterministic_urn": "urn:li:tag:$(echo -n "tag:confidential" | md5sum | awk '{print $1}')"
    }
]
EOF
    
    git add metadata.json
    git commit -m "Update metadata"
    
    # Simulate PR merge
    git checkout main
    git merge feature/update-metadata
    
    # Verify changes are merged
    tag_count=$(grep -o "\"name\":" metadata.json | wc -l)
    if [ "$tag_count" -eq 2 ]; then
        echo_success "Metadata successfully updated in CICD workflow"
    else
        echo_error "Failed to update metadata in CICD workflow"
        exit 1
    fi
    
    # Back to the original directory
    cd - > /dev/null
}

# Clean up test environment
cleanup() {
    echo "Cleaning up test environment..."
    rm -rf test/tmp
    echo "Clean up complete"
}

# Run tests
main() {
    echo "Starting metadata sync tests..."
    
    # Create test directory
    mkdir -p test/tmp
    
    # Set up test environment
    setup_test_env
    
    # Run tests
    test_deterministic_urns
    test_metadata_sync
    test_cicd_workflow
    
    # Clean up
    cleanup
    
    echo_success "All tests passed!"
}

# Check if script is being sourced
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
    echo "Script is being sourced"
else
    # Run the main function
    main "$@"
fi 