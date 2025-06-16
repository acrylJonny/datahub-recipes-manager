# DataHub Metadata Extraction and Import

This document describes how to extract and import metadata from/to DataHub using the provided utilities in this repository.

## Overview

The tools provided allow you to:

1. Extract domains, business glossaries, tags, structured properties, and metadata tests from DataHub
2. Import these metadata assets into another DataHub instance
3. Compare metadata between two DataHub instances

## Requirements

- DataHub instance with GraphQL API access
- Access token with appropriate permissions
- Python 3.7+

## Available Tools

### Metadata Client Utility (`utils/datahub_metadata_client.py`)

A utility class that provides methods for interacting with DataHub's metadata via GraphQL.

Key functionality:
- List domains, glossary nodes, terms, tags, etc.
- Export detailed metadata for individual entities
- Extract complete glossary hierarchies
- Import metadata back into DataHub (planned feature)

### Export Metadata Script (`scripts/export_metadata.py`)

Export metadata from DataHub to a JSON file.

```
python scripts/export_metadata.py --server-url http://localhost:8080 \
    --output-file metadata_export.json \
    --export-type all \
    --pretty-print
```

Options:
- `--server-url` - DataHub server URL
- `--output-file` - Path to save the exported metadata
- `--export-type` - Type of metadata to export (all, domains, glossary, tags, properties, tests)
- `--token-file` - File containing DataHub access token
- `--log-level` - Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--pretty-print` - Pretty print JSON output
- `--include-entities` - Include entities associated with domains (can significantly increase size)

### Import Metadata Script (`scripts/import_metadata.py`)

Import metadata from a JSON file into DataHub.

```
python scripts/import_metadata.py --server-url http://localhost:8080 \
    --input-file metadata_export.json \
    --import-type all
```

Options:
- `--server-url` - DataHub server URL
- `--input-file` - Path to the metadata file to import
- `--import-type` - Type of metadata to import (all, domains, glossary, tags, properties, tests)
- `--token-file` - File containing DataHub access token
- `--log-level` - Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--dry-run` - Validate the input file but don't perform the import
- `--overwrite` - Overwrite existing metadata if it exists

### Compare Metadata Script (`scripts/compare_metadata.py`)

Compare metadata between two DataHub instances.

```
python scripts/compare_metadata.py --source-url http://source-datahub:8080 \
    --target-url http://target-datahub:8080 \
    --output-file comparison_results.json \
    --compare-type all
```

Options:
- `--source-url` - Source DataHub server URL
- `--target-url` - Target DataHub server URL
- `--source-token-file` - File containing source DataHub access token
- `--target-token-file` - File containing target DataHub access token
- `--output-file` - Path to save the comparison results
- `--compare-type` - Type of metadata to compare (all, domains, glossary, tags, properties, tests)
- `--log-level` - Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--summary-only` - Only show summary of differences, not detailed differences

## Examples

### Export All Metadata

```bash
python scripts/export_metadata.py \
    --server-url http://localhost:8080 \
    --output-file ~/datahub_metadata_export.json \
    --export-type all \
    --pretty-print
```

### Export Only Domains

```bash
python scripts/export_metadata.py \
    --server-url http://localhost:8080 \
    --output-file ~/domains_export.json \
    --export-type domains \
    --pretty-print
```

### Import Metadata (Dry Run)

```bash
python scripts/import_metadata.py \
    --server-url http://localhost:8080 \
    --input-file ~/datahub_metadata_export.json \
    --import-type all \
    --dry-run
```

### Compare Metadata Between Environments

```bash
python scripts/compare_metadata.py \
    --source-url http://dev-datahub:8080 \
    --target-url http://prod-datahub:8080 \
    --output-file ~/metadata_comparison.json \
    --compare-type all \
    --summary-only
```

## Development and Testing

Run tests with:

```bash
cd test
./test_metadata_client.sh
```

## Limitations

- The import functionality is currently under development and not fully implemented yet
- Export of large metadata assets might require pagination or timeout adjustments
- Some DataHub versions might have slight GraphQL schema differences 

## Domain Management

The domain management functionality has been refactored into smaller, focused modules for better maintainability and ease of use.

### Modular Scripts

Individual scripts are available for each domain operation:

- `scripts/domains/list_domains.py` - List all domains
- `scripts/domains/get_domain.py` - Get details of a specific domain
- `scripts/domains/create_domain.py` - Create a new domain
- `scripts/domains/update_domain.py` - Update an existing domain
- `scripts/domains/delete_domain.py` - Delete a domain

Each script can be run independently with its own set of parameters.

### Domain Manager

For convenience, a unified `domain_manager.py` script provides a single entry point for all domain operations:

```bash
# List all domains
python scripts/domain_manager.py list --server-url http://localhost:8080

# Get a specific domain
python scripts/domain_manager.py get --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering

# Create a domain
python scripts/domain_manager.py create --server-url http://localhost:8080 --name "Engineering" --description "Engineering domain"

# Update a domain
python scripts/domain_manager.py update --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering --description "Updated description"

# Delete a domain
python scripts/domain_manager.py delete --server-url http://localhost:8080 --domain-urn urn:li:domain:engineering
```

### Testing Domain Scripts

The domain scripts have a dedicated test script:

```bash
# Run all domain script tests
test/test_domain_scripts.sh
``` 