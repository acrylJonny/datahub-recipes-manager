# DataHub Metadata Change Proposal (MCP) Scripts

This directory contains scripts for creating and managing Metadata Change Proposals (MCPs) for DataHub entities, starting with tags.

## Tag Actions

The `tag_actions.py` module provides three main functions for managing tags:

1. **Download Tag JSON**: Download a tag's raw JSON data
2. **Sync Tag to Local**: Sync a tag with the local database
3. **Add Tag to Staged Changes**: Create MCP files for a tag in the `metadata-manager/{environment}/tags` directory

### Command Line Usage

You can use the `cli_tag_actions.py` script to perform these actions from the command line:

```bash
# Download tag JSON
python scripts/mcps/cli_tag_actions.py download --tag-file path/to/tag.json --output path/to/output.json

# Sync tag to local database
python scripts/mcps/cli_tag_actions.py sync --tag-file path/to/tag.json --db-path local_db/tags.json

# Add tag to staged changes
python scripts/mcps/cli_tag_actions.py stage --tag-file path/to/tag.json --environment dev --owner yourUsername --mutation-name dev_mutation
```

### Direct MCP Generation

You can also directly create MCP files for tags using the `create_tag_mcps.py` script:

```bash
python scripts/mcps/create_tag_mcps.py --tag-id myTag --owner yourUsername --environment dev --mutation-name dev_mutation --description "My tag description" --color-hex "#FF5733"
```

## Mutation-Aware URNs

The scripts use mutation-aware URN generation to create deterministic URNs for entities. This ensures that entities with the same logical identity have consistent URNs across different environments.

### How Mutation Names Work

- **With mutation name**: When a mutation name is provided, the system generates a deterministic URN using an MD5 hash that includes the mutation name, creating a unique but consistent identifier.
- **Without mutation name**: If no mutation name is provided, the system preserves the original URN format, using the entity name directly (e.g., `urn:li:tag:my-tag`).

The mutation name typically comes from the environment's associated mutation configuration, not just the environment name itself.

To generate a deterministic URN:

```bash
# Generate URN with mutation name
python scripts/utils/generate_deterministic_urns.py --entity-type tag --name myTag --mutation-name dev_mutation

# Generate standard URN (without mutation)
python scripts/utils/generate_deterministic_urns.py --entity-type tag --name myTag
```

## Directory Structure

MCPs are saved in the following directory structure:

```
metadata-manager/
  ├── dev/                # Environment name
  │   ├── tags/           # Entity type
  │   │   ├── mytag_properties.json    # Properties MCP
  │   │   └── mytag_ownership.json     # Ownership MCP
  │   ├── glossary/       # (future implementation)
  │   └── domains/        # (future implementation)
  └── prod/               # Another environment
      └── ...
```

## Integration with UI

These scripts are designed to be integrated with the DataHub UI, allowing users to:
- Download tag data as raw JSON
- Sync tags with the local database
- Add tags to staged changes for later submission to DataHub

### UI Implementation Guidelines

To implement the three actions on the tags page:

1. **Download Action**:
   - Add a download button/icon
   - On click, call `download_tag_json()` function
   - Trigger browser download of the resulting JSON

2. **Sync to Local Action**:
   - Add a sync button/icon
   - On click, call `sync_tag_to_local()` function
   - Show success/error notification

3. **Add to Staged Changes Action**:
   - Add button with same icon as "Add to PR" on data products page
   - On click, call `add_tag_to_staged_changes()` function with the current environment and mutation name
   - Show success/error notification with paths to created MCP files 