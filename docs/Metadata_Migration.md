# Metadata Migration Between DataHub Environments

This system enables migration of metadata (tags, glossary terms, domains, structured properties) between different DataHub environments using entity matching based on browse paths and names.

## Overview

The metadata migration system consists of:

1. **Export Functionality**: Export entities with environment-specific mutations applied
2. **Processing Script**: Match entities between environments and generate MCPs
3. **GitHub Workflows**: Automated validation and migration workflows
4. **Platform Instance Filtering**: Support for "Empty" filter (entities without platform instances)

## Components

### 1. Export with Mutations

The "Export with Mutations" button on the Entities page exports filtered entities with environment mutations applied.

**Features:**
- Exports all filtered entities as JSON
- Applies environment-specific URN mutations
- Includes metadata for tags, glossary terms, domains, structured properties
- Supports schema field-level metadata

**Usage:**
1. Filter entities on the Entities page
2. Click "Export with Mutations" button
3. JSON file downloads with mutated URNs

### 2. Processing Script

`scripts/process_metadata_migration.py` processes exported entities and generates MCPs.

**Features:**
- Matches entities between environments using browse path + name
- Applies URN mutations for target environment
- Generates MCPs using DataHub internal schema classes
- Supports dry-run mode for validation

**Usage:**
```bash
# Dry run (validation only)
python scripts/process_metadata_migration.py \
  --input exported_entities.json \
  --target-env staging \
  --mutations-file mutations.json \
  --output-dir validation-output \
  --dry-run \
  --verbose

# Live migration
python scripts/process_metadata_migration.py \
  --input exported_entities.json \
  --target-env staging \
  --mutations-file mutations.json \
  --verbose
```

### 3. GitHub Workflows

#### Migrate Metadata Between Environments
**File**: `.github/workflows/migrate-metadata.yml`

Main workflow for metadata migration with the following jobs:
- **validate-inputs**: Validates file existence and JSON format
- **analyze-entities**: Analyzes exported entities for platforms and metadata
- **migrate-metadata**: Processes migration (dry-run or live)
- **validate-migration**: Validates results after live migration

**Usage:**
1. Upload exported entities JSON to repository
2. Run "Migrate Metadata Between Environments" workflow
3. Select source/target environments and dry-run option
4. Monitor progress and download artifacts

#### Validate Metadata Migration (Dry Run)
**File**: `.github/workflows/validate-metadata-migration.yml`

Validation-only workflow that:
- Validates JSON structure and URN formats
- Runs dry-run migration to generate MCPs
- Triggers automatically on PRs with migration files
- Comments on PRs with validation results

### 4. Platform Instance Filtering

Enhanced platform instance filtering with "Empty" support:

**Empty Filter Logic:**
- "Empty" matches entities WITHOUT platform instances
- Acts as inverse of entities with platform instances
- Can be combined with specific instance filters

**Updated Logic:**
```javascript
// Check for "Empty" filter - matches entities WITHOUT platform instances
if (hasEmptyFilter && (!entityInstance || entityInstance === '')) {
    matches = true;
}

// Check for specific instance matches
if (otherInstances.length > 0 && entityInstance) {
    matches = matches || otherInstances.some(instance => 
        entityInstance.toLowerCase() === instance.toLowerCase()
    );
}
```

## Entity Matching Algorithm

### 1. Browse Path Extraction
Uses the same logic as the JavaScript UI:
- Primary: `browsePathV2.path[].entity.properties.name`
- Fallback: `browsePaths[]`
- Format: `/path/segment1/segment2`

### 2. Entity Name Extraction
Checks multiple sources in order:
- `entity.name`
- `entity.editableProperties.name`
- `entity.properties.name`
- Extracted from URN as fallback

### 3. Matching Key
Entities matched using: `{entity_type}:{browse_path}:{name}` (case-insensitive)

Example: `dataset:/dev/database/schema:table_name`

## Mutations System

### Environment-Specific Mutations
Stored in environment configuration files:
- `params/environments/{environment}/mutations.json`
- `web_ui/environments/{environment}_mutations.json`

### Mutation Types

#### 1. Platform Instance Mapping
```json
{
  "platform_instance_mapping": {
    "dev_instance": "prod_instance",
    "staging_instance": "prod_instance"
  }
}
```

#### 2. Custom Properties (URN Transformations)
```json
{
  "custom_properties": {
    "DEV.": "PROD.",
    "staging.": "prod."
  }
}
```

#### 3. Environment Mapping
```json
{
  "environment_mapping": {
    "STAGING": "PROD",
    "dev": "production"
  }
}
```

## MCP Generation

Uses DataHub internal schema classes to generate proper MCPs:

### Supported Metadata Types
1. **Global Tags** (`GlobalTagsClass`)
2. **Glossary Terms** (`GlossaryTermsClass`)
3. **Domains** (`DomainsClass`)
4. **Structured Properties** (`StructuredPropertiesClass`)
5. **Schema Field Metadata** (field-level tags and glossary terms)

### MCP Structure
```python
MetadataChangeProposalWrapper(
    entityUrn=target_entity_urn,
    aspect=aspect_instance,
    changeType=ChangeTypeClass.UPSERT
)
```

## Workflow Usage Examples

### Example 1: Dev to Staging Migration

1. **Export from Dev Environment:**
   - Filter entities in dev environment
   - Click "Export with Mutations"
   - Download `entities_export_20240315_143022.json`

2. **Upload to Repository:**
   ```bash
   mkdir -p metadata-migration
   cp entities_export_20240315_143022.json metadata-migration/
   git add metadata-migration/
   git commit -m "Add entities for dev to staging migration"
   git push
   ```

3. **Run Migration Workflow:**
   - Go to Actions → "Migrate Metadata Between Environments"
   - Inputs:
     - Entities file: `metadata-migration/entities_export_20240315_143022.json`
     - Source environment: `dev`
     - Target environment: `staging`
     - Dry run: `true` (for validation first)

4. **Review Results:**
   - Download workflow artifacts
   - Review generated MCPs
   - Run again with dry run: `false` for live migration

### Example 2: Automated Validation on PR

1. **Create PR with Migration Files:**
   ```bash
   git checkout -b migrate-customer-metadata
   mkdir -p metadata-migration
   cp exported_entities.json metadata-migration/
   git add .
   git commit -m "Migrate customer metadata to production"
   git push -u origin migrate-customer-metadata
   ```

2. **Automatic Validation:**
   - Validation workflow triggers automatically
   - Comments added to PR with validation results
   - Artifacts available for review

3. **Merge and Execute:**
   - Merge PR after validation passes
   - Run live migration workflow manually

## Security and Environment Secrets

### Required Secrets (per environment)
- `DATAHUB_GMS_URL_DEV`
- `DATAHUB_GMS_TOKEN_DEV`
- `DATAHUB_GMS_URL_STAGING`
- `DATAHUB_GMS_TOKEN_STAGING`
- `DATAHUB_GMS_URL_PROD`
- `DATAHUB_GMS_TOKEN_PROD`

### Secret Format
```
DATAHUB_GMS_URL_{ENVIRONMENT} = https://datahub-{environment}.company.com
DATAHUB_GMS_TOKEN_{ENVIRONMENT} = datahub_token_here
```

## Best Practices

### 1. Always Use Dry Run First
- Validate entity matching before live migration
- Review generated MCPs for correctness
- Check mutation application

### 2. Environment-Specific Mutations
- Maintain mutation files for each environment
- Test mutations with sample entities
- Document mutation logic and rationale

### 3. Entity Filtering
- Use specific filters to reduce migration scope
- Verify entity counts before export
- Consider platform instance filtering for precision

### 4. Validation and Testing
- Use validation workflow for all migrations
- Review artifacts and logs
- Test with small samples first

### 5. Monitoring and Rollback
- Monitor DataHub after migration
- Keep backup of original metadata
- Plan rollback strategy for critical migrations

## Troubleshooting

### Common Issues

#### 1. No Entity Matches Found
**Symptoms:** `0 entity matches out of N source entities`

**Solutions:**
- Verify browse paths match between environments
- Check entity naming consistency
- Review entity type filters
- Validate URN format differences

#### 2. Mutation Not Applied
**Symptoms:** Original URNs in generated MCPs

**Solutions:**
- Check mutation file format and location
- Verify mutation key patterns
- Review mutation logic in logs
- Test mutations with sample URNs

#### 3. MCP Generation Fails
**Symptoms:** `0 MCPs generated` despite matches

**Solutions:**
- Check entity metadata presence
- Verify URN format compatibility
- Review DataHub schema classes import
- Check for malformed metadata structures

#### 4. Workflow Fails
**Symptoms:** Workflow errors or timeouts

**Solutions:**
- Check GitHub secrets configuration
- Verify DataHub connectivity
- Review file paths and permissions
- Check workflow input validation

### Debug Commands

#### Test Entity Matching
```python
python scripts/process_metadata_migration.py \
  --input test_entities.json \
  --target-env staging \
  --dry-run \
  --verbose \
  --output-dir debug-output
```

#### Validate Mutations
```python
from web_ui.metadata_manager.views import apply_urn_mutations

test_urn = "urn:li:tag:test_tag"
mutations = {"custom_properties": {"test": "production"}}
result = apply_urn_mutations(test_urn, mutations)
print(f"Original: {test_urn}")
print(f"Mutated: {result}")
```

#### Check Entity Structure
```python
import json

with open('exported_entities.json', 'r') as f:
    data = json.load(f)

entities = data.get('entities', [])
for entity in entities[:3]:  # Check first 3
    print(f"URN: {entity.get('urn')}")
    print(f"Type: {entity.get('type')}")
    print(f"Name: {entity.get('name')}")
    print(f"Browse Paths: {entity.get('browsePaths', [])}")
    print("---")
```

## Migration Checklist

### Pre-Migration
- [ ] Export entities with proper filtering
- [ ] Upload entities file to repository
- [ ] Configure environment mutations
- [ ] Set up GitHub secrets for target environment
- [ ] Run validation workflow (dry-run)
- [ ] Review generated MCPs and entity matches
- [ ] Verify mutation application in artifacts

### During Migration
- [ ] Monitor workflow execution
- [ ] Check for errors in workflow logs
- [ ] Verify entity matching statistics
- [ ] Monitor MCP generation and emission

### Post-Migration
- [ ] Validate metadata in target DataHub
- [ ] Spot-check sample entities
- [ ] Monitor DataHub performance
- [ ] Document any issues or discrepancies
- [ ] Clean up migration artifacts if successful

This comprehensive metadata migration system provides a robust, validated approach to moving metadata between DataHub environments while maintaining data integrity and providing full audit trails. 