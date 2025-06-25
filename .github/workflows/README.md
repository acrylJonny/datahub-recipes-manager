# DataHub Recipes Manager Workflows

This directory contains GitHub Actions workflows for managing DataHub metadata and configurations across different environments.

## 🔄 Core Workflows

### Infrastructure & Deployment
- **`deploy.yml`** - Deploys the web UI application to production
- **`ci.yml`** - Continuous integration tests and validation
- **`test-integration.yml`** - Integration tests for DataHub connectivity

### Recipe Management
- **`run-recipe.yml`** - Execute DataHub ingestion recipes
- **`run-now.yml`** - Immediately run specific DataHub ingestion sources
- **`patch-recipe.yml`** - Update existing DataHub recipes
- **`patch-ingestion-source.yml`** - Patch DataHub ingestion source configurations
- **`manage-ingestion.yml`** - Comprehensive ingestion source management

### Environment & Configuration
- **`manage-env-vars.yml`** - Manage environment variables across environments
- **`manage-secrets.yml`** - Handle DataHub secrets and credentials

### Validation & Quality
- **`pr-validation.yml`** - Validate pull requests before merge
- **`validate-metadata-migration.yml`** - Validate metadata migration operations
- **`update-workflow-docs.yml`** - Automatically update workflow documentation

## 📊 Metadata Entity Workflows

These workflows process `mcp_file.json` files using DataHub's [metadata-file source](https://docs.datahub.com/docs/generated/ingestion/sources/metadata-file) to ingest MetaChange Proposals (MCPs) directly into DataHub.

### Entity Type Workflows
- **`manage-tags.yml`** - Process tag metadata from `metadata-manager/**/tags/mcp_file.json`
- **`manage-glossary.yml`** - Process glossary terms from `metadata-manager/**/glossary/mcp_file.json`
- **`manage-domains.yml`** - Process domain metadata from `metadata-manager/**/domains/mcp_file.json`
- **`manage-structured-properties.yml`** - Process structured properties from `metadata-manager/**/structured_properties/mcp_file.json`
- **`manage-metadata-tests.yml`** - Process metadata tests from `metadata-manager/**/metadata_tests/mcp_file.json`
- **`manage-data-products.yml`** - Process data products from `metadata-manager/**/data_products/mcp_file.json`
- **`manage-assertions.yml`** - Process individual assertion JSON files from `metadata-manager/**/assertions/`

### How MCP File Workflows Work

1. **Trigger Conditions:**
   - Automatic: When `mcp_file.json` is modified in the respective entity directory
   - Manual: Via workflow dispatch with environment selection

2. **Processing Steps:**
   - Install `acryl-datahub[base]` package
   - Validate JSON format of the MCP file
   - Create DataHub ingestion recipe using the file source
   - Run ingestion with dry-run for PRs, actual ingestion for main/develop branches

3. **Environment Support:**
   - Processes files for `dev`, `staging`, and `prod` environments
   - Uses environment-specific secrets: `DATAHUB_GMS_URL_<ENV>` and `DATAHUB_GMS_TOKEN_<ENV>`
   - Falls back to default secrets if environment-specific ones don't exist

4. **Output:**
   - PR comments with processing results and entity counts
   - Artifact uploads with ingestion recipes and summaries
   - Dry-run validation for pull requests

## 🎯 Specialized Workflows

### Metadata Migration
- **`migrate-metadata.yml`** - Migrate metadata between DataHub environments
- **`validate-metadata-migration.yml`** - Validate metadata migration operations

### Policy Management  
- **`manage-policy.yml`** - Manage DataHub access policies

## 🔧 Environment Variables & Secrets

### Required Secrets
Each workflow requires DataHub connection credentials:

```yaml
# Global defaults
DATAHUB_GMS_URL: "https://your-datahub-instance.com:8080"
DATAHUB_GMS_TOKEN: "your-datahub-token"

# Environment-specific (optional, falls back to global)
DATAHUB_GMS_URL_DEV: "https://dev-datahub.com:8080"
DATAHUB_GMS_TOKEN_DEV: "dev-token"
DATAHUB_GMS_URL_STAGING: "https://staging-datahub.com:8080"  
DATAHUB_GMS_TOKEN_STAGING: "staging-token"
DATAHUB_GMS_URL_PROD: "https://prod-datahub.com:8080"
DATAHUB_GMS_TOKEN_PROD: "prod-token"
```

### Additional Secrets
- **Database connections:** `PG_HOST_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD`
- **Snowflake:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, etc.

## 📁 Directory Structure

```
metadata-manager/
├── dev/
│   ├── tags/mcp_file.json
│   ├── glossary/mcp_file.json  
│   ├── domains/mcp_file.json
│   ├── structured_properties/mcp_file.json
│   ├── metadata_tests/mcp_file.json
│   ├── data_products/mcp_file.json
│   └── assertions/
│       ├── assertion_id_1.json
│       └── assertion_id_2.json
├── staging/
│   └── [same structure]
└── prod/
    └── [same structure]
```

## 🚀 Usage Examples

### Manual Workflow Execution
```bash
# Process tags for dev environment (dry run)
gh workflow run manage-tags.yml -f environment=dev -f dry_run=true

# Process glossary for production
gh workflow run manage-glossary.yml -f environment=prod -f dry_run=false

# Run specific ingestion source
gh workflow run run-now.yml -f environment=dev -f source_id=postgres-source
```

### Automatic Triggers
- Push to `main` or `develop` branches with changes to MCP files
- Pull requests modifying MCP files (runs in dry-run mode)

## 📋 MCP File Format

MCP files contain MetaChange Proposals in JSON format:

```json
[
  {
    "entityType": "tag",
    "entityUrn": "urn:li:tag:example",
    "changeType": "UPSERT", 
    "aspectName": "tagProperties",
    "aspect": {
      "value": "{\"name\": \"Example\", \"description\": \"Example tag\"}",
      "contentType": "application/json"
    }
  }
]
```

## 🔍 Monitoring & Debugging

- **Workflow Runs:** Check the Actions tab in GitHub
- **PR Comments:** Automated comments show processing results
- **Artifacts:** Download ingestion recipes and summaries from workflow runs
- **Logs:** Detailed logs available in each workflow step

## 📚 References

- [DataHub Metadata File Source Documentation](https://docs.datahub.com/docs/generated/ingestion/sources/metadata-file)
- [DataHub CLI Documentation](https://datahubproject.io/docs/cli/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions) 