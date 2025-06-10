# DataHub Assertions Management

This directory contains assertion files that are managed through GitHub workflows and can be automatically deployed to DataHub.

## Directory Structure

```
metadata-manager/
├── {environment_name}/
│   └── assertions/
│       ├── create_FIELD_assertion_name.json
│       ├── update_SQL_another_assertion.json
│       └── ...
└── README.md
```

## File Naming Convention

Assertion files follow this naming pattern:
`{operation}_{assertion_type}_{assertion_name}.json`

Where:
- `operation`: `create` or `update`
- `assertion_type`: `FIELD`, `SQL`, `VOLUME`, `FRESHNESS`, `DATASET`, `SCHEMA`, or `CUSTOM`
- `assertion_name`: Sanitized version of the assertion name (lowercase, underscores)

## File Format

Each assertion file contains:

```json
{
  "operation": "create|update",
  "assertion_type": "SQL|FIELD|VOLUME|FRESHNESS|DATASET|SCHEMA|CUSTOM",
  "name": "Human readable assertion name",
  "description": "Assertion description",
  "config": {
    // Original assertion configuration
  },
  "local_id": "local_database_id",
  "graphql_input": {
    "mutation": "createSqlAssertion|createFieldAssertion|...",
    "input": {
      // GraphQL mutation input parameters
    }
  }
}
```

## Supported GraphQL Mutations

The system supports the following DataHub GraphQL mutations:

### Create Operations
- `createDatasetAssertion`
- `createFreshnessAssertion`
- `createVolumeAssertion`
- `createSqlAssertion`
- `createFieldAssertion`

### Update/Upsert Operations
- `updateDatasetAssertion`
- `upsertCustomAssertion`
- `upsertDatasetFreshnessAssertionMonitor`
- `upsertDatasetVolumeAssertionMonitor`
- `upsertDatasetSqlAssertionMonitor`
- `upsertDatasetFieldAssertionMonitor`
- `upsertDatasetSchemaAssertionMonitor`

## GitHub Workflow

The GitHub workflow (`manage-assertions.yml`) automatically:

1. **Triggers** on changes to `metadata-manager/**/assertions/**` files
2. **Processes** assertion files for each environment (dev, staging, prod)
3. **Executes** the appropriate GraphQL mutations against DataHub
4. **Reports** results as PR comments

### Environment Variables

The workflow expects these GitHub secrets:

- `DATAHUB_URL` or `DATAHUB_URL_{ENVIRONMENT}` (e.g., `DATAHUB_URL_DEV`)
- `DATAHUB_TOKEN` or `DATAHUB_TOKEN_{ENVIRONMENT}` (e.g., `DATAHUB_TOKEN_PROD`)

### Workflow Features

- **Environment-specific processing**: Each environment directory is processed separately
- **Dry run support**: Can validate without executing (manual workflow dispatch)
- **Error handling**: Comprehensive error reporting and validation
- **PR comments**: Automatic status updates on pull requests

## Usage

### From the Web UI

1. Navigate to the Assertions page
2. Click the "Add to PR" button (🔀) on any assertion
3. The assertion will be automatically converted and saved to the appropriate directory
4. Commit and push the changes to trigger the workflow

### Manual File Creation

1. Create assertion files in the appropriate environment directory
2. Follow the naming convention and file format
3. Commit and push to trigger the workflow

## Example Files

### SQL Assertion
```json
{
  "operation": "create",
  "assertion_type": "SQL",
  "name": "Data Quality Check",
  "description": "Ensures no null values in critical columns",
  "config": {
    "database_platform": "snowflake",
    "query": "SELECT COUNT(*) FROM table WHERE column IS NOT NULL",
    "expected_result": "SUCCESS"
  },
  "local_id": "123",
  "graphql_input": {
    "mutation": "createSqlAssertion",
    "input": {
      "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
      "statement": "SELECT COUNT(*) FROM table WHERE column IS NOT NULL",
      "operator": "EQUAL_TO",
      "expectedValue": "1",
      "description": "Ensures no null values in critical columns"
    }
  }
}
```

### Field Assertion
```json
{
  "operation": "create",
  "assertion_type": "FIELD",
  "name": "Required Field Check",
  "description": "Validates that required fields are not null",
  "config": {
    "fields": ["field1", "field2"],
    "field_assertion_type": "NOT_NULL"
  },
  "local_id": "124",
  "graphql_input": {
    "mutation": "createFieldAssertion",
    "input": {
      "datasetUrn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
      "fields": ["field1", "field2"],
      "type": "NOT_NULL",
      "description": "Validates that required fields are not null"
    }
  }
}
```

## Troubleshooting

### Common Issues

1. **Invalid JSON format**: Ensure files are valid JSON
2. **Missing required fields**: All fields in the schema are required
3. **Environment not found**: Ensure the environment directory exists
4. **Authentication errors**: Check GitHub secrets configuration
5. **GraphQL errors**: Verify DataHub connection and permissions

### Workflow Logs

Check the GitHub Actions logs for detailed error messages and execution status. 