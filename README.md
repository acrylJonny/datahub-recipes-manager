# DataHub Recipe Manager

This repository provides a complete solution for managing [DataHub](https://datahubproject.io/) ingestion recipes through version control and CI/CD pipelines. It enables you to:

- Store and version control your DataHub ingestion recipes as YAML templates
- Parameterize recipes to separate configuration from credentials
- Push and pull recipes to/from DataHub instances via the DataHub API
- Automate recipe deployment through CI/CD workflows
- Manage scheduling and executor configuration remotely
- Trigger immediate execution of ingestion sources

## Structure

```
datahub-recipes-manager/
├── .github/workflows/       # CI/CD workflows
├── recipes/                 # Recipe templates and instances
│   ├── templates/           # Parameterized YAML templates
│   └── instances/           # Concrete recipe instances by environment
├── params/                  # Parameter templates and defaults
│   └── default_params.yaml  # Default parameters template 
├── scripts/                 # Python scripts for SDK interaction
├── utils/                   # Utility modules
│   ├── template_renderer.py # Template rendering utilities
│   └── datahub_api.py       # DataHub SDK wrapper
└── config/                  # Configuration files
```

## Getting Started

### Prerequisites

- Python 3.8+
- Access to a DataHub instance with API access
- Personal Access Token (PAT) for DataHub authentication
- DataHub SDK (`acryl-datahub`)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-org/datahub-recipes-manager.git
   cd datahub-recipes-manager
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file based on `.env.example` and add your DataHub credentials:
   ```
   DATAHUB_GMS_URL=http://your-datahub-instance:8080
   DATAHUB_TOKEN=your_personal_access_token
   # Add additional secrets for your data sources
   MSSQL_PASSWORD=your_password
   ```

## Usage

### Parameter Templates

The repository includes a default parameters template (`params/default_params.yaml`) that provides a starting point for creating new recipe configurations. This template includes:

- Common execution parameters
- Source configurations
- Sink configurations
- Recipe-specific settings
- Advanced configuration options

You can use this template as a reference when creating new recipe instances, or inherit from it in your environment-specific parameter files.

### Creating a New Recipe

1. Choose an appropriate template from `recipes/templates/` or create a new one
2. Create a new instance file in `recipes/instances/<environment>/` with your specific parameters
3. Test the recipe locally before deployment

Example instance file:
```yaml
# recipes/instances/prod/analytics-db.yml
recipe_id: analytics-database-prod
recipe_type: mssql
description: "Production Analytics Database ingestion"

parameters:
  MSSQL_USERNAME: "datahub_reader"
  MSSQL_HOST: "analytics-db.example.com"
  MSSQL_PORT: "1433"
  MSSQL_DATABASE: "AnalyticsDB"
  # ... other parameters
  
  EXECUTOR_ID: "remote"
  SCHEDULE_CRON: "0 2 * * *"  # Run daily at 2 AM
  SCHEDULE_TIMEZONE: "UTC"

secret_references:
  - MSSQL_PASSWORD
  - DATAHUB_GMS_URL
  - DATAHUB_TOKEN
```

### Pushing a Recipe to DataHub

```bash
# Push a recipe to DataHub
python scripts/push_recipe.py --instance recipes/instances/prod/analytics-db.yml

# Push and trigger immediate ingestion
python scripts/push_recipe.py --instance recipes/instances/prod/analytics-db.yml --run-ingestion

# Push recipe and create secrets in DataHub
python scripts/push_recipe.py --instance recipes/instances/prod/analytics-db.yml --create-secrets
```

### Managing Secrets in DataHub

DataHub supports storing secrets securely for use in ingestion recipes. This repository provides tools to manage these secrets:

```bash
# Create a secret in DataHub
python scripts/manage_secrets.py create --name your_secret_name --value your_secret_value

# List all secrets in DataHub
python scripts/manage_secrets.py list

# Delete a secret from DataHub
python scripts/manage_secrets.py delete --name your_secret_name
```

Note: 
- Secret management requires that the GraphQL or OpenAPI endpoints are enabled and accessible on your DataHub instance.
- DataHub connection credentials (DATAHUB_GMS_URL and DATAHUB_TOKEN) are automatically excluded from being stored as secrets.

### Advanced DataHub Integration

This repository includes enhanced GraphQL support through the DataHubGraph client:

```bash
# Test GraphQL integration
python scripts/test_graph.py
```

When the `acryl-datahub` SDK is installed, the client will automatically use the more powerful DataHubGraph client for GraphQL operations. This enables:

- Advanced metadata queries and updates
- Better error handling and retry logic
- Support for complex DataHub operations

If the SDK is not available, the client will fall back to direct REST API calls, ensuring compatibility in all environments.

### Pulling Existing Recipes from DataHub

```bash
# Pull all recipes
python scripts/pull_recipe.py --output-dir recipes/pulled

# Pull a specific recipe
python scripts/pull_recipe.py --output-dir recipes/pulled --source-id your-recipe-id
```

### Updating a Recipe Schedule

```bash
# Update the schedule of an existing recipe
python scripts/update_schedule.py --source-id your-recipe-id --cron "0 */4 * * *" --timezone "America/Chicago"
```

### Validating Recipes

```bash
# Validate all recipes
python scripts/validate_recipe.py --templates recipes/templates/*.yml --instances recipes/instances/**/*.yml
```

### Triggering Immediate Execution of a Recipe

You can trigger an immediate execution of an ingestion source using the `run_now.py` script:

```bash
# Run an ingestion source immediately
python scripts/run_now.py --source-id your-recipe-id

# With specific server and token
python scripts/run_now.py --source-id your-recipe-id --server http://your-datahub:8080 --token your-token
```

This script uses DataHub's GraphQL API to trigger the execution request, with a fallback to REST API if the GraphQL mutation is not supported by your DataHub version.

### Managing DataHub Policies

You can manage DataHub policies (access control) using the `manage_policy.py` script:

```bash
# List all policies
python scripts/manage_policy.py list

# Get a specific policy
python scripts/manage_policy.py get my-policy-id

# Create a new policy
python scripts/manage_policy.py create --name "Test Policy" --description "Test policy description" --type METADATA_POLICY

# Update a policy
python scripts/manage_policy.py update my-policy-id --name "Updated Policy" --description "Updated description"

# Delete a policy
python scripts/manage_policy.py delete my-policy-id
```

The policy management functionality provides:
- Creating, retrieving, updating, and deleting DataHub policies
- Support for defining policy resources, privileges, and actors
- JSON input for complex policy configurations
- GraphQL-first approach with REST API fallbacks for compatibility with various DataHub versions

### Importing and Exporting Policies

You can export DataHub policies to files for backup or version control, and import them to another DataHub instance:

```bash
# Export all policies to a directory
python scripts/export_policy.py --output-dir policies/

# Export a specific policy
python scripts/export_policy.py --policy-id my-policy-id --output-dir policies/

# Import all policies from a directory
python scripts/import_policy.py --input-dir policies/

# Import a specific policy file
python scripts/import_policy.py --input-file policies/my_policy_123.json

# Skip existing policies or force update during import
python scripts/import_policy.py --input-dir policies/ --skip-existing
python scripts/import_policy.py --input-dir policies/ --force-update
```

These tools are useful for:
- Migrating policies between environments (dev → staging → prod)
- Backing up policies for disaster recovery
- Version controlling policies in Git
- Standardizing policies across multiple DataHub instances

## Features

### DataHub Integration
- **Push Recipes**: Deploy recipe instances to DataHub
- **Pull Recipes**: Retrieve existing recipes from DataHub
- **Patch Recipes**: Update existing recipes with new configuration or schedule
- **Run Ingestion**: Trigger immediate execution of ingestion sources
- **Manage Secrets**: Create, list, update, and delete DataHub secrets
- **Manage Policies**: Create, list, update, and delete DataHub access control policies

### Recipe Management
- **Validate**: Validate recipes to ensure they're complete and accurate
- **Render**: Generate final recipe files from templates and variable files
- **Test**: Comprehensive testing framework for recipe deployment

## CI/CD Integration

This repository includes GitHub Actions workflows for:

1. **Validating recipes**: The `ci.yml` workflow validates all recipe files on push or pull request
2. **Deploying recipes**: The `deploy.yml` workflow deploys recipes to DataHub on push to main or manually

### GitHub Secrets

For the CI/CD workflows to function, you need to set up the following secrets in your GitHub repository:

- `DATAHUB_GMS_URL`: The URL of your DataHub GMS server
- `DATAHUB_TOKEN`: Your DataHub Personal Access Token
- Any additional secrets referenced in your recipe instances (e.g., `MSSQL_PASSWORD`)

## GitHub Workflows Setup Guide

This section provides a detailed walkthrough for setting up and using the GitHub workflows for DataHub recipes and policy management.

### Initial Setup

1. **Fork or clone this repository**
   ```bash
   git clone https://github.com/your-org/datahub-recipes-manager.git
   cd datahub-recipes-manager
   ```

2. **Create a new GitHub repository** (if you don't want to use the forked repo)
   - Go to GitHub and create a new repository
   - Push the cloned code to your new repository:
     ```bash
     git remote set-url origin https://github.com/your-username/your-new-repo.git
     git push -u origin main
     ```

3. **Configure GitHub repository settings**
   - Navigate to your repository on GitHub
   - Go to "Settings" > "Actions" > "General"
   - Ensure "Allow all actions and reusable workflows" is selected
   - Save changes

### Setting Up GitHub Secrets

1. **Navigate to your repository's secrets**
   - Go to your repository on GitHub
   - Click "Settings" > "Secrets and variables" > "Actions"
   - Click "New repository secret"

2. **Add DataHub connection secrets** (required for all workflows)
   - Add `DATAHUB_SERVER` or `DATAHUB_GMS_URL` with the URL of your DataHub instance (e.g., `http://your-datahub-instance:8080`)
   - Add `DATAHUB_TOKEN` with your DataHub Personal Access Token

3. **Add environment-specific secrets** (for different environments)
   - Click "New environment" in the Environments section
   - Create environments for `dev`, `staging`, and `prod`
   - For each environment, add the appropriate secrets:
     ```
     DATAHUB_SERVER (environment-specific URL)
     DATAHUB_TOKEN (environment-specific token)
     ```
   
4. **Add data source credentials** (based on your data sources)
   - Add any credentials needed for your data sources, for example:
     ```
     MSSQL_PASSWORD
     SNOWFLAKE_PASSWORD
     BIGQUERY_CREDENTIALS (content of your service account JSON)
     ```

### Setting Up Environments

1. **Create environment configurations**
   - Navigate to "Settings" > "Environments"
   - For each environment (`dev`, `staging`, `prod`), click "Configure"
   
2. **Add deployment protection rules** (optional but recommended)
   - Enable "Required reviewers" for production environments
   - Add team members who need to approve production deployments
   
3. **Set environment variables** (if needed)
   - Add any environment-specific variables under each environment configuration

### Using the GitHub Workflows

#### Recipe Deployment Workflow

1. **To trigger the recipe deployment workflow manually**:
   - Go to the "Actions" tab in your repository
   - Select the "Deploy" workflow
   - Click "Run workflow"
   - Select the branch, environment, and recipe to deploy
   - Click "Run workflow"

2. **To use the automatic deployment via commits**:
   - Add or modify recipe files in the `recipes/instances/<environment>/` directory
   - Commit and push your changes to the main branch
   - The workflow will automatically deploy the recipes to the specified environment

#### Managing Ingestion Sources

1. **To list all ingestion sources**:
   - Go to the "Actions" tab
   - Select the "Manage Ingestion" workflow
   - Click "Run workflow"
   - Select the action "list" and the target environment
   - Click "Run workflow"

2. **To run an ingestion source immediately**:
   - Go to the "Actions" tab
   - Select the "Manage Ingestion" workflow
   - Click "Run workflow"
   - Set action to "run-now", select the environment, and provide the source ID
   - Click "Run workflow"

3. **To patch an ingestion source**:
   - Go to the "Actions" tab
   - Select the "Manage Ingestion" workflow
   - Click "Run workflow"
   - Set action to "patch", select the environment, and provide the source ID and any updates
   - Click "Run workflow"

#### Managing Secrets

1. **To create a new secret**:
   - Go to the "Actions" tab
   - Select the "Manage Secrets" workflow
   - Click "Run workflow"
   - Set action to "create", select the environment, and provide the secret name and value
   - Click "Run workflow"

2. **To list all secrets**:
   - Go to the "Actions" tab
   - Select the "Manage Secrets" workflow
   - Click "Run workflow"
   - Set action to "list" and select the environment
   - Click "Run workflow"

3. **To patch a secret**:
   - Go to the "Actions" tab
   - Select the "Manage Secrets" workflow
   - Click "Run workflow"
   - Set action to "patch", select the environment, and provide the secret name and new value
   - Click "Run workflow"

4. **To delete a secret**:
   - Go to the "Actions" tab
   - Select the "Manage Secrets" workflow
   - Click "Run workflow"
   - Set action to "delete", select the environment, and provide the secret name
   - Click "Run workflow"

#### Managing Policies

1. **To list all policies**:
   - Go to the "Actions" tab
   - Select the "Manage Policy" workflow
   - Click "Run workflow"
   - Set action to "list" and select the environment
   - Click "Run workflow"

2. **To create a new policy**:
   - Go to the "Actions" tab
   - Select the "Manage Policy" workflow
   - Click "Run workflow"
   - Set action to "create", select the environment, and fill in the required policy details:
     - policy_name (required)
     - policy_description (optional)
     - policy_type (e.g., METADATA_POLICY)
     - policy_state (e.g., ACTIVE)
     - policy_resources (JSON list)
     - policy_privileges (JSON list)
     - policy_actors (JSON object)
   - Click "Run workflow"

3. **To update a policy**:
   - Go to the "Actions" tab
   - Select the "Manage Policy" workflow
   - Click "Run workflow"
   - Set action to "update", select the environment, provide the policy_id, and any fields to update
   - Click "Run workflow"

4. **To delete a policy**:
   - Go to the "Actions" tab
   - Select the "Manage Policy" workflow
   - Click "Run workflow"
   - Set action to "delete", select the environment, and provide the policy_id
   - Click "Run workflow"

### Troubleshooting Workflows

1. **If a workflow fails**:
   - Click on the failed workflow run in the Actions tab
   - Examine the logs to identify the error
   - Common issues include:
     - Invalid credentials or expired tokens
     - Network connectivity issues
     - Permission problems
     - Invalid input parameters

2. **Debugging tips**:
   - Ensure your DataHub instance is accessible from GitHub Actions
   - Verify that your tokens have the necessary permissions
   - Check that your JSON inputs are properly formatted when providing complex parameters
   - Test locally using the corresponding scripts before running via GitHub Actions

3. **Updating workflows**:
   - If you need to modify a workflow:
     - Edit the corresponding `.yml` file in the `.github/workflows/` directory
     - Test changes on a non-production branch first
     - Commit and push changes when verified

### Example Workflow Usage Scenarios

#### Scenario 1: Setting up a new data source in multiple environments

1. Create recipe templates in `recipes/templates/`
2. Create recipe instances for each environment in `recipes/instances/<env>/`
3. Add necessary secrets to GitHub repository settings
4. Use the "Deploy" workflow to deploy recipes to each environment in sequence (dev → staging → prod)

#### Scenario 2: Managing access policies across environments

1. Create a base policy for development using the "Manage Policy" workflow
2. Test the policy in the dev environment
3. Use the workflow again to create the same policy in staging and production
4. Modify policies as needed using the update action

#### Scenario 3: Troubleshooting a failed ingestion

1. Use the "Manage Ingestion" workflow with "list" action to view all sources
2. Identify the problematic source
3. Use the "Patch Recipe" workflow to modify configuration if needed
4. Use the "Run Now" workflow to trigger immediate execution
5. Check logs for any issues

## Contributing

1. Create a new branch for your feature or fix
2. Make your changes and validate them locally
3. Submit a pull request with a clear description of your changes

## License

[Apache License 2.0](LICENSE)

## GitHub Actions

The project includes several GitHub Actions workflows for CI/CD:

- **CI**: Run tests and validate recipes
- **Deploy**: Deploy recipes to DataHub environments
- **Manage Ingestion**: List, patch, and run ingestion sources
- **Manage Secrets**: Create, list, update and delete DataHub secrets
- **Manage Policies**: Create, list, update, and delete DataHub policies
- **Patch Recipe**: Update an existing recipe in DataHub
- **Run Recipe**: Trigger immediate execution of an ingestion source
- **Run Now**: Execute an ingestion source immediately via GraphQL
- **Test Integration**: Run integration tests with DataHub

## API Compatibility

This tool is designed to work with various versions of DataHub APIs:

### API Approach Priority

1. **GraphQL API** (preferred) - Used for all operations when available
2. **OpenAPI v3 Endpoints** - Used as fallback when GraphQL is not available
3. **Direct HTTP Endpoints** - Used as last resort for older DataHub versions

### Key Features

- **Multiple Payload Formats**: Supports both entity wrapper format and direct format for OpenAPI v3
- **JSON Patch Support**: Implements proper JSON Patch format for updating ingestion sources
- **Robust Error Handling**: Gracefully falls back to alternative methods when primary methods fail
- **Test Environment Fallback**: Special handling for test environments to ensure tests pass

### Compatibility Matrix

| DataHub Version | GraphQL API | OpenAPI v3 | JSON Patch | Legacy Endpoints |
|----------------|------------|------------|-----------|-----------------|
| 1.0.0+         | ✅          | ✅          | ✅         | ✅               |
| 0.10.0+        | ✅          | ⚠️ Partial  | ⚠️ Limited | ✅               |
| Older versions | ❌          | ❌          | ❌         | ✅               |

### Troubleshooting

If you encounter API compatibility issues:

1. Check your DataHub version with `curl http://your-datahub-host/config`
2. Ensure you have proper authentication if your DataHub instance requires it
3. For older DataHub instances, set `DATAHUB_LEGACY_MODE=true` in your environment