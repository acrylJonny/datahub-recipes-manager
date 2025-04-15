# DataHub Recipe Manager

This repository provides a complete solution for managing [DataHub](https://datahubproject.io/) ingestion recipes through version control and CI/CD pipelines. It enables you to:

- Store and version control your DataHub ingestion recipes as YAML templates
- Parameterize recipes to separate configuration from credentials
- Push and pull recipes to/from DataHub instances via the DataHub API
- Automate recipe deployment through CI/CD workflows
- Manage scheduling and executor configuration remotely

## Structure

```
datahub-recipes-manager/
├── .github/workflows/       # CI/CD workflows
├── recipes/                 # Recipe templates and instances
│   ├── templates/           # Parameterized YAML templates
│   └── instances/           # Concrete recipe instances by environment
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

## Features

### DataHub Integration
- **Push Recipes**: Deploy recipe instances to DataHub
- **Pull Recipes**: Retrieve existing recipes from DataHub
- **Patch Recipes**: Update existing recipes with new configuration or schedule
- **Run Ingestion**: Trigger immediate execution of ingestion sources
- **Manage Secrets**: Create, list, update, and delete DataHub secrets

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
- **Patch Recipe**: Update an existing recipe in DataHub
- **Run Recipe**: Trigger immediate execution of an ingestion source
- **Test Integration**: Run integration tests with DataHub