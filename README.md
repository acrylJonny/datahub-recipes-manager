# DataHub Recipe Manager

This repository provides a complete solution for managing [DataHub](https://datahubproject.io/) ingestion recipes through version control and CI/CD pipelines. It enables you to:

- Store and version control your DataHub ingestion recipes as YAML templates
- Parameterize recipes to separate configuration from credentials
- Push and pull recipes to/from DataHub instances via the DataHub API
- Automate recipe deployment through CI/CD workflows
- Manage scheduling and executor configuration remotely
- Trigger immediate execution of ingestion sources

## Architecture Overview

The system separates recipes into three distinct layers:

1. **Recipe Templates** - Reusable, parameterized recipe patterns with environment variable placeholders
2. **Environment Variable Instances** - Sets of configuration values that populate recipe templates
3. **Recipe Instances** - The combination of a template and an environment variables instance

This separation allows you to:
- Reuse the same template with different configurations across environments
- Manage sensitive values separately from recipe templates
- Deploy/undeploy recipes to DataHub with appropriate environment values

## Structure

```
datahub-recipes-manager/
├── .github/workflows/       # CI/CD workflows
├── docker/                  # Docker configuration
│   ├── Dockerfile           # Container definition 
│   ├── docker-compose.yml   # Development setup
│   ├── docker-compose.prod.yml # Production setup
│   └── nginx/               # Web server configuration
├── helm/                    # Kubernetes Helm charts
│   └── datahub-recipes-manager/ # Helm chart for deployment
│       ├── Chart.yaml       # Chart metadata
│       ├── values.yaml      # Default configuration values
│       └── templates/       # Kubernetes template manifests
├── docs/                    # Documentation files
├── recipes/                 # Recipe templates and instances
│   ├── templates/           # Parameterized YAML templates
│   │   ├── dev/             # Environment-specific templates
│   │   ├── test/            # ...  
│   │   ├── staging/         # ...
│   │   └── prod/            # ...
│   ├── instances/           # Concrete recipe instances by environment
│   │   ├── dev/             # Dev environment recipe instances
│   │   ├── test/            # Test environment recipe instances 
│   │   ├── staging/         # Staging environment recipe instances
│   │   └── prod/            # Production environment recipe instances
│   └── pulled/              # Recipes pulled from DataHub
├── params/                  # Parameters and environment variables
│   ├── environments/        # Environment-specific variables
│   │   ├── dev/             # Dev environment variables
│   │   ├── test/            # Test environment variables
│   │   ├── staging/         # Staging environment variables
│   │   └── prod/            # Production environment variables
│   └── default_params.yaml  # Default parameters
├── policies/                # DataHub policies
│   ├── dev/                 # Dev environment policies
│   ├── test/                # ...
│   ├── staging/             # ...
│   └── prod/                # ...
├── scripts/                 # Python scripts for SDK interaction
├── test/                    # Test scripts and fixtures
├── utils/                   # Utility modules
│   ├── template_renderer.py # Template rendering utilities
│   └── datahub_api.py       # DataHub SDK wrapper
└── web_ui/                  # Web UI for recipe and policy management
    ├── manage.py            # Django management script
    ├── static/              # Static assets (CSS, JS, images)
    ├── templates/           # HTML templates
    ├── web_ui/              # Main application code
    │   ├── models.py        # Database models
    │   ├── views.py         # View controllers
    │   ├── services/        # Business logic services
    │   └── management/      # Management commands
    └── media/               # User-uploaded content
```

### Directory Descriptions

#### `/docker`
Contains all Docker-related configurations for containerized deployment.
- **Dockerfile**: Defines the container image build process
- **docker-compose.yml**: Development environment setup with hot-reloading
- **docker-compose.prod.yml**: Production configuration with optimized settings
- **nginx/**: NGINX configuration for serving the application

#### `/helm`
Kubernetes Helm charts for deploying the application to Kubernetes clusters.
- **datahub-recipes-manager/**: Main Helm chart
  - **Chart.yaml**: Metadata and version information
  - **values.yaml**: Default configuration settings
  - **templates/**: Kubernetes manifest templates
  - **charts/**: Dependent charts

#### `/docs`
Documentation files for the project, including guides and reference materials.

#### `/recipes`
Contains all DataHub ingestion recipe templates and instances. Recipes define how data is ingested into DataHub.
- **templates/**: Reusable parameterized recipe templates with environment variable placeholders
  - Organized by environment (dev, test, staging, prod)
  - Files are in YAML format with ${VARIABLE} placeholders
- **instances/**: Concrete recipe instances with specific configuration for each environment
  - Organized by environment (dev, test, staging, prod)
  - Each instance links to a template and environment variables
- **pulled/**: Recipes pulled directly from DataHub, used for importing existing recipes

#### `/params`
Stores all environment variables and parameters used by recipes. This replaces the traditional .env approach with structured YAML files.
- **environments/**: Environment-specific variable definitions
  - Organized by environment (dev, test, staging, prod)
  - Each file contains variable definitions in YAML format
  - Clearly separates secret from non-secret variables
- **default_params.yaml**: Global default parameters that apply across environments

#### `/policies`
Contains DataHub policies for access control and metadata management.
- Organized by environment (dev, test, staging, prod)
- Defines resources, privileges, and actors for each policy
- Exported in JSON format for version control

#### `/scripts`
Utility scripts for interacting with DataHub's API directly.
- Scripts for pushing, pulling, and patching recipes
- Tools for managing policies and secrets
- Command-line utilities for recipe validation and testing

#### `/test`
Test scripts and fixtures for the project

#### `/utils`
Core utility modules used by the scripts and web UI.
- Shared code for DataHub API interaction
- Template rendering utilities
- Helper functions and common operations

#### `/web_ui`
Django-based web interface for managing recipes, policies, and DataHub configurations.
- Full-featured dashboard for recipe and policy management
- Environment variable management interface
- Connection and secret management
- User authentication and authorization

This structure separates configuration from implementation and organizes everything by environment, making it easy to manage DataHub recipes across development, test, staging, and production environments.

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

3. Apply database migrations:
   ```bash
   cd web_ui
   python manage.py migrate
   cd ..
   ```
   
4. Initialize the repository structure:
   ```bash
   python web_ui/manage.py init_repo_structure
   ```

4. Set up your environment variables in params/environments/{env} directory as YAML files:
   ```yaml
   # params/environments/dev/database.yml
   name: "Database Parameters"
   description: "Database connection parameters"
   recipe_type: "mysql"
   parameters:
     HOST: "localhost"
     PORT: 3306
     DATABASE: "example_db"
   secret_references:
     - USERNAME
     - PASSWORD
   ```

### Repository Initialization

The application can load data directly from the file system at startup. When you first run the web application, it will:

1. Scan the repository structure for templates, instances, and environment variables
2. Import all environment definitions from the directory structure
3. Load templates from the recipes/templates directory 
4. Load instances from the recipes/instances directory
5. Load environment variables from params/environments directory

This allows you to manage your recipes and variables directly in Git and have the application automatically reflect your changes.

To enable repository loading on startup, set the `LOAD_REPOSITORY_DATA` environment variable to `true`:

```bash
LOAD_REPOSITORY_DATA=true python web_ui/manage.py runserver
```

Alternatively, you can run the data import manually:

```bash
python web_ui/manage.py import_repository_data
```

### Management Commands

The application includes several Django management commands to help you initialize and manage the repository:

#### Initialize Repository Structure

```bash
python web_ui/manage.py init_repo_structure
```

This command:
- Creates the full directory structure for recipes, templates, params, and policies
- Adds README files explaining each directory's purpose
- Creates example templates and environment variables for MySQL
- Sets up proper .gitignore files
- Creates a default_params.yaml file

It's the fastest way to get started with a new installation.

#### Import Repository Data

```bash
python web_ui/manage.py import_repository_data
```

This command:
- Loads all data from the repository file structure into the database
- Reads environments, templates, instances, and variables from files
- Creates relationships between objects
- Can be run anytime to refresh the database from files

#### Generate Environment File

```bash
python web_ui/manage.py generate_env_file [--force]
```

This command:
- Creates a default .env file with application settings
- Generates a random Django secret key
- Sets up DataHub connection placeholders
- Enables repository data loading by default
- Use `--force` to overwrite an existing .env file

#### Run Development Server

```bash
python web_ui/manage.py runserver
```

This command:
- Starts the Django development server
- Makes the web UI available at http://localhost:8000
- Automatically reloads when code changes

## Usage

### Environment Variables in YAML Format

Environment variables are now stored in YAML format in the `params/environments/{env}` directories instead of traditional .env files. This structured approach provides several key benefits:

1. **Better Organization**: Variables are organized by environment and purpose
2. **Type Support**: Properly handle different data types (strings, numbers, booleans)
3. **Metadata**: Add descriptions, validation rules, and type information
4. **Secret Management**: Clear separation between regular values and secrets
5. **Default Values**: Built-in support for default values
6. **GitHub Integration**: Seamless integration with GitHub Secrets and Actions

Example environment variable file:

```yaml
# params/environments/prod/analytics-db.yml
name: "Analytics Database"
description: "Production analytics database connection parameters"
recipe_type: "mssql"
parameters:
  MSSQL_HOST: "analytics-db.example.com"
  MSSQL_PORT: 1433
  MSSQL_DATABASE: "AnalyticsDB"
  INCLUDE_VIEWS: true
  INCLUDE_TABLES: "*"
secret_references:
  - MSSQL_USERNAME
  - MSSQL_PASSWORD
```

The YAML structure provides several advantages over flat .env files:

- **Structured Data**: Support for nested properties and complex data types
- **Documentation**: Built-in support for descriptions and metadata
- **Security**: Clear separation of sensitive data through secret_references
- **Validation**: Type information aids in validation
- **Environment Isolation**: Variables are clearly separated by environment

When the application loads these files, it:
1. Creates environment variable templates based on the structure
2. Loads the values for each environment
3. Makes connections between templates, instances, and variables
4. Automatically integrates with GitHub Secrets for secret values

You can manage these files directly in the file system or through the web UI.

### Creating a New Recipe

1. Choose an appropriate template from `recipes/templates/` or create a new one
2. Create environment variables in `params/environments/<environment>/` with your specific parameters
3. Create a new instance file in `recipes/instances/<environment>/` that links the template and environment variables
4. Test the recipe locally before deployment

Example template file:
```yaml
# recipes/templates/mssql.yml
name: "MSSQL Template"
description: "Template for MSSQL database ingestion"
recipe_type: "mssql"
source:
  type: "mssql"
  config:
    host: "${MSSQL_HOST}"
    port: "${MSSQL_PORT}"
    database: "${MSSQL_DATABASE}"
    username: "${MSSQL_USERNAME}"
    password: "${MSSQL_PASSWORD}"
    include_views: "${INCLUDE_VIEWS}"
    include_tables: "${INCLUDE_TABLES}"
```

Example instance file:
```yaml
# recipes/instances/prod/analytics-db.yml
name: "Analytics Database"
description: "Production Analytics Database ingestion"
recipe_type: "mssql"
template_name: "MSSQL Template"
env_vars_instance: "Analytics Database"
parameters:
  MSSQL_HOST: "analytics-db.example.com"
  MSSQL_PORT: 1433
  MSSQL_DATABASE: "AnalyticsDB"
  INCLUDE_VIEWS: true
  INCLUDE_TABLES: "*"
secret_references:
  - MSSQL_USERNAME
  - MSSQL_PASSWORD
```

### GitHub Integration

The application deeply integrates with GitHub for version control, secrets management, and CI/CD workflows.

#### File Management

When you save environment variables or recipe templates in the web UI, the application will:
1. Store the files in the appropriate directory structure with proper YAML formatting
2. Create links between templates, instances, and environment variables
3. Structure files according to their environment (dev, test, staging, prod)
4. Generate descriptive commit messages
5. Push changes to the configured branch

#### Secrets Management

For sensitive information like passwords and tokens:
1. Non-secret values are stored directly in YAML files in the `parameters` section
2. Secret values are stored in GitHub Secrets and only referenced in files under `secret_references`
3. Environment-specific secrets are stored as GitHub Environment Secrets when possible
4. Repository-level secrets are used for shared credentials

For example, if your YAML file includes:
```yaml
secret_references:
  - DATABASE_PASSWORD
  - API_TOKEN
```

The system will automatically:
1. Create GitHub repository secrets if they don't exist
2. Create environment-specific secrets if a matching GitHub Environment exists
3. Update existing secrets with new values
4. Link the secrets to the environment variable configuration

#### CI/CD Integration

This repository structure works seamlessly with CI/CD pipelines:

1. **GitHub Actions Workflows** can access variables and secrets during deployment
2. **Environment-specific deployments** use the correct variables for each environment
3. **Security** is maintained by separating sensitive values from templates
4. **Templating** allows reusable recipe patterns across environments

#### Managing GitHub Environments

For optimal secret management:

1. Create GitHub Environments matching your environment names (dev, test, staging, prod)
2. Add environment-specific protection rules and approvals for sensitive environments
3. Store environment-specific secrets in the appropriate GitHub Environment

This approach ensures that:
1. Templates and recipes are properly versioned
2. Sensitive information is securely stored
3. Environment separation is strictly maintained
4. Deployment follows proper CI/CD practices

## Docker and Kubernetes Deployment

The recipes and policies in this system follow a staging-to-deployment workflow:

1. **Staging**: Create and configure recipes, environment variables, and policies in the web interface
   - Recipes and policies created in the system are first saved locally in the database
   - They can be edited, previewed, and validated before being deployed to DataHub

2. **Deployment**: Deploy the configured items to the DataHub instance
   - Recipe instances combine a template with environment variables and deploy to DataHub
   - Environment variables marked as secrets are stored in DataHub's secure storage
   - Non-secret environment variables are populated directly in the recipe at deploy time
   - Policies are deployed to DataHub and immediately become active

3. **Undeployment**: Remove deployed items from DataHub while retaining the configuration locally
   - Undeployed items remain in the staging area for future redeployment
   - This allows for easy testing and version control of configurations

This workflow ensures that:
- Configuration is versioned and managed separately from the actual deployed resources
- Secrets are handled securely and never exposed in recipe definitions
- Recipes can be easily moved between environments by changing only the environment variable instances

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

## Troubleshooting

### Connection Issues

If you're having trouble connecting to DataHub:

1. Verify your DataHub instance is running and accessible
2. Check your connection URL and token in the `.env` file
3. Test the connection using:
   ```bash
   python scripts/test_connection.py
   ```
4. Use the Web UI's connection settings page to validate and troubleshoot

### Tests Failing in CI

Some tests require a live DataHub connection and are automatically skipped in CI environments. These tests include:

- `test_push_and_patch_recipe.py`
- Integration tests that require creating or updating DataHub resources

If you're running these tests locally, ensure you have a DataHub instance running and your credentials are correctly configured in the `.env` file.

### GraphQL Schema Validation Errors

If you see GraphQL schema validation errors, it's likely because your DataHub version has a different GraphQL schema than what the client is expecting. The client has built-in fallback mechanisms to handle these cases:

1. First, it attempts to use GraphQL for better performance and functionality
2. If a schema validation error occurs, it logs an informational message and falls back to the REST API
3. This ensures compatibility across different DataHub versions

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

## Web UI Interface

The DataHub CI/CD Manager now includes a powerful web-based user interface for managing your recipes, policies, and DataHub configurations through an intuitive dashboard.

### Web UI Features

- **Dashboard**: Get a quick overview of your recipes, policies, and connection status
- **Recipe Management**: Create, edit, list, import, and export recipes through a web form
- **Policy Management**: Manage DataHub access policies with a user-friendly interface
- **Connection Settings**: Configure and test your DataHub connections
- **Secrets Management**: Securely manage your DataHub secrets

### Setting Up the Web UI

We've included a setup script to help you quickly set up the Web UI:

```bash
# Run the setup script
python setup_web_ui.py

# Start the web server
cd web_ui && python manage.py runserver
```

The setup script will:
1. Verify your Python installation
2. Install required dependencies
3. Create necessary directories
4. Initialize the Django project structure
5. Apply database migrations

After running the setup script, you can access the web interface at http://localhost:8000.

### Web UI Screenshots

#### Dashboard
The dashboard provides a quick overview of your DataHub connection status, recipes, and policies.

#### Recipe Management
Create, edit, and manage your DataHub ingestion recipes with a user-friendly interface.

#### Policy Management
Manage DataHub access policies with easy-to-use forms for resources, privileges, and actors.

## Testing

The repository includes comprehensive tests for the Python scripts and utilities. To run the tests:

```bash
# Run all tests
bash test/run_all_tests.sh

# Run specific test
python -m pytest scripts/test_render_recipe.py
```

Note: Integration tests that require a live DataHub instance are automatically skipped in CI environments. To run these tests locally, you need a running DataHub instance accessible at the URL specified in your `.env` file.

### Test Directory Structure

The `/test` directory is organized to provide comprehensive test coverage for all aspects of the application:

```
test/
├── recipes/                   # Test recipe files
│   ├── templates/             # Recipe template test files
│   ├── instances/             # Recipe instance test files
│   └── pulled/                # Placeholder for recipes pulled during tests
├── run_all_tests.sh           # Main script to run the full test suite
├── setup_test_env.sh          # Set up test environment variables and resources
├── verify_network.sh          # Validate connectivity to DataHub
├── test_common.sh             # Shared functions used across test scripts
├── test_render.sh             # Test recipe template rendering
├── test_validate.sh           # Test recipe validation
├── test_push_recipe.sh        # Test recipe push to DataHub
├── test_pull_recipe.sh        # Test recipe pull from DataHub
├── test_patch_recipe.sh       # Test recipe updates
├── test_deploy.sh             # Test deployment workflows
├── test_update_secret.sh      # Test secret management
├── test_list_ingestion_sources.sh  # Test source listing
├── test_patch_ingestion_source.sh  # Test updating sources
├── test_run_now.sh            # Test immediate execution
├── test_run_fallback.sh       # Test fallback mechanisms
├── test_api_format.sh         # Test API compatibility
├── test_manage_policies.sh    # Test policy management
├── test_policy_management.sh  # Test policy CRUD operations
└── setup_postgres_testdata.sh # Set up test data for database tests
```

### Test Execution Environment

The tests support three modes of execution:

1. **Local Development**: Tests run against a live DataHub instance with your credentials
2. **CI Environment**: Tests run with mock API responses to validate code without a live instance
3. **Integration Testing**: Full end-to-end tests that require a live DataHub connection

The test environment is automatically detected and configured by the `setup_test_env.sh` script, which:
- Sets appropriate environment variables for the current context
- Creates temporary test files and directories
- Configures mock responses when appropriate
- Cleans up after tests complete

### Testing Without UI

If you're using this repository without the web UI, the test suite allows you to:
- Validate your recipe templates
- Test your environment variable configurations
- Verify connectivity to your DataHub instance
- Ensure your GitHub Actions workflows will function correctly

To run tests without the UI:

```bash
# Set up your .env file with DataHub credentials
cp .env.example .env
nano .env  # Edit with your credentials

# Run only the CLI-focused tests
bash test/run_all_tests.sh --skip-ui-tests
```

### Testing With UI

When using the web application, additional tests validate:
- Database models and migrations
- User interface functionality
- Form validation and processing
- Repository data loading and synchronization

To run the full test suite including UI tests:

```bash
# Make sure Django is properly configured
cd web_ui
python manage.py check

# Run the complete test suite
bash ../test/run_all_tests.sh
```

## GitHub Actions Workflows

The project includes several GitHub Actions workflows to automate testing, validation, and deployment:

### Core Workflows

- **CI (`ci.yml`)**: Runs on every push and PR to validate code, recipes, and configurations
  - Lints Python code
  - Runs the test suite
  - Validates recipe templates and instances
  - Ensures all referenced secrets exist

- **Deploy (`deploy.yml`)**: Deploys recipes to DataHub environments
  - Runs automatically when changes are merged to main
  - Can be triggered manually with environment and recipe selection
  - Generates detailed deployment reports

- **PR Validation (`pr-validation.yml`)**: Validates PRs before merging
  - Checks for breaking changes
  - Previews which recipes will be deployed
  - Validates secret references
  - Adds validation results as PR comments

### Utility Workflows

- **Manage Ingestion (`manage-ingestion.yml`)**: List, patch, and run ingestion sources
  - List all ingestion sources in an environment
  - Update configuration for existing sources
  - View detailed source information

- **Manage Secrets (`manage-secrets.yml`)**: Manage DataHub secrets
  - Create new secrets
  - List existing secrets
  - Update secret values
  - Delete unused secrets

- **Manage Policies (`manage-policy.yml`)**: Manage DataHub access policies
  - Create and update access policies
  - Manage privileges and permissions
  - Control DataHub resource access

- **Run Recipe (`run-recipe.yml`)**: Execute recipes immediately
  - Manually trigger ingestion runs
  - View execution results
  - Debug recipe issues

### Special Purpose Workflows

- **Patch Recipe (`patch-recipe.yml`)**: Update specific recipes
  - Make targeted changes to deployed recipes
  - Update schedule or configuration

- **Run Now (`run-now.yml`)**: Immediately execute ingestion sources
  - Uses GraphQL API for fastest execution
  - Falls back to REST API if needed

- **Test Integration (`test-integration.yml`)**: Verify DataHub connectivity
  - Test DataHub API access
  - Validate credentials
  - Check environment configuration

- **Manage Environment Variables (`manage-env-vars.yml`)**: Manage environment variables
  - Create and update environment variable configurations
  - Sync with GitHub Secrets
  - Link variables to recipes

### Using Workflows Without UI

For users who prefer to work directly with GitHub rather than the web UI:

1. **Clone the repository** to your local machine
2. **Create or modify recipe templates** in `recipes/templates/`
3. **Set up environment variables** in `params/environments/<env>/`
4. **Create recipe instances** in `recipes/instances/<env>/`
5. **Commit and push** your changes
6. **Use GitHub Actions UI** to manually trigger workflows:
   - Deploy recipes
   - Manage secrets
   - Run ingestion
   - View execution results

This workflow allows you to maintain all the benefits of version control and CI/CD without requiring the web UI.

### Integration with External CI/CD

The workflows can be customized to integrate with external CI/CD systems:

- Add webhook triggers for external systems
- Customize environment variables for different deployment targets
- Add custom validation steps
- Integrate with notification systems

For more details on each workflow, see the `.github/workflows/README.md` file.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Development Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Run tests to verify your setup: `bash test/run_all_tests.sh`

### Adding New Features

When adding new features:
1. Add appropriate tests
2. Update documentation
3. Follow the existing code style and patterns
4. Consider backwards compatibility with different DataHub versions

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [DataHub Project](https://datahubproject.io/) - The open-source metadata platform this manager integrates with
- [Acryl Data](https://www.acryldata.io/) - Maintainers of the DataHub SDK
- All contributors who have helped improve this project

## Staging and Deployment Workflow

The recipes and policies in this system follow a staging-to-deployment workflow:

1. **Staging**: Create and configure recipes, environment variables, and policies in the web interface
   - Recipes and policies created in the system are first saved locally in the database
   - They can be edited, previewed, and validated before being deployed to DataHub

2. **Deployment**: Deploy the configured items to the DataHub instance
   - Recipe instances combine a template with environment variables and deploy to DataHub
   - Environment variables marked as secrets are stored in DataHub's secure storage
   - Non-secret environment variables are populated directly in the recipe at deploy time
   - Policies are deployed to DataHub and immediately become active

3. **Undeployment**: Remove deployed items from DataHub while retaining the configuration locally
   - Undeployed items remain in the staging area for future redeployment
   - This allows for easy testing and version control of configurations

This workflow ensures that:
- Configuration is versioned and managed separately from the actual deployed resources
- Secrets are handled securely and never exposed in recipe definitions
- Recipes can be easily moved between environments by changing only the environment variable instances