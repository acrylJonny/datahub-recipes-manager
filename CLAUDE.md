# DataHub Recipes Manager - AI Assistant Context

## Repository Overview

This is a comprehensive **DataHub Recipes Manager** that enables teams to treat DataHub metadata as code with proper versioning, testing, and deployment practices. It provides a complete solution for managing DataHub ingestion recipes, metadata entities, and policies through version control, CI/CD pipelines, and a modern web interface.

## 🏗️ Architecture & Core Concepts

### Metadata-as-Code Philosophy
- **Individual JSON Files**: Each metadata entity (assertion, tag, glossary term, etc.) is stored as individual JSON files
- **MCP File Processing**: Uses DataHub's metadata-file source for batch operations
- **URN Mutation**: Automatically mutates URNs for cross-environment deployments
- **GitHub Integration**: Seamless integration with GitHub for version control and CI/CD

### Multi-Layer Architecture
1. **Templates** - Reusable, parameterized patterns with environment variable placeholders
2. **Environment Variables** - Environment-specific configuration values stored as YAML
3. **Instances** - Combination of templates and environment variables for deployment
4. **Metadata Files** - Individual JSON files for each metadata entity
5. **MCP Files** - Batch metadata change proposals processed by DataHub workflows

## 📁 Repository Structure

```
datahub-recipes-manager/
├── .github/workflows/           # GitHub Actions CI/CD workflows
├── datahub-cicd-client/         # Python package for DataHub CI/CD operations
├── docker/                      # Container deployment configurations
├── docs/                        # Comprehensive documentation
├── helm/                        # Kubernetes deployment charts
├── metadata-manager/            # Metadata entity storage (dev/staging/prod)
├── params/                      # Environment variables and parameters
├── policies/                    # DataHub access policies
├── recipes/                     # DataHub ingestion recipes
├── scripts/                     # Python utilities and automation
├── web_ui/                      # Django web application
├── utils/                       # Shared utilities and DataHub API wrappers
└── test/                        # Test configurations and data
```

## 🔧 Technology Stack

### Core Technologies
- **Python 3.8+** - Main programming language
- **Django 4.0+** - Web framework for the UI
- **DataHub CLI** - Official DataHub command-line interface
- **GitHub Actions** - CI/CD automation
- **Docker** - Containerization
- **Kubernetes/Helm** - Production deployment

### Key Dependencies
- **acryl-datahub>=1.0.0.3** - DataHub Python SDK
- **djangorestframework** - REST API framework
- **PyYAML** - YAML configuration handling
- **requests** - HTTP client library
- **pytest** - Testing framework

## 🚀 Development Setup

### Prerequisites
- Python 3.8+ with pip
- DataHub instance with API access
- Personal Access Token for DataHub authentication
- Git for version control
- GitHub account (for CI/CD features)

### Installation Steps
```bash
# 1. Clone and setup environment
git clone <repository-url>
cd datahub-recipes-manager
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt  # For development

# 3. Configure environment variables
cp .env.example .env
# Edit .env with your DataHub connection details

# 4. Initialize Django database
cd web_ui
python manage.py migrate
python manage.py collectstatic --noinput
cd ..

# 5. Start the web interface
cd web_ui
python manage.py runserver
```

### Environment Variables
```bash
# Required for DataHub connection
DATAHUB_GMS_URL=http://your-datahub-instance:8080
DATAHUB_GMS_TOKEN=your-personal-access-token

# Django settings
DJANGO_SECRET_KEY=your-secret-key
DEBUG=True  # Set to False in production
```

## 🌐 Web Interface (Django)

### Key Django Apps
- **metadata_manager** - Core metadata entity management
- **recipe_instances** - Recipe deployment and management
- **env_vars** - Environment variable management
- **environments** - Environment configuration
- **policies** - Policy management
- **github** - GitHub integration
- **script_runner** - Script execution interface

### Key Features
- **Connection Status**: Real-time DataHub connectivity monitoring
- **Metadata Management**: Tags, glossary, domains, properties, contracts, assertions
- **Recipe Management**: Templates, instances, environment variables
- **Policy Management**: Access policies and platform policies
- **GitHub Integration**: PR management and workflow triggers

## 🔄 CI/CD Workflows (GitHub Actions)

### Core Workflows
- **Metadata Processing**: `manage-tags.yml`, `manage-glossary.yml`, `manage-domains.yml`, etc.
- **Recipe Management**: `run-recipe.yml`, `patch-recipe.yml`, `manage-ingestion.yml`
- **Environment Management**: `manage-env-vars.yml`, `manage-secrets.yml`
- **Validation**: `pr-validation.yml`, `test-integration.yml`
- **Deployment**: `deploy.yml`

### Workflow Features
- **Automatic Triggers**: Run on changes to relevant files
- **Environment Matrix**: Process dev, staging, and prod separately
- **Dry Run Support**: PRs run in validation mode
- **Error Handling**: Comprehensive error reporting and rollback
- **Artifact Storage**: Store processing results and logs

## 📊 Metadata Management

### Metadata Entity Types
1. **Tags** - Hierarchical metadata tags
2. **Glossary Terms** - Business glossary with relationships
3. **Domains** - Logical business domains
4. **Structured Properties** - Custom metadata properties
5. **Data Contracts** - Data quality contracts
6. **Assertions** - Data quality assertions (individual JSON files)
7. **Data Products** - Data product definitions

### File Organization
```
metadata-manager/
├── dev/
│   ├── assertions/          # Individual assertion JSON files
│   ├── tags/mcp_file.json   # Tag MCP files
│   ├── glossary/mcp_file.json
│   ├── domains/mcp_file.json
│   ├── structured_properties/mcp_file.json
│   ├── metadata_tests/mcp_file.json
│   └── data_products/mcp_file.json
├── staging/                 # Same structure
└── prod/                    # Same structure
```

### MCP File Format
```json
{
  "version": "1.0",
  "source": {
    "type": "metadata-file",
    "config": {}
  },
  "sink": {
    "type": "datahub-rest",
    "config": {
      "server": "${DATAHUB_GMS_URL}",
      "token": "${DATAHUB_GMS_TOKEN}"
    }
  },
  "entities": [
    {
      "entityType": "tag",
      "entityUrn": "urn:li:tag:example",
      "aspects": [...]
    }
  ]
}
```

## 🍳 Recipe Management

### Recipe Structure
```
recipes/
├── templates/               # Parameterized recipe templates
│   ├── databricks.yml
│   ├── mssql.yml
│   └── dev/
├── instances/               # Environment-specific instances
│   └── dev/
│       ├── analytics-db.yml
│       └── customer-db.yml
└── pulled/                  # Recipes pulled from DataHub
```

### Template Example
```yaml
description: Template for databricks
name: databricks
recipe_type: databricks
source:
  config:
    dbx_platform_instance: ${DBX_PLATFORM_INSTANCE}
    dbx_token: ${DBX_TOKEN}
    dbx_warehouse_id: ${DBX_WAREHOUSE_ID}
    dbx_workspace_url: ${DBX_WORKSPACE_URL}
  type: databricks
```

### Environment Variables
```yaml
# params/environments/dev/analytics-db.yml
name: "Analytics Database"
description: "Development analytics database connection"
recipe_type: "postgres"
parameters:
  POSTGRES_HOST: "dev-analytics-db.company.com"
  POSTGRES_PORT: 5432
  POSTGRES_DATABASE: "analytics"
secret_references:
  - POSTGRES_USERNAME
  - POSTGRES_PASSWORD
```

## 🔐 Security & Configuration

### GitHub Secrets
```bash
# Global DataHub connection
DATAHUB_GMS_URL=https://your-datahub.company.com:8080
DATAHUB_GMS_TOKEN=your-global-token

# Environment-specific (optional)
DATAHUB_GMS_URL_DEV=https://dev-datahub.company.com:8080
DATAHUB_GMS_TOKEN_DEV=your-dev-token
DATAHUB_GMS_URL_PROD=https://prod-datahub.company.com:8080
DATAHUB_GMS_TOKEN_PROD=your-prod-token
```

### Security Best Practices
- **GitHub Secrets**: Store sensitive values in GitHub repository secrets
- **Environment Separation**: Use environment-specific secrets when possible
- **Token Rotation**: Regularly rotate DataHub Personal Access Tokens
- **Least Privilege**: Grant minimal necessary permissions
- **Branch Protection**: Require reviews for production deployments

## 🐳 Deployment Options

### Development
```bash
# Start development environment
docker-compose up -d

# Access web interface
open http://localhost:8000
```

### Production
```bash
# Build and deploy production
docker-compose -f docker-compose.prod.yml up -d

# With NGINX reverse proxy
docker-compose -f docker-compose.prod.yml up -d nginx
```

### Kubernetes
```bash
# Deploy with Helm
helm install datahub-recipes-manager ./helm/datahub-recipes-manager/

# Configure values
helm upgrade datahub-recipes-manager ./helm/datahub-recipes-manager/ \
  --set datahub.url=https://your-datahub.com:8080 \
  --set datahub.token=your-token
```

## 🧪 Testing

### Test Structure
```bash
# Run all tests
bash test/run_all_tests.sh

# Run specific test categories
python -m pytest scripts/test_*.py
python -m pytest web_ui/tests/

# Test DataHub connectivity
python scripts/test_connection.py
```

### Test Types
- **Unit Tests**: Individual component testing
- **Integration Tests**: DataHub API interactions
- **Mock Testing**: Test workflows without live DataHub connections
- **Validation**: Ensure all metadata files are properly formatted

## 🔧 Key Scripts & Utilities

### Core Scripts
- **test_connection.py** - Test DataHub connectivity
- **push_recipe.py** - Deploy recipes to DataHub
- **pull_recipe.py** - Retrieve recipes from DataHub
- **manage_policy.py** - Manage DataHub policies
- **import_metadata.py** - Import metadata from DataHub
- **export_metadata.py** - Export metadata from DataHub

### Utility Functions
- **DataHubMetadataClient** - DataHub API client wrapper
- **token_utils.py** - Token management utilities
- **env_loader.py** - Environment variable loading
- **datahub_client_adapter.py** - DataHub client adapter

## 📚 Documentation References

- **[Environment Variables Guide](docs/Environment_Variables.md)** - Complete environment variable reference
- **[Troubleshooting Guide](docs/Troubleshooting.md)** - Common issues and solutions
- **[Workflow Documentation](.github/workflows/README.md)** - GitHub Actions workflow details
- **[DataHub Documentation](https://datahubproject.io/docs/)** - Official DataHub documentation

## 🚨 Common Issues & Solutions

### Connection Issues
1. **403 Forbidden Errors**: Check environment variables and token permissions
2. **Token Issues**: Generate new token in DataHub UI → Settings → Access Tokens
3. **URL Format Issues**: Use correct DataHub GMS URL format

### Development Issues
1. **Import Errors**: Ensure virtual environment is activated
2. **Database Issues**: Run `python manage.py migrate` in web_ui directory
3. **Static Files**: Run `python manage.py collectstatic --noinput`

## 🤝 Contributing Guidelines

### Code Style
- Use **black** for Python formatting
- Use **flake8** for linting
- Use **mypy** for type checking
- Follow Django conventions for web UI code

### Development Workflow
1. Fork repository and create feature branch
2. Make changes with appropriate tests
3. Follow code style guidelines
4. Update documentation as needed
5. Submit pull request with clear description

### Testing Requirements
- All PRs must pass CI/CD tests
- Include unit tests for new functionality
- Test DataHub connectivity when applicable
- Validate metadata file formats

## 🎯 Key Development Patterns

### Adding New Metadata Types
1. Create MCP file structure in `metadata-manager/{env}/`
2. Add GitHub workflow in `.github/workflows/`
3. Update Django models in `web_ui/metadata_manager/models.py`
4. Add UI components in `web_ui/templates/metadata_manager/`
5. Add JavaScript handlers in `web_ui/static/metadata_manager/`

### Adding New Recipe Types
1. Create template in `recipes/templates/`
2. Add environment variables in `params/environments/`
3. Create instance configurations in `recipes/instances/`
4. Update Django models and views
5. Add GitHub workflows for deployment

### Adding New Workflows
1. Create workflow file in `.github/workflows/`
2. Define trigger conditions and environment matrix
3. Add error handling and artifact storage
4. Update workflow documentation
5. Test with dry-run mode first

This repository represents a comprehensive solution for DataHub metadata management with enterprise-grade features for version control, CI/CD, and multi-environment deployments. 