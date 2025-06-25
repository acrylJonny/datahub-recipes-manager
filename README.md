# DataHub Recipes Manager

A comprehensive solution for managing [DataHub](https://datahubproject.io/) metadata, ingestion recipes, and policies through version control, CI/CD pipelines, and a modern web interface. This project enables teams to treat DataHub metadata as code with proper versioning, testing, and deployment practices.

## 🚀 Features

### Core Capabilities
- **📋 Recipe Management**: Create, deploy, and manage DataHub ingestion recipes with templates and environment-specific configurations
- **🏷️ Metadata Management**: Manage tags, glossary terms, domains, structured properties, data contracts, and assertions
- **🔐 Policy Management**: Version control DataHub access policies with automated deployment
- **🌍 Multi-Environment Support**: Separate configurations for dev, staging, and production environments
- **🔄 CI/CD Integration**: Automated workflows for testing, validation, and deployment via GitHub Actions
- **🌐 Web Interface**: Modern Django-based UI for managing all aspects of your DataHub metadata
- **📊 Staging Workflow**: Stage changes locally before deploying to DataHub instances

### Metadata-as-Code Features
- **Individual JSON Files**: Each metadata entity (assertion, tag, glossary term, etc.) is stored as individual JSON files
- **MCP File Processing**: Uses DataHub's metadata-file source for batch operations
- **URN Mutation**: Automatically mutates URNs for cross-environment deployments
- **GitHub Integration**: Seamless integration with GitHub for version control and CI/CD

### Advanced Features
- **Platform Instance Mapping**: Map entities between different platform instances across environments
- **Mutation System**: Apply transformations to metadata when moving between environments
- **Secrets Management**: Secure handling of sensitive data through GitHub Secrets integration
- **Connection Management**: Multiple DataHub connection configurations with health monitoring

## 🏗️ Architecture Overview

The system separates metadata management into distinct layers:

1. **Templates** - Reusable, parameterized patterns with environment variable placeholders
2. **Environment Variables** - Environment-specific configuration values stored as YAML
3. **Instances** - Combination of templates and environment variables for deployment
4. **Metadata Files** - Individual JSON files for each metadata entity (tags, glossary, domains, etc.)
5. **MCP Files** - Batch metadata change proposals processed by DataHub workflows

## 📁 Project Structure

```
datahub-recipes-manager/
├── .github/workflows/           # GitHub Actions for CI/CD
│   ├── manage-*.yml            # Metadata processing workflows
│   ├── manage-assertions.yml   # Assertion processing
│   └── README.md               # Workflow documentation
├── docker/                     # Container deployment
│   ├── Dockerfile              # Application container
│   ├── docker-compose.yml      # Development setup
│   └── nginx/                  # Web server configuration
├── docs/                       # Comprehensive documentation
│   ├── Environment_Variables.md # DataHub CLI environment variables
│   ├── Troubleshooting.md      # Common issues and solutions
│   └── *.md                    # Additional guides
├── helm/                       # Kubernetes deployment charts
├── metadata-manager/           # Metadata entity storage
│   ├── dev/                    # Development environment
│   │   ├── assertions/         # Individual assertion JSON files
│   │   ├── tags/              # Tag MCP files
│   │   ├── glossary/          # Glossary MCP files
│   │   ├── domains/           # Domain MCP files
│   │   ├── structured_properties/ # Properties MCP files
│   │   ├── metadata_tests/    # Test MCP files
│   │   └── data_products/     # Data product MCP files
│   ├── staging/               # Staging environment
│   └── prod/                  # Production environment
├── recipes/                   # DataHub ingestion recipes
│   ├── templates/             # Parameterized recipe templates
│   ├── instances/             # Environment-specific instances
│   └── pulled/                # Recipes pulled from DataHub
├── params/                    # Environment variables and parameters
│   ├── environments/          # Environment-specific YAML configs
│   └── default_params.yaml    # Global defaults
├── policies/                  # DataHub access policies
├── scripts/                   # Python utilities and automation
│   ├── mcps/                  # Metadata change proposal utilities
│   ├── assertions/            # Assertion management scripts
│   ├── domains/               # Domain management scripts
│   ├── glossary/              # Glossary management scripts
│   └── tags/                  # Tag management scripts
├── web_ui/                    # Django web application
│   ├── metadata_manager/      # Metadata management app
│   ├── templates/             # HTML templates
│   ├── static/                # CSS, JavaScript, images
│   └── migrations/            # Database schema migrations
└── utils/                     # Shared utilities and DataHub API wrappers
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** with pip
- **DataHub instance** with API access
- **Personal Access Token** for DataHub authentication
- **Git** for version control
- **GitHub account** (for CI/CD features)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-org/datahub-recipes-manager.git
   cd datahub-recipes-manager
   ```

2. **Set up Python environment:**
   ```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   ```bash
   # Copy example environment file
   cp .env.example .env
   
   # Edit with your DataHub connection details
   nano .env
   ```

   Required environment variables:
   ```bash
   DATAHUB_GMS_URL=http://your-datahub-instance:8080
   DATAHUB_GMS_TOKEN=your-personal-access-token
   DJANGO_SECRET_KEY=your-secret-key
   ```

4. **Initialize the database:**
   ```bash
   cd web_ui
   python manage.py migrate
   python manage.py collectstatic --noinput
   cd ..
   ```

5. **Start the web interface:**
   ```bash
   cd web_ui
   python manage.py runserver
   ```

   Access the web interface at: http://localhost:8000

## 🌐 Web Interface

The Django-based web interface provides comprehensive management capabilities:

### Dashboard Features
- **Connection Status**: Real-time DataHub connectivity monitoring
- **Metadata Overview**: Summary of tags, glossary terms, domains, and other entities
- **Recent Activity**: Latest changes and deployments
- **Environment Health**: Status across dev, staging, and production

### Metadata Management
- **Tags**: Create, edit, and manage DataHub tags with hierarchical relationships
- **Glossary**: Manage business glossary terms and their relationships
- **Domains**: Organize data assets into logical business domains
- **Structured Properties**: Define and manage custom metadata properties
- **Data Contracts**: Version control data quality contracts
- **Assertions**: Create and manage data quality assertions

### Recipe Management
- **Templates**: Create reusable recipe patterns with parameterization
- **Instances**: Deploy recipes with environment-specific configurations
- **Environment Variables**: Manage secrets and configuration separately
- **Deployment Status**: Track which recipes are deployed where

### Policy Management
- **Access Policies**: Define who can access what data
- **Platform Policies**: Manage platform-level permissions
- **Policy Templates**: Create reusable policy patterns

## 🔄 Metadata-as-Code Workflow

### Individual JSON Files (Assertions)
Assertions are stored as individual JSON files in `metadata-manager/<environment>/assertions/`:

```json
{
  "id": "assertion_123",
  "type": "FRESHNESS",
  "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:postgres,public.users,PROD)",
  "source": "EXTERNAL",
  "config": {
    "type": "FRESHNESS",
    "freshnessAssertion": {
      "schedule": {
        "cron": "0 9 * * *",
        "timezone": "UTC"
      }
    }
  }
}
```

### MCP Files (Batch Operations)
Other metadata entities use MCP (Metadata Change Proposal) files for batch operations:

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
      "entityUrn": "urn:li:tag:PII",
      "aspects": [
        {
          "aspectName": "tagKey",
          "aspect": {
            "name": "PII"
          }
        }
      ]
    }
  ]
}
```

## 🔧 Environment Configuration

Environment variables are stored as structured YAML files in `params/environments/<env>/`:

```yaml
# params/environments/prod/analytics-db.yml
name: "Analytics Database"
description: "Production analytics database connection"
recipe_type: "postgres"
parameters:
  POSTGRES_HOST: "analytics-db.company.com"
  POSTGRES_PORT: 5432
  POSTGRES_DATABASE: "analytics"
  INCLUDE_VIEWS: true
  INCLUDE_TABLES: "*"
secret_references:
  - POSTGRES_USERNAME
  - POSTGRES_PASSWORD
```

## 🚀 GitHub Actions Workflows

The project includes comprehensive CI/CD workflows:

### Metadata Processing Workflows
- **`manage-tags.yml`** - Processes tag MCP files
- **`manage-glossary.yml`** - Processes glossary MCP files  
- **`manage-domains.yml`** - Processes domain MCP files
- **`manage-structured-properties.yml`** - Processes properties MCP files
- **`manage-metadata-tests.yml`** - Processes test MCP files
- **`manage-data-products.yml`** - Processes data product MCP files
- **`manage-assertions.yml`** - Processes individual assertion JSON files

### Key Features
- **Automatic Triggers**: Run on changes to relevant files
- **Environment Matrix**: Process dev, staging, and prod separately
- **Dry Run Support**: PRs run in validation mode
- **Error Handling**: Comprehensive error reporting and rollback
- **Artifact Storage**: Store processing results and logs

### GitHub Secrets Setup

Configure these secrets in your GitHub repository:

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

## 🔐 Security Best Practices

### Secrets Management
- **GitHub Secrets**: Store sensitive values in GitHub repository secrets
- **Environment Separation**: Use environment-specific secrets when possible
- **Token Rotation**: Regularly rotate DataHub Personal Access Tokens
- **Least Privilege**: Grant minimal necessary permissions

### Access Control
- **Branch Protection**: Require reviews for production deployments
- **Environment Protection**: Use GitHub Environments for sensitive deployments
- **Audit Logging**: All changes are tracked in Git history

## 🐳 Docker Deployment

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

## 🔧 DataHub CLI Environment Variables

This project uses the DataHub CLI extensively. The following environment variables are supported:

### Connection Configuration
- `DATAHUB_GMS_URL` (default: `http://localhost:8080`) - DataHub GMS instance URL
- `DATAHUB_GMS_TOKEN` (default: `None`) - Personal Access Token for authentication
- `DATAHUB_GMS_HOST` (default: `localhost`) - GMS host (prefer using DATAHUB_GMS_URL)
- `DATAHUB_GMS_PORT` (default: `8080`) - GMS port (prefer using DATAHUB_GMS_URL)
- `DATAHUB_GMS_PROTOCOL` (default: `http`) - Protocol (prefer using DATAHUB_GMS_URL)

### CLI Behavior
- `DATAHUB_SKIP_CONFIG` (default: `false`) - Skip creating configuration file
- `DATAHUB_TELEMETRY_ENABLED` (default: `true`) - Enable/disable telemetry
- `DATAHUB_TELEMETRY_TIMEOUT` (default: `10`) - Telemetry timeout in seconds
- `DATAHUB_DEBUG` (default: `false`) - Enable debug logging

### Docker Configuration
- `DATAHUB_VERSION` (default: `head`) - DataHub docker image version
- `ACTIONS_VERSION` (default: `head`) - DataHub actions container version
- `DATAHUB_ACTIONS_IMAGE` (default: `acryldata/datahub-actions`) - Actions image name

For detailed configuration guidance, see [Environment Variables Documentation](docs/Environment_Variables.md).

## 🧪 Testing

### Run Tests Locally
```bash
# Run all tests
bash test/run_all_tests.sh

# Run specific test categories
python -m pytest scripts/test_*.py
python -m pytest web_ui/tests/

# Test DataHub connectivity
python scripts/test_connection.py
```

### CI/CD Testing
- **Automated Testing**: All PRs run comprehensive test suites
- **Integration Tests**: Validate DataHub API interactions
- **Mock Testing**: Test workflows without live DataHub connections
- **Validation**: Ensure all metadata files are properly formatted

## 🛠️ Troubleshooting

### Common Connection Issues

1. **403 Forbidden Errors**
   ```bash
   # Check environment variables
   echo $DATAHUB_GMS_URL
   echo $DATAHUB_GMS_TOKEN
   
   # Test connectivity
   curl -f $DATAHUB_GMS_URL/health
   datahub check
   ```

2. **Token Issues**
   - Generate new token: DataHub UI → Settings → Access Tokens
   - Verify token permissions in DataHub
   - Check token expiration

3. **URL Format Issues**
   ```bash
   # Correct formats
   export DATAHUB_GMS_URL="http://localhost:8080"
   export DATAHUB_GMS_URL="https://your-instance.acryl.io/gms"
   ```

For comprehensive troubleshooting, see [Troubleshooting Guide](docs/Troubleshooting.md).

## 📚 Documentation

- **[Environment Variables Guide](docs/Environment_Variables.md)** - Complete environment variable reference
- **[Troubleshooting Guide](docs/Troubleshooting.md)** - Common issues and solutions
- **[Workflow Documentation](.github/workflows/README.md)** - GitHub Actions workflow details
- **[DataHub Documentation](https://datahubproject.io/docs/)** - Official DataHub documentation

## 🤝 Contributing

We welcome contributions! Please follow these steps:

1. **Fork the repository** and create a feature branch
2. **Make your changes** with appropriate tests
3. **Follow code style** guidelines (use `black` for Python formatting)
4. **Update documentation** as needed
5. **Submit a pull request** with a clear description

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run code formatting
black .

# Run linting
flake8 .

# Run tests before submitting
bash test/run_all_tests.sh
```

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **[DataHub Project](https://datahubproject.io/)** - The open-source metadata platform
- **[Acryl Data](https://www.acryldata.io/)** - Maintainers of the DataHub SDK
- **Community Contributors** - Everyone who has helped improve this project

## 🆘 Support

- **GitHub Issues**: Report bugs and request features
- **Discussions**: Ask questions and share ideas
- **Documentation**: Check the docs/ directory for detailed guides
- **Community**: Join the DataHub Slack community

---

**Ready to get started?** Follow the [Quick Start](#-quick-start) guide above, or jump into the [web interface](#-web-interface) for a guided experience!