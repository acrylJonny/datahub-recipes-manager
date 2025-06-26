# DataHub CI/CD Client

A modern Python client for DataHub with comprehensive CI/CD capabilities and input/output operations.
Perfect for automated workflows, data governance, and metadata management in CI/CD pipelines.

## Features

- **Modular Architecture**: Clean separation of concerns with service-based design
- **Comprehensive GraphQL Support**: Pre-built queries, mutations, and fragments
- **CI/CD Ready**: Command-line interface perfect for automation
- **Input/Output Operations**: Support for both synchronous and asynchronous operations
- **Docker Integration**: Built-in Docker networking support for testing environments
- **GitHub Integration**: Native GitHub API integration for repository management
- **URN Utilities**: Deterministic URN generation and management
- **Template Rendering**: YAML template rendering with parameter substitution

## Installation

```bash
pip install datahub-cicd-client
```

## Quick Start

### Using the CLI

```bash
# List tags
datahub-cicd tags list

# Create a domain
datahub-cicd domains create --id "finance" --name "Finance Domain" --description "Financial data domain"

# List structured properties
datahub-cicd properties list
```

### Using the Python API

```python
from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.tags import TagService

# Create connection
connection = DataHubConnection(
    server_url="http://localhost:8080",
    token="your-token"
)

# Use services
tag_service = TagService(connection)
tags = tag_service.list_tags()
```

## Architecture

The package is organized into several key modules:

- `core/`: Core infrastructure (connection, exceptions, base classes)
- `services/`: Domain-specific services (tags, domains, glossary, etc.)
- `graphql/`: GraphQL building blocks (fragments, queries, mutations)
- `outputs/`: Output operations (sync and async)
- `integrations/`: External integrations (Docker, GitHub, URN utils)
- `cli/`: Command-line interface

## Environment Variables

- `DATAHUB_SERVER_URL`: DataHub GMS server URL (default: http://localhost:8080)
- `DATAHUB_TOKEN`: DataHub authentication token
- `DATAHUB_VERIFY_SSL`: Whether to verify SSL certificates (default: true)

## License

MIT License - see LICENSE file for details. 