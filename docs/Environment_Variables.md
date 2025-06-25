# DataHub Environment Variables Configuration Guide

This document provides a comprehensive guide to configuring DataHub environment variables for use with the DataHub Recipes Manager.

## Overview

DataHub supports various environment variables that control its behavior, authentication, and connection settings. These variables take precedence over the DataHub CLI config created through the `datahub init` command.

## Core DataHub CLI Environment Variables

### Connection Configuration

These variables control how the DataHub CLI connects to your DataHub instance:

#### Primary Connection Variables
- **`DATAHUB_GMS_URL`** (default: `http://localhost:8080`)
  - The complete URL of your DataHub GMS (Graph Metadata Service) instance
  - Examples:
    - Local development: `http://localhost:8080`
    - DataHub Cloud: `https://your-instance.acryl.io/gms`
    - Custom deployment: `https://datahub.yourcompany.com:8080`
  - **Preferred method** for setting connection details

#### Alternative Connection Variables (use DATAHUB_GMS_URL instead)
- **`DATAHUB_GMS_HOST`** (default: `localhost`)
  - Set to a host of GMS instance
  - Only use if you can't use `DATAHUB_GMS_URL`

- **`DATAHUB_GMS_PORT`** (default: `8080`)
  - Set to a port of GMS instance
  - Only use if you can't use `DATAHUB_GMS_URL`

- **`DATAHUB_GMS_PROTOCOL`** (default: `http`)
  - Set to a protocol like `http` or `https`
  - Only use if you can't use `DATAHUB_GMS_URL`

#### Authentication
- **`DATAHUB_GMS_TOKEN`** (default: `None`)
  - Your DataHub Personal Access Token (PAT)
  - Required for authenticating with DataHub Cloud or secured instances
  - Generate from: DataHub UI → Settings → Access Tokens

### CLI Behavior Configuration

#### Configuration Management
- **`DATAHUB_SKIP_CONFIG`** (default: `false`)
  - Set to `true` to skip creating the configuration file
  - Useful in CI/CD environments where you want to rely solely on environment variables

#### Telemetry and Debugging
- **`DATAHUB_TELEMETRY_ENABLED`** (default: `true`)
  - Set to `false` to disable telemetry
  - Recommended to disable in environments with no access to public internet

- **`DATAHUB_TELEMETRY_TIMEOUT`** (default: `10`)
  - Timeout in seconds when sending telemetry
  - Increase if you have slow internet connectivity

- **`DATAHUB_DEBUG`** (default: `false`)
  - Set to `true` to enable debug logging for CLI
  - **Warning**: Exposes sensitive information in logs
  - Avoid enabling on production instances, especially with UI ingestion

### Docker and Actions Configuration

#### Version Control
- **`DATAHUB_VERSION`** (default: `head`)
  - Specific version to run quickstart with
  - Examples: `v0.10.0`, `latest`, `head`

- **`ACTIONS_VERSION`** (default: `head`)
  - Version for the `datahub-actions` container
  - Should typically match `DATAHUB_VERSION`

#### Container Configuration
- **`DATAHUB_ACTIONS_IMAGE`** (default: `acryldata/datahub-actions`)
  - Set to `acryldata/datahub-actions-slim` for a slimmer container without pyspark/deequ features
  - Use slim version if you don't need advanced data quality features

## Environment-Specific Configuration

### Development Environment
```bash
# Development - Local DataHub instance
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_GMS_TOKEN=your-dev-token
DATAHUB_DEBUG=true
DATAHUB_TELEMETRY_ENABLED=false
```

### Staging Environment
```bash
# Staging - Shared staging instance
DATAHUB_GMS_URL=https://datahub-staging.yourcompany.com:8080
DATAHUB_GMS_TOKEN=your-staging-token
DATAHUB_DEBUG=false
DATAHUB_TELEMETRY_ENABLED=false
```

### Production Environment
```bash
# Production - DataHub Cloud or production instance
DATAHUB_GMS_URL=https://your-instance.acryl.io/gms
DATAHUB_GMS_TOKEN=your-production-token
DATAHUB_DEBUG=false
DATAHUB_TELEMETRY_ENABLED=true
DATAHUB_TELEMETRY_TIMEOUT=30
```

## GitHub Actions Configuration

### Repository Secrets

For GitHub Actions workflows, configure these secrets in your repository:

#### Global Secrets (fallback)
- `DATAHUB_GMS_URL` - Default DataHub instance URL
- `DATAHUB_GMS_TOKEN` - Default DataHub token

#### Environment-Specific Secrets (recommended)
- `DATAHUB_GMS_URL_DEV` - Development instance URL
- `DATAHUB_GMS_TOKEN_DEV` - Development instance token
- `DATAHUB_GMS_URL_STAGING` - Staging instance URL
- `DATAHUB_GMS_TOKEN_STAGING` - Staging instance token
- `DATAHUB_GMS_URL_PROD` - Production instance URL
- `DATAHUB_GMS_TOKEN_PROD` - Production instance token

### Workflow Environment Variable Pattern

The workflows use this pattern to select environment-specific secrets:

```yaml
env:
  DATAHUB_GMS_URL: ${{ secrets[format('DATAHUB_GMS_URL_{0}', matrix.environment)] || secrets.DATAHUB_GMS_URL }}
  DATAHUB_GMS_TOKEN: ${{ secrets[format('DATAHUB_GMS_TOKEN_{0}', matrix.environment)] || secrets.DATAHUB_GMS_TOKEN }}
```

This automatically:
1. Tries to use environment-specific secrets first (e.g., `DATAHUB_GMS_URL_PROD`)
2. Falls back to global secrets if environment-specific ones don't exist

## Advanced Configuration

### Kafka Configuration (for DataHub deployment)

When deploying DataHub itself (not just using the CLI), additional Kafka-related environment variables are available:

```bash
# Kafka Connection
KAFKA_BOOTSTRAP_SERVER=broker:29092
KAFKA_SCHEMAREGISTRY_URL=http://schema-registry:8081

# SASL/GSSAPI Configuration
SPRING_KAFKA_PROPERTIES_SASL_KERBEROS_SERVICE_NAME=kafka
SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_PLAINTEXT
SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG=com.sun.security.auth.module.Krb5LoginModule required principal='principal@REALM' useKeyTab=true storeKey=true keyTab='/keytab';
```

### SSL Configuration

For secure connections:

```bash
# Schema Registry SSL
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SECURITY_PROTOCOL=SSL
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION=/path/to/keystore
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD=password
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION=/path/to/truststore
SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD=password
```

## Best Practices

### Security
1. **Never commit tokens to version control**
2. **Use environment-specific secrets** for different deployment stages
3. **Rotate tokens regularly** and update secrets accordingly
4. **Disable debug logging** in production environments
5. **Use HTTPS** for production DataHub instances

### Configuration Management
1. **Prefer `DATAHUB_GMS_URL`** over individual host/port/protocol variables
2. **Set `DATAHUB_SKIP_CONFIG=true`** in CI/CD environments
3. **Disable telemetry** in restricted network environments
4. **Use environment-specific configurations** for different deployment stages

### Troubleshooting
1. **Enable debug logging** temporarily for troubleshooting: `DATAHUB_DEBUG=true`
2. **Check connectivity** with `datahub check` command
3. **Verify token permissions** if getting authentication errors
4. **Validate URL format** - ensure it includes protocol and correct port

## Common Issues and Solutions

### Connection Issues
- **403 Forbidden**: Check if `DATAHUB_GMS_TOKEN` is set and valid
- **Connection refused**: Verify `DATAHUB_GMS_URL` is correct and DataHub is running
- **SSL errors**: Ensure URL uses `https://` for secure instances

### Authentication Issues
- **Invalid token**: Generate a new Personal Access Token from DataHub UI
- **Token expired**: Check token expiration and generate a new one if needed
- **Insufficient permissions**: Ensure the token has necessary privileges

### Network Issues
- **Timeout errors**: Increase `DATAHUB_TELEMETRY_TIMEOUT` or disable telemetry
- **Proxy issues**: Configure proxy settings in your environment
- **Firewall blocking**: Ensure DataHub ports are accessible

## References

- [Official DataHub CLI Documentation](https://datahubproject.io/docs/cli/)
- [DataHub Environment Variables](https://datahubproject.io/docs/how/kafka-config/)
- [DataHub Authentication](https://datahubproject.io/docs/authentication/)
- [DataHub Deployment Guide](https://datahubproject.io/docs/deploy/) 