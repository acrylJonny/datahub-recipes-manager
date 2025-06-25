# DataHub Troubleshooting Guide

This guide helps you diagnose and resolve common issues when working with DataHub and the DataHub Recipes Manager.

## Quick Diagnostics

### 1. Check DataHub CLI Installation
```bash
datahub version
```
If this fails, reinstall the DataHub CLI:
```bash
pip install --upgrade acryl-datahub
```

### 2. Test DataHub Connection
```bash
datahub check
```

### 3. Verify Environment Variables
```bash
echo $DATAHUB_GMS_URL
echo $DATAHUB_GMS_TOKEN
```

## Common Connection Issues

### 403 Forbidden Error

**Symptoms:**
- `Unable to connect to XXX with status_code: 403`
- `Please check your configuration and make sure you are talking to the DataHub GMS`

**Causes and Solutions:**

1. **Missing or Invalid Token**
   ```bash
   # Check if token is set
   echo $DATAHUB_GMS_TOKEN
   
   # If empty, set your token
   export DATAHUB_GMS_TOKEN="your-personal-access-token"
   ```

2. **Token Permissions**
   - Ensure the token has necessary permissions
   - Generate a new token from DataHub UI → Settings → Access Tokens
   - Use an admin account token for broader permissions

3. **Wrong URL Format**
   ```bash
   # Correct formats:
   export DATAHUB_GMS_URL="http://localhost:8080"
   export DATAHUB_GMS_URL="https://your-instance.acryl.io/gms"
   
   # Common mistakes:
   # ❌ Missing protocol: "localhost:8080"
   # ❌ Wrong path: "https://your-instance.acryl.io" (missing /gms)
   # ❌ Wrong port: "http://localhost:3000"
   ```

### Connection Refused Error

**Symptoms:**
- `Connection refused`
- `Failed to connect to DataHub`

**Solutions:**

1. **Verify DataHub is Running**
   ```bash
   # For local Docker deployment
   docker ps | grep datahub
   
   # Check if DataHub GMS port is accessible
   curl -f http://localhost:8080/health
   ```

2. **Check Network Connectivity**
   ```bash
   # Test basic connectivity
   ping your-datahub-host
   
   # Test port accessibility
   telnet your-datahub-host 8080
   ```

3. **Firewall and Network Issues**
   - Ensure DataHub ports (8080, 9002) are not blocked
   - Check corporate firewall settings
   - Verify VPN connectivity if required

### SSL/TLS Certificate Issues

**Symptoms:**
- `SSL certificate verify failed`
- `CERTIFICATE_VERIFY_FAILED`

**Solutions:**

1. **For Self-Signed Certificates**
   ```bash
   # Disable SSL verification (not recommended for production)
   export PYTHONHTTPSVERIFY=0
   ```

2. **For Corporate Certificates**
   ```bash
   # Add corporate CA certificate to Python
   export REQUESTS_CA_BUNDLE=/path/to/corporate-ca.pem
   ```

## Authentication Issues

### Invalid Token Error

**Symptoms:**
- `Authentication failed`
- `Invalid token`
- `Unauthorized`

**Solutions:**

1. **Generate New Token**
   - Go to DataHub UI → Settings → Access Tokens
   - Click "Generate new token"
   - Copy the token immediately (it won't be shown again)
   - Update your environment variable:
   ```bash
   export DATAHUB_GMS_TOKEN="your-new-token"
   ```

2. **Check Token Expiration**
   - Tokens may have expiration dates
   - Generate a new token if expired
   - Consider setting longer expiration periods

3. **Verify Token Format**
   ```bash
   # Token should be a long string, usually starting with "eyJ"
   echo $DATAHUB_GMS_TOKEN | head -c 20
   ```

### Permission Denied

**Symptoms:**
- `Permission denied`
- `Insufficient privileges`

**Solutions:**

1. **Use Admin Token**
   - Generate token from an admin account
   - Ensure the account has necessary permissions

2. **Check User Roles**
   - Verify user has required roles in DataHub
   - Contact DataHub admin to grant permissions

## Ingestion Issues

### No Metadata Produced

**Symptoms:**
- `No metadata was produced by the source`
- Empty ingestion results

**Solutions:**

1. **Check Source Configuration**
   ```bash
   # Validate your recipe file
   datahub ingest -c your-recipe.yml --dry-run
   ```

2. **Verify File Paths**
   ```yaml
   # Ensure file paths are correct
   source:
     type: file
     config:
       path: ./metadata-manager/dev/tags/mcp_file.json  # Check this path exists
   ```

3. **Validate JSON Format**
   ```bash
   # Check if JSON is valid
   python -m json.tool your-file.json
   ```

4. **Check File Permissions**
   ```bash
   # Ensure file is readable
   ls -la your-file.json
   ```

### Schema Validation Errors

**Symptoms:**
- `Schema validation failed`
- `Invalid metadata format`

**Solutions:**

1. **Check MCP Format**
   ```json
   {
     "entityType": "tag",
     "entityUrn": "urn:li:tag:your-tag",
     "aspectName": "tagKey",
     "aspect": {
       "name": "your-tag"
     }
   }
   ```

2. **Validate Against Schema**
   ```bash
   # Use dry-run to validate without ingesting
   datahub ingest -c your-recipe.yml --dry-run
   ```

## GitHub Actions Issues

### Workflow Failures

**Symptoms:**
- GitHub Actions failing with connection errors
- Environment variables not found

**Solutions:**

1. **Check Repository Secrets**
   - Go to GitHub Repository → Settings → Secrets and variables → Actions
   - Verify these secrets exist:
     - `DATAHUB_GMS_URL` or `DATAHUB_GMS_URL_ENV`
     - `DATAHUB_GMS_TOKEN` or `DATAHUB_GMS_TOKEN_ENV`

2. **Environment-Specific Secrets**
   ```yaml
   # Workflow tries environment-specific first, then falls back to global
   env:
     DATAHUB_GMS_URL: ${{ secrets[format('DATAHUB_GMS_URL_{0}', matrix.environment)] || secrets.DATAHUB_GMS_URL }}
     DATAHUB_GMS_TOKEN: ${{ secrets[format('DATAHUB_GMS_TOKEN_{0}', matrix.environment)] || secrets.DATAHUB_GMS_TOKEN }}
   ```

3. **Debug Workflow Variables**
   ```yaml
   - name: Debug Environment
     run: |
       echo "Environment: ${{ matrix.environment }}"
       echo "URL Secret Key: DATAHUB_GMS_URL_${{ matrix.environment }}"
       echo "Token Secret Key: DATAHUB_GMS_TOKEN_${{ matrix.environment }}"
   ```

### Secret Not Found

**Symptoms:**
- `Secret not found`
- Empty environment variables in workflows

**Solutions:**

1. **Verify Secret Names**
   - Ensure secret names match exactly (case-sensitive)
   - Common naming: `DATAHUB_GMS_URL_DEV`, `DATAHUB_GMS_TOKEN_PROD`

2. **Check Environment Names**
   - Ensure environment names in matrix match secret suffixes
   - Example: `dev` environment needs `DATAHUB_GMS_URL_DEV` secret

## Performance Issues

### Slow Ingestion

**Symptoms:**
- Ingestion takes very long time
- Timeouts during processing

**Solutions:**

1. **Increase Timeout Settings**
   ```bash
   export DATAHUB_TELEMETRY_TIMEOUT=30
   ```

2. **Process in Batches**
   - Split large files into smaller chunks
   - Use pagination for large datasets

3. **Disable Telemetry**
   ```bash
   export DATAHUB_TELEMETRY_ENABLED=false
   ```

### Memory Issues

**Symptoms:**
- Out of memory errors
- Process killed during ingestion

**Solutions:**

1. **Increase Memory Limits**
   ```yaml
   # In GitHub Actions
   - name: Run ingestion
     run: |
       export JAVA_OPTS="-Xmx2g"
       datahub ingest -c recipe.yml
   ```

2. **Process Smaller Batches**
   - Reduce file sizes
   - Use streaming ingestion for large datasets

## Debugging Techniques

### Enable Debug Logging

```bash
# Enable debug logging
export DATAHUB_DEBUG=true

# Run with debug output
datahub --debug ingest -c your-recipe.yml
```

### Check DataHub Logs

For local Docker deployment:
```bash
# Check GMS logs
docker logs datahub-gms

# Check frontend logs
docker logs datahub-frontend-react
```

### Network Debugging

```bash
# Test connectivity
curl -v http://your-datahub-host:8080/health

# Check DNS resolution
nslookup your-datahub-host

# Test with different tools
wget -O - http://your-datahub-host:8080/health
```

### Validate Configuration

```bash
# Check DataHub CLI configuration
cat ~/.datahubenv

# Validate recipe syntax
datahub check plugins
datahub ingest -c your-recipe.yml --dry-run
```

## Getting Help

### Collect Information

Before seeking help, collect this information:

1. **Environment Details**
   ```bash
   datahub version
   python --version
   pip show acryl-datahub
   ```

2. **Configuration**
   ```bash
   echo $DATAHUB_GMS_URL
   # Don't share the actual token, just confirm it's set
   echo "Token set: $(if [ -n "$DATAHUB_GMS_TOKEN" ]; then echo "Yes"; else echo "No"; fi)"
   ```

3. **Error Messages**
   - Full error output with stack traces
   - Debug logs if available
   - Network connectivity test results

### Community Resources

- [DataHub Slack Community](https://datahubspace.slack.com/)
- [DataHub GitHub Issues](https://github.com/datahub-project/datahub/issues)
- [DataHub Documentation](https://datahubproject.io/docs/)
- [DataHub Forum](https://forum.datahubproject.io/)

### Creating Bug Reports

Include this information in bug reports:

1. **Environment**: OS, Python version, DataHub CLI version
2. **Configuration**: DataHub instance type, version
3. **Steps to reproduce**: Exact commands and files used
4. **Expected behavior**: What should happen
5. **Actual behavior**: What actually happens
6. **Logs**: Relevant error messages and debug output

## Preventive Measures

### Regular Maintenance

1. **Keep CLI Updated**
   ```bash
   pip install --upgrade acryl-datahub
   ```

2. **Monitor Token Expiration**
   - Set calendar reminders for token renewal
   - Use tokens with appropriate expiration periods

3. **Test Connections Regularly**
   ```bash
   datahub check
   ```

### Best Practices

1. **Use Environment Variables**
   - Don't hardcode credentials in scripts
   - Use environment-specific configurations

2. **Validate Before Production**
   - Always test with `--dry-run` first
   - Validate JSON format before ingestion

3. **Monitor Workflows**
   - Set up notifications for failed workflows
   - Review logs regularly

4. **Backup Configurations**
   - Version control all configuration files
   - Document custom configurations 