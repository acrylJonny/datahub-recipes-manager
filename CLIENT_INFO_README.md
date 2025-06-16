# DataHub Client Info Management (Backend-Only)

This document describes the DataHub client information management functionality that runs automatically in the background of the DataHub CI/CD Manager.

## Overview

The system automatically retrieves and stores DataHub client information when a successful connection is established through the settings/configuration page. This includes:
- **Client ID**: Unique identifier for the DataHub client
- **Server ID**: DataHub server identifier  
- **Frontend Base URL**: URL to the DataHub frontend interface
- **Connection Status**: Current connection status (Connected/Failed/Unknown)
- **Server Configuration**: Raw server configuration details

## How It Works

### Automatic Retrieval
- **Trigger**: Client info is automatically retrieved when a successful DataHub connection test is performed in the settings page
- **Target**: Information is stored for the default environment
- **Background Operation**: Runs silently without user intervention
- **No UI Impact**: Does not affect the user interface or require manual actions

### Database Storage
- **Model**: `DataHubClientInfo` stores all client information
- **Relationship**: Linked to environments via foreign key
- **Tracking**: Records connection status, timestamps, and error messages
- **Persistence**: Data persists across application restarts

## Technical Implementation

### SDK Integration
Uses DataHub Python SDK to retrieve client information using the telemetry system:
```python
client_id: Optional[TelemetryClientIdClass] = client.get_aspect(
    "urn:li:telemetry:clientId", TelemetryClientIdClass
)
```

### Automatic Trigger
The `test_datahub_connection()` function in `utils/datahub_utils.py` automatically calls client info retrieval on successful connections:
```python
# If connection is successful, automatically retrieve client info for the default environment
if connected:
    try:
        _auto_retrieve_client_info(client)
    except Exception as e:
        logger.warning(f"Failed to auto-retrieve client info: {str(e)}")
        # Don't fail the connection test if client info retrieval fails
```

### Error Handling
- **Graceful Degradation**: Client info retrieval failures don't affect connection testing
- **Logging**: All operations are logged for debugging
- **Silent Operation**: Users are not notified of client info operations

## Management Commands

### Command Line Interface
```bash
# Show current status for all environments
python manage.py refresh_client_info

# Refresh client info for all environments
python manage.py refresh_client_info --all

# Refresh specific environment
python manage.py refresh_client_info --environment "Production"

# Verbose output with detailed information
python manage.py refresh_client_info --all --verbose
```

### Programmatic Access
```python
from web_ui.web_ui.datahub_utils import get_datahub_client_info
from web_ui.web_ui.models import Environment

# Get client info for an environment
environment = Environment.objects.get(name="Production")
result = get_datahub_client_info(environment)

if result['success']:
    print(f"Client ID: {result['client_id']}")
    print(f"Frontend URL: {result['frontend_base_url']}")
else:
    print(f"Error: {result['error']}")
```

## Database Schema

### DataHubClientInfo Model

| Field | Type | Description |
|-------|------|-------------|
| `client_id` | CharField | DataHub client ID (unique) |
| `server_id` | CharField | DataHub server ID |
| `frontend_base_url` | URLField | Frontend base URL |
| `server_config` | JSONField | Raw server configuration |
| `environment` | ForeignKey | Associated environment |
| `connection_status` | CharField | Connection status (connected/failed/unknown) |
| `error_message` | TextField | Last error message |
| `last_connection_test` | DateTimeField | Last connection test timestamp |
| `created_at` | DateTimeField | Record creation timestamp |
| `last_updated` | DateTimeField | Last update timestamp |

## Admin Interface

### Django Admin Access
- **URL**: `/admin/web_ui/datahubclientinfo/`
- **Features**: Full CRUD operations, filtering, searching
- **Fields**: Organized in logical fieldsets with collapsible sections
- **Readonly**: Timestamps are readonly for data integrity

### Admin Operations
- View all client info records
- Filter by connection status and environment
- Search by client ID or environment name
- Edit client info manually if needed

## Integration Points

### Settings Page Integration
- Client info is automatically retrieved when "Test Connection" is clicked
- No additional UI elements or user actions required
- Works with existing DataHub configuration workflow

### Environment Management
- Client info is associated with the default environment
- Multiple environments can have their own client info records
- Environment deletion cascades to remove associated client info

## Logging and Monitoring

### Log Messages
```
INFO: Auto-retrieving client info for default environment: Production
INFO: Successfully auto-retrieved client info: abc123-def456-ghi789
WARNING: Failed to auto-retrieve client info: Connection timeout
```

### Log Locations
- **Application Logs**: Standard Django logging
- **Database**: Error messages stored in DataHubClientInfo records
- **Management Commands**: Console output with status information

## Troubleshooting

### Common Issues

1. **"No default environment found"**
   - Ensure at least one environment is marked as default
   - Check environment configuration in admin or environments page

2. **"DataHub SDK not available"**
   - Verify `acryl-datahub` package is installed
   - Check Python environment and dependencies

3. **"Failed to get server id due to telemetry disabled"**
   - DataHub telemetry may be disabled
   - Client ID will show as "MISSING_SERVER_ID"
   - This is normal behavior and not an error

### Debug Steps

1. **Check Management Command**:
   ```bash
   python manage.py refresh_client_info --verbose
   ```

2. **Review Application Logs**:
   - Look for client info related log messages
   - Check for connection and SDK errors

3. **Verify Database Records**:
   ```bash
   python manage.py shell
   >>> from web_ui.web_ui.models import DataHubClientInfo
   >>> DataHubClientInfo.objects.all()
   ```

4. **Test Manual Retrieval**:
   ```python
   from web_ui.web_ui.datahub_utils import get_datahub_client_info
   from web_ui.web_ui.models import Environment
   env = Environment.objects.filter(is_default=True).first()
   result = get_datahub_client_info(env)
   print(result)
   ```

## Security Considerations

### Data Protection
- Client IDs are stored securely in the database
- No sensitive authentication tokens are stored in client info
- Server configuration may contain sensitive data - access is restricted

### Access Control
- Admin interface requires Django admin permissions
- Management commands require server access
- No client info exposed in regular user interfaces

## Future Enhancements

Potential improvements:
- **Periodic Refresh**: Automatic background refresh of client info
- **Health Monitoring**: Integration with system health checks
- **Metrics Collection**: Performance and usage metrics
- **Multi-Environment**: Auto-retrieval for all environments, not just default
- **Notification System**: Alerts when client info becomes stale or fails to update 