# Connection Monitoring

This document describes the connection monitoring features that automatically test DataHub connections and keep their status current.

## Overview

The connection monitoring system provides:

1. **Improved Authentication Testing** - Connection tests now validate tokens and permissions
2. **Manual Testing** - Test all connections via the web UI
3. **Automatic Periodic Testing** - Test connections every hour automatically
4. **Monitoring Dashboard** - View connection status and test results

## Features

### Enhanced Connection Testing

Connection tests now use `test_connection_with_permissions()` instead of the basic config endpoint test. This ensures:

- ✅ **Authentication validation** - Invalid tokens will be detected
- ✅ **Permission verification** - Tests ability to list policies and recipes
- ✅ **Real functionality** - Verifies the connection can actually perform operations

### Manual Testing

**Via Web UI:**
1. Go to **Manage Connections** page
2. Click **"Test All Connections"** button
3. View real-time results in a modal dialog
4. Connection statuses update automatically

**Via Command Line:**
```bash
# Test all connections
python manage.py test_connections

# Test specific connection
python manage.py test_connections --connection-id 123

# Force test (ignore 1-hour cooldown)
python manage.py test_connections --force
```

### Automatic Periodic Testing

**Option 1: Continuous Monitor (Recommended for Development)**
```bash
# Start continuous monitoring (tests every hour)
python manage.py start_connection_monitor

# Custom interval (test every 30 minutes)
python manage.py start_connection_monitor --interval 1800

# Run as daemon (minimal output)
python manage.py start_connection_monitor --daemon
```

**Option 2: Cron Job (Recommended for Production)**

Add to your crontab to test connections every hour:
```bash
# Edit crontab
crontab -e

# Add this line (adjust paths as needed)
0 * * * * cd /path/to/your/app && python manage.py test_connections >> /var/log/connection_tests.log 2>&1
```

**Option 3: Systemd Service (Production)**

Create `/etc/systemd/system/datahub-connection-monitor.service`:
```ini
[Unit]
Description=DataHub Connection Monitor
After=network.target

[Service]
Type=simple
User=your-app-user
WorkingDirectory=/path/to/your/app
ExecStart=/path/to/your/app/venv/bin/python manage.py start_connection_monitor --daemon
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable datahub-connection-monitor
sudo systemctl start datahub-connection-monitor
sudo systemctl status datahub-connection-monitor
```

## Connection Status Fields

Each connection tracks:

- **`connection_status`** - `'connected'`, `'failed'`, or `'pending'`
- **`error_message`** - Details about connection failures
- **`last_tested`** - Timestamp of last test
- **`is_active`** - Whether connection should be tested

## Benefits

### Before (Issues Fixed)
- ❌ Connection tests always succeeded (only tested public `/config` endpoint)
- ❌ Invalid tokens weren't detected
- ❌ No automatic testing
- ❌ Manual testing required individual connection clicks

### After (Current Implementation)
- ✅ Connection tests validate authentication and permissions
- ✅ Invalid tokens are properly detected and reported
- ✅ Automatic hourly testing keeps status current
- ✅ Bulk testing via "Test All Connections" button
- ✅ Detailed error messages for debugging
- ✅ Management commands for automation

## Troubleshooting

### Connection Always Fails
1. **Check URL format** - Should be `https://your-datahub.domain.com` (no trailing `/`)
2. **Verify token** - Must be a valid DataHub access token with list permissions
3. **Check permissions** - Token needs ability to list policies and ingestion sources
4. **SSL issues** - Try disabling SSL verification if using self-signed certificates

### Monitoring Not Running
1. **Check logs** - Look for errors in Django logs or systemd journal
2. **Verify permissions** - Make sure the user can write to the database
3. **Database connectivity** - Ensure Django can connect to the database
4. **Resource limits** - Check if the process is being killed due to memory/CPU limits

### Performance Impact
- Connection tests are lightweight GraphQL queries
- Tests run at most once per hour per connection (configurable)
- Failed connections are retested on the same schedule
- Database updates are minimal (3 fields per connection)

## Integration

The monitoring system integrates with:

- **Connection Management UI** - Shows real-time status
- **Policies/Recipes Pages** - Use connection-aware caching
- **Dashboard** - Can display connection health metrics
- **Alerts** (future) - Could notify on connection failures

## API Reference

### Management Commands

**`test_connections`**
```bash
python manage.py test_connections [options]

Options:
  --force                Force test regardless of last test time
  --connection-id ID     Test only specific connection
```

**`start_connection_monitor`**
```bash
python manage.py start_connection_monitor [options]

Options:
  --interval SECONDS     Testing interval (default: 3600)
  --daemon              Run with minimal output
```

### HTTP Endpoints

**`POST /connections/test-all/`**
- Tests all active connections
- Returns JSON with results and timing
- Requires CSRF token
- Updates connection statuses in database 