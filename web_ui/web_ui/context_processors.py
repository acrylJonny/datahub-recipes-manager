"""
Context processors for the web UI.
"""

import os
import time
from django.conf import settings
from dotenv import load_dotenv
from web_ui.models import Environment


def datahub_config(request):
    """
    Add DataHub configuration to the template context.
    """
    # Load environment variables from .env file
    if os.path.exists(settings.DATAHUB_CONFIG_FILE):
        load_dotenv(settings.DATAHUB_CONFIG_FILE)

    # Get DataHub connection details
    datahub_url = os.environ.get("DATAHUB_GMS_URL", "")
    datahub_token = os.environ.get("DATAHUB_TOKEN", "")

    # Determine if we have a valid connection configuration
    has_config = bool(datahub_url)

    return {
        "datahub_url": datahub_url,
        "has_datahub_token": bool(datahub_token),
        "has_datahub_config": has_config,
    }


def default_environment(request):
    """
    Add the default environment to the template context.
    """
    try:
        default_env = Environment.get_default()
        return {
            "default_environment": default_env,
        }
    except Exception:
        return {
            "default_environment": None,
        }


def connections_context(request):
    """
    Add connection-related data to all template contexts.
    """
    try:
        from web_ui.models import Connection
        from web_ui.views import get_current_connection
        
        # Get all active connections
        connections = Connection.get_active_connections()
        
        # Get current connection for this session
        current_connection = get_current_connection(request)
        
        return {
            'connections': connections,
            'current_connection': current_connection,
        }
        
    except Exception:
        # Return empty context if there's any error (e.g., during migrations)
        return {
            'connections': [],
            'current_connection': None,
        }


def cache_busting(request):
    """
    Add cache busting version to template context.
    This helps ensure users get fresh static files when the server restarts.
    """
    cache_version_file = os.path.join(settings.BASE_DIR, '.cache_version')
    
    # Try to read existing version
    try:
        with open(cache_version_file, 'r') as f:
            version = f.read().strip()
    except FileNotFoundError:
        # Generate new version if file doesn't exist
        version = str(int(time.time()))
        try:
            with open(cache_version_file, 'w') as f:
                f.write(version)
        except Exception:
            # If we can't write the file, use current timestamp
            version = str(int(time.time()))
    
    return {
        'cache_version': version,
    }
