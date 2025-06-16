"""
Context processors for the web UI.
"""

import os
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
