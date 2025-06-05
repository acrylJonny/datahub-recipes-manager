#!/usr/bin/env python3
"""
Utility functions for DataHub integration that can be used across the application.
Maintains consistent access to DataHub client and configuration.
"""

import os
import logging
from django.conf import settings
from utils.datahub_rest_client import DataHubRestClient

logger = logging.getLogger(__name__)


def get_datahub_client():
    """
    Get a DataHub client instance using centrally configured settings.
    This function should be used throughout the application for consistent
    DataHub access.

    Returns:
        DataHubRestClient: configured client, or None if connection is not possible
    """
    try:
        # Always try to get settings from AppSettings first
        try:
            from web_ui.models import AppSettings

            logger.debug("Getting DataHub settings from AppSettings")
            datahub_url = AppSettings.get("datahub_url")
            datahub_token = AppSettings.get("datahub_token")
            verify_ssl = AppSettings.get_bool("verify_ssl", True)

            # Log the URL being used (without token)
            logger.debug(f"Using DataHub URL from settings: {datahub_url}")

            # If we have both URL and token from AppSettings, use them
            if datahub_url and datahub_token:
                logger.info("Creating DataHub client with settings from AppSettings")
                client = DataHubRestClient(
                    server_url=datahub_url, token=datahub_token, verify_ssl=verify_ssl
                )
                return client

        except ImportError as e:
            logger.warning(f"Could not import AppSettings: {str(e)}")

        # If we get here, either AppSettings import failed or settings weren't found
        # Try Django settings
        datahub_url = getattr(settings, "DATAHUB_SERVER_URL", None)
        datahub_token = getattr(settings, "DATAHUB_TOKEN", None)

        # If not in Django settings, try environment variables
        if not datahub_url or not datahub_token:
            datahub_url = os.environ.get("DATAHUB_GMS_URL", "")
            datahub_token = os.environ.get("DATAHUB_TOKEN", "")

        # If we have both URL and token from any source, create and return the client
        if datahub_url and datahub_token:
            logger.info(f"Creating DataHub client with URL: {datahub_url}")
            client = DataHubRestClient(server_url=datahub_url, token=datahub_token)
            return client
        else:
            if not datahub_url:
                logger.warning("DataHub URL is not configured")
            if not datahub_token:
                logger.warning("DataHub token is not configured")
            return None

    except Exception as e:
        logger.error(f"Error creating DataHub client: {str(e)}")
        return None


def test_datahub_connection():
    """
    Test if the DataHub connection is working.

    Returns:
        tuple: (is_connected, client) - boolean indicating if connection works, and the client instance
    """
    logger.info("Testing DataHub connection...")
    client = get_datahub_client()

    if client:
        logger.debug(
            f"Got client with URL: {client.server_url}, token provided: {client.token is not None and len(client.token or '') > 0}"
        )
        try:
            # Test the connection
            connected = client.test_connection()
            logger.info(
                f"DataHub connection test result: {'Success' if connected else 'Failed'}"
            )
            return connected, client
        except Exception as e:
            logger.error(f"Error testing DataHub connection: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            return False, client
    else:
        logger.warning("Could not create DataHub client, check configuration")
    return False, None
