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
        # Try to get settings from Django settings first (for web UI)
        datahub_url = getattr(settings, 'DATAHUB_SERVER_URL', None)
        datahub_token = getattr(settings, 'DATAHUB_TOKEN', None)
        
        # Fall back to AppSettings if available (main web_ui approach)
        if not datahub_url or not datahub_token:
            try:
                # Import here to avoid circular imports
                from web_ui.web_ui.models import AppSettings
                logger.debug("Getting DataHub settings from AppSettings")
                datahub_url = AppSettings.get('datahub_url', os.environ.get('DATAHUB_GMS_URL', ''))
                datahub_token = AppSettings.get('datahub_token', os.environ.get('DATAHUB_TOKEN', ''))
                logger.debug(f"Retrieved datahub_url from AppSettings: {datahub_url is not None and len(datahub_url) > 0}")
            except ImportError as e:
                logger.warning(f"Could not import AppSettings: {str(e)}")
                # If AppSettings is not available, fall back to env vars
                datahub_url = os.environ.get('DATAHUB_GMS_URL', '')
                datahub_token = os.environ.get('DATAHUB_TOKEN', '')
        
        # If we have a URL, create and return the client
        if datahub_url:
            logger.debug(f"Creating DataHub client with URL: {datahub_url}")
            verify_ssl = True
            try:
                from web_ui.web_ui.models import AppSettings
                verify_ssl_setting = AppSettings.get_bool('verify_ssl', True)
                if verify_ssl_setting is not None:
                    verify_ssl = verify_ssl_setting
                    logger.debug(f"Using verify_ssl setting from AppSettings: {verify_ssl}")
            except Exception as e:
                logger.warning(f"Error getting verify_ssl setting: {str(e)}, using default: True")
                logger.warning(f"Exception details: {e.__class__.__name__}: {str(e)}")
            
            # Configure client with proper SSL verification settings
            client = DataHubRestClient(server_url=datahub_url, token=datahub_token, verify_ssl=verify_ssl)
            logger.info(f"Successfully created DataHub client for URL: {datahub_url}, verify_ssl: {verify_ssl}")
            return client
        else:
            logger.warning("DataHub URL is not configured")
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
        logger.debug(f"Got client with URL: {client.server_url}, token provided: {client.token is not None and len(client.token or '') > 0}")
        try:
            # Test the connection
            connected = client.test_connection()
            logger.info(f"DataHub connection test result: {'Success' if connected else 'Failed'}")
            return connected, client
        except Exception as e:
            logger.error(f"Error testing DataHub connection: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            return False, client
    else:
        logger.warning("Could not create DataHub client, check configuration")
    return False, None 