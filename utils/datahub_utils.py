#!/usr/bin/env python3
"""
Utility functions for DataHub integration that can be used across the application.
Maintains consistent access to DataHub client and configuration.
"""

import os
import logging
import time
from django.conf import settings
from django.core.cache import cache
from utils.datahub_rest_client import DataHubRestClient

logger = logging.getLogger(__name__)

# Cache timeout in seconds (1 minute)
CACHE_TIMEOUT = 60


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

            # If we have URL from AppSettings, use it (token is optional)
            if datahub_url:
                logger.info("Creating DataHub client with settings from AppSettings")
                
                # Get timeout setting
                timeout = AppSettings.get_int("timeout", 30)  # Default 30 seconds
                
                client = DataHubRestClient(
                    server_url=datahub_url, 
                    token=datahub_token if datahub_token else None, 
                    verify_ssl=verify_ssl,
                    timeout=timeout
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
    Test if the DataHub connection is working (lightweight test).

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
            # Test the connection (lightweight)
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


def test_datahub_connection_with_permissions():
    """
    Test if the DataHub connection is working with full permission testing.
    This method fetches policies and recipes to test permissions.

    Returns:
        tuple: (is_connected, client) - boolean indicating if connection works, and the client instance
    """
    logger.info("Testing DataHub connection with permissions...")
    client = get_datahub_client()

    if client:
        logger.debug(
            f"Got client with URL: {client.server_url}, token provided: {client.token is not None and len(client.token or '') > 0}"
        )
        try:
            # Test the connection with permissions
            connected = client.test_connection_with_permissions()
            logger.info(
                f"DataHub connection test with permissions result: {'Success' if connected else 'Failed'}"
            )
            return connected, client
        except Exception as e:
            logger.error(f"Error testing DataHub connection with permissions: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            return False, client
    else:
        logger.warning("Could not create DataHub client, check configuration")
        return False, None


def get_cached_policies(force_refresh=False):
    """
    Get policies from cache or fetch from DataHub if not cached or expired.
    
    Args:
        force_refresh (bool): If True, bypass cache and fetch fresh data
        
    Returns:
        list: List of policies
    """
    cache_key = "datahub_policies"
    
    if not force_refresh:
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug("Retrieved policies from cache")
            return cached_data
    
    # Fetch fresh data from DataHub
    logger.info("Fetching policies from DataHub")
    client = get_datahub_client()
    
    if not client:
        logger.error("Could not get DataHub client for policies")
        return []
        
    try:
        policies = client.list_policies()
        # Cache the data
        cache.set(cache_key, policies, CACHE_TIMEOUT)
        logger.info(f"Cached {len(policies)} policies for {CACHE_TIMEOUT} seconds")
        return policies
    except Exception as e:
        logger.error(f"Error fetching policies: {str(e)}")
        return []


def get_cached_recipes(force_refresh=False):
    """
    Get recipes (ingestion sources) from cache or fetch from DataHub if not cached or expired.
    
    Args:
        force_refresh (bool): If True, bypass cache and fetch fresh data
        
    Returns:
        list: List of ingestion sources
    """
    cache_key = "datahub_recipes"
    
    if not force_refresh:
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug("Retrieved recipes from cache")
            return cached_data
    
    # Fetch fresh data from DataHub
    logger.info("Fetching recipes from DataHub")
    client = get_datahub_client()
    
    if not client:
        logger.error("Could not get DataHub client for recipes")
        return []
        
    try:
        recipes = client.list_ingestion_sources()
        # Cache the data
        cache.set(cache_key, recipes, CACHE_TIMEOUT)
        logger.info(f"Cached {len(recipes)} recipes for {CACHE_TIMEOUT} seconds")
        return recipes
    except Exception as e:
        logger.error(f"Error fetching recipes: {str(e)}")
        return []


def invalidate_policies_cache():
    """Invalidate the policies cache."""
    cache.delete("datahub_policies")
    logger.info("Invalidated policies cache")


def invalidate_recipes_cache():
    """Invalidate the recipes cache.""" 
    cache.delete("datahub_recipes")
    logger.info("Invalidated recipes cache")
