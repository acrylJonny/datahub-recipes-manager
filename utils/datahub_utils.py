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


def get_datahub_client(connection_id=None, request=None):
    """
    Get a DataHub client instance using connection-based configuration.
    This function should be used throughout the application for consistent
    DataHub access.

    Args:
        connection_id (str, optional): ID of the specific connection to use.
        request (HttpRequest, optional): Request object to get session-based connection.
                                       If None, uses the default connection.

    Returns:
        DataHubRestClient: configured client, or None if connection is not possible
    """
    try:
        # Try to get from Connection model first (new multi-connection approach)
        try:
            from web_ui.models import Connection
            
            if connection_id:
                # Get specific connection
                connection = Connection.objects.filter(id=connection_id, is_active=True).first()
                logger.debug(f"Getting DataHub client for connection ID: {connection_id}")
            elif request and hasattr(request, 'session') and 'current_connection_id' in request.session:
                # Get connection from session
                session_connection_id = request.session['current_connection_id']
                connection = Connection.objects.filter(id=session_connection_id, is_active=True).first()
                logger.debug(f"Getting DataHub client for session connection ID: {session_connection_id}")
            else:
                # Get default connection
                connection = Connection.get_default()
                logger.debug("Getting DataHub client for default connection")
            
            if connection:
                logger.info(f"Creating DataHub client with connection: {connection.name}")
                return connection.get_client()
                
        except ImportError:
            logger.debug("Connection model not available, falling back to AppSettings")
        except Exception as e:
            logger.warning(f"Error getting connection: {str(e)}, falling back to AppSettings")

        # Fallback to legacy AppSettings approach
        try:
            from web_ui.models import AppSettings

            logger.debug("Getting DataHub settings from AppSettings (legacy fallback)")
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


def get_datahub_client_from_request(request):
    """
    Get a DataHub client instance based on the current request session.
    This is a convenience function for views that need session-aware client access.

    Args:
        request (HttpRequest): The request object containing session data

    Returns:
        DataHubRestClient: configured client, or None if connection is not possible
    """
    return get_datahub_client(request=request)


def test_datahub_connection(request=None):
    """
    Test if the DataHub connection is working (lightweight test).

    Returns:
        tuple: (is_connected, client) - boolean indicating if connection works, and the client instance
    """
    logger.info("Testing DataHub connection...")
    client = get_datahub_client(request=request)

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
            
            # Note: Removed automatic client info retrieval since Environment objects
            # don't have DataHub connection fields (datahub_gms_url, datahub_token).
            # Client info retrieval should be done explicitly for configured connections.
            
            return connected, client
        except Exception as e:
            logger.error(f"Error testing DataHub connection: {str(e)}")
            logger.error(f"Exception type: {type(e).__name__}")
            return False, client
    else:
        logger.warning("Could not create DataHub client, check configuration")
        return False, None


def _auto_retrieve_client_info(client):
    """
    DEPRECATED: Automatically retrieve and store client info for the default environment.
    This function is no longer used since Environment objects don't have DataHub connection fields.
    Use explicit client info retrieval for configured DataHub connections instead.
    
    Args:
        client: DataHub client instance
    """
    logger.warning("_auto_retrieve_client_info is deprecated and should not be called")
    # Function kept for backward compatibility but does nothing


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


def get_cached_policies(force_refresh=False, request=None):
    """
    Get policies from cache or fetch from DataHub if not cached or expired.
    
    Args:
        force_refresh (bool): If True, bypass cache and fetch fresh data
        request (HttpRequest, optional): Request object to get connection context
        
    Returns:
        list: List of policies
    """
    # Get connection-specific cache key
    connection_id = "default"
    if request and hasattr(request, 'session') and 'current_connection_id' in request.session:
        connection_id = request.session['current_connection_id']
    
    cache_key = f"datahub_policies_{connection_id}"
    
    if not force_refresh:
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Retrieved policies from cache for connection {connection_id}")
            return cached_data
    
    # Fetch fresh data from DataHub
    logger.info(f"Fetching policies from DataHub for connection {connection_id}")
    client = get_datahub_client(request=request)
    
    if not client:
        logger.error("Could not get DataHub client for policies")
        return []
        
    try:
        policies = client.list_policies()
        # Cache the data with connection-specific key
        cache.set(cache_key, policies, CACHE_TIMEOUT)
        logger.info(f"Cached {len(policies)} policies for connection {connection_id} for {CACHE_TIMEOUT} seconds")
        return policies
    except Exception as e:
        logger.error(f"Error fetching policies: {str(e)}")
        return []


def invalidate_policies_cache():
    """
    Invalidate the cached policies data to force a refresh on the next request.
    Call this function whenever policies are created, updated, or deleted.
    """
    cache_key = "datahub_policies"
    cache.delete(cache_key)
    logger.info("Invalidated policies cache")


def get_cached_recipes(force_refresh=False, request=None):
    """
    Get recipes (ingestion sources) from cache or fetch from DataHub if not cached or expired.
    
    Args:
        force_refresh (bool): If True, bypass cache and fetch fresh data
        request (HttpRequest, optional): Request object to get connection context
        
    Returns:
        list: List of ingestion sources
    """
    # Get connection-specific cache key
    connection_id = "default"
    if request and hasattr(request, 'session') and 'current_connection_id' in request.session:
        connection_id = request.session['current_connection_id']
    
    cache_key = f"datahub_recipes_{connection_id}"
    
    if not force_refresh:
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.debug(f"Retrieved recipes from cache for connection {connection_id}")
            return cached_data
    
    # Fetch fresh data from DataHub
    logger.info(f"Fetching recipes from DataHub for connection {connection_id}")
    client = get_datahub_client(request=request)
    
    if not client:
        logger.error("Could not get DataHub client for recipes")
        return []
        
    try:
        recipes = client.list_ingestion_sources()
        # Cache the data with connection-specific key
        cache.set(cache_key, recipes, CACHE_TIMEOUT)
        logger.info(f"Cached {len(recipes)} recipes for connection {connection_id} for {CACHE_TIMEOUT} seconds")
        return recipes
    except Exception as e:
        logger.error(f"Error fetching recipes: {str(e)}")
        return []


def invalidate_recipes_cache():
    """Invalidate the recipes cache.""" 
    cache.delete("datahub_recipes")
    logger.info("Invalidated recipes cache")


def clear_connection_cache(connection_id=None):
    """
    Clear cached data for a specific connection.
    
    Args:
        connection_id (str, optional): ID of the connection to clear cache for.
                                     If None, clears cache for all connections.
    """
    if connection_id:
        # Clear cache for specific connection
        cache_keys = [
            f"datahub_policies_{connection_id}",
            f"datahub_recipes_{connection_id}",
        ]
        for key in cache_keys:
            cache.delete(key)
            logger.debug(f"Cleared cache key: {key}")
    else:
        # Clear all connection-specific caches by trying common patterns
        # Since we can't easily enumerate all keys, clear known patterns
        try:
            # Try to clear cache for common connection IDs (not perfect but better than nothing)
            from web_ui.models import Connection
            connections = Connection.objects.filter(is_active=True)
            
            cleared_count = 0
            for connection in connections:
                conn_id = str(connection.id)
                cache_keys = [
                    f"datahub_policies_{conn_id}",
                    f"datahub_recipes_{conn_id}",
                ]
                for key in cache_keys:
                    if cache.delete(key):
                        cleared_count += 1
                        
            # Also clear the default connection cache
            default_keys = [
                "datahub_policies_default",
                "datahub_recipes_default",
            ]
            for key in default_keys:
                if cache.delete(key):
                    cleared_count += 1
                    
            logger.debug(f"Cleared {cleared_count} connection-specific cache keys")
            
        except Exception as e:
            logger.warning(f"Could not clear all connection caches: {str(e)}")
