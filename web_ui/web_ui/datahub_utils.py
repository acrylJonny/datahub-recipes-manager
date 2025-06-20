"""
Utility functions for DataHub SDK operations.
"""

import logging
from typing import Optional, Dict, Any
from django.utils import timezone

logger = logging.getLogger(__name__)

_MISSING_SERVER_ID = "MISSING_SERVER_ID"


def get_datahub_client_info(environment) -> Dict[str, Any]:
    """
    DEPRECATED: Get DataHub client information for an environment using the SDK.
    
    This function is deprecated because Environment objects don't have DataHub connection fields.
    Use the Connection model and its associated methods instead.
    
    Args:
        environment: Environment model instance
        
    Returns:
        Dict containing error details
    """
    logger.warning("get_datahub_client_info is deprecated - Environment objects don't have DataHub connection fields")
    return {
        'success': False,
        'client_id': None,
        'server_id': None,
        'frontend_base_url': None,
        'server_config': {},
        'error': 'Environment objects do not have DataHub connection fields. Use Connection model instead.'
    }


def _update_client_info_error(environment, error_message: str):
    """
    DEPRECATED: Update DataHubClientInfo with error status.
    
    This function is deprecated because Environment objects don't have DataHub connection fields.
    """
    logger.warning("_update_client_info_error is deprecated - Environment objects don't have DataHub connection fields")


def refresh_all_client_info():
    """
    DEPRECATED: Refresh client info for all environments.
    
    This function is deprecated because Environment objects don't have DataHub connection fields.
    Use the Connection model and its associated methods instead.
    """
    logger.warning("refresh_all_client_info is deprecated - Environment objects don't have DataHub connection fields")
    return []


def test_datahub_connection(environment) -> Dict[str, Any]:
    """
    DEPRECATED: Test DataHub connection for an environment.
    
    This function is deprecated because Environment objects don't have DataHub connection fields.
    Use the Connection model and its test_connection method instead.
    
    Args:
        environment: Environment model instance
        
    Returns:
        Dict containing error details
    """
    logger.warning("test_datahub_connection is deprecated - Environment objects don't have DataHub connection fields")
    return {
        'success': False,
        'message': 'Environment objects do not have DataHub connection fields. Use Connection model instead.',
        'details': {'error': 'Deprecated function - use Connection.test_connection() instead'}
    } 