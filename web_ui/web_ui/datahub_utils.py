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
    Get DataHub client information for an environment using the SDK.
    
    Args:
        environment: Environment model instance
        
    Returns:
        Dict containing client info or error details
    """
    from .models import DataHubClientInfo
    
    result = {
        'success': False,
        'client_id': None,
        'server_id': None,
        'frontend_base_url': None,
        'server_config': {},
        'error': None
    }
    
    try:
        # Import DataHub SDK components
        from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
        from datahub.telemetry.telemetry import telemetry_instance
        from datahub.metadata.schema_classes import TelemetryClientIdClass
        
        # Create DataHub client configuration
        config = DatahubClientConfig(
            server=environment.datahub_gms_url,
            token=environment.datahub_token if environment.datahub_token else None,
            timeout=30
        )
        
        # Create DataHub client
        client = DataHubGraph(config)
        
        # Test connection and get client info
        client.test_connection()
        
        # Get server ID (client ID)
        server_id = _MISSING_SERVER_ID
        
        if telemetry_instance.enabled:
            try:
                client_id_aspect: Optional[TelemetryClientIdClass] = client.get_aspect(
                    "urn:li:telemetry:clientId", TelemetryClientIdClass
                )
                server_id = client_id_aspect.clientId if client_id_aspect else _MISSING_SERVER_ID
            except Exception as e:
                logger.debug(f"Failed to get server id due to {e}")
                server_id = _MISSING_SERVER_ID
        
        # Get frontend base URL if available
        frontend_base_url = None
        server_config = {}
        
        try:
            if hasattr(client, 'server_config') and client.server_config:
                server_config = client.server_config.raw_config
                base_url = server_config.get("baseUrl")
                if base_url:
                    frontend_base_url = base_url
        except Exception as e:
            logger.debug(f"Failed to get frontend base URL: {e}")
        
        result.update({
            'success': True,
            'client_id': server_id,
            'server_id': server_id,
            'frontend_base_url': frontend_base_url,
            'server_config': server_config
        })
        
        # Update or create DataHubClientInfo record
        client_info, created = DataHubClientInfo.objects.get_or_create(
            environment=environment,
            defaults={
                'client_id': server_id,
                'server_id': server_id,
                'frontend_base_url': frontend_base_url,
                'server_config': server_config,
                'connection_status': 'connected',
                'last_connection_test': timezone.now()
            }
        )
        
        if not created:
            # Update existing record
            client_info.client_id = server_id
            client_info.server_id = server_id
            client_info.frontend_base_url = frontend_base_url
            client_info.server_config = server_config
            client_info.connection_status = 'connected'
            client_info.error_message = None
            client_info.last_connection_test = timezone.now()
            client_info.save()
        
        logger.info(f"Successfully retrieved client info for environment {environment.name}: {server_id}")
        
    except ImportError as e:
        error_msg = f"DataHub SDK not available: {e}"
        logger.error(error_msg)
        result['error'] = error_msg
        _update_client_info_error(environment, error_msg)
        
    except Exception as e:
        error_msg = f"Failed to get DataHub client info: {e}"
        logger.error(error_msg)
        result['error'] = error_msg
        _update_client_info_error(environment, error_msg)
    
    return result


def _update_client_info_error(environment, error_message: str):
    """Update DataHubClientInfo with error status."""
    from .models import DataHubClientInfo
    
    try:
        client_info = DataHubClientInfo.get_or_create_for_environment(environment)
        client_info.connection_status = 'failed'
        client_info.error_message = error_message
        client_info.last_connection_test = timezone.now()
        client_info.save()
    except Exception as e:
        logger.error(f"Failed to update client info error: {e}")


def refresh_all_client_info():
    """Refresh client info for all environments."""
    from .models import Environment
    
    results = []
    environments = Environment.objects.all()
    
    for environment in environments:
        logger.info(f"Refreshing client info for environment: {environment.name}")
        result = get_datahub_client_info(environment)
        results.append({
            'environment': environment.name,
            'success': result['success'],
            'client_id': result['client_id'],
            'error': result['error']
        })
    
    return results


def test_datahub_connection(environment) -> Dict[str, Any]:
    """
    Test DataHub connection for an environment.
    
    Args:
        environment: Environment model instance
        
    Returns:
        Dict containing connection test results
    """
    result = {
        'success': False,
        'message': '',
        'details': {}
    }
    
    try:
        from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
        
        config = DatahubClientConfig(
            server=environment.datahub_gms_url,
            token=environment.datahub_token if environment.datahub_token else None,
            timeout=30
        )
        
        client = DataHubGraph(config)
        client.test_connection()
        
        result.update({
            'success': True,
            'message': 'Connection successful',
            'details': {
                'server': environment.datahub_gms_url,
                'authenticated': bool(environment.datahub_token)
            }
        })
        
    except ImportError as e:
        result.update({
            'success': False,
            'message': 'DataHub SDK not available',
            'details': {'error': str(e)}
        })
        
    except Exception as e:
        result.update({
            'success': False,
            'message': f'Connection failed: {e}',
            'details': {'error': str(e)}
        })
    
    return result 