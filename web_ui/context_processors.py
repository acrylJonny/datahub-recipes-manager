def datahub_config(request):
    """
    Context processor that makes DataHub configuration available to all templates.
    
    This adds the following context variables:
    - datahub_config: Configuration details from .env
    - connection_status: Current status of DataHub connection
    - server_url: The DataHub server URL (shorthand)
    """
    # Get values from request (already processed by middleware)
    datahub_config = getattr(request, 'datahub_config', {
        'server_url': '',
        'token': '',
        'is_token_set': False
    })
    
    connection_status = getattr(request, 'datahub_connection', {
        'connected': False,
        'message': 'Connection status not available',
        'error': None
    })
    
    # Return a dictionary of context variables
    return {
        'datahub_config': datahub_config,
        'connection_status': connection_status,
        'server_url': datahub_config.get('server_url', '')
    } 