"""
Common utility functions for metadata manager views.
These provide reusable helper functions used across different entity types.
"""
import logging
import json
from django.http import JsonResponse
from django.utils import timezone

logger = logging.getLogger(__name__)


def sanitize_for_json(obj):
    """
    Recursively sanitize an object to be JSON serializable.
    Handles common Django/Python objects that aren't JSON serializable.
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}
    elif hasattr(obj, 'isoformat'):  # datetime objects
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):  # Custom objects
        return sanitize_for_json(obj.__dict__)
    else:
        return str(obj)


def extract_ownership_data(entity_data):
    """
    Extract and process ownership data from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Tuple of (ownership_data, owners_count, owner_names)
    """
    ownership_data = entity_data.get("ownership")
    owners_count = 0
    owner_names = []
    
    if ownership_data and ownership_data.get("owners"):
        owners = ownership_data["owners"]
        owners_count = len(owners)
        
        # Extract owner names for display
        for owner_info in owners:
            owner = owner_info.get("owner", {})
            if owner.get("properties"):
                name = (
                    owner["properties"].get("displayName") or
                    owner.get("username") or
                    owner.get("name") or
                    "Unknown"
                )
            else:
                name = owner.get("username") or owner.get("name") or "Unknown"
            owner_names.append(name)
    
    return ownership_data, owners_count, owner_names


def extract_relationships_data(entity_data):
    """
    Extract and process relationships data from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Tuple of (relationships_data, relationships_count)
    """
    relationships_data = entity_data.get("relationships")
    relationships_count = 0
    
    if relationships_data and relationships_data.get("relationships"):
        relationships_count = len(relationships_data["relationships"])
    
    return relationships_data, relationships_count


def extract_structured_properties_data(entity_data):
    """
    Extract and process structured properties data from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Tuple of (structured_properties_data, structured_properties_count)
    """
    structured_properties_data = entity_data.get("structuredProperties")
    structured_properties_count = 0
    
    if structured_properties_data and structured_properties_data.get("properties"):
        structured_properties_count = len(structured_properties_data["properties"])
    
    return structured_properties_data, structured_properties_count


def extract_tags_data(entity_data):
    """
    Extract and process tags data from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Tuple of (tags_data, tags_count)
    """
    tags_data = entity_data.get("tags") or entity_data.get("globalTags")
    tags_count = 0
    
    if tags_data and tags_data.get("tags"):
        tags_count = len(tags_data["tags"])
    
    return tags_data, tags_count


def extract_glossary_terms_data(entity_data):
    """
    Extract and process glossary terms data from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Tuple of (glossary_terms_data, glossary_terms_count)
    """
    glossary_terms_data = entity_data.get("glossaryTerms")
    glossary_terms_count = 0
    
    if glossary_terms_data and glossary_terms_data.get("terms"):
        glossary_terms_count = len(glossary_terms_data["terms"])
    
    return glossary_terms_data, glossary_terms_count


def normalize_description(description):
    """
    Normalize description text for consistent display.
    
    Args:
        description: Raw description string
    
    Returns:
        Normalized description string
    """
    if not description:
        return ""
    
    # Strip whitespace and normalize newlines
    normalized = description.strip().replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove excessive whitespace
    lines = [line.strip() for line in normalized.split('\n')]
    return '\n'.join(line for line in lines if line)


def extract_platform_info(entity_data):
    """
    Extract platform information from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Dictionary with platform information
    """
    platform_info = {}
    
    # Extract platform data
    platform_data = entity_data.get("platform")
    if platform_data:
        platform_info["platform_name"] = platform_data.get("name")
        platform_info["platform_urn"] = platform_data.get("urn")
    
    # Extract platform instance data
    platform_instance_data = entity_data.get("dataPlatformInstance")
    if platform_instance_data and platform_instance_data.get("properties"):
        platform_info["platform_instance"] = platform_instance_data["properties"].get("name")
    
    return platform_info


def extract_browse_path_info(entity_data):
    """
    Extract browse path information from DataHub entity data.
    
    Args:
        entity_data: Raw entity data from DataHub
    
    Returns:
        Dictionary with browse path information
    """
    browse_path_info = {}
    
    # Extract browse path
    browse_path = entity_data.get("computed_browse_path") or entity_data.get("browsePath")
    if browse_path:
        browse_path_info["browse_path"] = browse_path
    
    # Extract container information
    container_data = entity_data.get("container")
    if container_data:
        browse_path_info["container_urn"] = container_data.get("urn")
        if container_data.get("properties"):
            browse_path_info["container_name"] = container_data["properties"].get("name")
    
    return browse_path_info


def create_error_response(message, status_code=500):
    """
    Create a standardized error response.
    
    Args:
        message: Error message
        status_code: HTTP status code
    
    Returns:
        JsonResponse with error
    """
    logger.error(f"API Error: {message}")
    return JsonResponse({"success": False, "error": message}, status=status_code)


def create_success_response(data, message=None):
    """
    Create a standardized success response.
    
    Args:
        data: Response data
        message: Optional success message
    
    Returns:
        JsonResponse with success data
    """
    response = {"success": True, "data": data}
    if message:
        response["message"] = message
    return JsonResponse(response)


def get_sync_status_display(sync_status):
    """
    Get human-readable display text for sync status.
    
    Args:
        sync_status: Sync status value
    
    Returns:
        Human-readable sync status
    """
    status_map = {
        'LOCAL_ONLY': 'Local Only',
        'SYNCED': 'Synced',
        'MODIFIED': 'Modified',
        'REMOTE_ONLY': 'Remote Only',
        'PENDING': 'Pending',
        'ERROR': 'Error',
    }
    return status_map.get(sync_status, sync_status)


def validate_required_fields(data, required_fields):
    """
    Validate that required fields are present in data.
    
    Args:
        data: Data dictionary to validate
        required_fields: List of required field names
    
    Returns:
        Tuple of (is_valid, missing_fields)
    """
    missing_fields = []
    for field in required_fields:
        if not data.get(field):
            missing_fields.append(field)
    
    return len(missing_fields) == 0, missing_fields


def extract_entity_name_from_urn(urn):
    """
    Extract entity name from URN.
    
    Args:
        urn: DataHub URN
    
    Returns:
        Entity name or None
    """
    if not urn:
        return None
    
    try:
        # URN format: urn:li:entityType:(platform,name,env) or urn:li:entityType:name
        parts = urn.split(':')
        if len(parts) >= 4:
            # Handle complex URNs like datasets
            if '(' in parts[-1] and ')' in parts[-1]:
                # Extract from (platform,name,env) format
                inner = parts[-1].strip('()')
                inner_parts = inner.split(',')
                if len(inner_parts) >= 2:
                    return inner_parts[1]  # Name is typically the second part
            else:
                # Simple URN format
                return parts[-1]
    except Exception as e:
        logger.warning(f"Error extracting name from URN {urn}: {str(e)}")
    
    return None


def safe_get_nested(data, *keys, default=None):
    """
    Safely get nested dictionary value.
    
    Args:
        data: Dictionary to traverse
        *keys: Keys to traverse
        default: Default value if key not found
    
    Returns:
        Value or default
    """
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current 