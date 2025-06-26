from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
import logging
import os
import sys
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone
from django.urls import reverse
import json

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from datahub_cicd_client.integrations.urn_utils import get_full_urn_from_name
from utils.datahub_client_adapter import get_datahub_client, test_datahub_connection, get_datahub_client_from_request
from web_ui.models import GitSettings
from .models import StructuredProperty
# Git integration imports - handle gracefully if not available
try:
    from web_ui.git_manager.utils import GitIntegration
    from web_ui.constants import GIT_INTEGRATION_AVAILABLE
except ImportError:
    GitIntegration = None
    GIT_INTEGRATION_AVAILABLE = False

logger = logging.getLogger(__name__)


def _process_entity_types(entity_types_info):
    """
    Process entity types from remote structured property data.
    Extracts normalized type strings (e.g., 'dataset', 'container').
    
    Args:
        entity_types_info: List of entity type objects from DataHub API
        
    Returns:
        List of normalized entity type strings (e.g., 'dataset', 'container')
    """
    entity_types = []
    for et in entity_types_info:
        entity_type = None
        if isinstance(et, dict):
            # Handle nested structure: {"urn": "...", "type": "ENTITY_TYPE", "info": {"type": "DATASET"}}
            if "info" in et and isinstance(et["info"], dict):
                entity_type = et["info"].get("type")
            # Handle direct structure: {"type": "DATASET"}
            elif "type" in et:
                entity_type = et["type"]
        elif isinstance(et, str):
            entity_type = et
        if entity_type:
            # Normalize: strip URN prefix, lowercase, remove 'datahub.'
            if entity_type.startswith("urn:li:entityType:"):
                entity_type = entity_type.replace("urn:li:entityType:", "")
                if entity_type.startswith("datahub."):
                    entity_type = entity_type.replace("datahub.", "")
            entity_types.append(entity_type.lower())
    return entity_types


def _format_entity_type_for_display(entity_type):
    """
    Format entity type for display purposes.
    
    Args:
        entity_type: Entity type string (e.g., "DATASET", "urn:li:entityType:datahub.dataset")
        
    Returns:
        Display-friendly entity type (e.g., "Dataset")
    """
    if entity_type.startswith("urn:li:entityType:"):
        # Extract from URN format
        entity_type = entity_type.replace("urn:li:entityType:", "")
        if entity_type.startswith("datahub."):
            entity_type = entity_type.replace("datahub.", "")
    
    # Handle special cases and formatting
    entity_type = entity_type.upper()
    
    # Map common entity types to proper display names
    entity_type_map = {
        "DATASET": "Dataset",
        "DASHBOARD": "Dashboard", 
        "CHART": "Chart",
        "DATA_FLOW": "Data Flow",
        "DATA_JOB": "Data Job",
        "SCHEMA_FIELD": "Schema Field",
        "CONTAINER": "Container",
        "DOMAIN": "Domain",
        "GLOSSARY_TERM": "Glossary Term",
        "TAG": "Tag",
        "CORP_USER": "User",
        "CORP_GROUP": "Group"
    }
    
    return entity_type_map.get(entity_type, entity_type.replace("_", " ").title())


def _process_value_type(value_type_info):
    """
    Process value type from remote structured property data.
    Extracts clean display name from nested structure.
    
    Args:
        value_type_info: Value type object from DataHub API
        
    Returns:
        Clean value type name (e.g., "String", "Number")
    """
    if isinstance(value_type_info, dict):
        # Extract from nested info structure: valueType.info.type
        info = value_type_info.get('info', {}) or {}
        if isinstance(info, dict) and 'type' in info:
            value_type = info.get('type', 'STRING')
        else:
            value_type = value_type_info.get('type', 'STRING')
    else:
        value_type = str(value_type_info) if value_type_info else 'STRING'
    
    return _format_value_type_for_display(value_type)


def _format_value_type_for_display(value_type):
    """
    Format value type for display purposes.
    
    Args:
        value_type: Value type string (e.g., "STRING", "NUMBER")
        
    Returns:
        Display-friendly value type (e.g., "String", "Number")
    """
    # Handle URN format
    if value_type.startswith("urn:li:dataType:"):
        value_type = value_type.replace("urn:li:dataType:", "")
        if value_type.startswith("datahub."):
            value_type = value_type.replace("datahub.", "")
    
    # Map common value types to proper display names
    value_type_map = {
        "STRING": "String",
        "NUMBER": "Number", 
        "BOOLEAN": "Boolean",
        "DATE": "Date",
        "URN": "URN",
        "RICH_TEXT": "Rich Text",
        "BYTES": "Bytes"
    }
    
    return value_type_map.get(value_type.upper(), value_type.replace("_", " ").title())


class PropertyListView(View):
    """View to list and create structured properties"""

    def get(self, request):
        """Display list of structured properties or return JSON data for AJAX requests"""
        # Check if this is an AJAX request for data
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return self._get_properties_data(request)
        
        """Display list of structured properties"""
        try:
            logger.info("Starting PropertyListView.get")

            # Get current connection to filter properties by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
            
            # Get properties relevant to current connection (database-only operation)
            # Include: properties with no connection (local-only) + properties with current connection (synced)
            # BUT: if there's a synced property for current connection with same datahub_id, hide the other connection's version
            all_properties = StructuredProperty.objects.all().order_by("name")
            properties = filter_properties_by_connection(all_properties, current_connection)
            
            logger.debug(f"Found {len(properties)} properties relevant to current connection")

            # Get DataHub connection info
            logger.debug("Testing DataHub connection from PropertyListView")
            connected, client = test_datahub_connection(request)
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize context
            context = {
                "properties": properties,
                "page_title": "DataHub Structured Properties",
                "has_datahub_connection": connected,
                "has_git_integration": False,
            }

            # Fetch remote properties if connected
            synced_properties = []
            local_properties = []
            remote_only_properties = []
            datahub_url = None

            if connected and client:
                logger.debug("Connected to DataHub, fetching remote properties")
                try:
                    # Get all remote properties from DataHub
                    remote_properties = client.list_structured_properties(count=1000)
                    logger.debug(
                        f"Fetched {len(remote_properties) if remote_properties else 0} remote properties"
                    )

                    # Get DataHub URL for direct links
                    datahub_url = client.server_url
                    if datahub_url.endswith("/api/gms"):
                        datahub_url = datahub_url[
                            :-8
                        ]  # Remove /api/gms to get base URL

                    # Extract property URNs that exist locally
                    local_property_urns = set(
                        properties.values_list("urn", flat=True)
                    )

                    # Process properties by comparing local and remote
                    for prop in properties:
                        prop_urn = str(prop.urn)
                        remote_match = next(
                            (p for p in remote_properties if p.get("urn") == prop_urn),
                            None,
                        )

                        if remote_match:
                            synced_properties.append(
                                {"local": prop, "remote": remote_match}
                            )
                        else:
                            local_properties.append(prop)

                    # Find properties that exist remotely but not locally
                    remote_only_properties = [
                        p
                        for p in remote_properties
                        if p.get("urn") not in local_property_urns
                    ]

                    logger.debug(
                        f"Categorized properties: {len(synced_properties)} synced, {len(local_properties)} local-only, {len(remote_only_properties)} remote-only"
                    )

                except Exception as e:
                    logger.error(f"Error fetching remote property data: {str(e)}")
            else:
                # All properties are local-only if not connected
                local_properties = properties

            # Update context with processed properties
            context.update(
                {
                    "synced_properties": synced_properties,
                    "local_properties": local_properties,
                    "remote_only_properties": remote_only_properties,
                    "datahub_url": datahub_url,
                }
            )

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
                logger.debug(
                    f"Git integration enabled: {context['has_git_integration']}"
                )
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass

            logger.info("Rendering property list template")
            return render(request, "metadata_manager/properties/list.html", context)
        except Exception as e:
            logger.error(f"Error in property list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/properties/list.html",
                {"error": str(e), "page_title": "DataHub Structured Properties"},
            )

    def _get_properties_data(self, request):
        """Return JSON data for AJAX requests"""
        try:
            # Get current connection to filter properties by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            logger.debug(f"Using connection: {current_connection.name if current_connection else 'None'}")
            
            # Get ALL local properties - we'll categorize them based on connection logic
            all_local_properties = StructuredProperty.objects.all().order_by("name")
            logger.debug(f"Found {all_local_properties.count()} total local properties")
            
            # Separate properties based on connection logic:
            # 1. Properties with no connection (LOCAL_ONLY) - should appear as local-only
            # 2. Properties with current connection (SYNCED) - should appear as synced if they match remote
            # 3. Properties with different connection - should appear as local-only relative to current connection
            #    BUT: if there's a synced property for current connection with same datahub_id, hide the other connection's version
            local_properties = filter_properties_by_connection(all_local_properties, current_connection)
            
            logger.debug(f"Filtered to {len(local_properties)} properties relevant to current connection")
            
            # Check connection to DataHub for remote properties
            from utils.datahub_client_adapter import test_datahub_connection
            connected, client = test_datahub_connection(request)
            remote_properties = []
            datahub_url = None
            
            if connected and client:
                try:
                    remote_properties_response = client.list_structured_properties(count=1000)
                    if remote_properties_response:
                        remote_properties = remote_properties_response
                    
                    # Get DataHub URL for direct links
                    datahub_url = client.server_url
                    if datahub_url.endswith("/api/gms"):
                        datahub_url = datahub_url[:-8]  # Remove /api/gms to get base URL
                        
                except Exception as e:
                    logger.error(f"Error fetching remote properties: {str(e)}")
            
            # Build local properties dict for matching
            local_properties_dict = {prop.urn: prop for prop in local_properties if prop.urn}
            
            # Build remote properties dict for matching
            remote_properties_dict = {}
            if remote_properties:
                for remote_prop in remote_properties:
                    prop_urn = remote_prop.get('urn', '').strip()
                    if prop_urn:
                        remote_properties_dict[prop_urn] = remote_prop
            
            # Get local property URNs for comparison
            local_property_urns = set(local_properties_dict.keys())
            
            # Categorize properties
            synced_properties = []
            local_only_properties = []
            remote_only_properties = []
            
            # Create a mapping of urns to Property IDs for quick lookup
            local_urn_to_id_map = {str(prop.urn): str(prop.id) for prop in local_properties if prop.urn}
            
            # Process local properties
            for local_prop in local_properties:
                prop_urn = local_prop.urn
                if not prop_urn:
                    continue
                
                # Determine connection context for this property
                connection_context = "none"  # Default
                if local_prop.connection is None:
                    connection_context = "none"  # No connection
                elif current_connection and local_prop.connection == current_connection:
                    connection_context = "current"  # Current connection
                else:
                    connection_context = "different"  # Different connection
                
                # Check if this property has a remote match
                remote_match = remote_properties_dict.get(prop_urn)
                
                # Convert local property to dict
                local_prop_data = {
                    'id': str(local_prop.id),
                    'database_id': str(local_prop.id),  # Explicitly add database_id for clarity
                    'urn': prop_urn,
                    'name': local_prop.name,
                    'qualified_name': local_prop.qualified_name,
                    'description': local_prop.description,
                    'value_type': local_prop.value_type,
                    'cardinality': local_prop.cardinality,
                    'entity_types': local_prop.entity_types or [],
                    'allowedValues': local_prop.allowed_values or [],
                    'allowedValuesCount': len(local_prop.allowed_values or []),
                    'show_in_search_filters': local_prop.show_in_search_filters,
                    'show_as_asset_badge': local_prop.show_as_asset_badge,
                    'show_in_asset_summary': local_prop.show_in_asset_summary,
                    'show_in_columns_table': local_prop.show_in_columns_table,
                    'is_hidden': local_prop.is_hidden,
                    'immutable': local_prop.immutable,
                    'sync_status': local_prop.sync_status,
                    'sync_status_display': local_prop.get_sync_status_display(),
                    # Add connection context for frontend action determination
                    'connection_context': connection_context,
                    'has_remote_match': bool(remote_match),
                }
                
                # Determine categorization based on connection and remote match
                # With single-row-per-datahub_id logic:
                # - Synced: Property belongs to current connection AND (exists remotely OR was recently synced)
                # - Local-only: Property belongs to different connection OR no connection OR (no remote match AND not recently synced)
                
                # Check if property was recently synced (within last 30 seconds)
                from django.utils import timezone
                from datetime import timedelta
                
                recently_synced = (
                    local_prop.last_synced and 
                    local_prop.last_synced > timezone.now() - timedelta(seconds=30)
                )
                
                if local_prop.connection == current_connection and (remote_match or recently_synced):
                    # Property exists remotely OR was recently synced to current connection - categorize as synced
                    
                    # Enhanced sync status validation - check if property needs status update
                    # Skip validation if requested (e.g., after single property edits) or if recently synced
                    skip_sync_validation = request.GET.get("skip_sync_validation", "false").lower() == "true" or recently_synced
                    if not skip_sync_validation and remote_match:
                        needs_status_update = False
                        status_change_reason = []
                        
                        # Check description changes - handle null/empty values properly
                        local_description = local_prop.description or ""
                        remote_description = remote_match.get("definition", {}).get("description", "")
                        
                        # Normalize descriptions for comparison
                        # Handle cases where remote description might be the string "None", None, or empty
                        def normalize_description(desc):
                            if desc is None or desc == "None" or desc == "" or (isinstance(desc, str) and desc.strip() == ""):
                                return ""
                            return str(desc).strip()
                        
                        local_description_normalized = normalize_description(local_description)
                        remote_description_normalized = normalize_description(remote_description)
                        
                        if local_description_normalized != remote_description_normalized:
                            needs_status_update = True
                            status_change_reason.append("description")
                        
                        # Check name changes
                        local_name = local_prop.name or ""
                        remote_name = remote_match.get("definition", {}).get("displayName", "")
                        if local_name != remote_name:
                            needs_status_update = True
                            status_change_reason.append("name")
                        
                        # CRITICAL: Only update sync status for properties that belong to current connection
                        # Properties from other connections should not have their sync_status modified
                        if local_prop.connection == current_connection:
                            if needs_status_update:
                                # Property has been modified in DataHub or locally
                                if local_prop.sync_status != "MODIFIED":
                                    local_prop.sync_status = "MODIFIED"
                                    local_prop.save(update_fields=["sync_status"])
                                    logger.info(f"Updated property {local_prop.name} status to MODIFIED due to changes in: {', '.join(status_change_reason)}")
                            else:
                                # Property is in sync
                                if local_prop.sync_status != "SYNCED":
                                    local_prop.sync_status = "SYNCED" 
                                    local_prop.save(update_fields=["sync_status"])
                                    logger.debug(f"Updated property {local_prop.name} status to SYNCED")
                    
                    # Process remote data for synced properties (if available)
                    if remote_match:
                        definition = remote_match.get('definition', {}) or {}
                        settings = remote_match.get('settings', {}) or {}
                        
                        # Process value type from remote data
                        value_type_info = definition.get('valueType', {}) or {}
                        processed_value_type = _process_value_type(value_type_info)
                        
                        # Process entity types from remote data
                        entity_types_info = definition.get("entityTypes", []) or []
                        processed_entity_types = _process_entity_types(entity_types_info)
                        
                        # Process allowed values from remote data
                        allowed_values_info = definition.get("allowedValues", []) or []
                        allowed_values = []
                        for av in allowed_values_info:
                            description = av.get("description", "")
                            
                            # Handle both remote format {value: {stringValue: "..."}} and local format {value: "..."}
                            value_obj = av.get("value", {})
                            value = None
                            
                            if isinstance(value_obj, dict):
                                # Remote format: {value: {stringValue: "..."}, description: "..."}
                                if "stringValue" in value_obj:
                                    value = value_obj.get("stringValue")
                                elif "numberValue" in value_obj:
                                    value = value_obj.get("numberValue")
                                elif "booleanValue" in value_obj:
                                    value = value_obj.get("booleanValue")
                            else:
                                # Local format: {value: "...", description: "..."}
                                value = value_obj

                            if value is not None:
                                allowed_values.append(
                                    {"value": value, "description": description}
                                )
                        
                        # Update local property data with remote information
                        local_prop_data.update({
                            'value_type': processed_value_type,
                            'entity_types': processed_entity_types,
                            'allowedValues': allowed_values,
                            'allowedValuesCount': len(allowed_values),
                            'remote_data': remote_match
                        })
                    
                    # Update local property data with sync information
                    local_prop_data.update({
                        'sync_status': local_prop.sync_status,
                        'sync_status_display': local_prop.get_sync_status_display(),
                        'connection_context': connection_context,  # Keep the connection context
                        'has_remote_match': bool(remote_match) or recently_synced,  # True if remote match OR recently synced
                        'status': 'synced',
                    })
                    
                    # Create combined data with explicit database_id
                    combined_data = local_prop_data.copy()
                    combined_data["database_id"] = str(local_prop.id)  # Ensure database_id is set
                    
                    synced_properties.append(combined_data)
                else:
                    # Property is local-only relative to current connection
                    # This includes:
                    # 1. Properties with no connection (true local-only)
                    # 2. Properties with different connection (local-only relative to current connection)
                    # 3. Properties with current connection but no remote match
                    
                    # Check if the property was recently synced to avoid overriding fresh sync status
                    from django.utils import timezone
                    from datetime import timedelta
                    
                    recently_synced = (
                        local_prop.last_synced and 
                        local_prop.last_synced > timezone.now() - timedelta(minutes=5)  # 5 minute grace period
                    )
                    
                    # CRITICAL: Only update sync status for properties that belong to current connection
                    # Properties from other connections should maintain their original sync_status
                    # Skip validation if requested (e.g., after single property edits)
                    skip_sync_validation = request.GET.get("skip_sync_validation", "false").lower() == "true"
                    if not skip_sync_validation and local_prop.connection == current_connection:
                        # Only update status for properties belonging to current connection
                        expected_status = "LOCAL_ONLY"
                        if not remote_match:
                            # Property was synced to current connection but no longer exists remotely
                            expected_status = "REMOTE_DELETED"
                        
                        # Only update sync status if it wasn't recently synced and status is different
                        if not recently_synced and local_prop.sync_status != expected_status:
                            local_prop.sync_status = expected_status
                            local_prop.save(update_fields=["sync_status"])
                            logger.debug(f"Updated property {local_prop.name} status to {expected_status}")
                    elif skip_sync_validation:
                        logger.debug(f"Skipping sync status validation for property {local_prop.name} - skip_sync_validation=true")
                    
                    # Update local property data with current status
                    local_prop_data.update({
                        'sync_status': local_prop.sync_status,
                        'sync_status_display': local_prop.get_sync_status_display(),
                        'connection_context': connection_context,  # Keep the connection context
                        'has_remote_match': bool(remote_match),  # Whether this property has a remote match
                        'status': 'local_only'
                    })
                    
                    local_only_properties.append(local_prop_data)
            
            # Process remote-only properties
            for prop_urn, remote_prop in remote_properties_dict.items():
                if prop_urn not in local_property_urns:                    
                    # Extract data from the remote property structure
                    definition = remote_prop.get('definition', {}) or {}
                    settings = remote_prop.get('settings', {}) or {}
                    
                    # Get basic info
                    prop_name = definition.get('displayName', '') or remote_prop.get('name', '')
                    qualified_name = definition.get('qualifiedName', '') or remote_prop.get('qualifiedName', '')
                    description = definition.get('description', '') or remote_prop.get('description', '')
                    
                    # Get type info - extract from nested structure
                    value_type_info = definition.get('valueType', {}) or {}
                    value_type = _process_value_type(value_type_info)
                    
                    cardinality = definition.get('cardinality', 'SINGLE')
                    
                    # Extract entity types
                    entity_types_info = definition.get("entityTypes", []) or []
                    entity_types = _process_entity_types(entity_types_info)
                    
                    # Extract allowed values
                    allowed_values_info = definition.get("allowedValues", []) or []
                    allowed_values = []
                    for av in allowed_values_info:
                        description = av.get("description", "")
                        
                        # Handle both remote format {value: {stringValue: "..."}} and local format {value: "..."}
                        value_obj = av.get("value", {})
                        value = None
                        
                        if isinstance(value_obj, dict):
                            # Remote format: {value: {stringValue: "..."}, description: "..."}
                            if "stringValue" in value_obj:
                                value = value_obj.get("stringValue")
                            elif "numberValue" in value_obj:
                                value = value_obj.get("numberValue")
                            elif "booleanValue" in value_obj:
                                value = value_obj.get("booleanValue")
                        else:
                            # Local format: {value: "...", description: "..."}
                            value = value_obj

                        if value is not None:
                            allowed_values.append(
                                {"value": value, "description": description}
                            )
                    
                    # For remote-only properties, we need the property name for generating a consistent ID
                    # This is needed for frontend actions like deletion
                    property_name = prop_name or qualified_name or prop_urn.split(":")[-1]
                    
                    # Generate a deterministic UUID based on the urn
                    import hashlib
                    import uuid
                    # Create a stable UUID based on the urn - this ensures consistent IDs
                    urn_hash = hashlib.md5(prop_urn.encode()).hexdigest()
                    deterministic_uuid = str(uuid.UUID(urn_hash))
                    
                    remote_prop_enhanced = {
                        'id': deterministic_uuid,
                        'database_id': deterministic_uuid,  # Add a specific database_id field to make it clear this is for database operations
                        'urn': prop_urn,
                        'name': prop_name,
                        'qualified_name': qualified_name,
                        'description': description,
                        'value_type': value_type,
                        'cardinality': cardinality,
                        'entity_types': entity_types,
                        'allowedValues': allowed_values,
                        'allowedValuesCount': len(allowed_values),
                        'show_in_search_filters': settings.get('showInSearchFilters', True),
                        'show_as_asset_badge': settings.get('showAsAssetBadge', True),
                        'show_in_asset_summary': settings.get('showInAssetSummary', True),
                        'show_in_columns_table': settings.get('showInColumnsTable', False),
                        'is_hidden': settings.get('isHidden', False),
                        'immutable': definition.get('immutable', False),
                        'status': 'remote_only',
                        'sync_status': 'REMOTE_ONLY',
                        'sync_status_display': 'Remote Only',
                        'remote_data': remote_prop
                    }
                    
                    remote_only_properties.append(remote_prop_enhanced)
            
            # Combine all properties
            all_properties = synced_properties + local_only_properties + remote_only_properties
            
            # Calculate filter statistics
            value_type_counts = {}
            entity_type_counts = {}
            
            for prop in all_properties:
                # Count value types
                value_type = prop.get('value_type', 'STRING')
                value_type_counts[value_type] = value_type_counts.get(value_type, 0) + 1
                
                # Count entity types
                entity_types = prop.get('entity_types', [])
                for entity_type in entity_types:
                    entity_type_counts[entity_type] = entity_type_counts.get(entity_type, 0) + 1
            
            return JsonResponse({
                'success': True,
                'data': all_properties,
                'datahub_url': datahub_url,
                'statistics': {
                    'total_count': len(all_properties),
                    'synced_count': len(synced_properties),
                    'local_only_count': len(local_only_properties),
                    'remote_only_count': len(remote_only_properties),
                },
                'filters': {
                    'value_types': value_type_counts,
                    'entity_types': entity_type_counts,
                }
            })
            
        except Exception as e:
            logger.error(f"Error in _get_properties_data: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Error loading properties data: {str(e)}'
            })

    def post(self, request):
        """Create a new structured property"""
        try:
            # Get basic property info
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            qualified_name = request.POST.get("qualified_name", "").strip()
            value_type = request.POST.get("value_type", "string")
            cardinality = request.POST.get("cardinality", "SINGLE")
            immutable = request.POST.get("immutable") == "on"

            # Get entity types
            entity_types = request.POST.getlist("entity_types", [])
            if not entity_types:
                entity_types = ["dataset"]  # Default to dataset

            # Get allowed entity types (for URN type)
            allowed_entity_types = request.POST.getlist("allowed_entity_types", [])

            # Get allowed values
            allowed_values = []
            allowed_value_inputs = request.POST.getlist("allowed_values[]")
            allowed_value_descriptions = request.POST.getlist("allowed_value_descriptions[]")
            
            for i, value in enumerate(allowed_value_inputs):
                value = value.strip()
                if value:  # Only add non-empty values
                    description = allowed_value_descriptions[i].strip() if i < len(allowed_value_descriptions) else ""
                    allowed_values.append({"value": value, "description": description})

            # Get display settings
            show_in_search_filters = request.POST.get("show_in_search_filters") == "on"
            show_as_asset_badge = request.POST.get("show_as_asset_badge") == "on"
            show_in_asset_summary = request.POST.get("show_in_asset_summary") == "on"
            show_in_columns_table = request.POST.get("show_in_columns_table") == "on"
            is_hidden = request.POST.get("is_hidden") == "on"

            # Validation
            if not name:
                messages.error(request, "Property name is required")
                return redirect("metadata_manager:property_list")

            if not value_type:
                messages.error(request, "Value type is required")
                return redirect("metadata_manager:property_list")

            if not cardinality:
                messages.error(request, "Cardinality is required")
                return redirect("metadata_manager:property_list")

            if not entity_types:
                messages.error(request, "At least one entity type must be selected")
                return redirect("metadata_manager:property_list")

            # Validate URN-specific requirements
            if value_type == "urn" and not allowed_entity_types:
                messages.error(request, "Allowed entity types are required for URN properties")
                return redirect("metadata_manager:property_list")

            # Validate allowed values (no duplicates)
            if value_type != "urn" and allowed_values:
                values = [av["value"] for av in allowed_values]
                if len(values) != len(set(values)):
                    messages.error(request, "Duplicate values are not allowed in allowed values")
                    return redirect("metadata_manager:property_list")

            # Generate qualified name if not provided
            if not qualified_name:
                qualified_name = name.lower().replace(" ", "_")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name(
                "structuredProperty", qualified_name
            )

            # Check if property with this URN already exists
            if StructuredProperty.objects.filter(
                urn=deterministic_urn
            ).exists():
                messages.error(request, f"Property with name '{name}' already exists")
                return redirect("metadata_manager:property_list")

            # Normalize value_type for consistency with remote sync
            normalized_value_type = _process_value_type(value_type)

            # Create the property
            property_obj = StructuredProperty.objects.create(
                datahub_id=name,
                name=qualified_name,
                qualified_name=qualified_name,
                description=description,
                value_type=normalized_value_type,
                cardinality=cardinality,
                immutable=immutable,
                entity_types=entity_types,
                allowed_values=allowed_values if value_type != "urn" else [],
                allowed_entity_types=allowed_entity_types if value_type == "urn" else [],
                urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
                show_in_search_filters=show_in_search_filters,
                show_as_asset_badge=show_as_asset_badge,
                show_in_asset_summary=show_in_asset_summary,
                show_in_columns_table=show_in_columns_table,
                is_hidden=is_hidden,
            )

            messages.success(request, f"Property '{name}' created successfully")
            return redirect("metadata_manager:property_list")
        except Exception as e:
            logger.error(f"Error creating property: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")


class PropertyDetailView(View):
    """View to display, edit and delete structured properties"""

    def get(self, request, property_id):
        """Display property details"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Initialize context with property data
            context = {
                "property": property,
                "page_title": f"Property: {property.name}",
                "has_git_integration": False,  # Set this based on checking GitHub settings
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
            except:
                pass

            # Get property values if DataHub connection is available
            client = get_datahub_client_from_request(request)
            if client and client.test_connection():
                property_urn = property.urn

                # Get remote property information if possible
                if property.sync_status != "LOCAL_ONLY":
                    try:
                        # Since we removed get_structured_property, we'll use the existing property data
                        # The property should already have the latest data from the list view
                        context["has_datahub_connection"] = True
                        
                        # Check if the property needs to be synced based on last_synced timestamp
                        if property.last_synced:
                            # For now, assume it's in sync if we have a last_synced timestamp
                            if property.sync_status == "MODIFIED":
                                property.sync_status = "SYNCED"
                                property.save(update_fields=["sync_status"])
                        else:
                            # If no last_synced, mark as needing sync
                            if property.sync_status != "MODIFIED":
                                property.sync_status = "MODIFIED"
                                property.save(update_fields=["sync_status"])
                                
                    except Exception as e:
                        logger.warning(
                            f"Error processing property sync status: {str(e)}"
                        )

                context["has_datahub_connection"] = True
            else:
                context["has_datahub_connection"] = False

            return render(request, "metadata_manager/properties/detail.html", context)
        except Exception as e:
            logger.error(f"Error in property detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")

    def post(self, request, property_id):
        """Update a property"""
        try:
            # Check if this is an AJAX request
            is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Get basic property info for update
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            qualified_name = request.POST.get("qualified_name", "")
            value_type = request.POST.get("value_type", property.value_type)
            cardinality = request.POST.get("cardinality", property.cardinality)
            immutable = request.POST.get("immutable") == "on"

            # Get entity types
            entity_types = request.POST.getlist("entity_types", [])
            if not entity_types:
                entity_types = property.entity_types or []

            # Get allowed entity types (for URN type)
            allowed_entity_types = request.POST.getlist("allowed_entity_types", [])

            # Get allowed values
            allowed_values = []
            allowed_value_inputs = request.POST.getlist("allowed_values[]")
            allowed_value_descriptions = request.POST.getlist("allowed_value_descriptions[]")
            
            for i, value in enumerate(allowed_value_inputs):
                value = value.strip()
                if value:  # Only add non-empty values
                    description = allowed_value_descriptions[i].strip() if i < len(allowed_value_descriptions) else ""
                    allowed_values.append({"value": value, "description": description})

            # Get display settings
            show_in_search_filters = request.POST.get("show_in_search_filters") == "on"
            show_as_asset_badge = request.POST.get("show_as_asset_badge") == "on"
            show_in_asset_summary = request.POST.get("show_in_asset_summary") == "on"
            show_in_columns_table = request.POST.get("show_in_columns_table") == "on"
            is_hidden = request.POST.get("is_hidden") == "on"

            # Validation
            if not name:
                error_msg = "Property name is required"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            if not value_type:
                error_msg = "Value type is required"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            if not cardinality:
                error_msg = "Cardinality is required"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            if not entity_types:
                error_msg = "At least one entity type must be selected"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            # Validate URN-specific requirements
            if value_type == "urn" and not allowed_entity_types:
                error_msg = "Allowed entity types are required for URN properties"
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            # Validate allowed values (no duplicates)
            if value_type != "urn" and allowed_values:
                values = [av["value"] for av in allowed_values]
                if len(values) != len(set(values)):
                    error_msg = "Duplicate values are not allowed in allowed values"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
                    return redirect("metadata_manager:property_detail", property_id=property_id)

            # Check if anything changed
            changed = (
                property.name != name
                or property.description != description
                or property.qualified_name != qualified_name
                or property.value_type != value_type
                or property.cardinality != cardinality
                or property.entity_types != entity_types
                or property.allowed_values != (allowed_values if value_type != "urn" else [])
                or property.allowed_entity_types != (allowed_entity_types if value_type == "urn" else [])
                or property.show_in_search_filters != show_in_search_filters
                or property.show_as_asset_badge != show_as_asset_badge
                or property.show_in_asset_summary != show_in_asset_summary
                or property.show_in_columns_table != show_in_columns_table
                or property.is_hidden != is_hidden
                or property.immutable != immutable
            )

            if changed:
                # Update the property
                property.datahub_id = name
                property.name = qualified_name
                property.qualified_name = qualified_name
                property.description = description
                property.value_type = value_type
                property.cardinality = cardinality
                property.immutable = immutable
                property.entity_types = entity_types
                property.allowed_values = allowed_values if value_type != "urn" else []
                property.allowed_entity_types = allowed_entity_types if value_type == "urn" else []
                property.show_in_search_filters = show_in_search_filters
                property.show_as_asset_badge = show_as_asset_badge
                property.show_in_asset_summary = show_in_asset_summary
                property.show_in_columns_table = show_in_columns_table
                property.is_hidden = is_hidden

                # If already synced, mark as modified
                if property.sync_status == "SYNCED":
                    property.sync_status = "MODIFIED"

                property.save()

                success_msg = f"Property '{name}' updated successfully"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': success_msg})
                messages.success(request, success_msg)
            else:
                info_msg = "No changes were made"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': info_msg})
                messages.info(request, info_msg)

            if is_ajax:
                return JsonResponse({'success': True, 'message': 'Property updated successfully'})
            return redirect("metadata_manager:property_detail", property_id=property_id)
        except Exception as e:
            logger.error(f"Error updating property: {str(e)}")
            error_msg = f"An error occurred: {str(e)}"
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg})
            messages.error(request, error_msg)
            return redirect("metadata_manager:property_detail", property_id=property_id)

    def delete(self, request, property_id):
        """Delete a property"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Delete the property
            property_name = property.name
            property.delete()

            messages.success(
                request, f"Property '{property_name}' deleted successfully"
            )
            return JsonResponse({"success": True})
        except Exception as e:
            logger.error(f"Error deleting property: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


@method_decorator(require_POST, name="dispatch")
class PropertyDeployView(View):
    """View to deploy a property to DataHub"""

    def post(self, request, property_id):
        """Deploy a property to DataHub"""
        try:
            # Check if this is an AJAX request
            is_ajax = request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Get current connection from request session
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Check if this property already has a connection and it's different from current connection
            if property.connection is not None and current_connection and property.connection != current_connection:
                # Create a new row for the current connection instead of modifying existing one
                logger.info(f"Property '{property.name}' has connection {property.connection.name}, creating new row for {current_connection.name}")
                
                # Check if a property with same datahub_id already exists for current connection
                existing_for_current_connection = StructuredProperty.objects.filter(
                    datahub_id=property.datahub_id,
                    connection=current_connection
                ).first()
                
                if existing_for_current_connection:
                    error_msg = f"Property '{property.name}' already exists for the current connection"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
                    return redirect("metadata_manager:property_detail", property_id=property_id)
                
                # Create new property for current connection
                # Use original qualified_name, but create unique datahub_id if needed
                unique_datahub_id = f"{property.datahub_id}_{current_connection.name}" if current_connection else property.datahub_id
                
                new_property = StructuredProperty.objects.create(
                    name=property.name,
                    description=property.description,
                    qualified_name=property.qualified_name,  # Keep original qualified_name
                    value_type=property.value_type,
                    cardinality=property.cardinality,
                    immutable=property.immutable,
                    entity_types=property.entity_types,
                    allowed_values=property.allowed_values,
                    allowed_entity_types=property.allowed_entity_types,
                    urn=property.urn,
                    datahub_id=unique_datahub_id,  # Make datahub_id unique instead
                    sync_status="LOCAL_ONLY",  # Will be updated to SYNCED after successful sync
                    connection=current_connection,
                    show_in_search_filters=property.show_in_search_filters,
                    show_as_asset_badge=property.show_as_asset_badge,
                    show_in_asset_summary=property.show_in_asset_summary,
                    show_in_columns_table=property.show_in_columns_table,
                    is_hidden=property.is_hidden,
                )
                property = new_property  # Use the new property for the rest of the sync process

            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client or not client.test_connection():
                error_msg = "Not connected to DataHub. Please check your connection settings."
                if is_ajax:
                    return JsonResponse({'success': False, 'error': error_msg})
                messages.error(request, error_msg)
                return redirect("metadata_manager:property_detail", property_id=property_id)

            # Deploy to DataHub
            if property.sync_status == "LOCAL_ONLY":
                # Create new property
                urn = client.create_structured_property(
                    display_name=property.name,
                    description=property.description,
                    value_type=property.value_type,
                    entity_types=property.entity_types,
                    cardinality=property.cardinality,
                    allowedValues=property.allowed_values,
                    show_in_search=property.show_in_search_filters,
                    show_as_badge=property.show_as_asset_badge,
                    show_in_summary=property.show_in_asset_summary,
                    qualified_name=property.qualified_name,
                )

                if urn:
                    # Mark as synced and set connection
                    property.sync_status = "SYNCED"
                    property.last_synced = timezone.now()
                    if current_connection:
                        property.connection = current_connection
                    property.save()

                    success_msg = f"Property '{property.name}' created in DataHub successfully"
                    if is_ajax:
                        return JsonResponse({'success': True, 'message': success_msg})
                    messages.success(request, success_msg)
                else:
                    error_msg = f"Failed to create property '{property.name}' in DataHub"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
            elif property.sync_status == "MODIFIED":
                # Update existing property
                success = client.update_structured_property(
                    property_urn=property.urn,
                    display_name=property.name,
                    description=property.description,
                    show_in_search=property.show_in_search_filters,
                    show_as_badge=property.show_as_asset_badge,
                    show_in_summary=property.show_in_asset_summary,
                )

                if success:
                    # Mark as synced and set connection
                    property.sync_status = "SYNCED"
                    property.last_synced = timezone.now()
                    if current_connection:
                        property.connection = current_connection
                    property.save()

                    success_msg = f"Property '{property.name}' updated in DataHub successfully"
                    if is_ajax:
                        return JsonResponse({'success': True, 'message': success_msg})
                    messages.success(request, success_msg)
                else:
                    error_msg = f"Failed to update property '{property.name}' in DataHub"
                    if is_ajax:
                        return JsonResponse({'success': False, 'error': error_msg})
                    messages.error(request, error_msg)
            else:
                info_msg = f"Property '{property.name}' is already synced with DataHub"
                if is_ajax:
                    return JsonResponse({'success': True, 'message': info_msg})
                messages.info(request, info_msg)

            if is_ajax:
                return JsonResponse({'success': True, 'message': 'Property deployed successfully'})
            return redirect("metadata_manager:property_detail", property_id=property_id)
        except Exception as e:
            logger.error(f"Error deploying property: {str(e)}")
            error_msg = f"An error occurred: {str(e)}"
            if is_ajax:
                return JsonResponse({'success': False, 'error': error_msg})
            messages.error(request, error_msg)
            return redirect("metadata_manager:property_detail", property_id=property_id)


@method_decorator(require_POST, name="dispatch")
class PropertyPullView(View):
    """View to pull properties from DataHub"""

    def post(self, request, only_post=False):
        """Pull properties from DataHub"""
        try:
            # Get current connection from request session
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client or not client.test_connection():
                messages.error(
                    request,
                    "Not connected to DataHub. Please check your connection settings.",
                )
                return redirect("metadata_manager:property_list")

            # Get remote properties
            remote_properties = client.list_structured_properties(count=1000)

            if not remote_properties:
                messages.info(request, "No properties found in DataHub")
                return redirect("metadata_manager:property_list")

            # Process each remote property
            imported_count = 0
            updated_count = 0

            for remote_property in remote_properties:
                # Extract property data
                urn = remote_property.get("urn")

                # Skip if no URN
                if not urn:
                    continue

                # Check if we already have this property
                existing_property = StructuredProperty.objects.filter(
                    urn=urn
                ).first()

                # Process property data
                try:
                    definition = remote_property.get("definition", {}) or {}
                    settings = remote_property.get("settings", {}) or {}

                    # Extract basic property info
                    name = definition.get("displayName", "")
                    qualified_name = definition.get("qualifiedName", "")
                    description = definition.get("description", "")
                    cardinality = definition.get("cardinality", "SINGLE")
                    immutable = definition.get("immutable", False)

                    # Extract value type - handle nested structure
                    value_type_info = definition.get("valueType", {}) or {}
                    value_type = _process_value_type(value_type_info)

                    # Extract entity types
                    entity_types_info = definition.get("entityTypes", []) or []
                    entity_types = _process_entity_types(entity_types_info)

                    # Extract allowed values
                    allowed_values_info = definition.get("allowedValues", []) or []
                    allowed_values = []
                    for av in allowed_values_info:
                        description = av.get("description", "")
                        
                        # Handle both remote format {value: {stringValue: "..."}} and local format {value: "..."}
                        value_obj = av.get("value", {})
                        value = None
                        
                        if isinstance(value_obj, dict):
                            # Remote format: {value: {stringValue: "..."}, description: "..."}
                            if "stringValue" in value_obj:
                                value = value_obj.get("stringValue")
                            elif "numberValue" in value_obj:
                                value = value_obj.get("numberValue")
                            elif "booleanValue" in value_obj:
                                value = value_obj.get("booleanValue")
                        else:
                            # Local format: {value: "...", description: "..."}
                            value = value_obj

                        if value is not None:
                            allowed_values.append(
                                {"value": value, "description": description}
                            )

                    # Extract display settings
                    show_in_search_filters = settings.get("showInSearchFilters", True)
                    show_as_asset_badge = settings.get("showAsAssetBadge", True)
                    show_in_asset_summary = settings.get("showInAssetSummary", True)
                    show_in_columns_table = settings.get("showInColumnsTable", False)
                    is_hidden = settings.get("isHidden", False)

                    # Create or update property
                    if existing_property:
                        # Update existing property
                        existing_property.name = name
                        existing_property.description = description
                        existing_property.qualified_name = qualified_name
                        existing_property.value_type = value_type
                        existing_property.cardinality = cardinality
                        existing_property.immutable = immutable
                        existing_property.entity_types = entity_types
                        existing_property.allowed_values = allowed_values
                        existing_property.show_in_search_filters = (
                            show_in_search_filters
                        )
                        existing_property.show_as_asset_badge = show_as_asset_badge
                        existing_property.show_in_asset_summary = show_in_asset_summary
                        existing_property.show_in_columns_table = show_in_columns_table
                        existing_property.is_hidden = is_hidden
                        existing_property.sync_status = "SYNCED"
                        existing_property.last_synced = timezone.now()
                        # Set connection if available
                        if current_connection:
                            existing_property.connection = current_connection
                        existing_property.save()

                        updated_count += 1
                        logger.info(f"Updated property {name} with connection: {current_connection.name if current_connection else 'None'}")
                    else:
                        # Create new property with unique datahub_id
                        base_qualified_name = qualified_name
                        unique_datahub_id = f"{name}_{current_connection.name}" if current_connection else name
                        
                        StructuredProperty.objects.create(
                            datahub_id=unique_datahub_id,  # Make datahub_id unique instead
                            name=qualified_name,
                            qualified_name=base_qualified_name,  # Keep original qualified_name
                            description=description,
                            value_type=value_type,
                            cardinality=cardinality,
                            immutable=immutable,
                            entity_types=entity_types,
                            allowed_values=allowed_values,
                            urn=urn,
                            sync_status="SYNCED",
                            last_synced=timezone.now(),
                            connection=current_connection,  # Set connection
                            show_in_search_filters=show_in_search_filters,
                            show_as_asset_badge=show_as_asset_badge,
                            show_in_asset_summary=show_in_asset_summary,
                            show_in_columns_table=show_in_columns_table,
                            is_hidden=is_hidden,
                        )

                        imported_count += 1
                        logger.info(f"Created new property {name} with connection: {current_connection.name if current_connection else 'None'}")
                except Exception as e:
                    logger.error(f"Error processing property {urn}: {str(e)}")
                    continue

            # Show success message
            if imported_count > 0 or updated_count > 0:
                messages.success(
                    request,
                    f"Successfully imported {imported_count} new properties and updated {updated_count} existing properties",
                )
            else:
                messages.info(request, "No properties were imported or updated")

            return redirect("metadata_manager:property_list")
        except Exception as e:
            logger.error(f"Error pulling properties: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")


@method_decorator(csrf_exempt, name="dispatch")
class PropertyValuesView(View):
    """View to list entity property values"""

    def get(self, request):
        """List entities with their property values"""
        try:
            # Get query parameters
            entity_type = request.GET.get("entity_type", "")
            property_urn = request.GET.get("property_urn", "")
            query = request.GET.get("query", "*")
            start = int(request.GET.get("start", "0"))
            count = int(request.GET.get("count", "20"))

            # Get DataHub client
            client = get_datahub_client_from_request(request)
            if not client or not client.test_connection():
                return JsonResponse(
                    {
                        "success": False,
                        "error": "Not connected to DataHub. Please check your connection settings.",
                    }
                )

            # TODO: Implement fetching entities with property values when method is added to DataHubRestClient
            # For now, return placeholder data
            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "entity_type": entity_type,
                        "property_urn": property_urn,
                        "query": query,
                        "start": start,
                        "count": count,
                        "total": 0,
                        "entities": [],
                    },
                }
            )
        except Exception as e:
            logger.error(f"Error fetching property values: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


@require_POST
def add_property_to_pr(request, property_id):
    """AJAX endpoint to add a property to a pull request"""
    try:
        logger.info(f"Adding property {property_id} to PR")
        
        # Get the property
        property = get_object_or_404(StructuredProperty, id=property_id)
        
        # Check if git integration is available
        if not GIT_INTEGRATION_AVAILABLE or GitIntegration is None:
            return JsonResponse(
                {"success": False, "error": "Git integration not available"}
            )
        
        try:
            # Get current branch and environment info
            settings = GitSettings.objects.first()
            current_branch = settings.current_branch if settings else "main"
            
            commit_message = f"Add/update property: {property.name}"
            
            # Use GitIntegration to push to git
            logger.info(f"Staging property {property.id} to Git branch {current_branch}")
            git_integration = GitIntegration()
            result = git_integration.stage_changes(property)
            
            if result and result.get("success"):
                logger.info(f"Successfully staged property {property.id} to Git branch {current_branch}")
                
                return JsonResponse({
                    "success": True,
                    "message": f'Property "{property.name}" staged for commit to branch {current_branch}. You can create a PR from the Git Repository tab.',
                    "branch": current_branch,
                    "redirect_url": "/github/repo/"
                })
            else:
                error_message = f'Failed to stage property "{property.name}"'
                if isinstance(result, dict) and "error" in result:
                    error_message += f": {result['error']}"
                
                logger.error(f"Failed to stage property: {error_message}")
                return JsonResponse({"success": False, "error": error_message})
                
        except Exception as git_error:
            logger.error(f"Error staging property to git: {str(git_error)}")
            return JsonResponse({"success": False, "error": f"Git staging failed: {str(git_error)}"})

    except Exception as e:
        logger.error(f"Error adding property to PR: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def resync_property(request, property_id):
    """AJAX endpoint to resync a property with DataHub"""
    try:
        logger.info(f"Resyncing property {property_id}")
        
        # Get the property
        property = get_object_or_404(StructuredProperty, id=property_id)
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Since we removed get_structured_property, we'll just mark as synced
        # In a real implementation, you would fetch the latest data from DataHub
        logger.debug(f"Marking property as synced: {property.urn}")
        
        property.sync_status = "SYNCED"
        property.last_synced = timezone.now()
        property.save()
        
        return JsonResponse({
            "success": True,
            "message": f'Property "{property.name}" resynced successfully'
        })

    except Exception as e:
        logger.error(f"Error resyncing property: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST 
def sync_property_to_local(request):
    """AJAX endpoint to sync a remote property to local"""
    try:
        logger.info("Syncing remote property to local")
        
        # Get the property URN from the request
        property_urn = request.POST.get("property_urn")
        if not property_urn:
            return JsonResponse({"success": False, "error": "Property URN required"})
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Fetch property from DataHub using list method and filter by URN
        logger.debug(f"Fetching property from DataHub: {property_urn}")
        
        # Get all remote properties and find the one with matching URN
        remote_properties = client.list_structured_properties(count=1000)
        remote_property = None
        
        if remote_properties:
            for prop in remote_properties:
                if prop.get("urn") == property_urn:
                    remote_property = prop
                    break
        
        if not remote_property:
            return JsonResponse({"success": False, "error": "Property not found in DataHub"})

        # Extract property data from remote
        definition = remote_property.get("definition", {}) or {}
        settings = remote_property.get("settings", {}) or {}
        
        property_name = definition.get("displayName", "Unknown Property")
        
        # Get current connection from request session
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        
        # Check if we already have this property
        existing_property = StructuredProperty.objects.filter(
            urn=property_urn
        ).first()
        
        if existing_property:
            # Update existing property
            existing_property.name = property_name
            existing_property.description = definition.get("description", "")
            existing_property.show_in_search_filters = settings.get("showInSearchFilters", True)
            existing_property.show_as_asset_badge = settings.get("showAsAssetBadge", True)
            existing_property.show_in_asset_summary = settings.get("showInAssetSummary", True)
            existing_property.show_in_columns_table = settings.get("showInColumnsTable", False)
            existing_property.is_hidden = settings.get("isHidden", False)
            existing_property.sync_status = "SYNCED"
            existing_property.last_synced = timezone.now()
            # Set connection if available
            if current_connection:
                existing_property.connection = current_connection
            existing_property.save()
            
            message = f'Property "{existing_property.name}" updated successfully'
        else:
            # Create new property with unique datahub_id
            base_qualified_name = definition.get("qualifiedName", property_name)
            unique_datahub_id = f"{base_qualified_name}_{current_connection.name}" if current_connection else base_qualified_name
            
            new_property = StructuredProperty.objects.create(
                datahub_id=unique_datahub_id,  # Make datahub_id unique instead
                name=property_name,
                description=definition.get("description", ""),
                qualified_name=base_qualified_name,  # Keep original qualified_name
                value_type=_process_value_type(definition.get("valueType", "STRING")),
                cardinality=definition.get("cardinality", "SINGLE"),
                urn=property_urn,
                sync_status="SYNCED",
                last_synced=timezone.now(),
                connection=current_connection,  # Set connection
                show_in_search_filters=settings.get("showInSearchFilters", True),
                show_as_asset_badge=settings.get("showAsAssetBadge", True),
                show_in_asset_summary=settings.get("showInAssetSummary", True),
                show_in_columns_table=settings.get("showInColumnsTable", False),
                is_hidden=settings.get("isHidden", False),
            )
            
            message = f'Property "{property_name}" synced successfully'
        
        return JsonResponse({
            "success": True,
            "message": message
        })

    except Exception as e:
        logger.error(f"Error syncing property to local: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def add_remote_property_to_pr(request):
    """AJAX endpoint to add a remote property to staged changes (simplified)"""
    try:
        logger.info("Adding remote property to staged changes")
        
        # Get the property URN from the request
        property_urn = request.POST.get("property_urn")
        if not property_urn:
            return JsonResponse({"success": False, "error": "Property URN required"})
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Fetch property from DataHub
        remote_properties = client.list_structured_properties(count=1000)
        remote_property = None
        
        if remote_properties:
            for prop in remote_properties:
                if prop.get("urn") == property_urn:
                    remote_property = prop
                    break
        
        if not remote_property:
            return JsonResponse({"success": False, "error": "Property not found in DataHub"})

        # Get current environment and mutation from global state or settings
        current_environment = getattr(request, 'current_environment', {'name': 'dev'})
        mutation_name = getattr(current_environment, 'mutation_name', None)
        
        # Use the remote staging endpoint (like glossary does)
        import json
        response = PropertyRemoteAddToStagedChangesView().post(
            type('MockRequest', (), {
                'body': json.dumps({
                    'item_data': remote_property,
                    'environment': current_environment.get('name', 'dev'),
                    'mutation_name': mutation_name
                }).encode(),
                'user': request.user
            })()
        )
        
        if hasattr(response, 'content'):
            response_data = json.loads(response.content.decode())
            if response_data.get('status') == 'success':
                return JsonResponse({
                    "success": True,
                    "message": response_data.get('message', 'Remote property added to staged changes successfully')
                })
            else:
                return JsonResponse({
                    "success": False,
                    "error": response_data.get('error', 'Failed to add remote property to staged changes')
                })
        else:
            return JsonResponse({"success": False, "error": "Unexpected response format"})

    except Exception as e:
        logger.error(f"Error adding remote property to staged changes: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_POST
def delete_remote_property(request):
    """AJAX endpoint to delete a remote property from DataHub"""
    try:
        logger.info("Deleting remote property")
        
        # Get the property URN from the request
        property_urn = request.POST.get("property_urn")
        if not property_urn:
            return JsonResponse({"success": False, "error": "Property URN required"})
        
        # Test DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})

        # Get property details first to get the proper name
        remote_properties = client.list_structured_properties(count=1000)
        remote_property = None
        
        if remote_properties:
            for prop in remote_properties:
                if prop.get("urn") == property_urn:
                    remote_property = prop
                    break
        
        if not remote_property:
            return JsonResponse({"success": False, "error": "Property not found in DataHub"})
        
        # Extract property name from remote data
        definition = remote_property.get("definition", {}) or {}
        property_name = definition.get("displayName", "Unknown Property")

        # Delete from DataHub (this method should exist)
        try:
            success = client.delete_structured_property(property_urn)
        except AttributeError:
            # If delete method doesn't exist, return error
            return JsonResponse({"success": False, "error": "Delete functionality not available in current DataHub client"})
        
        if success:
            return JsonResponse({
                "success": True,
                "message": f'Property "{property_name}" deleted from DataHub successfully'
            })
        else:
            return JsonResponse({
                "success": False,
                "error": f'Failed to delete property "{property_name}" from DataHub'
            })

    except Exception as e:
        logger.error(f"Error deleting remote property: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@method_decorator(csrf_exempt, name="dispatch")
class PropertyAddToStagedChangesView(View):
    """API endpoint to add a property to staged changes"""
    
    def post(self, request, property_id):
        try:
            import json
            import os
            import sys
            from pathlib import Path
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.structured_property_actions import add_structured_property_to_staged_changes
            
            # Get the structured property from database
            try:
                property_obj = StructuredProperty.objects.get(id=property_id)
            except StructuredProperty.DoesNotExist:
                return JsonResponse({
                    "success": False,
                    "error": f"Structured property with id {property_id} not found"
                }, status=404)
            
            logger.info(f"Found property: {property_obj.name} (ID: {property_obj.id})")
            data = json.loads(request.body)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # Create property data dictionary
            property_data = {
                "id": str(property_obj.id),
                "name": property_obj.name,
                "qualified_name": property_obj.qualified_name,
                "description": property_obj.description,
                "value_type": property_obj.value_type,
                "cardinality": property_obj.cardinality,
                "entity_types": property_obj.entity_types or [],
                "allowed_values": property_obj.allowed_values or [],
                "allowed_entity_types": property_obj.allowed_entity_types or [],
                "urn": property_obj.urn,
                "show_in_search_filters": property_obj.show_in_search_filters,
                "show_as_asset_badge": property_obj.show_as_asset_badge,
                "show_in_asset_summary": property_obj.show_in_asset_summary,
                "show_in_columns_table": property_obj.show_in_columns_table,
                "is_hidden": property_obj.is_hidden,
                "immutable": property_obj.immutable,
                "sync_status": property_obj.sync_status,
            }
            
            # Get environment
            try:
                from web_ui.models import Environment
                environment = Environment.objects.get(name=environment_name)
            except Environment.DoesNotExist:
                # Create default environment if it doesn't exist
                environment = Environment.objects.create(
                    name=environment_name,
                    description=f"Auto-created {environment_name} environment"
                )
            
            # Add property to staged changes
            result = add_structured_property_to_staged_changes(
                property_id=str(property_obj.id),
                qualified_name=property_obj.qualified_name,
                display_name=property_obj.name,
                description=property_obj.description,
                value_type=property_obj.value_type,
                cardinality=property_obj.cardinality,
                allowedValues=property_obj.allowed_values or [],
                entity_types=property_obj.entity_types or [],
                environment=environment_name,
                owner=owner
                # base_dir will be automatically calculated to metadata-manager/{environment}/structured_properties
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "success": False,
                    "error": result.get("message", "Failed to add property to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"Property added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            # Return success response
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "mcps_created": mcps_created,
                "property_id": str(property_obj.id),
                "property_urn": property_obj.urn
            })
                
        except Exception as e:
            logger.error(f"Error adding property to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)


@require_POST
def resync_all_properties(request):
    """Resync all properties from DataHub"""
    try:
        logger.info("Starting resync of all properties")
        
        # Get DataHub client
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({
                'success': False,
                'error': 'Not connected to DataHub'
            })
        
        # Get all remote properties
        remote_properties = client.list_structured_properties(count=1000)
        if not remote_properties:
            return JsonResponse({
                'success': False,
                'error': 'No properties found in DataHub'
            })
        
        # Get local properties
        local_properties = StructuredProperty.objects.all()
        local_urns = {prop.urn for prop in local_properties if prop.urn}
        
        # Count properties that need to be synced
        synced_count = 0
        for remote_prop in remote_properties:
            remote_urn = remote_prop.get('urn')
            if remote_urn and remote_urn not in local_urns:
                # This property exists remotely but not locally
                synced_count += 1
        
        logger.info(f"Found {synced_count} properties to sync")
        
        return JsonResponse({
            'success': True,
            'count': synced_count,
            'message': f'Successfully identified {synced_count} properties to sync'
        })
        
    except Exception as e:
        logger.error(f"Error resyncing all properties: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error resyncing properties: {str(e)}'
        })

@require_POST
def export_all_properties(request):
    """Export all properties to JSON"""
    try:
        logger.info("Starting export of all properties")
        
        # Get all properties
        properties = StructuredProperty.objects.all()
        
        # Convert to JSON-serializable format
        properties_data = []
        for prop in properties:
            prop_data = {
                'id': str(prop.id),
                'name': prop.name,
                'qualified_name': prop.qualified_name,
                'description': prop.description,
                'value_type': prop.value_type,
                'cardinality': prop.cardinality,
                'entity_types': prop.entity_types,
                'urn': prop.urn,
                'created_at': prop.created_at.isoformat() if prop.created_at else None,
                'updated_at': prop.updated_at.isoformat() if prop.updated_at else None,
            }
            properties_data.append(prop_data)
        
        # Create response with JSON data
        from django.http import HttpResponse
        response = HttpResponse(
            json.dumps(properties_data, indent=2, default=str),
            content_type='application/json'
        )
        response['Content-Disposition'] = f'attachment; filename="properties_export_{timezone.now().strftime("%Y%m%d_%H%M%S")}.json"'
        
        logger.info(f"Exported {len(properties_data)} properties")
        return response
        
    except Exception as e:
        logger.error(f"Error exporting properties: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error exporting properties: {str(e)}'
        })

@method_decorator(csrf_exempt, name="dispatch")
class PropertyAddAllToStagedChangesView(View):
    """API view for adding all properties to staged changes"""
    
    def post(self, request):
        try:
            data = json.loads(request.body)
            environment = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            
            # Get current connection to filter properties by connection
            from web_ui.views import get_current_connection
            current_connection = get_current_connection(request)
            
            # Get properties relevant to current connection using the same logic as the list view
            all_properties = StructuredProperty.objects.all()
            properties = filter_properties_by_connection(all_properties, current_connection)
            
            if not properties:
                return JsonResponse({
                    'success': False,
                    'error': 'No properties found to add to staged changes for current connection'
                }, status=400)
            
            # Import the property actions module
            sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            from scripts.mcps.structured_property_actions import add_structured_property_to_staged_changes
            
            success_count = 0
            error_count = 0
            files_created_count = 0
            mcps_created_count = 0
            errors = []
            all_created_files = []
            
            for prop in properties:
                try:
                    # Add property to staged changes - pass only supported parameters
                    result = add_structured_property_to_staged_changes(
                        property_id=str(prop.id),
                        qualified_name=prop.qualified_name,
                        display_name=prop.name,
                        description=prop.description,
                        value_type=prop.value_type,
                        cardinality=prop.cardinality,
                        allowedValues=prop.allowed_values or [],
                        entity_types=prop.entity_types or [],
                        environment=environment,
                        owner="system"  # Default owner
                        # base_dir will be automatically calculated to metadata-manager/{environment}/structured_properties
                    )
                    
                    if result.get("success"):
                        success_count += 1
                        files_created_count += len(result.get("files_saved", []))
                        mcps_created_count += result.get("mcps_created", 0)
                        all_created_files.extend(result.get("files_saved", []))
                    else:
                        error_count += 1
                        errors.append(f"Property {prop.name}: {result.get('message', 'Unknown error')}")
                    
                except Exception as e:
                    error_count += 1
                    errors.append(f"Property {prop.name}: {str(e)}")
                    logger.error(f"Error adding property {prop.name} to staged changes: {str(e)}")
            
            message = f"Add all to staged changes completed: {success_count} properties processed, {mcps_created_count} MCPs created, {files_created_count} files saved, {error_count} failed"
            if errors:
                message += f". Errors: {'; '.join(errors[:5])}"  # Show first 5 errors
                if len(errors) > 5:
                    message += f" and {len(errors) - 5} more..."
            
            return JsonResponse({
                'success': True,
                'message': message,
                'success_count': success_count,
                'error_count': error_count,
                'files_created_count': files_created_count,
                'mcps_created_count': mcps_created_count,
                'errors': errors,
                'files_created': all_created_files
            })
            
        except Exception as e:
            logger.error(f"Error adding all properties to staged changes: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)

@require_POST
def import_properties(request):
    """Import properties from JSON file"""
    try:
        logger.info("Starting import of properties")
        
        # Check if file was uploaded
        if 'import_file' not in request.FILES:
            return JsonResponse({
                'success': False,
                'error': 'No file uploaded'
            })
        
        import_file = request.FILES['import_file']
        overwrite_existing = request.POST.get('overwrite_existing', 'false').lower() == 'true'
        
        # Read and parse JSON file
        try:
            import_data = json.loads(import_file.read().decode('utf-8'))
        except json.JSONDecodeError as e:
            return JsonResponse({
                'success': False,
                'error': f'Invalid JSON file: {str(e)}'
            })
        
        # Validate data structure
        if not isinstance(import_data, list):
            return JsonResponse({
                'success': False,
                'error': 'JSON file should contain an array of properties'
            })
        
        # Process each property
        imported_count = 0
        updated_count = 0
        skipped_count = 0
        
        for property_data in import_data:
            try:
                # Check if property already exists
                existing_property = None
                if property_data.get('qualified_name'):
                    existing_property = StructuredProperty.objects.filter(
                        qualified_name=property_data['qualified_name']
                    ).first()
                elif property_data.get('name'):
                    existing_property = StructuredProperty.objects.filter(
                        name=property_data['name']
                    ).first()
                
                if existing_property:
                    if overwrite_existing:
                        # Update existing property
                        for field, value in property_data.items():
                            if hasattr(existing_property, field) and field not in ['id', 'created_at', 'updated_at']:
                                setattr(existing_property, field, value)
                        existing_property.save()
                        updated_count += 1
                    else:
                        # Skip existing property
                        skipped_count += 1
                        continue
                else:
                    # Create new property with unique datahub_id
                    base_qualified_name = property_data.get('qualified_name', '')
                    # Get current connection for unique naming
                    from web_ui.views import get_current_connection
                    current_connection = get_current_connection(request)
                    unique_datahub_id = f"{property_data.get('name', '')}_{current_connection.name}" if current_connection else property_data.get('name', '')
                    
                    StructuredProperty.objects.create(
                        datahub_id=unique_datahub_id,  # Make datahub_id unique instead
                        name=property_data.get('qualified_name', ''),
                        qualified_name=base_qualified_name,  # Keep original qualified_name
                        description=property_data.get('description', ''),
                        value_type=_process_value_type(property_data.get('value_type', 'STRING')),
                        cardinality=property_data.get('cardinality', 'SINGLE'),
                        entity_types=property_data.get('entity_types', []),
                        urn=property_data.get('urn', ''),
                        connection=current_connection,  # Set connection
                    )
                    imported_count += 1
                    
            except Exception as e:
                logger.error(f"Error importing property {property_data.get('name', 'Unknown')}: {str(e)}")
                continue
        
        logger.info(f"Import completed: {imported_count} imported, {updated_count} updated, {skipped_count} skipped")
        
        return JsonResponse({
            'success': True,
            'imported': imported_count,
            'updated': updated_count,
            'skipped': skipped_count,
            'message': f'Successfully imported {imported_count} properties and updated {updated_count} existing properties'
        })
        
    except Exception as e:
        logger.error(f"Error importing properties: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'Error importing properties: {str(e)}'
        })

@method_decorator(csrf_exempt, name="dispatch")
class PropertyDownloadJsonView(View):
    """View to download a property as JSON"""

    def get(self, request, property_id):
        """Download property as JSON"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)
            
            # Convert to JSON-serializable format
            property_data = property.to_dict()
            
            # Add additional metadata
            property_data.update({
                'id': str(property.id),
                'created_at': property.created_at.isoformat() if property.created_at else None,
                'updated_at': property.updated_at.isoformat() if property.updated_at else None,
                'sync_status': property.sync_status,
                'last_synced': property.last_synced.isoformat() if property.last_synced else None,
            })
            
            # Create response with JSON data
            from django.http import HttpResponse
            response = HttpResponse(
                json.dumps(property_data, indent=2, default=str),
                content_type='application/json'
            )
            response['Content-Disposition'] = f'attachment; filename="property_{property.name}_{timezone.now().strftime("%Y%m%d_%H%M%S")}.json"'
            
            return response
            
        except Exception as e:
            logger.error(f"Error downloading property: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Error downloading property: {str(e)}'
            })

def filter_properties_by_connection(all_properties, current_connection):
    """
    Filter and return properties based on connection logic - only one row per datahub_id:
    1. If property is available for current connection_id -> show in synced
    2. If property is available for different connection_id -> show in local-only  
    3. If property has no connection_id -> show in local-only
    Priority: current connection > different connection > no connection
    """
    properties = []
    
    # Group properties by datahub_id to implement the single-row-per-datahub_id logic
    properties_by_datahub_id = {}
    for prop in all_properties:
        datahub_id = prop.datahub_id or f'no_datahub_id_{prop.id}'  # Use unique identifier for properties without datahub_id
        if datahub_id not in properties_by_datahub_id:
            properties_by_datahub_id[datahub_id] = []
        properties_by_datahub_id[datahub_id].append(prop)
    
    # For each datahub_id, select the best property based on connection priority
    for datahub_id, property_list in properties_by_datahub_id.items():
        if len(property_list) == 1:
            # Only one property with this datahub_id
            properties.append(property_list[0])
        else:
            # Multiple properties with same datahub_id - apply priority logic
            current_connection_props = [p for p in property_list if p.connection == current_connection]
            different_connection_props = [p for p in property_list if p.connection is not None and p.connection != current_connection]
            no_connection_props = [p for p in property_list if p.connection is None]
            
            if current_connection_props:
                # Priority 1: Property for current connection
                properties.append(current_connection_props[0])
            elif different_connection_props:
                # Priority 2: Property for different connection (will appear as local-only)
                properties.append(different_connection_props[0])
            elif no_connection_props:
                # Priority 3: Property with no connection (will appear as local-only)
                properties.append(no_connection_props[0])
    
    return properties

@method_decorator(csrf_exempt, name="dispatch")
class PropertyRemoteAddToStagedChangesView(View):
    """API endpoint to add a remote property to staged changes without syncing to local first"""
    
    def post(self, request):
        try:
            import json
            import os
            import sys
            
            # Add project root to path to import our Python modules
            sys.path.append(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            )
            
            # Import the function
            from scripts.mcps.structured_property_actions import add_structured_property_to_staged_changes
            
            data = json.loads(request.body)
            
            # Get environment and mutation name
            environment_name = data.get('environment', 'dev')
            mutation_name = data.get('mutation_name')
            property_data = data.get('item_data')
            
            if not property_data:
                return JsonResponse({
                    "success": False,
                    "error": "Property data is required"
                }, status=400)
            
            # Get current user as owner
            owner = request.user.username if request.user.is_authenticated else "admin"
            
            # Extract property information from remote data
            definition = property_data.get("definition", {}) or {}
            settings = property_data.get("settings", {}) or {}
            property_urn = property_data.get("urn")
            
            if not property_urn:
                return JsonResponse({
                    "success": False,
                    "error": "Property URN is required"
                }, status=400)
            
            # Extract property ID from URN (last part after colon)
            property_id = property_urn.split(':')[-1] if property_urn else None
            if not property_id:
                return JsonResponse({
                    "success": False,
                    "error": "Could not extract property ID from URN"
                }, status=400)
            
            property_name = definition.get("displayName", "Unknown Property")
            qualified_name = definition.get("qualifiedName", property_name)
            
            logger.info(f"Adding remote property '{property_name}' to staged changes...")
            
            # Add remote property to staged changes using the simplified function
            result = add_structured_property_to_staged_changes(
                property_id=property_id,
                qualified_name=qualified_name,
                display_name=property_name,
                description=definition.get("description", ""),
                value_type=definition.get("valueType", "STRING"),
                cardinality=definition.get("cardinality", "SINGLE"),
                allowedValues=definition.get("allowedValues", []),
                entity_types=definition.get("entityTypes", []),
                environment=environment_name,
                owner=owner
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "success": False,
                    "error": result.get("message", "Failed to add remote property to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"Remote property added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "mcps_created": mcps_created,
                "property_urn": property_urn
            })
                
        except Exception as e:
            logger.error(f"Error adding remote property to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)
