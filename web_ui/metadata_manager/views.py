from django.shortcuts import render
from django.views import View
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
import os
import logging
import sys
import re
from django.core.cache import cache
import hashlib

# Add project root to sys.path
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)

# Import the deterministic URN utilities

from utils.datahub_rest_client import DataHubRestClient
from .models import Tag, GlossaryNode, GlossaryTerm, Domain, Assertion, Environment, StructuredProperty

# Create a logger
logger = logging.getLogger(__name__)


class MetadataIndexView(View):
    """Main index view for the metadata manager"""
    
    def get(self, request):
        """Display the main dashboard for metadata management"""
        try:
            # Get summary statistics
            stats = {
                "tags_count": Tag.objects.count(),
                "glossary_nodes_count": GlossaryNode.objects.count(),
                "glossary_terms_count": GlossaryTerm.objects.count(),
                "domains_count": Domain.objects.count(),
                "assertions_count": Assertion.objects.count(),
                "structured_properties_count": StructuredProperty.objects.count(),
            }

            return render(
                request,
                "metadata_manager/index.html",
                {"stats": stats, "page_title": "Metadata Manager"},
            )
        except Exception as e:
            logger.error(f"Error in metadata index view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/index.html",
                {"error": str(e), "page_title": "Metadata Manager"},
            )


def ensure_default_environment():
    """Ensure there's at least one default environment in the database."""
    try:
        default_env = Environment.objects.filter(is_default=True).first()
        if not default_env:
            logger.warning(
                "No default environment found, checking for any environment..."
            )
            # Check if there's any environment we can make default
            any_env = Environment.objects.first()
            if any_env:
                logger.info(f"Making environment '{any_env.name}' the default")
                any_env.is_default = True
                any_env.save(update_fields=["is_default"])
            else:
                logger.warning("No environments found, creating a default one")
                # Try to get settings from AppSettings
                try:
                    from web_ui.models import AppSettings

                    datahub_url = AppSettings.get(
                        "datahub_url", "http://localhost:8080"
                    )
                    datahub_token = AppSettings.get("datahub_token", "")
                    logger.info(f"Using DataHub URL from AppSettings: {datahub_url}")
                except Exception as e:
                    logger.error(f"Error getting settings from AppSettings: {str(e)}")
                    datahub_url = "http://localhost:8080"
                    datahub_token = ""

                # Create a default environment - user will need to update it
                Environment.objects.create(
                    name="Default Environment",
                    description="Default environment for DataHub integration",
                    is_default=True,
                    datahub_url=datahub_url,  # Use value from AppSettings if available
                    datahub_token=datahub_token,
                )
        else:
            # Sync with AppSettings
            try:
                from web_ui.models import AppSettings

                if default_env.datahub_url:
                    AppSettings.set("datahub_url", default_env.datahub_url)
                if default_env.datahub_token:
                    AppSettings.set("datahub_token", default_env.datahub_token)
                logger.info(
                    f"Synced environment settings to AppSettings: {default_env.datahub_url}"
                )
            except Exception as e:
                logger.error(f"Error syncing environment to AppSettings: {str(e)}")

        return Environment.objects.filter(is_default=True).first()
    except Exception as e:
        logger.error(f"Error ensuring default environment: {str(e)}")
        return None


@login_required
def editable_properties_view(request):
    """View for managing editable properties of all entities."""
    # Ensure there's a default environment
    environment = ensure_default_environment()

    # Check if there's a default environment and log its details
    if environment:
        logger.info(
            f"Default environment: {environment.name}, URL: {environment.datahub_url}"
        )
        # Try to create a client and test connection
        try:
            from utils.datahub_rest_client import DataHubRestClient

            client = DataHubRestClient(
                environment.datahub_url, environment.datahub_token
            )
            connection_working = client.test_connection()
            logger.info(
                f"DataHub connection test result: {'SUCCESS' if connection_working else 'FAILED'}"
            )
        except Exception as e:
            logger.error(f"Error testing DataHub connection: {str(e)}")
    else:
        logger.warning("No default environment configured!")

    return render(
        request,
        "metadata_manager/entities/editable_properties.html",
        {"page_title": "Editable Properties"},
    )


def has_editable_properties(entity):
    """
    Check if an entity has editable properties or editable schema metadata.

    Args:
        entity (dict): The entity dictionary from the API response

    Returns:
        bool: True if the entity has editable properties or schema metadata, False otherwise
    """
    if not entity:
        return False

    # Check for editableProperties
    if entity.get("editableProperties"):
        editable_props = entity["editableProperties"]
        logger.debug(f"Entity has editableProperties: {editable_props}")
        # If editableProperties exists, consider it as having editable properties
        # even if some fields are empty
        return True

    # Check for editableSchemaMetadata
    if entity.get("editableSchemaMetadata"):
        schema_metadata = entity["editableSchemaMetadata"]
        logger.debug(f"Entity has editableSchemaMetadata: {schema_metadata}")

        # Check if there are editable schema field info entries
        if schema_metadata.get("editableSchemaFieldInfo"):
            field_info = schema_metadata["editableSchemaFieldInfo"]
            if isinstance(field_info, list) and len(field_info) > 0:
                logger.debug(f"Entity has {len(field_info)} editable schema fields")
                return True

        # If editableSchemaMetadata exists but no field info, still consider it editable
        return True

    # For debugging: log what properties the entity does have
    entity_keys = list(entity.keys()) if isinstance(entity, dict) else []
    logger.debug(
        f"Entity with URN {entity.get('urn', 'unknown')} has keys: {entity_keys}"
    )

    return False


def get_platform_instances(client, platform, entity_type=None):
    """
    Get a list of platform instances for a given platform.

    Args:
        client: DataHub REST client instance
        platform: Platform name (e.g., 'snowflake', 'bigquery')
        entity_type: Type of entity to filter instances (optional)

    Returns:
        list: List of platform instance identifiers
    """
    try:
        # This could use the DataHub client's GraphQL capabilities to fetch distinct instances
        # For now, we'll return a simpler approach by querying with the platform name and analyzing results

        # Build a query that just looks for this platform
        query = f"platform:{platform}"
        if entity_type:
            query += f" AND type:{entity_type}"

        # Fetch a small sample to identify instances
        result = client.get_editable_entities(
            start=0,
            count=100,  # Sample size - balance between getting enough instances without too much overhead
            query=query,
            entity_type=entity_type,
        )

        if result is None or "searchResults" not in result:
            logger.warning(f"No results found for platform {platform}")
            return []

        # Extract instance identifiers from URNs
        instances = set()
        for search_result in result.get("searchResults", []):
            entity = search_result.get("entity", {})
            urn = entity.get("urn", "")

            # Parse URN to extract instance identifier
            # Format is typically: urn:li:dataset:(urn:li:dataPlatform:platform, instance.database.schema.table, env)
            try:
                if "urn:li:dataset:" in urn or "urn:li:container:" in urn:
                    parts = urn.split(",")
                    if len(parts) >= 2:
                        # The second part may contain instance.database format
                        instance_part = parts[1].strip()
                        # Extract just the instance part
                        instance_id = instance_part.split(".")[0].strip()
                        if instance_id and len(instance_id) > 0:
                            instances.add(instance_id)
            except Exception as e:
                logger.debug(f"Error parsing URN {urn}: {str(e)}")
                continue

        logger.info(
            f"Found {len(instances)} instances for platform {platform}: {instances}"
        )
        return list(instances)
    except Exception as e:
        logger.error(f"Error getting platform instances: {str(e)}")
        return []


# Extend the existing get_platform_list function to include known instances
def get_platform_list(client, entity_type):
    """
    Get a list of platforms for the given entity type to use for pagination.

    Args:
        client: DataHub REST client instance
        entity_type: Type of entity to get platforms for (e.g., 'DATASET')

    Returns:
        list: List of platform names
    """
    try:
        # This could use the DataHub client's GraphQL capabilities to fetch distinct platforms
        # For now, return a hardcoded list of common platforms
        if entity_type == "DATASET":
            return [
                "postgres",
                "mysql",
                "snowflake",
                "bigquery",
                "redshift",
                "databricks",
                "azure",
                "hive",
                "kafka",
                "oracle",
                "mssql",
                "teradata",
                "glue",
                "tableau",
                "looker",
                "metabase",
                "superset",
                "powerbi",
                "dbt",
            ]
        elif entity_type == "CONTAINER":
            return [
                "postgres",
                "mysql",
                "snowflake",
                "bigquery",
                "redshift",
                "azure",
                "glue",
            ]
        elif entity_type in ["CHART", "DASHBOARD"]:
            return ["tableau", "looker", "metabase", "superset", "powerbi"]
        elif entity_type == "DATAFLOW":
            return ["airflow", "glue", "databricks", "azure"]
        elif entity_type == "DATAJOB":
            return ["airflow", "glue", "databricks", "azure"]
        else:
            return []
    except Exception as e:
        logger.error(f"Error getting platform list: {str(e)}")
        return []


@login_required
@require_http_methods(["GET"])
def get_editable_entities(request):
    """
    Get entities with editable properties and return them as JSON.
    Results are cached for 5 minutes.

    Args:
        request: The HTTP request

    Returns:
        JsonResponse: The entities in JSON format
    """
    try:
        # Get query parameters - handle both old and new parameter names for compatibility
        query = request.GET.get("query") or request.GET.get("searchQuery", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 100))
        entity_type = request.GET.get("entity_type") or request.GET.get("entityType")
        platform = request.GET.get("platform")
        sort_by = request.GET.get("sort_by") or request.GET.get("sortBy", "name")
        editable_only = (
            request.GET.get("editable_only", "false").lower() == "true"
        )  # Temporarily set to false
        use_platform_pagination = (
            request.GET.get("use_platform_pagination", "false").lower() == "true"
        )

        logger.info(
            f"Search parameters: query='{query}', entity_type='{entity_type}', platform='{platform}', editable_only={editable_only}, use_platform_pagination={use_platform_pagination}"
        )

        # Create cache key based on all parameters
        cache_params = f"{query}_{start}_{count}_{entity_type}_{platform}_{sort_by}_{editable_only}_{use_platform_pagination}"
        cache_key = (
            f"editable_entities_{hashlib.md5(cache_params.encode()).hexdigest()}"
        )

        # Try to get from cache first
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            logger.info(f"Returning cached results for query: {query}")
            return JsonResponse(cached_result)

        # Get client from session
        client = get_client_from_session(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "Not connected to DataHub"}, status=400
            )

        # Temporarily always use direct search for debugging
        batch_size = min(count * 5, 500)  # Request 5x the needed amount or up to 500

        result = client.get_editable_entities(
            start=start,
            count=batch_size,
            query=query,
            entity_type=entity_type,
            platform=platform,
            sort_by=sort_by,
            editable_only=False,  # We'll filter ourselves
        )

        # Handle the client response
        if result:
            if not result.get("success", True):
                logger.error(
                    f"Client returned error: {result.get('error', 'Unknown error')}"
                )
                return JsonResponse(
                    {"success": False, "error": result.get("error", "Unknown error")},
                    status=500,
                )

            # Extract the actual data from the client response
            if "data" in result:
                result = result["data"]

        if not result or "searchResults" not in result:
            logger.error("Failed to get entities from DataHub or empty response")
            return JsonResponse(
                {"success": False, "error": "Failed to get entities from DataHub"},
                status=500,
            )

        # Get all search results
        search_results = result.get("searchResults", [])

        # Filter to only include entities with editable properties if requested
        if editable_only:
            filtered_results = []
            for search_result in search_results:
                entity = search_result.get("entity")
                if entity and has_editable_properties(entity):
                    filtered_results.append(search_result)

            search_results = filtered_results

        # Update the result with filtered search results
        result["searchResults"] = search_results

        # Structure the response
        response_data = {
            "start": result.get("start", 0),
            "count": len(search_results),  # Use actual count of filtered results
            "total": len(search_results),  # Use actual total of filtered results
            "searchResults": search_results,
        }

        # Wrap in the expected structure for the frontend
        response = {"success": True, "data": response_data}

        # Cache the result for 5 minutes (300 seconds)
        cache.set(cache_key, response, 300)
        logger.info(
            f"Cached results for query: {query}, found {len(search_results)} entities"
        )

        return JsonResponse(response)
    except Exception as e:
        logger.error(f"Error in get_editable_entities: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


def get_comprehensive_search_results(
    client, query, start, count, entity_type, platform, sort_by, editable_only
):
    """
    Perform comprehensive search across platforms and entity types to gather more results.
    """
    # Define all supported entity types for multi-type pagination
    all_entity_types = [
        "DATASET",
        "CONTAINER",
        "DASHBOARD",
        "CHART",
        "DATAFLOW",
        "DATAJOB",
        "DOMAIN",
        "GLOSSARY_TERM",
        "GLOSSARY_NODE",
        "TAG",
    ]

    # Initialize an empty result set
    combined_result = {"searchResults": [], "start": start, "count": count, "total": 0}

    # Keep track of seen URNs to avoid duplicates
    seen_urns = set()
    target_count = count * 3  # Try to get 3x the requested amount for better pagination

    # If no entity type is specified, cycle through all types
    entity_types_to_query = [entity_type] if entity_type else all_entity_types

    logger.info(
        f"Starting comprehensive search for {target_count} results across {len(entity_types_to_query)} entity types"
    )

    # First pass: Direct entity type searches with larger batch sizes
    for current_entity_type in entity_types_to_query:
        if len(combined_result["searchResults"]) >= target_count:
            break

        logger.info(f"Processing entity type: {current_entity_type}")

        try:
            # Use a larger batch size
            batch_size = min(target_count, 500)

            type_result = client.get_editable_entities(
                start=0,
                count=batch_size,
                query=query,
                entity_type=current_entity_type,
                platform=platform,
                sort_by=sort_by,
                editable_only=False,
            )

            if type_result and "searchResults" in type_result:
                # Filter and add results
                for search_result in type_result.get("searchResults", []):
                    entity = search_result.get("entity")
                    if not entity or "urn" not in entity:
                        continue

                    entity_urn = entity["urn"]

                    # Skip if we've already seen this entity
                    if entity_urn in seen_urns:
                        continue

                    # Check if it has editable properties if filtering is enabled
                    if editable_only and not has_editable_properties(entity):
                        continue

                    # Add to results
                    combined_result["searchResults"].append(search_result)
                    seen_urns.add(entity_urn)

                    if len(combined_result["searchResults"]) >= target_count:
                        break

                # Add to total count
                combined_result["total"] += type_result.get("total", 0)

        except Exception as e:
            logger.error(
                f"Error processing entity type {current_entity_type}: {str(e)}"
            )
            continue

    # Second pass: Platform-based search with common platforms only
    if len(combined_result["searchResults"]) < target_count and not platform:
        logger.info("Attempting platform-based search for additional results")

        # Use a simple list of common platforms instead of calling get_platform_list
        common_platforms = [
            "postgres",
            "mysql",
            "snowflake",
            "bigquery",
            "redshift",
            "databricks",
            "glue",
            "hive",
            "kafka",
        ]

        for platform_name in common_platforms:
            if len(combined_result["searchResults"]) >= target_count:
                break

            logger.info(f"Searching platform: {platform_name}")

            try:
                platform_result = client.get_editable_entities(
                    start=0,
                    count=min(
                        200, target_count - len(combined_result["searchResults"])
                    ),
                    query=query,
                    entity_type=entity_type,
                    platform=platform_name,
                    sort_by=sort_by,
                    editable_only=False,
                )

                if platform_result and "searchResults" in platform_result:
                    for search_result in platform_result.get("searchResults", []):
                        entity = search_result.get("entity")
                        if not entity or "urn" not in entity:
                            continue

                        entity_urn = entity["urn"]

                        # Skip if we've already seen this entity
                        if entity_urn in seen_urns:
                            continue

                        # Check if it has editable properties if filtering is enabled
                        if editable_only and not has_editable_properties(entity):
                            continue

                        # Add to results
                        combined_result["searchResults"].append(search_result)
                        seen_urns.add(entity_urn)

                        if len(combined_result["searchResults"]) >= target_count:
                            break

                    combined_result["total"] += platform_result.get("total", 0)

            except Exception as e:
                logger.error(f"Error processing platform {platform_name}: {str(e)}")
                continue

    logger.info(
        f"Comprehensive search completed: {len(combined_result['searchResults'])} entities found from {len(seen_urns)} unique entities"
    )

    return combined_result


@login_required
@require_http_methods(["POST"])
def update_entity_properties(request):
    """Update editable properties of an entity."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse(
                {"success": False, "error": "No active environment configured"}
            )
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get entity details from request
        entity_urn = request.POST.get("entityUrn")
        entity_type = request.POST.get("entityType")
        
        if not entity_urn or not entity_type:
            return JsonResponse(
                {"success": False, "error": "Missing required parameters"}
            )
        
        # Prepare properties update
        properties = {"editableProperties": {}}
        
        # Add name if provided (only for Dataset)
        if entity_type == "DATASET" and request.POST.get("name"):
            properties["editableProperties"]["name"] = request.POST.get("name")
        
        # Add description if provided
        if request.POST.get("description"):
            properties["editableProperties"]["description"] = request.POST.get(
                "description"
            )
        
        # Handle schema metadata for datasets
        if entity_type == "DATASET" and "schemaFields" in request.POST:
            schema_fields = []
            for field in request.POST.getlist("schemaFields"):
                schema_fields.append(
                    {
                        "fieldPath": field.get("fieldPath"),
                        "description": field.get("description"),
                        "tags": field.get("tags", []),
                    }
                )
            properties["editableSchemaMetadata"] = {
                "editableSchemaFieldInfo": schema_fields
            }
        
        # Use the client method to update properties
        success = client.update_entity_properties(
            entity_urn=entity_urn, entity_type=entity_type, properties=properties
        )
        
        if success:
            return JsonResponse({"success": True, "data": {"urn": entity_urn}})
        else:
            return JsonResponse(
                {"success": False, "error": "Failed to update entity properties"}
            )
        
    except Exception as e:
        logger.error(f"Error updating entity properties: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


# Views for specific entity types
from .views_tags import *
from .views_glossary import *
from .views_domains import *
from .views_assertions import *
from .views_sync import *


@login_required
@require_http_methods(["GET"])
def get_entity_details(request, urn):
    """Get details of a specific entity."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse(
                {"success": False, "error": "No active environment configured"}
            )
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get entity details
        entity = client.get_entity(urn)
        
        if not entity:
            return JsonResponse({"success": False, "error": "Entity not found"})
        
        return JsonResponse({"success": True, "entity": entity})
        
    except Exception as e:
        logger.error(f"Error getting entity details: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@login_required
@require_http_methods(["GET"])
def get_entity_schema(request, urn):
    """Get schema details for a dataset entity."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse(
                {"success": False, "error": "No active environment configured"}
            )
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get schema details
        schema = client.get_schema(urn)
        
        if not schema:
            return JsonResponse({"success": False, "error": "Schema not found"})
        
        return JsonResponse({"success": True, "schema": schema})
        
    except Exception as e:
        logger.error(f"Error getting schema details: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@login_required
@require_http_methods(["POST"])
def sync_metadata(request):
    """Sync metadata with DataHub."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse(
                {"success": False, "error": "No active environment configured"}
            )
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Sync metadata
        success = client.sync_metadata()
        
        if success:
            return JsonResponse(
                {"success": True, "message": "Metadata synced successfully"}
            )
        else:
            return JsonResponse({"success": False, "error": "Failed to sync metadata"})
        
    except Exception as e:
        logger.error(f"Error syncing metadata: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


def extract_entity_name(entity):
    """
    Extract the name from an entity, prioritizing different sources.

    Args:
        entity (dict): The entity dictionary from the GraphQL response

    Returns:
        str: The entity name or a fallback
    """
    # Priority 1: Check editableProperties.name (if available)
    if entity.get("editableProperties") and entity["editableProperties"].get("name"):
        return entity["editableProperties"]["name"]

    # Priority 2: Check for direct name property (often available for datasets)
    if "name" in entity:
        return entity["name"]

    # Priority 3: Check properties.name (container and other entities)
    if entity.get("properties") and entity["properties"].get("name"):
        return entity["properties"]["name"]

    # Priority 4: For containers, charts, dashboards, etc. check in entity-specific structure
    entity_type = entity.get("type", "").lower()
    if entity_type in ["container", "chart", "dashboard", "dataflow", "datajob"]:
        # Check for info.name inside entity-specific object
        entity_obj = entity.get(entity_type)
        if entity_obj and entity_obj.get("info") and entity_obj["info"].get("name"):
            return entity_obj["info"]["name"]

    # Fallback: Extract from URN
    if "urn" in entity:
        urn = entity["urn"]
        # For datasets, extract the name from the URN pattern: urn:li:dataset:(platform,name,env)
        if urn.startswith("urn:li:dataset:"):
            try:
                # Parse the complex URN structure for datasets
                matches = re.search(
                    r"urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,)]+)", urn
                )
                if matches and matches.group(2):
                    return matches.group(2)
            except Exception as e:
                logger.error(f"Error extracting name from dataset URN: {str(e)}")

        # For other entities, just return the last part of the URN
        parts = urn.split(":")
        if len(parts) > 0:
            last_part = parts[-1]
            if "/" in last_part:
                return last_part.split("/")[-1]
            return last_part

    # Final fallback
    return "Unnamed Entity"


def extract_browse_paths(entity):
    """
    Extract browse paths from an entity, prioritizing different sources.

    Args:
        entity (dict): The entity dictionary from the GraphQL response

    Returns:
        list: List of browse paths
    """
    paths = []

    # Priority 1: browsePathV2 - this is the most detailed browse path information
    if entity.get("browsePathV2") and entity["browsePathV2"].get("path"):
        try:
            # Extract container names from the path
            path_parts = []
            for path_item in entity["browsePathV2"]["path"]:
                # Check if it has a container with a name
                if (
                    path_item.get("entity")
                    and path_item["entity"].get("container")
                    and path_item["entity"]["container"].get("properties")
                    and path_item["entity"]["container"]["properties"].get("name")
                ):
                    path_parts.append(
                        path_item["entity"]["container"]["properties"]["name"]
                    )
                # Fallback to the raw name (usually a URN)
                elif path_item.get("name"):
                    # Extract a more readable name from the URN
                    if path_item["name"].startswith("urn:li:container:"):
                        path_parts.append(
                            f"container-{path_item['name'].split(':')[-1][:6]}"
                        )
                    else:
                        path_parts.append(path_item["name"])

            if path_parts:
                paths.append("/" + "/".join(path_parts))
        except Exception as e:
            logger.error(f"Error extracting browsePathV2: {str(e)}")

    # Priority 2: browsePaths with path property (array format)
    if entity.get("browsePaths") and isinstance(entity["browsePaths"], list):
        for browse_path in entity["browsePaths"]:
            if isinstance(browse_path, dict) and browse_path.get("path"):
                if isinstance(browse_path["path"], list):
                    path_str = "/" + "/".join(browse_path["path"])
                    paths.append(path_str)
                elif isinstance(browse_path["path"], str):
                    path_str = (
                        browse_path["path"]
                        if browse_path["path"].startswith("/")
                        else "/" + browse_path["path"]
                    )
                    paths.append(path_str)
            # Handle simple string format
            elif isinstance(browse_path, str):
                path_str = (
                    browse_path if browse_path.startswith("/") else "/" + browse_path
                )
                paths.append(path_str)

    # Priority 3: Check properties.browsePaths
    if entity.get("properties") and entity["properties"].get("browsePaths"):
        browse_paths = entity["properties"]["browsePaths"]
        if isinstance(browse_paths, list):
            for path in browse_paths:
                if isinstance(path, str):
                    path_str = path if path.startswith("/") else "/" + path
                    paths.append(path_str)

    # Priority 4: For datasets, check origin browsePaths
    if (
        entity.get("type") == "DATASET"
        and entity.get("dataset")
        and entity["dataset"].get("origin")
    ):
        origin = entity["dataset"]["origin"]
        if origin.get("browsePaths") and isinstance(origin["browsePaths"], list):
            for path in origin["browsePaths"]:
                if isinstance(path, str):
                    path_str = path if path.startswith("/") else "/" + path
                    paths.append(path_str)

    # Check for container path
    if (
        (entity.get("type") in ["CONTAINER", "DATASET"])
        and entity.get("container")
        and entity["container"].get("path")
    ):
        container_path = entity["container"]["path"]
        path_str = (
            container_path if container_path.startswith("/") else "/" + container_path
        )
        paths.append(path_str)

    return paths


def get_browse_paths_hierarchy(client, entity_type=None, parent_path="/"):
    """
    Recursively discover browse path hierarchy for more thorough entity discovery.
    Uses browsePathV2 with proper filtering from DataHub API.

    Args:
        client: DataHub REST client instance
        entity_type: Type of entity to get browse paths for (optional)
        parent_path: Parent path to start discovery from (default: root)

    Returns:
        list: List of discovered browse paths
    """
    try:
        discovered_paths = [parent_path]

        # Use proper GraphQL query for browsing if client supports it
        if hasattr(client, "execute_graphql") and callable(client.execute_graphql):
            # Extract the path name from the parent path - remove leading/trailing slashes
            path_name = parent_path.strip("/")
            if not path_name:
                # For root path, we need to get all top-level entities
                # We'll use a simpler query first to discover top-level containers
                graphql_query = """
                query GetTopLevelEntities($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                      entity {
                        urn
                        type
                        ... on Dataset {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Container {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Dashboard {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Chart {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on DataFlow {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on DataJob {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                      }
                    }
                  }
                }
                """

                # For top-level, use query without browsePathV2 filter, but limit to 100 results
                variables = {"input": {"query": "*", "start": 0, "count": 100}}

                # Add entity type filter if specified
                if entity_type:
                    variables["input"]["types"] = [entity_type]
            else:
                # For non-root paths, use the proper browsePathV2 filter
                graphql_query = """
                query GetBrowsePathEntities($input: SearchAcrossEntitiesInput!) {
                  searchAcrossEntities(input: $input) {
                    start
                    count
                    total
                    searchResults {
                      entity {
                        urn
                        type
                        ... on Dataset {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Container {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Dashboard {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on Chart {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on DataFlow {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                        ... on DataJob {
                          browsePaths { path }
                          browsePathV2 { path { name } }
                        }
                      }
                    }
                  }
                }
                """

                # For non-root paths, use the special browsePathV2 format with the EQUAL condition
                # Note: DataHub uses a special character (␟) before the path name
                variables = {
                    "input": {
                        "query": "*",
                        "start": 0,
                        "count": 100,
                        "orFilters": [
                            {
                                "and": [
                                    {
                                        "field": "browsePathV2",
                                        "condition": "EQUAL",
                                        "values": [f"␟{path_name}"],
                                        "negated": False,
                                    }
                                ]
                            }
                        ],
                    }
                }

                # Add entity type filter if specified
                if entity_type:
                    variables["input"]["types"] = [entity_type]

            # Execute the query
            try:
                result = client.execute_graphql(graphql_query, variables)

                # Process the results
                if (
                    result
                    and "data" in result
                    and "searchAcrossEntities" in result["data"]
                ):
                    search_results = result["data"]["searchAcrossEntities"][
                        "searchResults"
                    ]

                    # Extract and process discovered paths
                    for search_result in search_results:
                        entity = search_result.get("entity", {})

                        # First try to get paths from browsePathV2
                        if "browsePathV2" in entity and entity["browsePathV2"]:
                            browse_path_v2 = entity["browsePathV2"]

                            if "path" in browse_path_v2:
                                # Create the full path by joining the path names
                                path_names = []
                                for path_entry in browse_path_v2["path"]:
                                    if "name" in path_entry:
                                        path_names.append(path_entry["name"])

                                if path_names:
                                    full_path = "/" + "/".join(path_names)

                                    # Extract the next level component based on parent_path
                                    if full_path.startswith(
                                        parent_path if parent_path != "/" else ""
                                    ):
                                        parent_components = parent_path.strip(
                                            "/"
                                        ).split("/")
                                        parent_depth = (
                                            len(parent_components)
                                            if parent_components[0] != ""
                                            else 0
                                        )

                                        path_components = full_path.strip("/").split(
                                            "/"
                                        )
                                        if len(path_components) > parent_depth:
                                            # Get the next level path
                                            next_level_path = "/" + "/".join(
                                                path_components[: parent_depth + 1]
                                            )
                                            if next_level_path not in discovered_paths:
                                                discovered_paths.append(next_level_path)

                        # Fallback to browsePaths if browsePathV2 didn't yield results
                        elif "browsePaths" in entity:
                            browse_paths = entity["browsePaths"]
                            for browse_path in browse_paths:
                                if "path" in browse_path:
                                    path = browse_path["path"]

                                    # Extract the next level component based on parent_path
                                    if path.startswith(
                                        parent_path if parent_path != "/" else ""
                                    ):
                                        path_components = path.strip("/").split("/")
                                        parent_components = parent_path.strip(
                                            "/"
                                        ).split("/")
                                        parent_depth = (
                                            len(parent_components)
                                            if parent_components[0] != ""
                                            else 0
                                        )

                                        if len(path_components) > parent_depth:
                                            # Get the next level path
                                            next_level_path = "/" + "/".join(
                                                path_components[: parent_depth + 1]
                                            )
                                            if next_level_path not in discovered_paths:
                                                discovered_paths.append(next_level_path)
            except Exception as e:
                logger.warning(
                    f"Error executing GraphQL for browse path discovery: {str(e)}"
                )
                # Fall back to regular search if GraphQL fails

        # If GraphQL approach failed or isn't available, fall back to regular search
        if len(discovered_paths) <= 1:  # Only the parent path was found
            # Build query to find entities with this parent path
            path_query = f'browsePaths:"{parent_path}*"'
            if entity_type:
                path_query += f" AND type:{entity_type}"

            logger.info(
                f"Falling back to regular search for path discovery: {path_query}"
            )

            path_result = client.get_editable_entities(
                start=0, count=100, query=path_query, entity_type=entity_type
            )

            if path_result is None or "searchResults" not in path_result:
                return discovered_paths

            # Extract child paths from results
            child_paths = set()
            for search_result in path_result.get("searchResults", []):
                entity = search_result.get("entity", {})

                # Extract browse paths using our helper function that handles both formats
                browse_paths = extract_browse_paths(entity)

                for path in browse_paths:
                    # Check if this path starts with the parent path
                    if path.startswith(parent_path):
                        # Split path into components
                        path_components = path.split("/")

                        # Find the next level component after parent_path
                        parent_components = parent_path.strip("/").split("/")
                        parent_depth = (
                            len(parent_components) if parent_components[0] != "" else 0
                        )

                        # Get the next level component if it exists
                        if len(path_components) > parent_depth + 1:
                            next_level = "/".join(path_components[: parent_depth + 2])
                            if next_level != parent_path and next_level:
                                child_paths.add(next_level)

            # Add all discovered child paths
            discovered_paths.extend(child_paths)

        # Recursively explore child paths, but limit depth to avoid too many requests
        parent_components = parent_path.strip("/").split("/")
        current_depth = len(parent_components) if parent_components[0] != "" else 0

        # Limit recursion depth to 3 levels to avoid excessive API calls
        if (
            current_depth < 3 and len(discovered_paths) > 1
        ):  # More than just the parent path
            # Sort paths by depth and alphabetically within the same depth
            sorted_paths = sorted(
                discovered_paths[1:], key=lambda p: (len(p.split("/")), p)
            )

            # Only recurse into the first few child paths to avoid explosion
            for child_path in sorted_paths[:3]:  # Limit to top 3 children
                if child_path != parent_path:  # Avoid recursing into the same path
                    child_results = get_browse_paths_hierarchy(
                        client, entity_type, child_path
                    )
                    # Add unique paths that aren't already in the list
                    for path in child_results:
                        if path not in discovered_paths:
                            discovered_paths.append(path)

        return discovered_paths
    except Exception as e:
        logger.error(f"Error discovering browse paths hierarchy: {str(e)}")
        return [parent_path]  # Return at least the parent path


def get_common_browse_paths(client, entity_type=None):
    """
    Get a list of common browse paths to use for pagination.
    This function combines static common paths with dynamically discovered paths.

    Args:
        client: DataHub REST client instance
        entity_type: Type of entity to get browse paths for (optional)

    Returns:
        list: List of browse path prefixes to query
    """
    try:
        # Start with common static paths
        common_paths = [
            "/",  # Root path
            "/prod",
            "/dev",
            "/test",
            "/staging",  # Common environment paths
            "/data",
            "/analytics",
            "/reporting",  # Common functional paths
            "/public",
            "/private",
            "/shared",  # Common access level paths
        ]

        # Add entity-type specific paths
        if entity_type == "DATASET":
            common_paths.extend(
                [
                    "/warehouse",
                    "/lake",
                    "/raw",
                    "/curated",
                    "/consumption",
                    "/finance",
                    "/marketing",
                    "/sales",
                    "/hr",
                    "/operations",
                ]
            )
        elif entity_type in ["CHART", "DASHBOARD"]:
            common_paths.extend(
                [
                    "/dashboards",
                    "/reports",
                    "/kpis",
                    "/metrics",
                    "/executive",
                    "/departmental",
                    "/operational",
                ]
            )
        elif entity_type in ["DATAFLOW", "DATAJOB"]:
            common_paths.extend(
                [
                    "/pipelines",
                    "/jobs",
                    "/workflows",
                    "/etl",
                    "/ingestion",
                    "/processing",
                    "/export",
                ]
            )
        elif entity_type == "CONTAINER":
            common_paths.extend(
                [
                    "/databases",
                    "/schemas",
                    "/projects",
                    "/datasets",
                    "/collections",
                    "/folders",
                ]
            )

        # Try to discover actual browse paths from the system
        # Start with root and a few top-level paths
        starting_paths = ["/", "/prod", "/dev"]
        discovered_paths = []

        # Only do discovery if we have a specific entity type to avoid too many API calls
        if entity_type:
            for start_path in starting_paths:
                path_results = get_browse_paths_hierarchy(
                    client, entity_type, start_path
                )
                discovered_paths.extend(path_results)

            # Remove duplicates while preserving order
            unique_discovered = []
            for path in discovered_paths:
                if path not in unique_discovered:
                    unique_discovered.append(path)

            logger.info(
                f"Discovered {len(unique_discovered)} browse paths for {entity_type}"
            )

            # Combine static and discovered paths, prioritizing discovered ones
            combined_paths = unique_discovered.copy()
            for path in common_paths:
                if path not in combined_paths:
                    combined_paths.append(path)

            return combined_paths
        else:
            return common_paths
    except Exception as e:
        logger.error(f"Error getting common browse paths: {str(e)}")
        return ["/"]  # Return at least the root path


def metadata_entities_editable_list(request):
    """List entities that have editable properties or schema metadata."""
    try:
        # Get search parameters from request
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 20))
        search_query = request.GET.get("searchQuery", "*")
        entity_type = request.GET.get("entityType", "")
        platform = request.GET.get("platform", "")
        use_platform_pagination = (
            request.GET.get("use_platform_pagination", "false").lower() == "true"
        )
        sort_by = request.GET.get("sortBy", "name")
        editable_only = request.GET.get("editable_only", "true").lower() == "true"

        # Get client
        client = get_datahub_client()

        # Call the improved get_editable_entities method with all parameters
        result = client.get_editable_entities(
            start=start,
            count=count,
            query=search_query,
            entity_type=entity_type if entity_type else None,
            platform=platform if platform else None,
            use_platform_pagination=use_platform_pagination,
            sort_by=sort_by,
            editable_only=editable_only,
        )

        if not result.get("success", False):
            logger.error(
                f"Error getting editable entities: {result.get('error', 'Unknown error')}"
            )
            return JsonResponse(
                {
                    "success": False,
                    "error": f"Failed to retrieve entities: {result.get('error', 'Unknown error')}",
                }
            )

        # Return the data from the improved client method
        return JsonResponse({"success": True, "data": result.get("data")})

    except Exception as e:
        logger.exception(f"Error in metadata_entities_editable_list: {str(e)}")
        return JsonResponse(
            {"success": False, "error": f"Failed to retrieve entities: {str(e)}"}
        )


def get_client_from_session(request):
    """
    Get a DataHub client from the session or create a new one.

    Args:
        request (HttpRequest): The request object

    Returns:
        DataHubRestClient: The client instance or None if not connected
    """
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            logger.error("No active environment configured")
            return None

        # Initialize and return a client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        return client
    except Exception as e:
        logger.error(f"Error creating DataHub client: {str(e)}")
        return None


@login_required
@require_http_methods(["POST"])
def clear_editable_entities_cache(request):
    """
    Clear the cache for editable entities to force a fresh search.

    Returns:
        JsonResponse: Success/error message
    """
    try:
        # Clear all cache keys that start with 'editable_entities_'

        # Django's cache doesn't have a pattern-based delete, so we'll use a simple approach
        # and just clear all cache if we can't find a better way
        if hasattr(cache, "delete_pattern"):
            cache.delete_pattern("editable_entities_*")
        else:
            # Fallback: clear the entire cache (not ideal but works)
            cache.clear()

        logger.info("Cleared editable entities cache")
        return JsonResponse({"success": True, "message": "Cache cleared successfully"})
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        return JsonResponse({"error": str(e)}, status=500)
