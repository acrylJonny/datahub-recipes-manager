from django.shortcuts import render
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, FileResponse
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
from utils.datahub_utils import get_datahub_client, test_datahub_connection, get_datahub_client_from_request
from utils.datahub_rest_client import DataHubRestClient
from .models import Tag, GlossaryNode, GlossaryTerm, Domain, Assertion, Environment, StructuredProperty, SearchResultCache, SearchProgress

# Import MutationStore at module level - this should work properly now
try:
    from services.mutation_store import MutationStore
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.error(f"Could not import MutationStore: {e}")
    MutationStore = None

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


def editable_properties_view(request):
    """View for managing editable properties of all entities."""
    # Ensure there's a default environment
    environment = ensure_default_environment()

    # Check if there's a default environment and log its details
    if environment:
        logger.info(f"Default environment: {environment.name}")
        
        # Note: Environment objects don't have datahub_url/datahub_token fields.
        # Using the default connection configuration instead.
        try:
            from utils.datahub_rest_client import DataHubRestClient
            from web_ui.models import Connection

            default_connection = Connection.get_default()
            if not default_connection:
                logger.warning("No default DataHub connection configured!")
                return render(
                    request,
                    "metadata_manager/entities/editable_properties.html",
                    {"page_title": "Editable Properties"},
                )

            client = DataHubRestClient(
                default_connection.datahub_url, default_connection.datahub_token
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
                # Reduce verbose logging - only log unique errors
                if "Error parsing URN" not in str(e):
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


@require_http_methods(["GET"])
def get_editable_entities(request):
    """
    Get entities with editable properties and return them as JSON.
    Results are cached in database with session support and real-time progress tracking.

    Args:
        request: The HTTP request

    Returns:
        JsonResponse: The entities in JSON format
    """
    try:
        # Get query parameters
        query = request.GET.get("query") or request.GET.get("searchQuery", "*")
        start = int(request.GET.get("start", 0))
        count = int(request.GET.get("count", 20))
        entity_type = request.GET.get("entity_type") or request.GET.get("entityType")
        platform = request.GET.get("platform")
        sort_by = request.GET.get("sort_by") or request.GET.get("sortBy", "name")
        editable_only = (
            request.GET.get("editable_only", "false").lower() == "true"
        )
        use_platform_pagination = (
            request.GET.get("use_platform_pagination", "false").lower() == "true"
        )
        refresh_cache = (
            request.GET.get("refresh_cache", "false").lower() == "true"
        )
        
        # Parse orFilters if provided
        or_filters = None
        or_filters_param = request.GET.get("orFilters")
        if or_filters_param:
            try:
                import json
                or_filters = json.loads(or_filters_param)
                logger.info(f"Received orFilters with {len(or_filters)} conditions")
            except Exception as e:
                logger.error(f"Error parsing orFilters: {str(e)}")
                or_filters = None

        logger.info(
            f"Search parameters: query='{query}', entity_type='{entity_type}', platform='{platform}', editable_only={editable_only}, use_platform_pagination={use_platform_pagination}, refresh_cache={refresh_cache}"
        )

        # Get session key
        session_key = request.session.session_key
        if not session_key:
            request.session.create()
            session_key = request.session.session_key

        # Create cache key based on all parameters
        or_filters_str = "no_filters"
        if or_filters:
            # Create a deterministic string representation of the filters
            try:
                import json
                or_filters_str = json.dumps(or_filters, sort_keys=True)[:100]  # Limit length for the cache key
            except:
                or_filters_str = f"filters_{len(or_filters)}"
                
        cache_params = f"{query}_{entity_type}_{platform}_{sort_by}_{editable_only}_{use_platform_pagination}_{or_filters_str}"
        cache_key = hashlib.md5(cache_params.encode()).hexdigest()

        # Import the new models
        from .models import SearchResultCache, SearchProgress
        from django.utils import timezone
        from datetime import timedelta

        # Check cache age (1 hour default)
        cache_expiry_hours = 1
        cache_cutoff = timezone.now() - timedelta(hours=cache_expiry_hours)
        
        # Clear cache if refresh requested or if cache is older than 1 hour
        if refresh_cache:
            logger.info(f"Refresh cache requested - clearing cache for session {session_key}")
            SearchResultCache.clear_cache(session_key, cache_key)
            SearchProgress.objects.filter(session_key=session_key, cache_key=cache_key).delete()
        else:
            # Check if cache is expired
            oldest_cache_entry = SearchResultCache.objects.filter(
                session_key=session_key,
                cache_key=cache_key
            ).order_by('created_at').first()
            
            if oldest_cache_entry and oldest_cache_entry.created_at < cache_cutoff:
                logger.info(f"Cache expired (older than {cache_expiry_hours} hour) - clearing cache for session {session_key}")
                SearchResultCache.clear_cache(session_key, cache_key)
                SearchProgress.objects.filter(session_key=session_key, cache_key=cache_key).delete()

        # Check if we have cached results
        total_cached = SearchResultCache.get_total_count(session_key, cache_key)
        
        if total_cached > 0:
            logger.info(f"Found {total_cached} cached results for session {session_key}")
            
            # Get paginated results from cache
            cached_results = SearchResultCache.get_cached_results(session_key, cache_key, start, count)
            
            # Structure the response
            response_data = {
                "start": start,
                "count": len(cached_results),
                "total": total_cached,
                "filtered_total": total_cached,
                "searchResults": [{"entity": result} for result in cached_results],
                "cache_complete": True,  # Indicate this is from complete cache
            }
            
            return JsonResponse({"success": True, "data": response_data})

        # Check if search is in progress
        progress = SearchProgress.get_progress(session_key, cache_key)
        if progress and not progress.is_complete:
            # Return progress status
            return JsonResponse({
                "success": True,
                "in_progress": True,
                "cache_key": cache_key,
                "progress": {
                    "current_step": progress.current_step,
                    "current_entity_type": progress.current_entity_type,
                    "current_platform": progress.current_platform,
                    "completed_combinations": progress.completed_combinations,
                    "total_combinations": progress.total_combinations,
                    "total_results_found": progress.total_results_found,
                    "percentage": (progress.completed_combinations / max(progress.total_combinations, 1)) * 100
                }
            })

        # Start new search - create progress record FIRST to avoid race condition
        import threading
        
        # Initialize progress record immediately
        SearchProgress.update_progress(
            session_key=session_key,
            cache_key=cache_key,
            current_step="Initializing search...",
            total_combinations=0,
            completed_combinations=0,
            total_results_found=0,
            is_complete=False
        )
        
        # Start background search
        search_thread = threading.Thread(
            target=_perform_background_search,
            args=(session_key, cache_key, query, entity_type, platform, sort_by, use_platform_pagination, request, or_filters)
        )
        search_thread.daemon = True
        search_thread.start()
        
        # Return initial progress status with cache_key
        return JsonResponse({
            "success": True,
            "in_progress": True,
            "cache_key": cache_key,
            "progress": {
                "current_step": "Initializing search...",
                "current_entity_type": "",
                "current_platform": "",
                "completed_combinations": 0,
                "total_combinations": 0,
                "total_results_found": 0,
                "percentage": 0
            }
        })

    except Exception as e:
        logger.error(f"Error in get_editable_entities: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


def _perform_background_search(session_key, cache_key, query, entity_type, platform, sort_by, use_platform_pagination, request, or_filters=None):
    """Perform the actual search in background with progress updates"""
    from .models import SearchResultCache, SearchProgress
    
    try:
        # Get client
        client = get_client_from_session(request)
        if not client:
            SearchProgress.update_progress(
                session_key=session_key,
                cache_key=cache_key,
                current_step="Error: Not connected to DataHub",
                is_complete=True,
                error_message="Not connected to DataHub"
            )
            return

        # Clear any existing cache for this search
        SearchResultCache.clear_cache(session_key, cache_key)
        
        # If we have orFilters, use them for more efficient search
        if or_filters and len(or_filters) > 0:
            logger.info(f"Using advanced search with {len(or_filters)} orFilters")
            _perform_direct_search_with_progress(
                client, query, entity_type, platform, sort_by, session_key, cache_key, or_filters
            )
        # Use comprehensive search if platform pagination is enabled (default behavior)
        elif use_platform_pagination or (not entity_type and not platform):
            logger.info("Using comprehensive search across all platforms and entity types")
            _perform_comprehensive_search_with_progress(
                client, query, entity_type, platform, sort_by, session_key, cache_key
            )
        else:
            logger.info(f"Using direct search for entity_type={entity_type}, platform={platform}")
            _perform_direct_search_with_progress(
                client, query, entity_type, platform, sort_by, session_key, cache_key
            )
            
        # Mark as complete
        progress = SearchProgress.get_progress(session_key, cache_key)
        if progress:
            total_cached = SearchResultCache.get_total_count(session_key, cache_key)
            SearchProgress.update_progress(
                session_key=session_key,
                cache_key=cache_key,
                current_step=f"Search completed - cached {total_cached} results",
                is_complete=True,
                total_results_found=total_cached
            )
            
    except Exception as e:
        logger.error(f"Error in background search: {str(e)}")
        SearchProgress.update_progress(
            session_key=session_key,
            cache_key=cache_key,
            current_step=f"Error: {str(e)}",
            is_complete=True,
            error_message=str(e)
        )


def _perform_comprehensive_search_with_progress(client, query, entity_type, platform, sort_by, session_key, cache_key):
    """Perform comprehensive search with real-time progress updates"""
    from .models import SearchResultCache, SearchProgress
    
    # Define all supported entity types
    all_entity_types = [
        "DATASET", "CONTAINER", "DASHBOARD", "CHART", "DATA_FLOW", "DATA_JOB",
        "MLFEATURE_TABLE", "MLFEATURE", "MLMODEL", "MLMODEL_GROUP", "MLPRIMARY_KEY",
    ]
    
    # Determine entity types and platforms to search
    if entity_type or platform:
        entity_types_to_query = [entity_type] if entity_type else all_entity_types
        platforms_to_query = [platform] if platform else []
    else:
        entity_types_to_query = all_entity_types
        platforms_to_query = []

    # Update progress with step 1
    SearchProgress.update_progress(
        session_key=session_key,
        cache_key=cache_key,
        current_step="Discovering platforms...",
        total_combinations=len(entity_types_to_query),
        completed_combinations=0
    )

    # Discover platforms if not specified
    if not platforms_to_query:
        platforms_to_query = _discover_platforms(client, query, sort_by)
        logger.info(f"Discovered {len(platforms_to_query)} platforms to search")

    # Calculate total combinations - only count platform searches, not the no-platform search
    total_combinations = len(entity_types_to_query) * len(platforms_to_query)
    if total_combinations == 0:
        # If no platforms were discovered, we need to do at least one search
        total_combinations = len(entity_types_to_query)
    
    SearchProgress.update_progress(
        session_key=session_key,
        cache_key=cache_key,
        current_step="Starting comprehensive search...",
        total_combinations=total_combinations,
        completed_combinations=0
    )

    seen_urns = set()
    current_combination = 0
    
    # Search by entity type + platform combinations
    for current_entity_type in entity_types_to_query:
        # Skip the search without platform filter and go directly to platform-specific searches
        if platforms_to_query:
            # Search with each platform
            for platform_name in platforms_to_query:
                current_combination += 1
                SearchProgress.update_progress(
                    session_key=session_key,
                    cache_key=cache_key,
                    current_step=f"Searching {current_entity_type} + {platform_name}",
                    current_entity_type=current_entity_type,
                    current_platform=platform_name,
                    completed_combinations=current_combination
                )
                
                results = _search_with_pagination_and_cache(
                    client, query, current_entity_type, platform_name, sort_by, seen_urns, session_key, cache_key
                )
        else:
            # If no platforms were discovered, we still need to do one search without platform filter
            current_combination += 1
            SearchProgress.update_progress(
                session_key=session_key,
                cache_key=cache_key,
                current_step=f"Searching {current_entity_type} (all platforms)",
                current_entity_type=current_entity_type,
                current_platform="",
                completed_combinations=current_combination
            )
            
            results = _search_with_pagination_and_cache(
                client, query, current_entity_type, None, sort_by, seen_urns, session_key, cache_key
            )

    # Update final progress
    total_results = SearchResultCache.get_total_count(session_key, cache_key)
    SearchProgress.update_progress(
        session_key=session_key,
        cache_key=cache_key,
        current_step=f"Comprehensive search completed - cached {total_results} results for fast pagination",
        completed_combinations=total_combinations,
        total_results_found=total_results,
        is_complete=True
    )


def _search_with_pagination_and_cache(client, query, entity_type, platform, sort_by, seen_urns, session_key, cache_key):
    """Search with pagination and store results in database cache"""
    from .models import SearchResultCache, SearchProgress
    from utils.datahub_rest_client import DataHubRestClient
    
    all_results = []
    start = 0
    batch_size = 1000
    
    while True:
        try:
            logger.info(f"Getting editable entities - start: {start}, count: {batch_size}, query: {query}, entity_type: {entity_type}, platform: {platform}")
            result = client.get_editable_entities(
                start=start,
                count=batch_size,
                query=query,
                entity_type=entity_type,
                platform=platform,
                sort_by=sort_by,
                editable_only=False,
            )
            
            # Handle client response format
            if result and result.get("success") and "data" in result:
                result_data = result["data"]
            elif result and "searchResults" in result:
                result_data = result
            else:
                break
                
            if not result_data or "searchResults" not in result_data:
                break
                
            search_results = result_data.get("searchResults", [])
            if not search_results:
                break
                
            # Process and cache results
            new_results = []
            for search_result in search_results:
                entity = search_result.get("entity")
                if entity and "urn" in entity:
                    entity_urn = entity["urn"]
                    if entity_urn not in seen_urns:
                        # Filter for entities with metadata
                        counts = client._count_entity_metadata(entity)
                        total_metadata_count = sum(counts.values())
                        
                        if total_metadata_count > 0:
                            seen_urns.add(entity_urn)
                            new_results.append(search_result)
                            
                            # Store in database cache
                            try:
                                SearchResultCache.objects.get_or_create(
                                    session_key=session_key,
                                    cache_key=cache_key,
                                    entity_urn=entity_urn,
                                    defaults={
                                        'entity_data': entity,
                                        'search_params': {
                                            'query': query,
                                            'entity_type': entity_type,
                                            'platform': platform,
                                            'sort_by': sort_by
                                        }
                                    }
                                )
                            except Exception as e:
                                logger.error(f"Error caching result for {entity_urn}: {str(e)}")
                        
            all_results.extend(new_results)
            
            # Update progress with current results count
            total_cached = SearchResultCache.get_total_count(session_key, cache_key)
            progress = SearchProgress.get_progress(session_key, cache_key)
            if progress:
                SearchProgress.update_progress(
                    session_key=session_key,
                    cache_key=cache_key,
                    total_results_found=total_cached
                )
            
            # Check if we got fewer results than requested (end of results)
            if len(search_results) < batch_size:
                break
                
            start += batch_size
            
        except Exception as e:
            logger.error(f"Error in pagination for {entity_type}+{platform}: {str(e)}")
            break
            
    logger.info(f"Found {len(all_results)} results for {entity_type}+{platform}")
    return all_results


def _perform_direct_search_with_progress(client, query, entity_type, platform, sort_by, session_key, cache_key, or_filters=None):
    """Perform direct search with progress updates"""
    from .models import SearchResultCache, SearchProgress
    
    # Update progress message based on whether we're using advanced filters
    if or_filters and len(or_filters) > 0:
        step_message = f"Searching with advanced filters ({len(or_filters)} conditions)"
    else:
        step_message = f"Searching {entity_type or 'all types'} + {platform or 'all platforms'}"
    
    SearchProgress.update_progress(
        session_key=session_key,
        cache_key=cache_key,
        current_step=step_message,
        current_entity_type=entity_type or "",
        current_platform=platform or "",
        total_combinations=1,
        completed_combinations=0
    )
    
    seen_urns = set()
    
    # If we have orFilters, use them for more efficient search
    if or_filters and len(or_filters) > 0:
        logger.info(f"Using advanced search with {len(or_filters)} orFilters")
        
        try:
            # Direct search with orFilters
            start = 0
            batch_size = 1000  # Use larger batches for efficiency
            all_results = []
            
            while True:
                try:
                    # Use the client with orFilters
                    logger.info(f"Getting editable entities with orFilters - start: {start}, count: {batch_size}, query: {query}, entity_type: {entity_type}, platform: {platform}, orFilters: {or_filters}")
                    result = client.get_editable_entities(
                        start=start,
                        count=batch_size,
                        query=query,
                        entity_type=entity_type,
                        platform=platform,
                        sort_by=sort_by,
                        editable_only=True,
                        orFilters=or_filters
                    )
                    
                    # Handle client response format
                    if result and result.get("success") and "data" in result:
                        result_data = result["data"]
                    elif result and "searchResults" in result:
                        result_data = result
                    else:
                        break
                        
                    if not result_data or "searchResults" not in result_data:
                        break
                        
                    search_results = result_data.get("searchResults", [])
                    if not search_results:
                        break
                        
                    # Cache results directly
                    for search_result in search_results:
                        entity = search_result.get("entity")
                        if entity and "urn" in entity:
                            entity_urn = entity["urn"]
                            if entity_urn not in seen_urns:
                                seen_urns.add(entity_urn)
                                
                                try:
                                    SearchResultCache.objects.get_or_create(
                                        session_key=session_key,
                                        cache_key=cache_key,
                                        entity_urn=entity_urn,
                                        defaults={
                                            'entity_data': entity,
                                            'search_params': {
                                                'query': query,
                                                'entity_type': entity_type,
                                                'platform': platform,
                                                'sort_by': sort_by,
                                                'orFilters': True
                                            }
                                        }
                                    )
                                except Exception as e:
                                    logger.error(f"Error caching result for {entity_urn}: {str(e)}")
                    
                    # Update progress with current results count
                    total_cached = SearchResultCache.get_total_count(session_key, cache_key)
                    SearchProgress.update_progress(
                        session_key=session_key,
                        cache_key=cache_key,
                        total_results_found=total_cached
                    )
                    
                    # Check if we got fewer results than requested (end of results)
                    if len(search_results) < batch_size:
                        break
                        
                    start += batch_size
                    
                except Exception as e:
                    logger.error(f"Error in advanced search pagination: {str(e)}")
                    break
                    
            logger.info(f"Found {len(seen_urns)} results with advanced filters")
            
        except Exception as e:
            logger.error(f"Error in advanced search: {str(e)}")
            # Fall back to standard search
            results = _search_with_pagination_and_cache(
                client, query, entity_type, platform, sort_by, seen_urns, session_key, cache_key
            )
    else:
        # Standard search
        results = _search_with_pagination_and_cache(
            client, query, entity_type, platform, sort_by, seen_urns, session_key, cache_key
        )
    
    # Update final progress
    total_results = SearchResultCache.get_total_count(session_key, cache_key)
    SearchProgress.update_progress(
        session_key=session_key,
        cache_key=cache_key,
        current_step="Direct search completed",
        completed_combinations=1,
        total_results_found=total_results
    )


def _discover_platforms(client, query, sort_by):
    """Discover all platforms available in DataHub"""
    platforms_to_search = set()
    
    try:
        # Quick discovery search
        logger.info(f"Platform discovery - querying for platforms with query: '*', count: 1000")
        platform_discovery_result = client.get_editable_entities(
            start=0,
            count=1000,  # Get more for better platform discovery
            query="*",
            entity_type=None,
            platform=None,
            sort_by=sort_by,
            editable_only=False,
        )
        
        # Handle client response format
        if platform_discovery_result and platform_discovery_result.get("success") and "data" in platform_discovery_result:
            discovery_data = platform_discovery_result["data"]
        elif platform_discovery_result and "searchResults" in platform_discovery_result:
            discovery_data = platform_discovery_result
        else:
            discovery_data = None
            
        if discovery_data and "searchResults" in discovery_data:
            for search_result in discovery_data.get("searchResults", []):
                entity = search_result.get("entity", {})
                
                # Extract platform from various locations
                if entity.get("platform") and entity["platform"].get("name"):
                    platforms_to_search.add(entity["platform"]["name"])
                elif entity.get("dataPlatformInstance") and entity["dataPlatformInstance"].get("platform"):
                    platforms_to_search.add(entity["dataPlatformInstance"]["platform"].get("name"))
                
                # Also check for platform in URN
                if entity.get("urn") and "urn:li:dataPlatform:" in entity["urn"]:
                    try:
                        urn_platform = entity["urn"].split("urn:li:dataPlatform:")[1].split(",")[0]
                        if urn_platform:
                            platforms_to_search.add(urn_platform)
                    except:
                        pass
    except Exception as e:
        logger.warning(f"Could not discover platforms from DataHub: {str(e)}")
    
    # Add comprehensive list of common platforms
    common_platforms = [
        "postgres", "mysql", "snowflake", "bigquery", "redshift", 
        "databricks", "azure", "glue", "hive", "kafka", "oracle", 
        "mssql", "teradata", "tableau", "looker", "metabase", 
        "superset", "powerbi", "airflow", "spark", "delta", "unity-catalog"
    ]
    platforms_to_search.update(common_platforms)
    
    return sorted(list(platforms_to_search))


def _search_with_pagination(client, query, entity_type, platform, sort_by, seen_urns):
    """Search with automatic pagination until all results are retrieved"""
    all_results = []
    start = 0
    batch_size = 1000  # Use larger batches
    
    while True:
        try:
            logger.info(f"Getting editable entities (pagination) - start: {start}, count: {batch_size}, query: {query}, entity_type: {entity_type}, platform: {platform}")
            result = client.get_editable_entities(
                start=start,
                count=batch_size,
                query=query,
                entity_type=entity_type,
                platform=platform,
                sort_by=sort_by,
                editable_only=False,
            )
            
            # Handle client response format
            if result and result.get("success") and "data" in result:
                result_data = result["data"]
            elif result and "searchResults" in result:
                result_data = result
            else:
                break
                
            if not result_data or "searchResults" not in result_data:
                break
                
            search_results = result_data.get("searchResults", [])
            if not search_results:
                break
                
            # Add unique results
            new_results = []
            for search_result in search_results:
                entity = search_result.get("entity")
                if entity and "urn" in entity:
                    entity_urn = entity["urn"]
                    if entity_urn not in seen_urns:
                        seen_urns.add(entity_urn)
                        new_results.append(search_result)
                        
            all_results.extend(new_results)
            
            # Check if we got fewer results than requested (end of results)
            if len(search_results) < batch_size:
                break
                
            # Check if we hit the 10,000 limit - if so, we need more granular filtering
            total_available = result_data.get("total", 0)
            if total_available >= 10000:
                logger.warning(f"Hit 10,000 result limit for {entity_type}+{platform}, may need browse path filtering")
                
            start += batch_size
            
        except Exception as e:
            logger.error(f"Error in pagination for {entity_type}+{platform}: {str(e)}")
            break
            
    logger.info(f"Found {len(all_results)} results for {entity_type}+{platform}")
    return all_results


def _search_containers_with_browse_paths(client, query, platforms, sort_by, seen_urns):
    """Special handling for containers using browse path filtering"""
    container_results = []
    
    # First get all containers to identify browse paths
    for platform in platforms:
        try:
            # Get containers for this platform
            result = client.get_editable_entities(
                start=0,
                count=1000,
                query=query,
                entity_type="CONTAINER",
                platform=platform,
                sort_by=sort_by,
                editable_only=False,
            )
            
            # Handle response format
            if result and result.get("success") and "data" in result:
                result_data = result["data"]
            elif result and "searchResults" in result:
                result_data = result
            else:
                continue
                
            if not result_data or "searchResults" not in result_data:
                continue
                
            # Check if we hit the limit
            total_available = result_data.get("total", 0)
            if total_available >= 10000:
                logger.info(f"Container search for {platform} hit 10,000 limit, using browse path filtering")
                
                # Get container URNs from the first batch to use as browse path filters
                container_urns = []
                for search_result in result_data.get("searchResults", []):
                    entity = search_result.get("entity")
                    if entity and "urn" in entity and entity["type"] == "CONTAINER":
                        container_urns.append(entity["urn"])
                
                # Now search for entities under each container using browsePathV2
                for container_urn in container_urns[:50]:  # Limit to first 50 containers to avoid too many requests
                    try:
                        # Search for entities with this container in their browse path
                        browse_path_result = _search_with_browse_path_filter(
                            client, query, None, platform, container_urn, sort_by, seen_urns
                        )
                        container_results.extend(browse_path_result)
                        
                    except Exception as e:
                        logger.error(f"Error searching with browse path filter for container {container_urn}: {str(e)}")
                        continue
                        
            # Add unique results from the initial container search
            for search_result in result_data.get("searchResults", []):
                entity = search_result.get("entity")
                if entity and "urn" in entity:
                    entity_urn = entity["urn"]
                    if entity_urn not in seen_urns:
                        seen_urns.add(entity_urn)
                        container_results.append(search_result)
                        
        except Exception as e:
            logger.error(f"Error searching containers for platform {platform}: {str(e)}")
            continue
            
    return container_results


def _search_with_browse_path_filter(client, query, entity_type, platform, container_urn, sort_by, seen_urns):
    """Search for entities with a specific container in their browse path"""
    results = []
    
    # Use the client's browse path filtering capability
    # This would require implementing browsePathV2 filtering in the GraphQL query
    try:
        # For now, we'll use a simpler approach and search for entities that might be under this container
        # In a full implementation, we'd modify the GraphQL query to include browsePathV2 filters
        
        # Search for entities that might be related to this container
        result = client.get_editable_entities(
            start=0,
            count=500,
            query=f"{query} AND container:{container_urn.split(':')[-1]}",  # Simple container name search
            entity_type=entity_type,
            platform=platform,
            sort_by=sort_by,
            editable_only=False,
        )
        
        # Handle response format
        if result and result.get("success") and "data" in result:
            result_data = result["data"]
        elif result and "searchResults" in result:
            result_data = result
        else:
            return results
            
        if result_data and "searchResults" in result_data:
            for search_result in result_data.get("searchResults", []):
                entity = search_result.get("entity")
                if entity and "urn" in entity:
                    entity_urn = entity["urn"]
                    if entity_urn not in seen_urns:
                        # Check if this entity actually has the container in its browse path
                        if _entity_has_container_in_browse_path(entity, container_urn):
                            seen_urns.add(entity_urn)
                            results.append(search_result)
                            
    except Exception as e:
        logger.error(f"Error in browse path filtering: {str(e)}")
        
    return results


def _entity_has_container_in_browse_path(entity, container_urn):
    """Check if an entity has a specific container in its browse path"""
    try:
        # Check browsePathV2 structure
        if entity.get("browsePathV2") and entity["browsePathV2"].get("path"):
            for path_element in entity["browsePathV2"]["path"]:
                if (path_element.get("entity") and 
                    path_element["entity"].get("container") and 
                    path_element["entity"]["container"].get("urn") == container_urn):
                    return True
                    
        # Check traditional browsePaths
        if entity.get("browsePaths"):
            for browse_path in entity["browsePaths"]:
                if browse_path.get("path") and container_urn.split(":")[-1] in browse_path["path"]:
                    return True
                    
        return False
        
    except Exception as e:
        logger.debug(f"Error checking browse path for entity: {str(e)}")
        return False


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
        default_connection = Connection.get_default()
        if not default_connection:
            logger.warning("No default DataHub connection configured for client creation!")
            return JsonResponse({'error': 'No DataHub connection configured'}, status=500)
        
        client = DataHubRestClient(default_connection.datahub_url, default_connection.datahub_token)
        
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


@require_http_methods(["GET"])
def get_entity_details(request, urn):
    """Get details of a specific entity."""
    try:
        # Get DataHub client using connection system
        client = get_client_from_session(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "No active connection configured"}
            )
        
        # Get entity details
        entity = client.get_entity(urn)
        
        if not entity:
            return JsonResponse({"success": False, "error": "Entity not found"})
        
        return JsonResponse({"success": True, "entity": entity})
        
    except Exception as e:
        logger.error(f"Error getting entity details: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["GET"])
def get_entity_schema(request, urn):
    """Get schema details for a dataset entity."""
    try:
        # Get DataHub client using connection system
        client = get_client_from_session(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "No active connection configured"}
            )
        
        # Get schema details
        schema = client.get_schema(urn)
        
        if not schema:
            return JsonResponse({"success": False, "error": "Schema not found"})
        
        return JsonResponse({"success": True, "schema": schema})
        
    except Exception as e:
        logger.error(f"Error getting schema details: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["POST"])
def sync_metadata(request):
    """Sync metadata with DataHub."""
    try:
        # Get DataHub client using connection system
        client = get_client_from_session(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "No active connection configured"}
            )
        
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
                logger.info(f"Executing GraphQL query for browse paths hierarchy - parent_path: {parent_path}, entity_type: {entity_type}")
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
        client = get_datahub_client_from_request(request)

        # Call the improved get_editable_entities method with all parameters
        logger.info(f"Metadata entities editable list - start: {start}, count: {count}, query: {search_query}, entity_type: {entity_type}, platform: {platform}, use_platform_pagination: {use_platform_pagination}, sort_by: {sort_by}, editable_only: {editable_only}")
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
    Get a DataHub client from the session or create a new one using the connection system.

    Args:
        request (HttpRequest): The request object

    Returns:
        DataHubRestClient: The client instance or None if not connected
    """
    try:
        from utils.datahub_utils import get_datahub_client_from_request
        return get_datahub_client_from_request(request)
    except Exception as e:
        logger.error(f"Error creating DataHub client: {str(e)}")
        return None


@require_http_methods(["GET"])
def get_platforms(request):
    """Get list of platforms from DataHub."""
    try:
        # Get connection-specific client
        client = get_datahub_client_from_request(request)
        if not client:
            return JsonResponse(
                {"success": False, "error": "Not connected to DataHub"}, status=400
            )
        
        entity_type = request.GET.get("entity_type")
        
        # Fast platform discovery using DataHub's aggregation API
        platforms = set()
        
        try:
            logger.info(f"Starting fast platform discovery using aggregation API for entity_type='{entity_type}'")
            
            # Use DataHub's aggregateAcrossEntities GraphQL query for efficient platform discovery
            result = client.aggregate_across_entities(
                facets=["platform"],
                query="*",
                entity_types=[entity_type] if entity_type else None,
                max_agg_values=1000  # Get up to 1000 platforms - increased from 100
            )
            
            if result and result.get("success") and "data" in result:
                facets = result["data"].get("facets", [])
                
                for facet in facets:
                    if facet.get("field") == "platform":
                        aggregations = facet.get("aggregations", [])
                        logger.info(f"Found {len(aggregations)} platform aggregations from GraphQL API")
                        
                        for agg in aggregations:
                            entity = agg.get("entity", {})
                            if entity.get("type") == "DATA_PLATFORM":
                                platform_name = entity.get("name")
                                if platform_name:
                                    platforms.add(platform_name)
                                    logger.debug(f"Added platform: {platform_name}")
                            else:
                                logger.debug(f"Skipped non-DATA_PLATFORM entity: {entity.get('type', 'unknown')}")
                
                logger.info(f"Fast platform discovery completed: found {len(platforms)} platforms using aggregation API")
                if len(platforms) >= 1000:
                    logger.warning("Platform discovery may have hit the 1000 platform limit - some platforms might be missing")
                
            else:
                logger.warning("Aggregation API failed or returned no data, falling back to entity search")
                # Fallback to a single search if aggregation fails
                logger.info(f"Platform aggregation fallback - querying for platforms with entity_type: {entity_type}, count: 10000")
                result = client.get_editable_entities(
                    start=0,
                    count=10000,  # Much larger sample for comprehensive platform discovery
                    query="*",
                    entity_type=entity_type,
                    platform=None,
                    sort_by="name",
                    editable_only=False,
                )
                
                # Handle client response format
                if result and result.get("success") and "data" in result:
                    search_results = result["data"].get("searchResults", [])
                elif result and "searchResults" in result:
                    search_results = result.get("searchResults", [])
                else:
                    search_results = []
                    
                logger.info(f"Fallback entity search returned {len(search_results)} entities for platform discovery")
                    
                for search_result in search_results:
                    entity = search_result.get("entity", {})
                    
                    # Extract platform from all possible locations
                    platform_names = []
                    
                    # Method 1: Direct platform reference
                    if entity.get("platform") and entity["platform"].get("name"):
                        platform_names.append(entity["platform"]["name"])
                    
                    # Method 2: Platform instance reference  
                    if entity.get("dataPlatformInstance") and entity["dataPlatformInstance"].get("platform"):
                        platform_names.append(entity["dataPlatformInstance"]["platform"].get("name"))
                    
                    # Method 3: DataFlow platform (for DataJobs)
                    if entity.get("dataFlow") and entity["dataFlow"].get("platform"):
                        platform_names.append(entity["dataFlow"]["platform"].get("name"))
                    
                    # Add all found platform names
                    for platform_name in platform_names:
                        if platform_name:
                            platforms.add(platform_name)
                            
                    # Method 4: Extract from URN (multiple URN formats)
                    if entity.get("urn"):
                        urn = entity["urn"]
                        try:
                            # Format: urn:li:dataset:(urn:li:dataPlatform:platform_name,...)
                            if "urn:li:dataPlatform:" in urn:
                                urn_platform = urn.split("urn:li:dataPlatform:")[1].split(",")[0].split(")")[0]
                                if urn_platform:
                                    platforms.add(urn_platform)
                            
                            # Format: urn:li:dataFlow:(platform_name,...)
                            elif urn.startswith("urn:li:dataFlow:("):
                                urn_platform = urn.split("urn:li:dataFlow:(")[1].split(",")[0]
                                if urn_platform:
                                    platforms.add(urn_platform)
                            
                            # Format: urn:li:dataJob:(urn:li:dataFlow:(platform_name,...),...)
                            elif "urn:li:dataFlow:(" in urn:
                                urn_platform = urn.split("urn:li:dataFlow:(")[1].split(",")[0]
                                if urn_platform:
                                    platforms.add(urn_platform)
                                    
                        except Exception as e:
                            logger.debug(f"Failed to extract platform from URN {urn}: {e}")
                            pass
                
                logger.info(f"Fallback platform discovery completed: found {len(platforms)} platforms from {len(search_results)} entities")
                    
        except Exception as e:
            logger.error(f"Error in platform discovery: {str(e)}")
        
        logger.info(f"Platform discovery final result: found {len(platforms)} unique platforms")
        
        # Convert to sorted list
        platform_list = sorted(list(platforms))
        
        return JsonResponse({
            "success": True, 
            "platforms": platform_list,
            "entity_type": entity_type
        })
        
    except Exception as e:
        logger.error(f"Error getting platforms: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


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


@require_http_methods(["GET"])
def get_search_progress(request):
    """Get the current search progress for the user's session"""
    try:
        from .models import SearchProgress
        
        # Get session key
        session_key = request.session.session_key
        if not session_key:
            return JsonResponse({"success": False, "error": "No active session"})

        # Get cache key from request
        cache_key = request.GET.get("cache_key")
        if not cache_key:
            return JsonResponse({"success": False, "error": "No cache key provided"})

        # Get progress
        progress = SearchProgress.get_progress(session_key, cache_key)
        if not progress:
            return JsonResponse({"success": False, "error": "No search in progress"})

        # Return progress data
        return JsonResponse({
            "success": True,
            "progress": {
                "current_step": progress.current_step,
                "current_entity_type": progress.current_entity_type,
                "current_platform": progress.current_platform,
                "completed_combinations": progress.completed_combinations,
                "total_combinations": progress.total_combinations,
                "total_results_found": progress.total_results_found,
                "is_complete": progress.is_complete,
                "error_message": progress.error_message,
                "percentage": (progress.completed_combinations / max(progress.total_combinations, 1)) * 100 if progress.total_combinations > 0 else 0
            }
        })

    except Exception as e:
        logger.error(f"Error getting search progress: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


@require_http_methods(["GET"])
def get_datahub_url_config(request):
    """Get DataHub URL configuration for frontend use"""
    try:
        # Use the default connection instead of environment
        from web_ui.models import Connection
        
        default_connection = Connection.get_default()
        if not default_connection:
            return JsonResponse({"success": False, "error": "No default DataHub connection configured"})
        
        return JsonResponse({
            "success": True,
            "url": default_connection.datahub_url
        })
    except Exception as e:
        logger.error(f"Error getting DataHub URL config: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["GET"])
def get_structured_properties(request):
    """
    Get all structured properties from DataHub.
    This endpoint is used to build filters for the editable entities search.
    Results are cached for 2 minutes per connection.
    """
    try:
        # Get current connection for connection-specific caching
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        connection_id = current_connection.id if current_connection else 'default'
        
        # Create connection-specific cache key
        cache_key = f"structured_properties_cache_{connection_id}"
        
        # Check if we should bypass cache (when connection_id or timestamp params are present)
        force_refresh = request.GET.get('connection_id') or request.GET.get('t')
        
        if not force_refresh:
            cached_data = cache.get(cache_key)
            if cached_data:
                logger.info(f"Returning cached structured properties for connection: {connection_id}")
                return JsonResponse(cached_data)
        
        # Get client from session (connection-specific)
        client = get_client_from_session(request)
        if not client:
            return JsonResponse({"success": False, "error": "Could not connect to DataHub"})
        
        logger.info(f"Fetching fresh structured properties from DataHub for connection: {connection_id}")
        
        # Fetch structured properties from DataHub
        result = client.get_structured_properties(count=1000)
        
        if not result or not result.get("success", False):
            error_msg = result.get("error", "Unknown error fetching structured properties")
            logger.error(f"Error fetching structured properties: {error_msg}")
            return JsonResponse({"success": False, "error": error_msg})
        
        # Log some information about the structured properties
        structured_properties = result.get("structured_properties", [])
        total = result.get("total", 0)
        logger.info(f"Retrieved {len(structured_properties)} structured properties out of {total} total for connection: {connection_id}")
        
        if structured_properties and len(structured_properties) > 0:
            # Log a few examples
            examples = structured_properties[:3]
            logger.info(f"Example structured properties for connection {connection_id}: {examples}")
        
        # Cache the result for 2 minutes (connection-specific)
        cache.set(cache_key, result, 2 * 60)
        
        return JsonResponse(result)
    except Exception as e:
        logger.error(f"Error getting structured properties: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["GET", "POST"])
def export_entities_with_mutations(request):
    """Export entities with environment-specific mutations applied and save to environment directory."""
    try:
        import json
        import os
        from datetime import datetime
        from django.conf import settings
        from web_ui.models import Environment
        
        # Handle both GET (for tests) and POST requests
        if request.method == 'GET':
            # For GET requests (tests), return a simple success response
            return JsonResponse({
                "success": True,
                "message": "Export endpoint is available",
                "supported_methods": ["GET", "POST"]
            })
        
        # Parse request data for POST requests
        data = json.loads(request.body)
        entities = data.get('entities', [])
        include_mutations = data.get('include_mutations', True)
        export_format = data.get('export_format', 'metadata_migration')
        
        if not entities:
            return JsonResponse(
                {"success": False, "error": "No entities provided for export"}, 
                status=400
            )
        
        # Get current connection and environment (same pattern as other views)
        from web_ui.views import get_current_connection
        current_connection = get_current_connection(request)
        
        # Determine environment name
        current_environment = getattr(current_connection, 'environment', 'dev')
        
        logger.info(f"Exporting {len(entities)} entities with mutations for environment: {current_environment}")
        
        # Process entities and apply mutations if requested
        processed_entities = []
        mutation_config = None
        
        if include_mutations:
            try:
                # Import URN utilities
                from utils.urn_utils import (
                    get_mutation_config_for_environment,
                    apply_urn_mutations_to_associations
                )
                
                # Get mutation configuration for the current environment
                mutation_config = get_mutation_config_for_environment(current_environment)
                
                if mutation_config:
                    logger.info(f"Found mutation configuration for environment '{current_environment}': {mutation_config}")
                else:
                    logger.warning(f"No mutation configuration found for environment '{current_environment}'")
                    
            except ImportError as e:
                logger.warning(f"URN utilities not available - exporting without mutations: {e}")
                include_mutations = False
        
        # Process each entity
        for entity in entities:
            try:
                # Create a copy of the entity to avoid modifying the original
                processed_entity = json.loads(json.dumps(entity))
                
                # Apply mutations if requested
                if include_mutations and mutation_config:
                    try:
                        # For editable entities, we DON'T mutate the main entity URN (e.g. dataset URN)
                        # We ONLY mutate the associated entity URNs (domain, tags, glossary terms, etc.)
                        processed_entity = apply_urn_mutations_to_associations(
                            processed_entity, 
                            current_environment, 
                            mutation_config,
                            mutate_main_entity_urn=False  # Don't mutate main URN for editable entities
                        )
                        logger.debug(f"Applied association mutations to entity: {processed_entity.get('urn', 'unknown')}")
                        
                    except Exception as e:
                        logger.error(f"Error applying mutations to entity {processed_entity.get('urn', 'unknown')}: {str(e)}")
                        # Continue with the original entity if mutation fails
                        processed_entity = entity
                
                processed_entities.append(processed_entity)
                
            except Exception as e:
                logger.error(f"Error processing entity: {str(e)}")
                # Include the original entity if processing fails
                processed_entities.append(entity)
        
        # Create the export data structure
        export_data = {
            "metadata": {
                "export_timestamp": datetime.now().isoformat(),
                "environment": current_environment,
                "mutations_applied": include_mutations and mutation_config is not None,
                "entity_count": len(processed_entities),
                "export_format": export_format,
                "mutation_config": mutation_config if include_mutations else None
            },
            "entities": processed_entities
        }
        
        # Create environment directory if it doesn't exist
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        env_dir = os.path.join(project_root, 'metadata-manager', current_environment, 'editable_entities')
        os.makedirs(env_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        mutations_suffix = "_with_mutations" if include_mutations and mutation_config else "_no_mutations"
        filename = f"exported_entities{mutations_suffix}_{timestamp}.json"
        file_path = os.path.join(env_dir, filename)
        
        # Save to file
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        logger.info(f"Successfully exported {len(processed_entities)} entities to {file_path}")
        
        # Return success response with file information
        return JsonResponse({
            "success": True,
            "message": f"Successfully exported {len(processed_entities)} entities to {current_environment} environment",
            "file_path": file_path,
            "filename": filename,
            "environment": current_environment,
            "entity_count": len(processed_entities),
            "mutations_applied": include_mutations and mutation_config is not None,
            "export_format": export_format
        })
        
    except Exception as e:
        logger.error(f"Error exporting entities with mutations: {str(e)}")
        return JsonResponse(
            {"success": False, "error": f"Failed to export entities: {str(e)}"}, 
            status=500
        )


@require_http_methods(["GET"])
def get_all_platform_instances(request):
    """Get list of all platform instances from cached search results."""
    try:
        entity_type = request.GET.get("entity_type")
        platform = request.GET.get("platform")  # Optional platform filter
        
        logger.info(f"Getting platform instances from cache for entity_type='{entity_type}', platform='{platform}'")
        
        # Get session key
        session_key = request.session.session_key
        if not session_key:
            request.session.create()
            session_key = request.session.session_key
        
        # Import SearchResultCache model
        from .models import SearchResultCache
        
        # Get all cached entities for this session
        cached_results = SearchResultCache.objects.filter(session_key=session_key)
        
        logger.info(f"Found {cached_results.count()} cached entities for session {session_key}")
        
        # Extract platform instances from cached entities
        platform_instances = set()
        
        for cached_result in cached_results:
            entity = cached_result.entity_data
            
            # Filter by entity type if specified
            if entity_type and entity.get("type") != entity_type:
                continue
            
            # Extract platform instance from dataPlatformInstance field
            if entity.get("dataPlatformInstance"):
                platform_instance_data = entity["dataPlatformInstance"]
                
                # Filter by platform if specified
                if platform:
                    entity_platform = None
                    if platform_instance_data.get("platform"):
                        entity_platform = platform_instance_data["platform"].get("name")
                    elif entity.get("platform"):
                        entity_platform = entity["platform"].get("name")
                    
                    if entity_platform and entity_platform.lower() != platform.lower():
                        continue
                
                # Get instance ID from instanceId field
                instance_id = platform_instance_data.get("instanceId")
                if instance_id:
                    platform_instances.add(instance_id)
                
                # Also check properties.name as fallback
                if not instance_id and platform_instance_data.get("properties"):
                    instance_name = platform_instance_data["properties"].get("name")
                    if instance_name:
                        platform_instances.add(instance_name)
            
            # Also extract from URN as fallback for legacy format
            if entity.get("urn"):
                urn = entity["urn"]
                try:
                    # Format: urn:li:dataset:(urn:li:dataPlatform:platform, instance.database.schema.table, env)
                    if "urn:li:dataset:" in urn or "urn:li:container:" in urn:
                        parts = urn.split(",")
                        if len(parts) >= 2:
                            # The second part may contain instance.database format
                            instance_part = parts[1].strip()
                            # Extract just the instance part (before first dot)
                            if "." in instance_part:
                                instance_id = instance_part.split(".")[0].strip()
                                if instance_id and len(instance_id) > 0:
                                    platform_instances.add(instance_id)
                except Exception as e:
                    # Only log errors, not individual URN parsing issues
                    if "Error extracting platform instance from URN" not in str(e):
                        logger.debug(f"Error parsing URN {urn}: {str(e)}")
                    continue
        
        logger.info(f"Extracted {len(platform_instances)} unique platform instances from cache")
        
        # Convert to sorted list
        platform_instances_list = sorted(list(platform_instances))
        
        return JsonResponse({
            "success": True, 
            "platform_instances": platform_instances_list,
            "entity_type": entity_type,
            "platform": platform
        })
        
    except Exception as e:
        logger.error(f"Error getting platform instances from cache: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


@require_http_methods(["GET"])
def download_exported_entities(request):
    """Download the exported entities JSON file"""
    try:
        file_path = request.GET.get('file_path')
        filename = request.GET.get('filename')
        
        if not file_path or not filename:
            return JsonResponse({"success": False, "error": "File path and filename are required"}, status=400)
        
        import os
        from django.http import FileResponse
        from django.conf import settings
        
        # Ensure the file exists and is within the expected directory
        if not os.path.exists(file_path):
            return JsonResponse({"success": False, "error": "File not found"}, status=404)
        
        # Security check - ensure the file is within the metadata-manager directory
        web_ui_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        workspace_root = os.path.dirname(web_ui_dir)
        expected_dir = os.path.join(workspace_root, 'metadata-manager')
        
        if not os.path.abspath(file_path).startswith(os.path.abspath(expected_dir)):
            return JsonResponse({"success": False, "error": "Invalid file path"}, status=400)
        
        # Return the file as a download
        response = FileResponse(
            open(file_path, 'rb'),
            as_attachment=True,
            filename=filename
        )
        response['Content-Type'] = 'application/json'
        return response
        
    except Exception as e:
        logger.error(f"Error downloading exported entities: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}, status=500)


