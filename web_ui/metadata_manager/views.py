from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.conf import settings
import os
import json
import logging
import sys
import importlib

# Add project root to sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Import the deterministic URN utilities
from utils.urn_utils import (
    generate_deterministic_urn,
    get_full_urn_from_name,
    extract_name_from_properties,
    get_parent_path
)

from utils.datahub_rest_client import DataHubRestClient
from .models import Tag, GlossaryNode, GlossaryTerm, Domain, Assertion, Environment

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
                "assertions_count": Assertion.objects.count()
            }
            
            return render(request, 'metadata_manager/index.html', {
                'stats': stats,
                'page_title': 'Metadata Manager'
            })
        except Exception as e:
            logger.error(f"Error in metadata index view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(request, 'metadata_manager/index.html', {
                'error': str(e),
                'page_title': 'Metadata Manager'
            })

def ensure_default_environment():
    """Ensure there's at least one default environment in the database."""
    try:
        default_env = Environment.objects.filter(is_default=True).first()
        if not default_env:
            logger.warning("No default environment found, checking for any environment...")
            # Check if there's any environment we can make default
            any_env = Environment.objects.first()
            if any_env:
                logger.info(f"Making environment '{any_env.name}' the default")
                any_env.is_default = True
                any_env.save(update_fields=['is_default'])
            else:
                logger.warning("No environments found, creating a default one")
                # Try to get settings from AppSettings
                try:
                    from web_ui.models import AppSettings
                    datahub_url = AppSettings.get('datahub_url', 'http://localhost:8080')
                    datahub_token = AppSettings.get('datahub_token', '')
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
                    datahub_token=datahub_token
                )
        else:
            # Sync with AppSettings
            try:
                from web_ui.models import AppSettings
                if default_env.datahub_url:
                    AppSettings.set('datahub_url', default_env.datahub_url)
                if default_env.datahub_token:
                    AppSettings.set('datahub_token', default_env.datahub_token)
                logger.info(f"Synced environment settings to AppSettings: {default_env.datahub_url}")
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
        logger.info(f"Default environment: {environment.name}, URL: {environment.datahub_url}")
        # Try to create a client and test connection
        try:
            from utils.datahub_rest_client import DataHubRestClient
            client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
            connection_working = client.test_connection()
            logger.info(f"DataHub connection test result: {'SUCCESS' if connection_working else 'FAILED'}")
        except Exception as e:
            logger.error(f"Error testing DataHub connection: {str(e)}")
    else:
        logger.warning("No default environment configured!")
    
    return render(request, 'metadata_manager/entities/editable_properties.html', {
        'page_title': 'Editable Properties'
    })

def has_editable_properties(entity):
    """
    Check if an entity has editable properties that we want to display.
    
    Args:
        entity (dict): The entity dictionary from DataHub
        
    Returns:
        bool: True if the entity has editable properties, False otherwise
    """
    # Safety check - if entity is None or not a dict, it has no properties
    if not entity or not isinstance(entity, dict):
        return False
        
    # Check for editableProperties
    editable_props = entity.get('editableProperties', {})
    if editable_props and isinstance(editable_props, dict):
        # Check if there are any non-empty editable properties
        if any(value for key, value in editable_props.items() if key not in ('created', 'lastModified')):
            return True
    
    # Check for editableSchemaMetadata (for datasets)
    if entity.get('type') == 'DATASET':
        schema_metadata = entity.get('editableSchemaMetadata', {})
        if isinstance(schema_metadata, dict):
            schema_fields = schema_metadata.get('editableSchemaFieldInfo', [])
            if schema_fields and isinstance(schema_fields, list) and len(schema_fields) > 0:
                return True
    
    # For some entity types, we want to include them even if they don't have
    # explicit editable properties in the response
    always_include_types = ['TAG', 'GLOSSARY_TERM', 'GLOSSARY_NODE', 'DOMAIN']
    if entity.get('type') in always_include_types:
        return True
    
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
            entity_type=entity_type
        )
        
        if result is None or 'searchResults' not in result:
            logger.warning(f"No results found for platform {platform}")
            return []
            
        # Extract instance identifiers from URNs
        instances = set()
        for search_result in result.get('searchResults', []):
            entity = search_result.get('entity', {})
            urn = entity.get('urn', '')
            
            # Parse URN to extract instance identifier
            # Format is typically: urn:li:dataset:(urn:li:dataPlatform:platform, instance.database.schema.table, env)
            try:
                if 'urn:li:dataset:' in urn or 'urn:li:container:' in urn:
                    parts = urn.split(',')
                    if len(parts) >= 2:
                        # The second part may contain instance.database format
                        instance_part = parts[1].strip()
                        # Extract just the instance part
                        instance_id = instance_part.split('.')[0].strip()
                        if instance_id and len(instance_id) > 0:
                            instances.add(instance_id)
            except Exception as e:
                logger.debug(f"Error parsing URN {urn}: {str(e)}")
                continue
                
        logger.info(f"Found {len(instances)} instances for platform {platform}: {instances}")
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
        if entity_type == 'DATASET':
            return [
                'postgres', 'mysql', 'snowflake', 'bigquery', 'redshift', 'databricks', 
                'azure', 'hive', 'kafka', 'oracle', 'mssql', 'teradata', 'glue',
                'tableau', 'looker', 'metabase', 'superset', 'powerbi', 'dbt'
            ]
        elif entity_type == 'CONTAINER':
            return ['postgres', 'mysql', 'snowflake', 'bigquery', 'redshift', 'azure', 'glue']
        elif entity_type in ['CHART', 'DASHBOARD']:
            return ['tableau', 'looker', 'metabase', 'superset', 'powerbi']
        elif entity_type == 'DATAFLOW':
            return ['airflow', 'glue', 'databricks', 'azure']
        elif entity_type == 'DATAJOB':
            return ['airflow', 'glue', 'databricks', 'azure']
        else:
            return []
    except Exception as e:
        logger.error(f"Error getting platform list: {str(e)}")
        return []

@login_required
@require_http_methods(["GET"])
def get_editable_entities(request):
    """Get entities with editable properties."""
    try:
        # Add detailed logging
        logger.info("==== get_editable_entities called ====")
        logger.info(f"Request query params: {request.GET}")
        
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            logger.error("No active environment configured")
            return JsonResponse({
                'success': False,
                'error': 'No active environment configured'
            })
        
        logger.info(f"Using environment: {environment.name}, URL: {environment.datahub_url}")
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Check if the client has the ability to execute custom GraphQL queries
        has_graphql_capability = hasattr(client, 'execute_graphql') and callable(getattr(client, 'execute_graphql'))
        logger.info(f"Client has GraphQL capability: {has_graphql_capability}")

        # Extract and clean search parameters
        try:
            start = int(request.GET.get('start', 0))
            count = int(request.GET.get('count', 20))
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid pagination parameters: {e}")
            start = 0
            count = 20

        # Extract and clean search parameters
        query = request.GET.get('searchQuery', '*').strip()
        entity_type = request.GET.get('entityType', '').strip()
        platform = request.GET.get('platform', '').strip()
        sort_by = request.GET.get('sortBy', 'name').strip()
        use_platform_pagination = request.GET.get('use_platform_pagination', 'false').lower() == 'true'

        # Validate and clean parameters
        if entity_type == '':
            entity_type = None
        
        # Log what we're going to do
        logger.info(f"Fetching entities with start={start}, count={count}, query={query}, entity_type={entity_type}, "
                    f"platform={platform}, sort_by={sort_by}, use_platform_pagination={use_platform_pagination}")
        
        # Define all supported entity types for multi-type pagination
        all_entity_types = [
            'DATASET', 'CONTAINER', 'DASHBOARD', 'CHART', 
            'DATAFLOW', 'DATAJOB', 'DOMAIN', 'GLOSSARY_TERM', 
            'GLOSSARY_NODE', 'TAG'
        ]
        
        # Handle comprehensive pagination (cycle through entity types and platforms)
        if use_platform_pagination and not platform:
            # Initialize an empty result set
            combined_result = {
                'searchResults': [],
                'start': start,
                'count': count,
                'total': 0
            }
            
            # Keep track of how many entities we've processed
            processed_count = 0
            
            # If no entity type is specified, cycle through all types
            entity_types_to_query = [entity_type] if entity_type else all_entity_types
            
            for current_entity_type in entity_types_to_query:
                # Skip entity types that don't make sense to query
                if not current_entity_type:
                    continue
                
                logger.info(f"Processing entity type: {current_entity_type}")
                
                # Get platforms for this entity type
                platforms = get_platform_list(client, current_entity_type)
                
                # If there are no specific platforms or the entity type doesn't use platforms,
                # try a direct query without platform filter
                if not platforms or current_entity_type in ['TAG', 'GLOSSARY_TERM', 'GLOSSARY_NODE', 'DOMAIN']:
                    try:
                        logger.info(f"Querying entity type {current_entity_type} without platform filter")
                        
                        type_result = client.get_editable_entities(
                            start=0,  # Start from beginning for each type
                            count=min(count * 2, 100),  # Fetch enough to account for filtering
                            query=query,
                            entity_type=current_entity_type
                        )
                        
                        if type_result is None:
                            logger.warning(f"Received None result for entity type {current_entity_type}")
                            continue
                        
                        # Filter results
                        filtered_type_results = []
                        for search_result in type_result.get('searchResults', []):
                            entity = search_result.get('entity', {})
                            if has_editable_properties(entity):
                                filtered_type_results.append(search_result)
                        
                        # Add filtered results to the combined set
                        type_filtered_count = len(filtered_type_results)
                        
                        if type_filtered_count > 0:
                            logger.info(f"Found {type_filtered_count} entities with editable properties for type {current_entity_type}")
                            
                            # Add type total to the combined total
                            combined_result['total'] += type_result.get('total', type_filtered_count)
                            
                            # Add results, considering pagination
                            if processed_count < start:
                                # Skip results that come before our start position
                                if processed_count + type_filtered_count <= start:
                                    # All results from this type are before our start
                                    processed_count += type_filtered_count
                                else:
                                    # Some results are after our start position
                                    skip_count = start - processed_count
                                    take_count = min(count - len(combined_result['searchResults']), 
                                                   type_filtered_count - skip_count)
                                    
                                    combined_result['searchResults'].extend(
                                        filtered_type_results[skip_count:skip_count + take_count]
                                    )
                                    processed_count += type_filtered_count
                            else:
                                # We're already past our start position
                                take_count = min(count - len(combined_result['searchResults']), 
                                                type_filtered_count)
                                combined_result['searchResults'].extend(
                                    filtered_type_results[:take_count]
                                )
                                processed_count += type_filtered_count
                            
                            # Check if we have enough results for this page
                            if len(combined_result['searchResults']) >= count:
                                break
                    except Exception as e:
                        logger.error(f"Error processing entity type {current_entity_type} without platform: {str(e)}")
                        continue
                
                # If we need more results and this entity type uses platforms, query by platform
                if len(combined_result['searchResults']) < count and platforms:
                    # Try each platform until we have enough results
                    for platform_name in platforms:
                        try:
                            # Get platform instances for this platform
                            platform_instances = get_platform_instances(client, platform_name, current_entity_type)
                            
                            # If we found specific instances, query each one separately
                            if platform_instances and len(platform_instances) > 0:
                                logger.info(f"Found {len(platform_instances)} instances for platform {platform_name}")
                                
                                for instance_id in platform_instances:
                                    # Build instance-specific query
                                    instance_query = f"{query}"
                                    if query and query != '*':
                                        instance_query += f" AND platform:{platform_name} AND instance:{instance_id}"
                                    else:
                                        instance_query = f"platform:{platform_name} AND instance:{instance_id}"
                                        
                                    logger.info(f"Fetching entities for type {current_entity_type}, platform: {platform_name}, instance: {instance_id}")
                                    
                                    # Fetch entities for this platform instance
                                    instance_result = client.get_editable_entities(
                                        start=0,
                                        count=min(count * 2, 100),
                                        query=instance_query,
                                        entity_type=current_entity_type
                                    )
                                    
                                    if instance_result is None:
                                        logger.warning(f"Received None result for platform {platform_name}, instance {instance_id}")
                                        continue
                                    
                                    # Filter results
                                    filtered_instance_results = []
                                    for search_result in instance_result.get('searchResults', []):
                                        entity = search_result.get('entity', {})
                                        if has_editable_properties(entity):
                                            filtered_instance_results.append(search_result)
                                    
                                    # Add filtered results to the combined set
                                    instance_filtered_count = len(filtered_instance_results)
                                    if instance_filtered_count > 0:
                                        logger.info(f"Found {instance_filtered_count} entities with editable properties for platform {platform_name}, instance {instance_id}")
                                        
                                        # Add instance total to the combined total
                                        combined_result['total'] += instance_result.get('total', instance_filtered_count)
                                        
                                        # Add results, considering pagination
                                        if processed_count < start:
                                            # Skip results that come before our start position
                                            if processed_count + instance_filtered_count <= start:
                                                # All results from this instance are before our start
                                                processed_count += instance_filtered_count
                                            else:
                                                # Some results are after our start position
                                                skip_count = start - processed_count
                                                take_count = min(count - len(combined_result['searchResults']), 
                                                                instance_filtered_count - skip_count)
                                                
                                                combined_result['searchResults'].extend(
                                                    filtered_instance_results[skip_count:skip_count + take_count]
                                                )
                                                processed_count += instance_filtered_count
                                        else:
                                            # We're already past our start position
                                            take_count = min(count - len(combined_result['searchResults']), 
                                                            instance_filtered_count)
                                            combined_result['searchResults'].extend(
                                                filtered_instance_results[:take_count]
                                            )
                                            processed_count += instance_filtered_count
                                        
                                        # Check if we have enough results for this page
                                        if len(combined_result['searchResults']) >= count:
                                            break
                                
                                # If we have enough results from instances, don't do the general platform query
                                if len(combined_result['searchResults']) >= count:
                                    break
                            
                            # If we didn't get enough results from instances, or couldn't identify instances,
                            # do a general platform query
                            if len(combined_result['searchResults']) < count:
                                # Build platform-specific query
                                platform_query = f"{query}"
                                if query and query != '*':
                                    platform_query += f" AND platform:{platform_name}"
                                else:
                                    platform_query = f"platform:{platform_name}"
                                    
                                logger.info(f"Fetching entities for type {current_entity_type}, platform: {platform_name} with query: {platform_query}")
                                
                                # Fetch entities for this platform
                                platform_result = client.get_editable_entities(
                                    start=0,  # Start from beginning for each platform
                                    count=min(count * 2, 100),  # Fetch enough to account for filtering
                                    query=platform_query,
                                    entity_type=current_entity_type
                                )
                                
                                if platform_result is None:
                                    logger.warning(f"Received None result for platform {platform_name}")
                                    continue
                                
                                # Filter results
                                filtered_platform_results = []
                                for search_result in platform_result.get('searchResults', []):
                                    entity = search_result.get('entity', {})
                                    if has_editable_properties(entity):
                                        filtered_platform_results.append(search_result)
                                
                                # Add filtered results to the combined set
                                platform_filtered_count = len(filtered_platform_results)
                                if platform_filtered_count > 0:
                                    logger.info(f"Found {platform_filtered_count} entities with editable properties for platform {platform_name}")
                                    
                                    # Add platform total to the combined total
                                    combined_result['total'] += platform_result.get('total', platform_filtered_count)
                                    
                                    # Add results, considering pagination
                                    if processed_count < start:
                                        # Skip results that come before our start position
                                        if processed_count + platform_filtered_count <= start:
                                            # All results from this platform are before our start
                                            processed_count += platform_filtered_count
                                        else:
                                            # Some results are after our start position
                                            skip_count = start - processed_count
                                            take_count = min(count - len(combined_result['searchResults']), 
                                                            platform_filtered_count - skip_count)
                                            
                                            combined_result['searchResults'].extend(
                                                filtered_platform_results[skip_count:skip_count + take_count]
                                            )
                                            processed_count += platform_filtered_count
                                    else:
                                        # We're already past our start position
                                        take_count = min(count - len(combined_result['searchResults']), 
                                                        platform_filtered_count)
                                        combined_result['searchResults'].extend(
                                            filtered_platform_results[:take_count]
                                        )
                                        processed_count += platform_filtered_count
                                    
                                    # Check if we have enough results for this page
                                    if len(combined_result['searchResults']) >= count:
                                        break
                                    
                        except Exception as e:
                            logger.error(f"Error processing platform {platform_name}: {str(e)}")
                            continue
                
                # If we have enough results, stop querying more entity types
                if len(combined_result['searchResults']) >= count:
                    break
            
            logger.info(f"Combined results: {len(combined_result['searchResults'])} entities")
            
            # Check if we need more results and if browse path pagination could help
            if len(combined_result['searchResults']) < count:
                logger.info("Attempting to fetch additional results using browse paths")
                
                # Track URNs we've already seen to avoid duplicates
                seen_urns = set()
                for result in combined_result['searchResults']:
                    if 'entity' in result and 'urn' in result['entity']:
                        seen_urns.add(result['entity']['urn'])
                
                # Get common browse paths to query - this will include both static and discovered paths
                browse_paths = get_common_browse_paths(client, entity_type)
                logger.info(f"Using {len(browse_paths)} browse paths for searching")
                
                # Track how many additional entities we've found
                additional_entities_count = 0
                
                for browse_path in browse_paths:
                    # Skip if we already have enough results
                    if len(combined_result['searchResults']) >= count:
                        break
                        
                    # Build browse path query
                    browse_query = f"{query}"
                    if query and query != '*':
                        browse_query += f" AND browsePaths:\"{browse_path}*\""
                    else:
                        browse_query = f"browsePaths:\"{browse_path}*\""
                        
                    logger.info(f"Fetching entities with browse path: {browse_path}")
                    
                    # Add entity type if specified
                    entity_type_param = entity_type if entity_type else None
                    
                    # Use a custom GraphQL query if the client supports it
                    browse_result = None
                    if has_graphql_capability:
                        try:
                            # Construct a GraphQL query that properly uses browsePathV2 filters
                            graphql_query = """
                            query GetEntitiesWithBrowsePathsForSearch($input: SearchAcrossEntitiesInput!) {
                              searchAcrossEntities(input: $input) {
                                start
                                count
                                total
                                searchResults {
                                  entity {
                                    urn
                                    type
                                    ... on Dataset {
                                      name
                                      browsePaths { path }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        name
                                        description
                                      }
                                      editableSchemaMetadata {
                                        editableSchemaFieldInfo {
                                          fieldPath
                                          description
                                          tags {
                                            tags {
                                              associatedUrn
                                              tag { urn }
                                            }
                                          }
                                        }
                                      }
                                    }
                                    ... on Container {
                                      properties { name }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        description
                                      }
                                    }
                                    ... on Chart {
                                      properties { name }
                                      browsePaths { path }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        description
                                      }
                                    }
                                    ... on Dashboard {
                                      properties { name }
                                      browsePaths { path }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        description
                                      }
                                    }
                                    ... on DataFlow {
                                      properties { name }
                                      browsePaths { path }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        description
                                      }
                                    }
                                    ... on DataJob {
                                      properties { name }
                                      browsePaths { path }
                                      browsePathV2 {
                                        path {
                                          name
                                          entity {
                                            ... on Container {
                                              container {
                                                urn
                                                properties { name }
                                              }
                                            }
                                          }
                                        }
                                      }
                                      editableProperties {
                                        description
                                      }
                                    }
                                  }
                                }
                              }
                            }
                            """
                            
                            # Extract the path name from the browse path - remove leading/trailing slashes
                            path_name = browse_path.strip('/')
                            
                            # Set up variables for the GraphQL query using the proper browsePathV2 filter format
                            variables = {
                                "input": {
                                    "query": query if query and query != '*' else "",
                                    "start": 0,
                                    "count": min(count * 2, 100)
                                }
                            }
                            
                            # Add entity type filter if specified
                            if entity_type_param:
                                variables["input"]["types"] = [entity_type_param]
                                
                            # Add the browsePathV2 filter using the proper format
                            # DataHub uses a special character (␟) before the path name
                            if path_name:
                                variables["input"]["orFilters"] = [
                                    {
                                        "and": [
                                            {
                                                "field": "browsePathV2",
                                                "condition": "EQUAL",
                                                "values": [f"␟{path_name}"],
                                                "negated": False
                                            }
                                        ]
                                    }
                                ]
                            
                            # Execute the query
                            result = client.execute_graphql(graphql_query, variables)
                            
                            # Extract the search results
                            if result and 'data' in result and 'searchAcrossEntities' in result['data']:
                                browse_result = {
                                    'searchResults': result['data']['searchAcrossEntities']['searchResults'],
                                    'total': result['data']['searchAcrossEntities']['total']
                                }
                        except Exception as e:
                            logger.warning(f"Error executing custom GraphQL for browse path search: {str(e)}")
                            # Fall back to regular search
                    
                    # Use regular search if GraphQL approach failed or isn't available
                    if browse_result is None:
                        try:
                            # Fetch entities with this browse path
                            browse_result = client.get_editable_entities(
                                start=0,
                                count=min(count * 2, 100),
                                query=browse_query,
                                entity_type=entity_type_param
                            )
                        except Exception as e:
                            logger.error(f"Error processing browse path {browse_path}: {str(e)}")
                            continue
                    
                    if browse_result is None:
                        logger.warning(f"Received None result for browse path {browse_path}")
                        continue
                    
                    # Filter results
                    filtered_browse_results = []
                    for search_result in browse_result.get('searchResults', []):
                        entity = search_result.get('entity', {})
                        
                        if not entity or 'urn' not in entity:
                            continue
                            
                        entity_urn = entity['urn']
                        
                        # Skip if we've already seen this entity
                        if entity_urn in seen_urns:
                            continue
                            
                        # Check if it has editable properties
                        if has_editable_properties(entity):
                            # Enhance the entity with extracted browse paths if needed
                            if 'browsePaths' not in entity or not entity['browsePaths']:
                                entity['browsePaths'] = extract_browse_paths(entity)
                                
                            # Ensure the entity has a name using our helper
                            if ('name' not in entity and 'properties' not in entity) or \
                               ('properties' in entity and 'name' not in entity['properties']):
                                entity_name = extract_entity_name(entity)
                                # Add the name to the appropriate place based on entity type
                                if entity['type'] == 'DATASET':
                                    entity['name'] = entity_name
                                else:
                                    if 'properties' not in entity:
                                        entity['properties'] = {}
                                    entity['properties']['name'] = entity_name
                                
                            filtered_browse_results.append(search_result)
                            seen_urns.add(entity_urn)  # Mark as seen
                    
                    # Add filtered results to the combined set
                    browse_filtered_count = len(filtered_browse_results)
                    if browse_filtered_count > 0:
                        logger.info(f"Found {browse_filtered_count} additional entities with browse path {browse_path}")
                        
                        # Add browse path total to the combined total
                        combined_result['total'] += browse_filtered_count
                        
                        # Add results, considering pagination
                        take_count = min(count - len(combined_result['searchResults']), browse_filtered_count)
                        combined_result['searchResults'].extend(filtered_browse_results[:take_count])
                        additional_entities_count += take_count
                        
                        # Check if we have enough results
                        if len(combined_result['searchResults']) >= count:
                            break
                
                logger.info(f"Browse path search added {additional_entities_count} entities to results")
            
            try:
                return JsonResponse({
                    'success': True,
                    'data': combined_result
                })
            except Exception as e:
                logger.error(f"Error creating JSON response: {str(e)}")
                return JsonResponse({
                    'success': False,
                    'error': f"Error processing results: {str(e)}"
                })
        
        # Handle platform-based pagination for a specific entity type
        elif use_platform_pagination and entity_type and not platform:
            # Get list of platforms for this entity type
            platforms = get_platform_list(client, entity_type)
            
            if platforms:
                logger.info(f"Using platform-based pagination with platforms: {platforms}")
                
                # Initialize an empty result set
                combined_result = {
                    'searchResults': [],
                    'start': start,
                    'count': count,
                    'total': 0
                }
                
                # Keep track of how many entities we've processed
                processed_count = 0
                
                # Try each platform until we have enough results
                for platform_name in platforms:
                    try:
                        # Get platform instances for this platform
                        platform_instances = get_platform_instances(client, platform_name, entity_type)
                        
                        # If we found specific instances, query each one separately
                        if platform_instances and len(platform_instances) > 0:
                            logger.info(f"Found {len(platform_instances)} instances for platform {platform_name}")
                            
                            for instance_id in platform_instances:
                                # Build instance-specific query
                                instance_query = f"{query}"
                                if query and query != '*':
                                    instance_query += f" AND platform:{platform_name} AND instance:{instance_id}"
                                else:
                                    instance_query = f"platform:{platform_name} AND instance:{instance_id}"
                                    
                                    logger.info(f"Fetching entities for type {entity_type}, platform: {platform_name}, instance: {instance_id}")
                                    
                                    # Fetch entities for this platform instance
                                    instance_result = client.get_editable_entities(
                                        start=0,
                                        count=min(count * 2, 100),
                                        query=instance_query,
                                        entity_type=entity_type
                                    )
                                    
                                    if instance_result is None:
                                        logger.warning(f"Received None result for platform {platform_name}, instance {instance_id}")
                                        continue
                                    
                                    # Filter results
                                    filtered_instance_results = []
                                    for search_result in instance_result.get('searchResults', []):
                                        entity = search_result.get('entity', {})
                                        if has_editable_properties(entity):
                                            filtered_instance_results.append(search_result)
                                    
                                    # Add filtered results to the combined set
                                    instance_filtered_count = len(filtered_instance_results)
                                    if instance_filtered_count > 0:
                                        logger.info(f"Found {instance_filtered_count} entities with editable properties for platform {platform_name}, instance {instance_id}")
                                        
                                        # Add instance total to the combined total
                                        combined_result['total'] += instance_result.get('total', instance_filtered_count)
                                        
                                        # Add results, considering pagination
                                        if processed_count < start:
                                            # Skip results that come before our start position
                                            if processed_count + instance_filtered_count <= start:
                                                # All results from this instance are before our start
                                                processed_count += instance_filtered_count
                                            else:
                                                # Some results are after our start position
                                                skip_count = start - processed_count
                                                take_count = min(count - len(combined_result['searchResults']), 
                                                                instance_filtered_count - skip_count)
                                                
                                                combined_result['searchResults'].extend(
                                                    filtered_instance_results[skip_count:skip_count + take_count]
                                                )
                                                processed_count += instance_filtered_count
                                        else:
                                            # We're already past our start position
                                            take_count = min(count - len(combined_result['searchResults']), 
                                                            instance_filtered_count)
                                            combined_result['searchResults'].extend(
                                                filtered_instance_results[:take_count]
                                            )
                                            processed_count += instance_filtered_count
                                        
                                        # Check if we have enough results for this page
                                        if len(combined_result['searchResults']) >= count:
                                            break
                                
                            # If we have enough results from instances, don't do the general platform query
                            if len(combined_result['searchResults']) >= count:
                                break
                        
                        # If we didn't get enough results from instances, or couldn't identify instances,
                        # do a general platform query
                        if len(combined_result['searchResults']) < count:
                            # Build platform-specific query
                            platform_query = f"{query}"
                            if query and query != '*':
                                platform_query += f" AND platform:{platform_name}"
                            else:
                                platform_query = f"platform:{platform_name}"
                                
                            logger.info(f"Fetching entities for type {entity_type}, platform: {platform_name} with query: {platform_query}")
                            
                            # Fetch entities for this platform
                            platform_result = client.get_editable_entities(
                                start=0,  # Start from beginning for each platform
                                count=min(count * 2, 100),  # Fetch enough to account for filtering
                                query=platform_query,
                                entity_type=entity_type
                            )
                            
                            if platform_result is None:
                                logger.warning(f"Received None result for platform {platform_name}")
                                continue
                            
                            # Filter results
                            filtered_platform_results = []
                            for search_result in platform_result.get('searchResults', []):
                                entity = search_result.get('entity', {})
                                if has_editable_properties(entity):
                                    filtered_platform_results.append(search_result)
                            
                            # Add filtered results to the combined set
                            platform_filtered_count = len(filtered_platform_results)
                            if platform_filtered_count > 0:
                                logger.info(f"Found {platform_filtered_count} entities with editable properties for platform {platform_name}")
                                
                                # Add platform total to the combined total
                                combined_result['total'] += platform_result.get('total', platform_filtered_count)
                                
                                # Add results, considering pagination
                                if processed_count < start:
                                    # Skip results that come before our start position
                                    if processed_count + platform_filtered_count <= start:
                                        # All results from this platform are before our start
                                        processed_count += platform_filtered_count
                                    else:
                                        # Some results are after our start position
                                        skip_count = start - processed_count
                                        take_count = min(count - len(combined_result['searchResults']), 
                                                        platform_filtered_count - skip_count)
                                        
                                        combined_result['searchResults'].extend(
                                            filtered_platform_results[skip_count:skip_count + take_count]
                                        )
                                        processed_count += platform_filtered_count
                                else:
                                    # We're already past our start position
                                    take_count = min(count - len(combined_result['searchResults']), 
                                                    platform_filtered_count)
                                    combined_result['searchResults'].extend(
                                        filtered_platform_results[:take_count]
                                    )
                                    processed_count += platform_filtered_count
                                
                                # Check if we have enough results for this page
                                if len(combined_result['searchResults']) >= count:
                                    break
                                
                    except Exception as e:
                        logger.error(f"Error processing platform {platform_name}: {str(e)}")
                        continue
                
                logger.info(f"Combined results from {len(platforms)} platforms: {len(combined_result['searchResults'])} entities")
                return JsonResponse({
                    'success': True,
                    'data': combined_result
                })
        
        # If not using platform pagination or if platform is specified, use regular approach
        try:
            # If platform is specified, add it to the query
            if platform:
                if query and query != '*':
                    query = f"{query} AND platform:{platform}"
                else:
                    query = f"platform:{platform}"
            
            result = client.get_editable_entities(
                start=start,
                count=count,
                query=query,
                entity_type=entity_type
            )
        except Exception as e:
            logger.error(f"Error calling get_editable_entities: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f'Error retrieving entities: {str(e)}'
            })
        
        # Check if result is None (which can happen if there's an error in the GraphQL request)
        if result is None:
            logger.error("Received None result from get_editable_entities call")
            return JsonResponse({
                'success': False,
                'error': 'Failed to fetch entities from DataHub. Check connection settings and logs for details.'
            })
        
        logger.info(f"Got result with {len(result.get('searchResults', []))} entities")
        
        # Filter entities to only include those with actual editable properties
        if 'searchResults' in result:
            filtered_results = []
            
            for search_result in result['searchResults']:
                entity = search_result.get('entity', {})
                
                if has_editable_properties(entity):
                    filtered_results.append(search_result)
            
            # Update the results with filtered list
            result['searchResults'] = filtered_results
            
            # If we have filtered out a significant number of results and we're not
            # on the last page, we may need to fetch more results to fill the page
            original_total = result.get('total', 0)
            filtered_count = len(filtered_results)
            
            # Only update the total if it makes sense (some DataHub versions don't return accurate totals)
            if original_total > 0 and filtered_count < count and original_total > (start + count):
                # Calculate approximately how many more entities we might need to fetch
                # based on the filter ratio we've observed
                original_count = len(result.get('searchResults', []))
                if original_count > 0:  # Avoid division by zero
                    filter_ratio = filtered_count / original_count
                    
                    # Only attempt to fetch more if our filter ratio suggests it's worth it
                    if filter_ratio > 0 and filter_ratio < 0.8:
                        logger.info(f"Filter ratio: {filter_ratio}, attempting to fetch more entities")
                        
                        # Fetch additional entities to try to fill the page
                        additional_start = start + count
                        additional_count = min(count * 2, 100)  # Don't fetch too many
                        
                        try:
                            additional_result = client.get_editable_entities(
                                start=additional_start,
                                count=additional_count,
                                query=query,
                                entity_type=entity_type
                            )
                            
                            # Skip processing if the additional result is None
                            if additional_result is None:
                                logger.warning("Received None for additional results")
                            else:
                                additional_filtered = []
                                for search_result in additional_result.get('searchResults', []):
                                    if isinstance(search_result, dict) and 'entity' in search_result:
                                        entity = search_result.get('entity', {})
                                        
                                        # Use the helper function
                                        if has_editable_properties(entity):
                                            additional_filtered.append(search_result)
                                
                                # Add the additional filtered results until we reach the requested count
                                needed = count - filtered_count
                                if needed > 0 and additional_filtered:
                                    result['searchResults'].extend(additional_filtered[:needed])
                                    logger.info(f"Added {min(needed, len(additional_filtered))} additional entities")
                        except Exception as e:
                            logger.error(f"Error fetching additional entities: {str(e)}")
            
            # Update the total count in the result
            try:
                result['total'] = original_total  # Keep the original total for proper pagination
                result['filtered_total'] = len(filtered_results)  # Add the filtered total for client info
            except Exception as e:
                logger.error(f"Error updating result totals: {str(e)}")
            
            logger.info(f"Filtered down to {len(result['searchResults'])} entities with editable properties")
        
        try:
            return JsonResponse({
                'success': True,
                'data': result
            })
        except Exception as e:
            logger.error(f"Error creating JSON response: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': f"Error processing results: {str(e)}"
            })
        
    except Exception as e:
        logger.error(f"Error getting editable entities: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

@login_required
@require_http_methods(["POST"])
def update_entity_properties(request):
    """Update editable properties of an entity."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse({
                'success': False,
                'error': 'No active environment configured'
            })
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get entity details from request
        entity_urn = request.POST.get('entityUrn')
        entity_type = request.POST.get('entityType')
        
        if not entity_urn or not entity_type:
            return JsonResponse({
                'success': False,
                'error': 'Missing required parameters'
            })
        
        # Prepare properties update
        properties = {
            'editableProperties': {}
        }
        
        # Add name if provided (only for Dataset)
        if entity_type == 'DATASET' and request.POST.get('name'):
            properties['editableProperties']['name'] = request.POST.get('name')
        
        # Add description if provided
        if request.POST.get('description'):
            properties['editableProperties']['description'] = request.POST.get('description')
        
        # Handle schema metadata for datasets
        if entity_type == 'DATASET' and 'schemaFields' in request.POST:
            schema_fields = []
            for field in request.POST.getlist('schemaFields'):
                schema_fields.append({
                    'fieldPath': field.get('fieldPath'),
                    'description': field.get('description'),
                    'tags': field.get('tags', [])
                })
            properties['editableSchemaMetadata'] = {
                'editableSchemaFieldInfo': schema_fields
            }
        
        # Use the client method to update properties
        success = client.update_entity_properties(
            entity_urn=entity_urn,
            entity_type=entity_type,
            properties=properties
        )
        
        if success:
            return JsonResponse({
                'success': True,
                'data': {'urn': entity_urn}
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to update entity properties'
            })
        
    except Exception as e:
        logger.error(f"Error updating entity properties: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

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
            return JsonResponse({
                'success': False,
                'error': 'No active environment configured'
            })
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get entity details
        entity = client.get_entity(urn)
        
        if not entity:
            return JsonResponse({
                'success': False,
                'error': 'Entity not found'
            })
        
        return JsonResponse({
            'success': True,
            'entity': entity
        })
        
    except Exception as e:
        logger.error(f"Error getting entity details: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

@login_required
@require_http_methods(["GET"])
def get_entity_schema(request, urn):
    """Get schema details for a dataset entity."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse({
                'success': False,
                'error': 'No active environment configured'
            })
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Get schema details
        schema = client.get_schema(urn)
        
        if not schema:
            return JsonResponse({
                'success': False,
                'error': 'Schema not found'
            })
        
        return JsonResponse({
            'success': True,
            'schema': schema
        })
        
    except Exception as e:
        logger.error(f"Error getting schema details: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

@login_required
@require_http_methods(["POST"])
def sync_metadata(request):
    """Sync metadata with DataHub."""
    try:
        # Get active environment
        environment = Environment.objects.filter(is_default=True).first()
        if not environment:
            return JsonResponse({
                'success': False,
                'error': 'No active environment configured'
            })
        
        # Initialize DataHub client
        client = DataHubRestClient(environment.datahub_url, environment.datahub_token)
        
        # Sync metadata
        success = client.sync_metadata()
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Metadata synced successfully'
            })
        else:
            return JsonResponse({
                'success': False,
                'error': 'Failed to sync metadata'
            })
        
    except Exception as e:
        logger.error(f"Error syncing metadata: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        })

def extract_entity_name(entity):
    """
    Extract the name of an entity based on its type and available properties.
    Different entity types store names in different ways.
    
    Args:
        entity: The entity object from DataHub API response
        
    Returns:
        str: The entity name or a default if not found
    """
    try:
        if not entity:
            return "Unnamed Entity"
            
        # Check entity type to determine where to find the name
        entity_type = entity.get('type', '')
        
        # Check for direct name property (used by Dataset, MLFeature, etc.)
        if 'name' in entity:
            return entity['name']
            
        # Check for properties.name (used by Container, Chart, Dashboard, etc.)
        if 'properties' in entity and isinstance(entity['properties'], dict):
            if 'name' in entity['properties']:
                return entity['properties']['name']
                
        # Check for info.title (used by Notebook)
        if 'info' in entity and isinstance(entity['info'], dict):
            if 'title' in entity['info']:
                return entity['info']['title']
                
        # Check for editableProperties.name (might be present for some entities)
        if 'editableProperties' in entity and isinstance(entity['editableProperties'], dict):
            if 'name' in entity['editableProperties']:
                return entity['editableProperties']['name']
                
        # Last resort: extract name from URN
        urn = entity.get('urn', '')
        if urn:
            # Extract the last part of the URN as a fallback name
            urn_parts = urn.split('/')
            if urn_parts:
                return urn_parts[-1]
                
        return "Unnamed Entity"
    except Exception as e:
        logger.error(f"Error extracting entity name: {str(e)}")
        return "Unnamed Entity"

def extract_browse_paths(entity):
    """
    Extract browse paths from an entity, preferring browsePathV2 when available.
    
    Args:
        entity: The entity object from DataHub API response
        
    Returns:
        list: List of browse paths as strings
    """
    try:
        if not entity:
            return []
            
        paths = []
        
        # First try to extract from browsePathV2 (preferred format)
        if 'browsePathV2' in entity and entity['browsePathV2']:
            browse_path_v2 = entity['browsePathV2']
            
            # browsePathV2 contains a list of path entries
            if 'path' in browse_path_v2:
                for path_entry in browse_path_v2['path']:
                    if 'entity' in path_entry and 'container' in path_entry['entity']:
                        container = path_entry['entity']['container']
                        if 'properties' in container and 'name' in container['properties']:
                            container_name = container['properties']['name']
                            paths.append(f"/{container_name}")
                            
        # If we couldn't extract from browsePathV2 or it's empty, try browsePaths
        if not paths and 'browsePaths' in entity and entity['browsePaths']:
            for browse_path in entity['browsePaths']:
                if 'path' in browse_path:
                    paths.append(browse_path['path'])
                    
        # Legacy approach: entity might have browsePaths as simple string array
        if not paths and 'browsePaths' in entity and isinstance(entity['browsePaths'], list):
            for path in entity['browsePaths']:
                if isinstance(path, str):
                    paths.append(path)
                    
        return paths
    except Exception as e:
        logger.error(f"Error extracting browse paths: {str(e)}")
        return []

def get_browse_paths_hierarchy(client, entity_type=None, parent_path='/'):
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
        if hasattr(client, 'execute_graphql') and callable(client.execute_graphql):
            # Extract the path name from the parent path - remove leading/trailing slashes
            path_name = parent_path.strip('/')
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
                variables = {
                    "input": {
                        "query": "*",
                        "start": 0,
                        "count": 100
                    }
                }
                
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
                                        "negated": False
                                    }
                                ]
                            }
                        ]
                    }
                }
                
                # Add entity type filter if specified
                if entity_type:
                    variables["input"]["types"] = [entity_type]
            
            # Execute the query
            try:
                result = client.execute_graphql(graphql_query, variables)
                
                # Process the results
                if result and 'data' in result and 'searchAcrossEntities' in result['data']:
                    search_results = result['data']['searchAcrossEntities']['searchResults']
                    
                    # Extract and process discovered paths
                    for search_result in search_results:
                        entity = search_result.get('entity', {})
                        
                        # First try to get paths from browsePathV2
                        if 'browsePathV2' in entity and entity['browsePathV2']:
                            browse_path_v2 = entity['browsePathV2']
                            
                            if 'path' in browse_path_v2:
                                # Create the full path by joining the path names
                                path_names = []
                                for path_entry in browse_path_v2['path']:
                                    if 'name' in path_entry:
                                        path_names.append(path_entry['name'])
                                
                                if path_names:
                                    full_path = '/' + '/'.join(path_names)
                                    
                                    # Extract the next level component based on parent_path
                                    if full_path.startswith(parent_path if parent_path != '/' else ''):
                                        parent_components = parent_path.strip('/').split('/')
                                        parent_depth = len(parent_components) if parent_components[0] != '' else 0
                                        
                                        path_components = full_path.strip('/').split('/')
                                        if len(path_components) > parent_depth:
                                            # Get the next level path
                                            next_level_path = '/' + '/'.join(path_components[:parent_depth + 1])
                                            if next_level_path not in discovered_paths:
                                                discovered_paths.append(next_level_path)
                        
                        # Fallback to browsePaths if browsePathV2 didn't yield results
                        elif 'browsePaths' in entity:
                            browse_paths = entity['browsePaths']
                            for browse_path in browse_paths:
                                if 'path' in browse_path:
                                    path = browse_path['path']
                                    
                                    # Extract the next level component based on parent_path
                                    if path.startswith(parent_path if parent_path != '/' else ''):
                                        path_components = path.strip('/').split('/')
                                        parent_components = parent_path.strip('/').split('/')
                                        parent_depth = len(parent_components) if parent_components[0] != '' else 0
                                        
                                        if len(path_components) > parent_depth:
                                            # Get the next level path
                                            next_level_path = '/' + '/'.join(path_components[:parent_depth + 1])
                                            if next_level_path not in discovered_paths:
                                                discovered_paths.append(next_level_path)
            except Exception as e:
                logger.warning(f"Error executing GraphQL for browse path discovery: {str(e)}")
                # Fall back to regular search if GraphQL fails
        
        # If GraphQL approach failed or isn't available, fall back to regular search
        if len(discovered_paths) <= 1:  # Only the parent path was found
            # Build query to find entities with this parent path
            path_query = f"browsePaths:\"{parent_path}*\""
            if entity_type:
                path_query += f" AND type:{entity_type}"
                
            logger.info(f"Falling back to regular search for path discovery: {path_query}")
            
            path_result = client.get_editable_entities(
                start=0,
                count=100,
                query=path_query,
                entity_type=entity_type
            )
            
            if path_result is None or 'searchResults' not in path_result:
                return discovered_paths
                
            # Extract child paths from results
            child_paths = set()
            for search_result in path_result.get('searchResults', []):
                entity = search_result.get('entity', {})
                
                # Extract browse paths using our helper function that handles both formats
                browse_paths = extract_browse_paths(entity)
                
                for path in browse_paths:
                    # Check if this path starts with the parent path
                    if path.startswith(parent_path):
                        # Split path into components
                        path_components = path.split('/')
                        
                        # Find the next level component after parent_path
                        parent_components = parent_path.strip('/').split('/')
                        parent_depth = len(parent_components) if parent_components[0] != '' else 0
                        
                        # Get the next level component if it exists
                        if len(path_components) > parent_depth + 1:
                            next_level = '/'.join(path_components[:parent_depth + 2])
                            if next_level != parent_path and next_level:
                                child_paths.add(next_level)
            
            # Add all discovered child paths
            discovered_paths.extend(child_paths)
        
        # Recursively explore child paths, but limit depth to avoid too many requests
        parent_components = parent_path.strip('/').split('/')
        current_depth = len(parent_components) if parent_components[0] != '' else 0
        
        # Limit recursion depth to 3 levels to avoid excessive API calls
        if current_depth < 3 and len(discovered_paths) > 1:  # More than just the parent path
            # Sort paths by depth and alphabetically within the same depth
            sorted_paths = sorted(discovered_paths[1:], key=lambda p: (len(p.split('/')), p))
            
            # Only recurse into the first few child paths to avoid explosion
            for child_path in sorted_paths[:3]:  # Limit to top 3 children
                if child_path != parent_path:  # Avoid recursing into the same path
                    child_results = get_browse_paths_hierarchy(client, entity_type, child_path)
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
            '/',  # Root path
            '/prod', '/dev', '/test', '/staging',  # Common environment paths
            '/data', '/analytics', '/reporting',    # Common functional paths
            '/public', '/private', '/shared',       # Common access level paths
        ]
        
        # Add entity-type specific paths
        if entity_type == 'DATASET':
            common_paths.extend([
                '/warehouse', '/lake', '/raw', '/curated', '/consumption',
                '/finance', '/marketing', '/sales', '/hr', '/operations'
            ])
        elif entity_type in ['CHART', 'DASHBOARD']:
            common_paths.extend([
                '/dashboards', '/reports', '/kpis', '/metrics',
                '/executive', '/departmental', '/operational'
            ])
        elif entity_type in ['DATAFLOW', 'DATAJOB']:
            common_paths.extend([
                '/pipelines', '/jobs', '/workflows', '/etl', 
                '/ingestion', '/processing', '/export'
            ])
        elif entity_type == 'CONTAINER':
            common_paths.extend([
                '/databases', '/schemas', '/projects', '/datasets',
                '/collections', '/folders'
            ])
            
        # Try to discover actual browse paths from the system
        # Start with root and a few top-level paths
        starting_paths = ['/', '/prod', '/dev']
        discovered_paths = []
        
        # Only do discovery if we have a specific entity type to avoid too many API calls
        if entity_type:
            for start_path in starting_paths:
                path_results = get_browse_paths_hierarchy(client, entity_type, start_path)
                discovered_paths.extend(path_results)
                
            # Remove duplicates while preserving order
            unique_discovered = []
            for path in discovered_paths:
                if path not in unique_discovered:
                    unique_discovered.append(path)
            
            logger.info(f"Discovered {len(unique_discovered)} browse paths for {entity_type}")
            
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
        return ['/']  # Return at least the root path