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

# Editable Properties Views
@login_required
def editable_properties_view(request):
    """View for managing editable properties of all entities."""
    return render(request, 'metadata_manager/entities/editable_properties.html', {
        'page_title': 'Editable Properties'
    })

@login_required
@require_http_methods(["GET"])
def get_editable_entities(request):
    """Get entities with editable properties."""
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
        
        # Get parameters from request
        start = int(request.GET.get('start', 0))
        count = int(request.GET.get('count', 20))
        query = request.GET.get('searchQuery', '*')
        entity_type = request.GET.get('entityType')
        
        if entity_type == '':
            entity_type = None
        
        # Use the client method to get editable entities
        result = client.get_editable_entities(
            start=start,
            count=count,
            query=query,
            entity_type=entity_type
        )
        
        return JsonResponse({
            'success': True,
            'data': result
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