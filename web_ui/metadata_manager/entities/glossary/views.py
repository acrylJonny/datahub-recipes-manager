"""
Glossary views for the metadata manager.
"""
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

from ..common.base_views import (
    create_sync_view_function,
    create_staging_view_classes,
    create_git_view_function,
    create_remote_data_view_function
)
from .operations import (
    GlossaryNodeSyncOperations,
    GlossaryTermSyncOperations,
    GlossaryNodeStagingOperations,
    GlossaryTermStagingOperations,
    GlossaryGitOperations,
    GlossaryRemoteDataOperations
)


# Create view functions using factory pattern for nodes
sync_glossary_nodes = create_sync_view_function(
    GlossaryNodeSyncOperations(),
    template_name='metadata_manager/glossary/sync_nodes.html'
)

# Create view functions using factory pattern for terms
sync_glossary_terms = create_sync_view_function(
    GlossaryTermSyncOperations(),
    template_name='metadata_manager/glossary/sync_terms.html'
)

get_remote_glossary_data = create_remote_data_view_function(
    GlossaryRemoteDataOperations()
)

create_glossary_pr = create_git_view_function(
    GlossaryGitOperations()
)

# Create staging view classes for nodes
GlossaryNodeStageChangesView, GlossaryNodeConfirmStagingView = create_staging_view_classes(
    GlossaryNodeStagingOperations(),
    template_names={
        'stage': 'metadata_manager/glossary/stage_node_changes.html',
        'confirm': 'metadata_manager/glossary/confirm_node_staging.html'
    }
)

# Create staging view classes for terms
GlossaryTermStageChangesView, GlossaryTermConfirmStagingView = create_staging_view_classes(
    GlossaryTermStagingOperations(),
    template_names={
        'stage': 'metadata_manager/glossary/stage_term_changes.html',
        'confirm': 'metadata_manager/glossary/confirm_term_staging.html'
    }
)

# Convert class-based views to function-based views for URL routing
stage_glossary_node_changes = GlossaryNodeStageChangesView.as_view()
confirm_glossary_node_staging = GlossaryNodeConfirmStagingView.as_view()
stage_glossary_term_changes = GlossaryTermStageChangesView.as_view()
confirm_glossary_term_staging = GlossaryTermConfirmStagingView.as_view()


@require_http_methods(["GET"])
def glossary_index(request):
    """Glossary management index page."""
    return render(request, 'metadata_manager/glossary/index.html', {
        'page_title': 'Glossary Management',
        'entity_type': 'glossary',
        'available_actions': [
            {
                'name': 'Sync Nodes',
                'url': 'metadata_manager:sync_glossary_nodes',
                'description': 'Synchronize glossary nodes with DataHub'
            },
            {
                'name': 'Sync Terms',
                'url': 'metadata_manager:sync_glossary_terms',
                'description': 'Synchronize glossary terms with DataHub'
            },
            {
                'name': 'Stage Node Changes',
                'url': 'metadata_manager:stage_glossary_node_changes',
                'description': 'Stage glossary node changes for review'
            },
            {
                'name': 'Stage Term Changes',
                'url': 'metadata_manager:stage_glossary_term_changes',
                'description': 'Stage glossary term changes for review'
            },
            {
                'name': 'View Remote Data',
                'url': 'metadata_manager:get_remote_glossary_data',
                'description': 'View glossary data from DataHub'
            }
        ]
    })


@require_http_methods(["GET"])
def glossary_node_detail(request, node_name):
    """Glossary node detail view."""
    try:
        # Use the remote data operations to get node details
        operations = GlossaryRemoteDataOperations()
        node_data = operations.get_entity_detail(node_name)
        
        return render(request, 'metadata_manager/glossary/node_detail.html', {
            'node': node_data,
            'page_title': f'Glossary Node: {node_name}',
            'entity_type': 'glossary_node'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def glossary_term_detail(request, term_name):
    """Glossary term detail view."""
    try:
        # Use the remote data operations to get term details
        operations = GlossaryRemoteDataOperations()
        term_data = operations.get_entity_detail(term_name)
        
        return render(request, 'metadata_manager/glossary/term_detail.html', {
            'term': term_data,
            'page_title': f'Glossary Term: {term_name}',
            'entity_type': 'glossary_term'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def validate_glossary_node_data(request):
    """Validate glossary node data via AJAX."""
    try:
        import json
        data = json.loads(request.body)
        
        operations = GlossaryNodeSyncOperations()
        is_valid = operations.validate_entity_data(data)
        
        return JsonResponse({
            'success': True,
            'valid': is_valid,
            'message': 'Glossary node data is valid' if is_valid else 'Glossary node data is invalid'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def validate_glossary_term_data(request):
    """Validate glossary term data via AJAX."""
    try:
        import json
        data = json.loads(request.body)
        
        operations = GlossaryTermSyncOperations()
        is_valid = operations.validate_entity_data(data)
        
        return JsonResponse({
            'success': True,
            'valid': is_valid,
            'message': 'Glossary term data is valid' if is_valid else 'Glossary term data is invalid'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500) 