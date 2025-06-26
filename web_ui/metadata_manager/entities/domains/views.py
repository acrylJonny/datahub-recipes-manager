"""
Domain views for the metadata manager.
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
    DomainSyncOperations,
    DomainStagingOperations,
    DomainGitOperations,
    DomainRemoteDataOperations
)


# Create view functions using factory pattern
sync_domains = create_sync_view_function(
    DomainSyncOperations(),
    template_name='metadata_manager/domains/sync.html'
)

get_remote_domains_data = create_remote_data_view_function(
    DomainRemoteDataOperations()
)

create_domain_pr = create_git_view_function(
    DomainGitOperations()
)

# Create staging view classes
DomainStageChangesView, DomainConfirmStagingView = create_staging_view_classes(
    DomainStagingOperations(),
    template_names={
        'stage': 'metadata_manager/domains/stage_changes.html',
        'confirm': 'metadata_manager/domains/confirm_staging.html'
    }
)

# Convert class-based views to function-based views for URL routing
stage_domain_changes = DomainStageChangesView.as_view()
confirm_domain_staging = DomainConfirmStagingView.as_view()


@require_http_methods(["GET"])
def domains_index(request):
    """Domain management index page."""
    return render(request, 'metadata_manager/domains/index.html', {
        'page_title': 'Domain Management',
        'entity_type': 'domains',
        'available_actions': [
            {
                'name': 'Sync Domains',
                'url': 'metadata_manager:sync_domains',
                'description': 'Synchronize domains with DataHub'
            },
            {
                'name': 'Stage Changes',
                'url': 'metadata_manager:stage_domain_changes',
                'description': 'Stage domain changes for review'
            },
            {
                'name': 'View Remote Data',
                'url': 'metadata_manager:get_remote_domains_data',
                'description': 'View domains from DataHub'
            }
        ]
    })


@require_http_methods(["GET"])
def domain_detail(request, domain_name):
    """Domain detail view."""
    try:
        # Use the remote data operations to get domain details
        operations = DomainRemoteDataOperations()
        domain_data = operations.get_entity_detail(domain_name)
        
        return render(request, 'metadata_manager/domains/detail.html', {
            'domain': domain_data,
            'page_title': f'Domain: {domain_name}',
            'entity_type': 'domains'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def validate_domain_data(request):
    """Validate domain data via AJAX."""
    try:
        import json
        data = json.loads(request.body)
        
        operations = DomainSyncOperations()
        is_valid = operations.validate_entity_data(data)
        
        return JsonResponse({
            'success': True,
            'valid': is_valid,
            'message': 'Domain data is valid' if is_valid else 'Domain data is invalid'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500) 