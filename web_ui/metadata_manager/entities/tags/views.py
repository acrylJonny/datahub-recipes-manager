"""
Refactored tag views using common base classes and operations.
This demonstrates the new modular approach with significantly reduced code duplication.
"""
import logging
from django.shortcuts import redirect
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from web_ui.metadata_manager.models import Tag
from web_ui.metadata_manager.common.base_views import (
    BaseListView, BaseDetailView
)
from web_ui.metadata_manager.common.sync_operations import create_sync_view_function
from web_ui.metadata_manager.common.staging_operations import create_staging_view_classes
from web_ui.metadata_manager.common.git_operations import create_git_view_function
from web_ui.metadata_manager.common.remote_data_operations import create_remote_data_view_function
from .operations import (
    TagSyncOperations, TagStagingOperations, TagGitOperations, TagRemoteDataOperations
)

logger = logging.getLogger(__name__)


class TagListView(BaseListView):
    """Tag list view using common base class"""
    
    @property
    def template_name(self):
        return "metadata_manager/tags/list.html"
    
    @property
    def page_title(self):
        return "Tags"
    
    @property
    def model_class(self):
        return Tag
    
    @property
    def entity_name(self):
        return "tag"
    
    def handle_create(self, request):
        """Handle tag creation"""
        name = request.POST.get("name")
        description = request.POST.get("description", "")
        color_hex = request.POST.get("color_hex", "#1890FF")
        
        if not name:
            messages.error(request, "Tag name is required")
            return redirect(self.get_success_url())
        
        # Check if tag already exists
        if Tag.objects.filter(name=name).exists():
            messages.error(request, f"Tag '{name}' already exists")
            return redirect(self.get_success_url())
        
        # Create the tag
        Tag.objects.create(
            name=name,
            description=description,
            color_hex=color_hex,
            sync_status="LOCAL_ONLY"
        )
        
        messages.success(request, f"Tag '{name}' created successfully")
        return redirect(self.get_success_url())
    
    def get_success_url(self):
        return "/metadata/tags/"


class TagDetailView(BaseDetailView):
    """Tag detail view using common base class"""
    
    @property
    def template_name(self):
        return "metadata_manager/tags/detail.html"
    
    @property
    def model_class(self):
        return Tag
    
    @property
    def entity_name(self):
        return "tag"
    
    def handle_update(self, request, item):
        """Handle tag update"""
        name = request.POST.get("name")
        description = request.POST.get("description", "")
        color_hex = request.POST.get("color_hex", item.color_hex)
        
        if not name:
            messages.error(request, "Tag name is required")
            return redirect(self.get_success_url())
        
        # Check if another tag with this name exists
        existing = Tag.objects.filter(name=name).exclude(id=item.id)
        if existing.exists():
            messages.error(request, f"Another tag with name '{name}' already exists")
            return redirect(self.get_success_url())
        
        # Update the tag
        item.name = name
        item.description = description
        item.color_hex = color_hex
        
        # Update sync status if it was synced before
        if item.sync_status == "SYNCED":
            item.sync_status = "MODIFIED"
        
        item.save()
        
        messages.success(request, f"Tag '{name}' updated successfully")
        return redirect(self.get_success_url())
    
    def handle_delete(self, request, item):
        """Handle tag deletion"""
        tag_name = item.name
        item.delete()
        
        return JsonResponse({
            "success": True,
            "message": f"Tag '{tag_name}' deleted successfully"
        })
    
    def get_success_url(self):
        return "/metadata/tags/"


# Create sync view functions using the factory
sync_views = create_sync_view_function(TagSyncOperations)
sync_tag_to_local = sync_views['sync_to_local']
resync_tag = sync_views['resync_item']
delete_local_tag = sync_views['delete_local_item']

# Create staging view classes using the factory
staging_views = create_staging_view_classes(TagStagingOperations)
TagAddToStagedChangesView = staging_views['LocalStagingView']
TagRemoteAddToStagedChangesView = staging_views['RemoteStagingView']

# Create Git view functions using the factory
git_views = create_git_view_function(TagGitOperations)
add_tag_to_pr = git_views['add_to_pr']

# Create remote data view function using the factory
get_remote_tags_data = create_remote_data_view_function(TagRemoteDataOperations)


# Additional tag-specific views that don't fit the common patterns
@require_http_methods(["POST"])
def set_tag_color(request, tag_id):
    """Set tag color - tag-specific functionality"""
    try:
        import json
        data = json.loads(request.body)
        color_hex = data.get('color_hex')
        
        if not color_hex:
            return JsonResponse({"success": False, "error": "Color is required"})
        
        tag = Tag.objects.get(id=tag_id)
        tag.color_hex = color_hex
        
        # Update sync status if it was synced before
        if tag.sync_status == "SYNCED":
            tag.sync_status = "MODIFIED"
        
        tag.save()
        
        return JsonResponse({
            "success": True,
            "message": f"Tag color updated successfully"
        })
        
    except Tag.DoesNotExist:
        return JsonResponse({"success": False, "error": "Tag not found"})
    except Exception as e:
        logger.error(f"Error setting tag color: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)})


@require_http_methods(["GET"])
def get_users_and_groups(request):
    """Get users and groups for ownership - this could be moved to common utils"""
    try:
        from utils.datahub_client_adapter import test_datahub_connection
        
        # Get DataHub connection
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return JsonResponse({"success": False, "error": "Not connected to DataHub"})
        
        # Fetch users and groups
        users = client.list_users() or []
        groups = client.list_groups() or []
        
        return JsonResponse({
            "success": True,
            "users": users,
            "groups": groups
        })
        
    except Exception as e:
        logger.error(f"Error fetching users and groups: {str(e)}")
        return JsonResponse({"success": False, "error": str(e)}) 