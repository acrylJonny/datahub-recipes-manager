"""
Abstract base views for metadata manager entities.
These provide common patterns for CRUD operations, sync operations, and staging changes.
"""
import logging
from abc import ABC, abstractmethod
from django.shortcuts import render, get_object_or_404, redirect
from django.http import JsonResponse
from django.views import View
from django.contrib import messages
from django.utils import timezone
from django.views.decorators.http import require_POST, require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from utils.datahub_client_adapter import test_datahub_connection, get_datahub_client_from_request

logger = logging.getLogger(__name__)


class BaseListView(View, ABC):
    """
    Abstract base class for list views with common patterns:
    - Display local items
    - Load remote data via AJAX
    - Handle create operations
    """
    
    @property
    @abstractmethod
    def template_name(self):
        """Template to render for the list view"""
        pass
    
    @property
    @abstractmethod
    def page_title(self):
        """Page title for the view"""
        pass
    
    @property
    @abstractmethod
    def model_class(self):
        """Django model class for this entity"""
        pass
    
    @property
    @abstractmethod
    def entity_name(self):
        """Human-readable entity name (e.g., 'tag', 'domain')"""
        pass
    
    def get_local_items(self, request):
        """Get local items from database. Override for custom filtering."""
        return self.model_class.objects.all().order_by("name")
    
    def get_context_data(self, request):
        """Get context data for template. Override to add custom context."""
        local_items = self.get_local_items(request)
        connected, client = test_datahub_connection(request)
        
        return {
            "local_items": local_items,
            "remote_items": [],  # Will be populated via AJAX
            "synced_items": [],  # Will be populated via AJAX
            "has_datahub_connection": connected,
            "page_title": self.page_title,
            "entity_name": self.entity_name,
        }
    
    def get(self, request):
        """Handle GET request - display list with local data"""
        try:
            logger.info(f"Starting {self.entity_name} list view")
            context = self.get_context_data(request)
            return render(request, self.template_name, context)
        except Exception as e:
            logger.error(f"Error in {self.entity_name} list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                self.template_name,
                {"error": str(e), "page_title": self.page_title},
            )
    
    def post(self, request):
        """Handle POST request - create new item"""
        try:
            return self.handle_create(request)
        except Exception as e:
            logger.error(f"Error creating {self.entity_name}: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect(self.get_success_url())
    
    @abstractmethod
    def handle_create(self, request):
        """Handle creation of new item. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_success_url(self):
        """Get URL to redirect to after successful operations"""
        pass


class BaseDetailView(View, ABC):
    """
    Abstract base class for detail views with common patterns:
    - Display item details
    - Handle edit operations
    - Handle delete operations
    """
    
    @property
    @abstractmethod
    def template_name(self):
        """Template to render for the detail view"""
        pass
    
    @property
    @abstractmethod
    def model_class(self):
        """Django model class for this entity"""
        pass
    
    @property
    @abstractmethod
    def entity_name(self):
        """Human-readable entity name"""
        pass
    
    def get_object(self, item_id):
        """Get object by ID. Override for custom lookup."""
        return get_object_or_404(self.model_class, id=item_id)
    
    def get_context_data(self, request, item):
        """Get context data for template. Override to add custom context."""
        return {
            "item": item,
            "page_title": f"{self.entity_name.title()}: {item.name}",
        }
    
    def get(self, request, item_id):
        """Handle GET request - display item details"""
        try:
            item = self.get_object(item_id)
            context = self.get_context_data(request, item)
            return render(request, self.template_name, context)
        except Exception as e:
            logger.error(f"Error in {self.entity_name} detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect(self.get_success_url())
    
    def post(self, request, item_id):
        """Handle POST request - update item"""
        try:
            item = self.get_object(item_id)
            return self.handle_update(request, item)
        except Exception as e:
            logger.error(f"Error updating {self.entity_name}: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect(self.get_success_url())
    
    def delete(self, request, item_id):
        """Handle DELETE request - delete item"""
        try:
            item = self.get_object(item_id)
            return self.handle_delete(request, item)
        except Exception as e:
            logger.error(f"Error deleting {self.entity_name}: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})
    
    @abstractmethod
    def handle_update(self, request, item):
        """Handle update of item. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def handle_delete(self, request, item):
        """Handle deletion of item. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_success_url(self):
        """Get URL to redirect to after successful operations"""
        pass


class BaseSyncView(View, ABC):
    """
    Abstract base class for sync operations with common patterns:
    - Sync from DataHub to local
    - Resync existing items
    - Push to DataHub
    """
    
    @property
    @abstractmethod
    def model_class(self):
        """Django model class for this entity"""
        pass
    
    @property
    @abstractmethod
    def entity_name(self):
        """Human-readable entity name"""
        pass
    
    def get_datahub_client(self, request):
        """Get DataHub client with error handling"""
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            raise ValueError("Cannot connect to DataHub. Please check your connection settings.")
        return client
    
    def get_current_connection(self, request):
        """Get current connection context"""
        try:
            from web_ui.views import get_current_connection
            return get_current_connection(request)
        except Exception as e:
            logger.warning(f"Could not get current connection: {str(e)}")
            return None
    
    @abstractmethod
    def sync_from_remote(self, request, remote_data):
        """Sync item from remote DataHub data. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def get_remote_data(self, client, urn):
        """Get remote data for a specific URN. Must be implemented by subclasses."""
        pass


class BaseStageChangesView(View, ABC):
    """
    Abstract base class for staging changes (MCP operations) with common patterns:
    - Add local items to staged changes
    - Add remote items to staged changes
    - Bulk operations
    """
    
    @property
    @abstractmethod
    def entity_name(self):
        """Human-readable entity name"""
        pass
    
    @property
    @abstractmethod
    def model_class(self):
        """Django model class for this entity"""
        pass
    
    def get_environment_info(self, request, data):
        """Extract environment and mutation info from request"""
        environment_name = data.get('environment', 'dev')
        mutation_name = data.get('mutation_name')
        owner = request.user.username if request.user.is_authenticated else "admin"
        
        return environment_name, mutation_name, owner
    
    @abstractmethod
    def add_to_staged_changes(self, item_data, environment, owner, mutation_name):
        """Add item to staged changes. Must be implemented by subclasses."""
        pass
    
    @method_decorator(csrf_exempt, name="dispatch")
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)


class BaseRemoteDataView(View, ABC):
    """
    Abstract base class for fetching and processing remote data with common patterns:
    - Fetch remote data from DataHub
    - Categorize as synced/local-only/remote-only
    - Calculate statistics
    """
    
    @property
    @abstractmethod
    def entity_name(self):
        """Human-readable entity name"""
        pass
    
    @property
    @abstractmethod
    def model_class(self):
        """Django model class for this entity"""
        pass
    
    def get_local_items(self, request):
        """Get local items from database"""
        return self.model_class.objects.all().order_by("name")
    
    def get_datahub_client(self, request):
        """Get DataHub client with error handling"""
        connected, client = test_datahub_connection(request)
        if not connected or not client:
            return None, "Not connected to DataHub"
        return client, None
    
    def get_current_connection(self, request):
        """Get current connection context"""
        try:
            from web_ui.views import get_current_connection
            return get_current_connection(request)
        except Exception as e:
            logger.warning(f"Could not get current connection: {str(e)}")
            return None
    
    @abstractmethod
    def fetch_remote_data(self, client):
        """Fetch remote data from DataHub. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def process_local_item(self, item, current_connection):
        """Process a local item into standardized format. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def process_remote_item(self, item_data):
        """Process a remote item into standardized format. Must be implemented by subclasses."""
        pass
    
    def categorize_items(self, local_items, remote_items, current_connection):
        """Categorize items as synced/local-only/remote-only"""
        synced_items = []
        local_only_items = []
        remote_only_items = []
        
        # Create lookup for remote items by URN
        remote_lookup = {item.get('urn'): item for item in remote_items if item.get('urn')}
        matched_remote_urns = set()
        
        # Process local items
        for local_item in local_items:
            local_data = self.process_local_item(local_item, current_connection)
            
            # Check for remote match
            item_urn = getattr(local_item, 'urn', None)
            remote_match = remote_lookup.get(item_urn) if item_urn else None
            
            if remote_match and local_item.sync_status == "SYNCED":
                # Synced item
                matched_remote_urns.add(item_urn)
                synced_items.append({
                    "local": local_data,
                    "remote": remote_match,
                    "combined": {**local_data, **remote_match, "sync_status": "SYNCED"}
                })
            else:
                # Local-only item
                local_only_items.append(local_data)
        
        # Remaining remote items are remote-only
        remote_only_items = [
            self.process_remote_item(item) 
            for item in remote_items 
            if item.get('urn') not in matched_remote_urns
        ]
        
        return synced_items, local_only_items, remote_only_items
    
    def calculate_statistics(self, synced_items, local_only_items, remote_only_items):
        """Calculate statistics for the items"""
        all_items = synced_items + local_only_items + remote_only_items
        
        return {
            "total_items": len(all_items),
            "synced_count": len(synced_items),
            "local_only_count": len(local_only_items),
            "remote_only_count": len(remote_only_items),
        }
    
    def get(self, request):
        """Handle GET request - return remote data as JSON"""
        try:
            logger.info(f"Loading remote {self.entity_name} data")
            
            # Get DataHub client
            client, error = self.get_datahub_client(request)
            if error:
                return JsonResponse({"success": False, "error": error})
            
            # Get current connection
            current_connection = self.get_current_connection(request)
            
            # Fetch data
            local_items = self.get_local_items(request)
            remote_items = self.fetch_remote_data(client)
            
            # Categorize items
            synced_items, local_only_items, remote_only_items = self.categorize_items(
                local_items, remote_items, current_connection
            )
            
            # Calculate statistics
            statistics = self.calculate_statistics(synced_items, local_only_items, remote_only_items)
            
            # Get DataHub URL
            datahub_url = client.server_url
            if datahub_url.endswith("/api/gms"):
                datahub_url = datahub_url[:-8]
            
            return JsonResponse({
                "success": True,
                "data": {
                    "synced_items": synced_items,
                    "local_only_items": local_only_items,
                    "remote_only_items": remote_only_items,
                    "statistics": statistics,
                    "datahub_url": datahub_url,
                }
            })
            
        except Exception as e:
            logger.error(f"Error in get_remote_{self.entity_name}_data: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)}) 