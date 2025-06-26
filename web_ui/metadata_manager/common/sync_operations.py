"""
Common sync operations for metadata manager entities.
These provide reusable patterns for syncing with DataHub.
"""
import logging
from abc import ABC, abstractmethod
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.http import require_http_methods
from utils.datahub_client_adapter import test_datahub_connection

logger = logging.getLogger(__name__)


class BaseSyncOperations(ABC):
    """
    Abstract base class for sync operations.
    Provides common patterns for sync/resync/push operations.
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
    def extract_item_data(self, remote_data):
        """Extract standardized item data from remote response"""
        pass
    
    @abstractmethod
    def create_local_item(self, item_data, current_connection):
        """Create local item from remote data"""
        pass
    
    @abstractmethod
    def update_local_item(self, local_item, item_data, current_connection):
        """Update existing local item with remote data"""
        pass
    
    @abstractmethod
    def get_remote_item(self, client, urn):
        """Get remote item by URN"""
        pass
    
    def sync_to_local(self, request, urn=None, item_data=None):
        """
        Sync item from DataHub to local storage.
        
        Args:
            request: Django request object
            urn: URN of item to sync (if not provided in item_data)
            item_data: Pre-fetched item data (optional)
        
        Returns:
            JsonResponse with success/error status
        """
        try:
            # Get DataHub client and connection context
            client = self.get_datahub_client(request)
            current_connection = self.get_current_connection(request)
            
            # Get item data if not provided
            if item_data is None:
                if not urn:
                    return JsonResponse({"success": False, "error": f"{self.entity_name.title()} URN is required"})
                
                # Fetch from DataHub
                remote_response = self.get_remote_item(client, urn)
                if not remote_response:
                    return JsonResponse({"success": False, "error": f"{self.entity_name.title()} not found in DataHub"})
                
                item_data = self.extract_item_data(remote_response)
            
            # Extract URN from item data
            item_urn = item_data.get('urn')
            if not item_urn:
                return JsonResponse({"success": False, "error": "Invalid item data - no URN found"})
            
            # Check if item already exists locally
            existing_item = None
            try:
                existing_item = self.model_class.objects.get(urn=item_urn)
            except self.model_class.DoesNotExist:
                pass
            
            if existing_item:
                # Update existing item
                self.update_local_item(existing_item, item_data, current_connection)
                action = "updated"
                item = existing_item
            else:
                # Create new item
                item = self.create_local_item(item_data, current_connection)
                action = "created"
            
            logger.info(f"Successfully {action} {self.entity_name}: {item_urn}")
            return JsonResponse({
                "success": True,
                "message": f"{self.entity_name.title()} '{item.name}' {action} successfully",
                "item_id": str(item.id),
                "sync_status": item.sync_status,
                "action": action
            })
            
        except Exception as e:
            logger.error(f"Error syncing {self.entity_name} to local: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})
    
    def resync_item(self, request, item_id, urn=None):
        """
        Resync existing local item with its DataHub counterpart.
        
        Args:
            request: Django request object
            item_id: Local item ID
            urn: Optional URN override
        
        Returns:
            JsonResponse with success/error status
        """
        try:
            # Get local item
            try:
                item = self.model_class.objects.get(id=item_id)
            except self.model_class.DoesNotExist:
                return JsonResponse({"success": False, "error": f"{self.entity_name.title()} not found"})
            
            # Use provided URN or item's URN
            item_urn = urn or item.urn
            if not item_urn:
                return JsonResponse({"success": False, "error": f"{self.entity_name.title()} URN is required"})
            
            # Get DataHub client and connection context
            client = self.get_datahub_client(request)
            current_connection = self.get_current_connection(request)
            
            # Fetch latest data from DataHub
            remote_response = self.get_remote_item(client, item_urn)
            if not remote_response:
                return JsonResponse({"success": False, "error": f"{self.entity_name.title()} not found in DataHub"})
            
            # Extract and update
            item_data = self.extract_item_data(remote_response)
            self.update_local_item(item, item_data, current_connection)
            
            logger.info(f"Successfully resynced {self.entity_name}: {item_id} with {item_urn}")
            return JsonResponse({
                "success": True,
                "message": f"{self.entity_name.title()} resynced successfully",
                "sync_status": item.sync_status
            })
            
        except Exception as e:
            logger.error(f"Error resyncing {self.entity_name}: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})
    
    def delete_local_item(self, request, item_id):
        """
        Delete local item (local storage only, not DataHub).
        
        Args:
            request: Django request object
            item_id: Local item ID
        
        Returns:
            JsonResponse with success/error status
        """
        try:
            item = self.model_class.objects.get(id=item_id)
            item_name = item.name
            
            # Delete the local item
            item.delete()
            
            logger.info(f"Successfully deleted local {self.entity_name}: {item_id} ({item_name})")
            return JsonResponse({
                "success": True,
                "message": f"Local {self.entity_name} '{item_name}' deleted successfully"
            })
            
        except self.model_class.DoesNotExist:
            return JsonResponse({"success": False, "error": f"{self.entity_name.title()} not found"})
        except Exception as e:
            logger.error(f"Error deleting local {self.entity_name}: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


def create_sync_view_function(sync_operations_class):
    """
    Factory function to create sync view functions using a sync operations class.
    
    Args:
        sync_operations_class: Class implementing BaseSyncOperations
    
    Returns:
        Dictionary of view functions
    """
    sync_ops = sync_operations_class()
    
    @require_http_methods(["POST"])
    def sync_to_local(request):
        """Sync item from DataHub to local storage"""
        import json
        try:
            data = json.loads(request.body)
            urn = data.get('urn')
            return sync_ops.sync_to_local(request, urn=urn)
        except json.JSONDecodeError:
            return JsonResponse({"success": False, "error": "Invalid JSON data"})
    
    @require_http_methods(["POST"])
    def resync_item(request, item_id):
        """Resync existing local item with DataHub"""
        import json
        try:
            data = json.loads(request.body)
            urn = data.get('urn')
            return sync_ops.resync_item(request, item_id, urn=urn)
        except json.JSONDecodeError:
            return sync_ops.resync_item(request, item_id)
    
    @require_http_methods(["DELETE"])
    def delete_local_item(request, item_id):
        """Delete local item"""
        return sync_ops.delete_local_item(request, item_id)
    
    return {
        'sync_to_local': sync_to_local,
        'resync_item': resync_item,
        'delete_local_item': delete_local_item,
    } 