"""
Common remote data operations for metadata manager entities.
These provide reusable patterns for fetching and processing remote DataHub data.
"""
import logging
from abc import ABC, abstractmethod
from django.http import JsonResponse
from utils.datahub_client_adapter import test_datahub_connection

logger = logging.getLogger(__name__)


class BaseRemoteDataOperations(ABC):
    """
    Abstract base class for remote data operations.
    Provides common patterns for fetching and categorizing remote data.
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
    
    def get_local_items(self, request):
        """Get local items from database"""
        return self.model_class.objects.all().order_by("name")
    
    @abstractmethod
    def fetch_remote_items(self, client):
        """Fetch remote items from DataHub. Must be implemented by subclasses."""
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
        """
        Categorize items as synced/local-only/remote-only.
        
        Args:
            local_items: List of local model instances
            remote_items: List of remote item data dicts
            current_connection: Current connection context
        
        Returns:
            Tuple of (synced_items, local_only_items, remote_only_items)
        """
        synced_items = []
        local_only_items = []
        remote_only_items = []
        
        # Create lookup for remote items by URN
        remote_lookup = {item.get('urn'): item for item in remote_items if item.get('urn')}
        matched_remote_urns = set()
        
        # Process local items
        for local_item in local_items:
            local_data = self.process_local_item(local_item, current_connection)
            
            # Determine connection context for this item
            connection_context = "none"  # Default
            if local_item.connection is None:
                connection_context = "none"  # No connection
            elif current_connection and local_item.connection == current_connection:
                connection_context = "current"  # Current connection
            else:
                connection_context = "different"  # Different connection
            
            # Check for remote match
            item_urn = getattr(local_item, 'urn', None)
            remote_match = remote_lookup.get(item_urn) if item_urn else None
            
            # Apply categorization logic based on sync status and connection context
            sync_status = getattr(local_item, 'sync_status', 'LOCAL_ONLY')
            
            if (sync_status == "SYNCED" and 
                connection_context == "current" and 
                current_connection):
                # This is a synced item for the current connection
                if remote_match:
                    # Found in remote results - perfect sync
                    matched_remote_urns.add(item_urn)
                    synced_items.append({
                        "local": local_data,
                        "remote": remote_match,
                        "combined": {
                            **local_data,
                            "sync_status": "SYNCED",
                            "sync_status_display": "Synced",
                            "connection_context": connection_context,
                            "has_remote_match": True,
                        }
                    })
                else:
                    # Synced but not found in current remote search (could be indexing delay)
                    synced_items.append({
                        "local": local_data,
                        "remote": None,  # Not found in current search
                        "combined": {
                            **local_data,
                            "sync_status": "SYNCED",
                            "sync_status_display": "Synced (Remote Pending Index)",
                            "connection_context": connection_context,
                            "has_remote_match": False,
                        }
                    })
            else:
                # Local-only relative to current connection
                # This includes: different connection, no connection, or not synced
                local_data.update({
                    "connection_context": connection_context,
                    "has_remote_match": bool(remote_match),
                })
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
        
        # Count items with ownership
        owned_items = sum(1 for item in all_items 
                         if (item.get("combined", item) if "combined" in item else item).get("owners_count", 0) > 0)
        
        # Count items with relationships
        items_with_relationships = sum(1 for item in all_items 
                                     if (item.get("combined", item) if "combined" in item else item).get("relationships_count", 0) > 0)
        
        return {
            "total_items": len(all_items),
            "synced_count": len(synced_items),
            "local_only_count": len(local_only_items),
            "remote_only_count": len(remote_only_items),
            "owned_items": owned_items,
            "items_with_relationships": items_with_relationships,
        }
    
    def get_remote_data(self, request):
        """
        Main method to fetch and categorize remote data.
        
        Args:
            request: Django request object
        
        Returns:
            JsonResponse with categorized data and statistics
        """
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
            remote_items = self.fetch_remote_items(client)
            
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
            
            logger.debug(
                f"Enhanced {self.entity_name} categorization: {len(synced_items)} synced, "
                f"{len(local_only_items)} local-only, {len(remote_only_items)} remote-only"
            )
            
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


def create_remote_data_view_function(remote_data_operations_class):
    """
    Factory function to create remote data view functions using a remote data operations class.
    
    Args:
        remote_data_operations_class: Class implementing BaseRemoteDataOperations
    
    Returns:
        View function for getting remote data
    """
    remote_ops = remote_data_operations_class()
    
    def get_remote_data(request):
        """Get remote data with categorization"""
        return remote_ops.get_remote_data(request)
    
    return get_remote_data 