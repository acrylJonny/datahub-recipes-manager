"""
Common staging operations for metadata manager entities.
These provide reusable patterns for MCP (staged changes) operations.
"""
import logging
import json
import os
import sys
from abc import ABC, abstractmethod
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.views import View

logger = logging.getLogger(__name__)


class BaseStagingOperations(ABC):
    """
    Abstract base class for staging operations.
    Provides common patterns for adding items to staged changes (MCPs).
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
    
    @property
    @abstractmethod
    def staging_script_module(self):
        """Module path for the staging script (e.g., 'scripts.mcps.tag_actions')"""
        pass
    
    @property
    @abstractmethod
    def staging_function_name(self):
        """Function name in the staging script (e.g., 'add_tag_to_staged_changes')"""
        pass
    
    def get_staging_function(self):
        """Import and return the staging function"""
        # Add project root to path
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        )
        sys.path.append(project_root)
        
        # Import the function
        module = __import__(self.staging_script_module, fromlist=[self.staging_function_name])
        return getattr(module, self.staging_function_name)
    
    def get_environment_info(self, request, data):
        """Extract environment and mutation info from request"""
        environment_name = data.get('environment', 'dev')
        mutation_name = data.get('mutation_name')
        owner = request.user.username if request.user.is_authenticated else "admin"
        
        return environment_name, mutation_name, owner
    
    @abstractmethod
    def prepare_item_data(self, item):
        """Prepare item data for staging. Must be implemented by subclasses."""
        pass
    
    def stage_local_item(self, request, item_id):
        """
        Stage a local/synced item to staged changes.
        
        Args:
            request: Django request object
            item_id: Local item ID
        
        Returns:
            JsonResponse with success/error status
        """
        try:
            # Get the item
            try:
                item = self.model_class.objects.get(id=item_id)
            except self.model_class.DoesNotExist:
                return JsonResponse({
                    'success': False,
                    'error': f'{self.entity_name.title()} not found'
                }, status=404)
            
            # Parse request data
            data = json.loads(request.body)
            environment_name, mutation_name, owner = self.get_environment_info(request, data)
            
            logger.info(f"Adding {self.entity_name} '{item.name}' to staged changes...")
            
            # Prepare item data
            item_data = self.prepare_item_data(item)
            
            # Get staging function and call it
            staging_function = self.get_staging_function()
            result = staging_function(
                **item_data,
                environment=environment_name,
                owner=owner,
                mutation_name=mutation_name
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "success": False,
                    "error": result.get("message", f"Failed to add {self.entity_name} to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"{self.entity_name.title()} added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            return JsonResponse({
                'success': True,
                'message': message,
                'files_created': files_created,
                'files_created_count': files_created_count,
                'mcps_created': mcps_created,
                'item_id': str(item.id),
                'item_urn': getattr(item, 'urn', None)
            })
            
        except Exception as e:
            logger.error(f"Error adding {self.entity_name} to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)
    
    def stage_remote_item(self, request):
        """
        Stage a remote-only item to staged changes without syncing to local first.
        
        Args:
            request: Django request object
        
        Returns:
            JsonResponse with success/error status
        """
        try:
            # Parse request data
            data = json.loads(request.body)
            item_data = data.get('item_data')
            
            if not item_data:
                return JsonResponse({
                    "status": "error",
                    "error": f"{self.entity_name.title()} data is required"
                }, status=400)
            
            environment_name, mutation_name, owner = self.get_environment_info(request, data)
            
            # Extract item information from remote data
            item_urn = item_data.get('urn')
            if not item_urn:
                return JsonResponse({
                    "status": "error",
                    "error": f"{self.entity_name.title()} URN is required"
                }, status=400)
            
            logger.info(f"Adding remote {self.entity_name} '{item_data.get('name', 'Unknown')}' to staged changes...")
            
            # Prepare remote item data
            prepared_data = self.prepare_remote_item_data(item_data)
            
            # Get staging function and call it
            staging_function = self.get_staging_function()
            result = staging_function(
                **prepared_data,
                environment=environment_name,
                owner=owner,
                mutation_name=mutation_name
            )
            
            if not result.get("success"):
                return JsonResponse({
                    "status": "error",
                    "error": result.get("message", f"Failed to add remote {self.entity_name} to staged changes")
                }, status=500)
            
            # Provide feedback about the operation
            files_created = result.get("files_saved", [])
            files_created_count = len(files_created)
            mcps_created = result.get("mcps_created", 0)
            
            message = f"Remote {self.entity_name} added to staged changes: {mcps_created} MCPs created, {files_created_count} files saved"
            
            return JsonResponse({
                "status": "success",
                "message": message,
                "files_created": files_created,
                "files_created_count": files_created_count,
                "mcps_created": mcps_created,
                "item_urn": item_urn
            })
                
        except Exception as e:
            logger.error(f"Error adding remote {self.entity_name} to staged changes: {str(e)}")
            return JsonResponse({"error": str(e)}, status=500)
    
    @abstractmethod
    def prepare_remote_item_data(self, item_data):
        """Prepare remote item data for staging. Must be implemented by subclasses."""
        pass


class BaseStagingView(View, ABC):
    """
    Abstract base class for staging views.
    Provides common patterns for staging view endpoints.
    """
    
    @property
    @abstractmethod
    def staging_operations_class(self):
        """Staging operations class for this entity"""
        pass
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.staging_ops = self.staging_operations_class()
    
    @method_decorator(csrf_exempt, name="dispatch")
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)


class LocalItemStagingView(BaseStagingView):
    """View for staging local/synced items"""
    
    def post(self, request, item_id):
        return self.staging_ops.stage_local_item(request, item_id)


class RemoteItemStagingView(BaseStagingView):
    """View for staging remote-only items"""
    
    def post(self, request):
        return self.staging_ops.stage_remote_item(request)


def create_staging_view_classes(staging_operations_class):
    """
    Factory function to create staging view classes using a staging operations class.
    
    Args:
        staging_operations_class: Class implementing BaseStagingOperations
    
    Returns:
        Dictionary of view classes
    """
    
    class LocalStagingView(LocalItemStagingView):
        @property
        def staging_operations_class(self):
            return staging_operations_class
    
    class RemoteStagingView(RemoteItemStagingView):
        @property
        def staging_operations_class(self):
            return staging_operations_class
    
    return {
        'LocalStagingView': LocalStagingView,
        'RemoteStagingView': RemoteStagingView,
    } 