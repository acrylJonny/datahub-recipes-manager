"""
Tag-specific operations using common base classes.
This demonstrates how to implement entity-specific logic while reusing common patterns.
"""
import logging
from django.utils import timezone
from web_ui.metadata_manager.models import Tag
from web_ui.metadata_manager.common.sync_operations import BaseSyncOperations
from web_ui.metadata_manager.common.staging_operations import BaseStagingOperations
from web_ui.metadata_manager.common.git_operations import BaseGitOperations
from web_ui.metadata_manager.common.remote_data_operations import BaseRemoteDataOperations
from web_ui.metadata_manager.common.utils import (
    extract_ownership_data, extract_relationships_data, sanitize_for_json
)

logger = logging.getLogger(__name__)


class TagSyncOperations(BaseSyncOperations):
    """Tag-specific sync operations"""
    
    @property
    def entity_name(self):
        return "tag"
    
    @property
    def model_class(self):
        return Tag
    
    def extract_item_data(self, remote_data):
        """Extract standardized tag data from remote response"""
        if isinstance(remote_data, list) and len(remote_data) > 0:
            tag_data = remote_data[0].get("entity", {})
        else:
            tag_data = remote_data
        
        # Extract basic properties
        properties = tag_data.get("properties", {})
        
        # Extract ownership and relationships
        ownership_data, owners_count, owner_names = extract_ownership_data(tag_data)
        relationships_data, relationships_count = extract_relationships_data(tag_data)
        
        return {
            "urn": tag_data.get("urn"),
            "name": properties.get("name", ""),
            "description": properties.get("description", ""),
            "color_hex": properties.get("colorHex"),
            "ownership_data": ownership_data,
            "relationships_data": relationships_data,
            "raw_data": tag_data,
        }
    
    def create_local_item(self, item_data, current_connection):
        """Create local tag from remote data"""
        tag = Tag.objects.create(
            name=item_data["name"],
            description=item_data["description"],
            urn=item_data["urn"],
            color_hex=item_data.get("color_hex"),
            sync_status="SYNCED",
            last_synced=timezone.now(),
            connection=current_connection,
            ownership_data=item_data.get("ownership_data"),
            relationships_data=item_data.get("relationships_data"),
            raw_data=item_data.get("raw_data"),
        )
        return tag
    
    def update_local_item(self, local_item, item_data, current_connection):
        """Update existing local tag with remote data"""
        local_item.name = item_data["name"]
        local_item.description = item_data["description"]
        local_item.color_hex = item_data.get("color_hex")
        local_item.sync_status = "SYNCED"
        local_item.last_synced = timezone.now()
        local_item.connection = current_connection
        local_item.ownership_data = item_data.get("ownership_data")
        local_item.relationships_data = item_data.get("relationships_data")
        local_item.raw_data = item_data.get("raw_data")
        local_item.save()
        return local_item
    
    def get_remote_item(self, client, urn):
        """Get remote tag by URN"""
        return client.get_tag(urn)


class TagStagingOperations(BaseStagingOperations):
    """Tag-specific staging operations"""
    
    @property
    def entity_name(self):
        return "tag"
    
    @property
    def model_class(self):
        return Tag
    
    @property
    def staging_script_module(self):
        return "scripts.mcps.tag_actions"
    
    @property
    def staging_function_name(self):
        return "add_tag_to_staged_changes"
    
    def prepare_item_data(self, item):
        """Prepare tag data for staging"""
        return {
            "tag_id": str(item.id),
            "tag_urn": item.urn,
            "tag_name": item.name,
            "description": item.description,
            "color_hex": item.color_hex,
        }
    
    def prepare_remote_item_data(self, item_data):
        """Prepare remote tag data for staging"""
        # Extract tag ID from URN (last part after colon)
        tag_urn = item_data.get('urn')
        tag_id = tag_urn.split(':')[-1] if tag_urn else None
        
        return {
            "tag_id": tag_id,
            "tag_urn": tag_urn,
            "tag_name": item_data.get('name', 'Unknown Tag'),
            "description": item_data.get('description', ''),
            "color_hex": item_data.get('color_hex'),
        }


class TagGitOperations(BaseGitOperations):
    """Tag-specific Git operations"""
    
    @property
    def entity_name(self):
        return "tag"
    
    @property
    def model_class(self):
        return Tag
    
    def create_git_item_wrapper(self, item, environment):
        """Create a tag wrapper object that GitIntegration can handle"""
        class TagForGit:
            def __init__(self, tag, environment):
                self.id = tag.id
                self.name = tag.name
                self.description = tag.description
                self.color_hex = tag.color_hex
                self.environment = environment
                self.sync_status = tag.sync_status
                
            def to_dict(self):
                """Convert tag to dictionary format for file output"""
                operation = "create"  # Default for local tags
                if self.sync_status == "SYNCED":
                    operation = "update"
                
                return {
                    "operation": operation,
                    "name": self.name,
                    "description": self.description,
                    "color_hex": self.color_hex,
                    "local_id": str(self.id),
                }
        
        return TagForGit(item, environment)


class TagRemoteDataOperations(BaseRemoteDataOperations):
    """Tag-specific remote data operations"""
    
    @property
    def entity_name(self):
        return "tag"
    
    @property
    def model_class(self):
        return Tag
    
    def fetch_remote_items(self, client):
        """Fetch remote tags from DataHub"""
        try:
            # Get all remote tags from DataHub
            result = client.list_tags(count=1000)
            if not result:
                return []
            
            remote_tags = []
            for tag_data in result:
                if tag_data:
                    # Extract ownership and relationships
                    ownership_data, owners_count, owner_names = extract_ownership_data(tag_data)
                    relationships_data, relationships_count = extract_relationships_data(tag_data)
                    
                    # Process tag properties
                    properties = tag_data.get("properties", {})
                    
                    enhanced_tag = {
                        "urn": tag_data.get("urn"),
                        "name": properties.get("name", ""),
                        "description": properties.get("description", ""),
                        "color_hex": properties.get("colorHex"),
                        "sync_status": "REMOTE_ONLY",
                        "sync_status_display": "Remote Only",
                        
                        # Ownership data
                        "ownership": ownership_data,
                        "owners_count": owners_count,
                        "owner_names": owner_names,
                        
                        # Relationships data
                        "relationships": relationships_data,
                        "relationships_count": relationships_count,
                        
                        # Raw data
                        "raw_data": tag_data
                    }
                    
                    remote_tags.append(enhanced_tag)
            
            return remote_tags
            
        except Exception as e:
            logger.error(f"Error fetching remote tags: {str(e)}")
            return []
    
    def process_local_item(self, item, current_connection):
        """Process a local tag into standardized format"""
        # Determine connection context
        connection_context = "none"
        if item.connection is None:
            connection_context = "none"
        elif current_connection and item.connection == current_connection:
            connection_context = "current"
        else:
            connection_context = "different"
        
        return {
            "id": str(item.id),
            "urn": item.urn,
            "name": item.name,
            "description": item.description or "",
            "color_hex": item.color_hex,
            "sync_status": item.sync_status,
            "sync_status_display": item.get_sync_status_display(),
            "connection_context": connection_context,
            
            # Ownership data
            "ownership": item.ownership_data,
            "owners_count": len(item.ownership_data.get('owners', [])) if item.ownership_data else 0,
            "owner_names": self._extract_owner_names(item.ownership_data),
            
            # Relationships data
            "relationships": item.relationships_data,
            "relationships_count": len(item.relationships_data.get('relationships', [])) if item.relationships_data else 0,
            
            # Timestamps
            "last_synced": item.last_synced.isoformat() if item.last_synced else None,
            "created_at": item.created_at.isoformat() if item.created_at else None,
            "updated_at": item.updated_at.isoformat() if item.updated_at else None,
        }
    
    def process_remote_item(self, item_data):
        """Process a remote tag into standardized format"""
        return {
            **item_data,
            "sync_status": "REMOTE_ONLY",
            "sync_status_display": "Remote Only",
        }
    
    def _extract_owner_names(self, ownership_data):
        """Extract owner names from ownership data"""
        if not ownership_data or not ownership_data.get("owners"):
            return []
        
        owner_names = []
        for owner_info in ownership_data["owners"]:
            owner = owner_info.get("owner", {})
            if owner.get("properties"):
                name = (
                    owner["properties"].get("displayName") or
                    owner.get("username") or
                    owner.get("name") or
                    "Unknown"
                )
            else:
                name = owner.get("username") or owner.get("name") or "Unknown"
            owner_names.append(name)
        
        return owner_names 