"""
Domain-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class DomainSyncOperations(BaseSyncOperations):
    """Domain-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "domains"
    
    def get_sync_script_path(self) -> str:
        return "scripts/domains/sync_domains.py"
    
    def get_entity_display_name(self) -> str:
        return "Domain"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate domain-specific data."""
        required_fields = ['name', 'description']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform domain data for sync."""
        return {
            'name': entity_data.get('name', ''),
            'description': entity_data.get('description', ''),
            'owners': entity_data.get('owners', []),
            'structured_properties': entity_data.get('structured_properties', {}),
        }


class DomainStagingOperations(BaseStagingOperations):
    """Domain-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "domains"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/domains/create_domain.py"
    
    def get_entity_display_name(self) -> str:
        return "Domain"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate domain staging data."""
        required_fields = ['name', 'description']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform domain data for staging."""
        return {
            'name': staging_data.get('name', ''),
            'description': staging_data.get('description', ''),
            'owners': staging_data.get('owners', []),
            'structured_properties': staging_data.get('structured_properties', {}),
        }


class DomainGitOperations(BaseGitOperations):
    """Domain-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "domains"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/domains/{name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update domain: {name}"
    
    def get_pr_title_template(self) -> str:
        return "Domain Updates: {name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Domain Changes

### Domain: {name}

**Description:** {description}

### Changes Made:
- Updated domain metadata
- Modified structured properties
- Updated ownership information

### Review Checklist:
- [ ] Domain name follows naming conventions
- [ ] Description is clear and comprehensive
- [ ] Owners are correctly assigned
- [ ] Structured properties are valid
        """


class DomainRemoteDataOperations(BaseRemoteDataOperations):
    """Domain-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "domains"
    
    def get_remote_data_method(self) -> str:
        return "list_domains"
    
    def get_entity_display_name(self) -> str:
        return "Domain"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote domain data for display."""
        transformed = []
        for domain in remote_data:
            transformed.append({
                'urn': domain.get('urn', ''),
                'name': domain.get('name', ''),
                'description': domain.get('description', ''),
                'owners': domain.get('owners', []),
                'entity_count': domain.get('entity_count', 0),
                'structured_properties': domain.get('structured_properties', {}),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for domain."""
        return entity_data.get('urn', entity_data.get('name', '')) 