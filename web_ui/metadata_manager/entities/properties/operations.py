"""
Structured Properties-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class PropertySyncOperations(BaseSyncOperations):
    """Structured property-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "structured_properties"
    
    def get_sync_script_path(self) -> str:
        return "scripts/structured_properties/sync_properties.py"
    
    def get_entity_display_name(self) -> str:
        return "Structured Property"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate structured property-specific data."""
        required_fields = ['qualified_name', 'value_type', 'cardinality']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform structured property data for sync."""
        return {
            'qualified_name': entity_data.get('qualified_name', ''),
            'display_name': entity_data.get('display_name', ''),
            'description': entity_data.get('description', ''),
            'value_type': entity_data.get('value_type', 'STRING'),
            'cardinality': entity_data.get('cardinality', 'SINGLE'),
            'allowed_values': entity_data.get('allowed_values', []),
            'entity_types': entity_data.get('entity_types', []),
            'owners': entity_data.get('owners', []),
        }


class PropertyStagingOperations(BaseStagingOperations):
    """Structured property-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "structured_properties"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/structured_properties/create_property.py"
    
    def get_entity_display_name(self) -> str:
        return "Structured Property"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate structured property staging data."""
        required_fields = ['qualified_name', 'value_type']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform structured property data for staging."""
        return {
            'qualified_name': staging_data.get('qualified_name', ''),
            'display_name': staging_data.get('display_name', ''),
            'description': staging_data.get('description', ''),
            'value_type': staging_data.get('value_type', 'STRING'),
            'cardinality': staging_data.get('cardinality', 'SINGLE'),
            'allowed_values': staging_data.get('allowed_values', []),
            'entity_types': staging_data.get('entity_types', []),
            'owners': staging_data.get('owners', []),
        }


class PropertyGitOperations(BaseGitOperations):
    """Structured property-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "structured_properties"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/structured_properties/{qualified_name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update structured property: {qualified_name}"
    
    def get_pr_title_template(self) -> str:
        return "Structured Property Updates: {qualified_name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Structured Property Changes

### Property: {qualified_name}

**Display Name:** {display_name}
**Description:** {description}
**Value Type:** {value_type}
**Cardinality:** {cardinality}

### Changes Made:
- Updated property definition
- Modified value constraints
- Updated entity type associations
- Updated ownership information

### Review Checklist:
- [ ] Qualified name follows naming conventions
- [ ] Value type is appropriate for use case
- [ ] Cardinality is correctly specified
- [ ] Entity types are properly configured
- [ ] Owners are correctly assigned
- [ ] Allowed values are valid (if applicable)
        """


class PropertyRemoteDataOperations(BaseRemoteDataOperations):
    """Structured property-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "structured_properties"
    
    def get_remote_data_method(self) -> str:
        return "list_structured_properties"
    
    def get_entity_display_name(self) -> str:
        return "Structured Property"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote structured property data for display."""
        transformed = []
        for prop in remote_data:
            transformed.append({
                'urn': prop.get('urn', ''),
                'qualified_name': prop.get('qualified_name', ''),
                'display_name': prop.get('display_name', ''),
                'description': prop.get('description', ''),
                'value_type': prop.get('value_type', 'STRING'),
                'cardinality': prop.get('cardinality', 'SINGLE'),
                'allowed_values': prop.get('allowed_values', []),
                'entity_types': prop.get('entity_types', []),
                'owners': prop.get('owners', []),
                'usage_count': prop.get('usage_count', 0),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for structured property."""
        return entity_data.get('urn', entity_data.get('qualified_name', '')) 