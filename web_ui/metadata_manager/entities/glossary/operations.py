"""
Glossary-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class GlossaryNodeSyncOperations(BaseSyncOperations):
    """Glossary node-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "glossary_nodes"
    
    def get_sync_script_path(self) -> str:
        return "scripts/glossary/sync_glossary_nodes.py"
    
    def get_entity_display_name(self) -> str:
        return "Glossary Node"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate glossary node-specific data."""
        required_fields = ['name', 'description']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform glossary node data for sync."""
        return {
            'name': entity_data.get('name', ''),
            'description': entity_data.get('description', ''),
            'parent_node': entity_data.get('parent_node'),
            'owners': entity_data.get('owners', []),
            'structured_properties': entity_data.get('structured_properties', {}),
        }


class GlossaryTermSyncOperations(BaseSyncOperations):
    """Glossary term-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "glossary_terms"
    
    def get_sync_script_path(self) -> str:
        return "scripts/glossary/sync_glossary_terms.py"
    
    def get_entity_display_name(self) -> str:
        return "Glossary Term"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate glossary term-specific data."""
        required_fields = ['name', 'definition', 'parent_node']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform glossary term data for sync."""
        return {
            'name': entity_data.get('name', ''),
            'description': entity_data.get('description', ''),
            'definition': entity_data.get('definition', ''),
            'parent_node': entity_data.get('parent_node'),
            'related_terms': entity_data.get('related_terms', []),
            'owners': entity_data.get('owners', []),
            'structured_properties': entity_data.get('structured_properties', {}),
        }


class GlossaryNodeStagingOperations(BaseStagingOperations):
    """Glossary node-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "glossary_nodes"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/glossary/create_glossary_node.py"
    
    def get_entity_display_name(self) -> str:
        return "Glossary Node"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate glossary node staging data."""
        required_fields = ['name', 'description']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform glossary node data for staging."""
        return {
            'name': staging_data.get('name', ''),
            'description': staging_data.get('description', ''),
            'parent_node': staging_data.get('parent_node'),
            'owners': staging_data.get('owners', []),
            'structured_properties': staging_data.get('structured_properties', {}),
        }


class GlossaryTermStagingOperations(BaseStagingOperations):
    """Glossary term-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "glossary_terms"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/glossary/create_glossary_term.py"
    
    def get_entity_display_name(self) -> str:
        return "Glossary Term"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate glossary term staging data."""
        required_fields = ['name', 'definition', 'parent_node']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform glossary term data for staging."""
        return {
            'name': staging_data.get('name', ''),
            'description': staging_data.get('description', ''),
            'definition': staging_data.get('definition', ''),
            'parent_node': staging_data.get('parent_node'),
            'related_terms': staging_data.get('related_terms', []),
            'owners': staging_data.get('owners', []),
            'structured_properties': staging_data.get('structured_properties', {}),
        }


class GlossaryGitOperations(BaseGitOperations):
    """Glossary-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "glossary"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/glossary/{name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update glossary: {name}"
    
    def get_pr_title_template(self) -> str:
        return "Glossary Updates: {name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Glossary Changes

### Item: {name}

**Description:** {description}

### Changes Made:
- Updated glossary metadata
- Modified structured properties
- Updated ownership information
- Updated relationships

### Review Checklist:
- [ ] Glossary name follows naming conventions
- [ ] Description is clear and comprehensive
- [ ] Owners are correctly assigned
- [ ] Relationships are properly defined
- [ ] Structured properties are valid
        """


class GlossaryRemoteDataOperations(BaseRemoteDataOperations):
    """Glossary-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "glossary"
    
    def get_remote_data_method(self) -> str:
        return "get_comprehensive_glossary"
    
    def get_entity_display_name(self) -> str:
        return "Glossary"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote glossary data for display."""
        transformed = []
        
        # Handle both nodes and terms
        nodes = remote_data.get('nodes', []) if isinstance(remote_data, dict) else []
        terms = remote_data.get('terms', []) if isinstance(remote_data, dict) else []
        
        # Transform nodes
        for node in nodes:
            transformed.append({
                'type': 'node',
                'urn': node.get('urn', ''),
                'name': node.get('name', ''),
                'description': node.get('description', ''),
                'owners': node.get('owners', []),
                'parent_node': node.get('parent_node'),
                'child_nodes': node.get('child_nodes', []),
                'terms': node.get('terms', []),
                'structured_properties': node.get('structured_properties', {}),
            })
        
        # Transform terms
        for term in terms:
            transformed.append({
                'type': 'term',
                'urn': term.get('urn', ''),
                'name': term.get('name', ''),
                'description': term.get('description', ''),
                'definition': term.get('definition', ''),
                'owners': term.get('owners', []),
                'parent_node': term.get('parent_node'),
                'related_terms': term.get('related_terms', []),
                'entity_count': term.get('entity_count', 0),
                'structured_properties': term.get('structured_properties', {}),
            })
        
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for glossary item."""
        return entity_data.get('urn', entity_data.get('name', '')) 