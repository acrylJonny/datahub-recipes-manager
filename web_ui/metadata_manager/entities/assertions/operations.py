"""
Assertions-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class AssertionSyncOperations(BaseSyncOperations):
    """Assertion-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "assertions"
    
    def get_sync_script_path(self) -> str:
        return "scripts/assertions/create_field_assertion.py"
    
    def get_entity_display_name(self) -> str:
        return "Assertion"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate assertion-specific data."""
        required_fields = ['entity_urn', 'assertion_type', 'field_path']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform assertion data for sync."""
        return {
            'entity_urn': entity_data.get('entity_urn', ''),
            'assertion_type': entity_data.get('assertion_type', 'FIELD'),
            'field_path': entity_data.get('field_path', ''),
            'operator': entity_data.get('operator', 'EQUAL_TO'),
            'expected_value': entity_data.get('expected_value', ''),
            'description': entity_data.get('description', ''),
            'logic': entity_data.get('logic', 'AND'),
            'source': entity_data.get('source', 'EXTERNAL'),
            'owners': entity_data.get('owners', []),
            'tags': entity_data.get('tags', []),
            'glossary_terms': entity_data.get('glossary_terms', []),
            'properties': entity_data.get('properties', {}),
        }


class AssertionStagingOperations(BaseStagingOperations):
    """Assertion-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "assertions"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/assertions/assertion_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Assertion"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate assertion staging data."""
        required_fields = ['entity_urn', 'assertion_type']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform assertion data for staging."""
        return {
            'entity_urn': staging_data.get('entity_urn', ''),
            'assertion_type': staging_data.get('assertion_type', 'FIELD'),
            'field_path': staging_data.get('field_path', ''),
            'operator': staging_data.get('operator', 'EQUAL_TO'),
            'expected_value': staging_data.get('expected_value', ''),
            'description': staging_data.get('description', ''),
            'logic': staging_data.get('logic', 'AND'),
            'source': staging_data.get('source', 'EXTERNAL'),
            'owners': staging_data.get('owners', []),
            'tags': staging_data.get('tags', []),
            'glossary_terms': staging_data.get('glossary_terms', []),
            'properties': staging_data.get('properties', {}),
        }


class AssertionGitOperations(BaseGitOperations):
    """Assertion-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "assertions"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/assertions/{assertion_name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update assertion: {assertion_name}"
    
    def get_pr_title_template(self) -> str:
        return "Assertion Updates: {assertion_name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Assertion Changes

### Assertion: {assertion_name}

**Entity:** {entity_urn}
**Type:** {assertion_type}
**Field Path:** {field_path}
**Operator:** {operator}
**Expected Value:** {expected_value}

### Changes Made:
- Updated assertion definition
- Modified validation rules
- Updated field path specifications
- Updated expected values
- Updated ownership information

### Review Checklist:
- [ ] Entity URN is valid and exists
- [ ] Assertion type is appropriate
- [ ] Field path is correct
- [ ] Operator is suitable for the field type
- [ ] Expected value is realistic
- [ ] Logic is correctly specified
- [ ] Owners are properly assigned
        """


class AssertionRemoteDataOperations(BaseRemoteDataOperations):
    """Assertion-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "assertions"
    
    def get_remote_data_method(self) -> str:
        return "list_assertions"
    
    def get_entity_display_name(self) -> str:
        return "Assertion"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote assertion data for display."""
        transformed = []
        for assertion in remote_data:
            transformed.append({
                'urn': assertion.get('urn', ''),
                'entity_urn': assertion.get('entity_urn', ''),
                'entity_name': self._extract_entity_name(assertion.get('entity_urn', '')),
                'assertion_type': assertion.get('assertion_type', 'UNKNOWN'),
                'field_path': assertion.get('field_path', ''),
                'operator': assertion.get('operator', ''),
                'expected_value': assertion.get('expected_value', ''),
                'description': assertion.get('description', ''),
                'logic': assertion.get('logic', 'AND'),
                'source': assertion.get('source', 'EXTERNAL'),
                'owners': assertion.get('owners', []),
                'tags': assertion.get('tags', []),
                'glossary_terms': assertion.get('glossary_terms', []),
                'properties': assertion.get('properties', {}),
                'last_updated': assertion.get('last_updated', ''),
                'status': assertion.get('status', 'ACTIVE'),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for assertion."""
        return entity_data.get('urn', f"{entity_data.get('entity_urn', '')}-{entity_data.get('field_path', '')}")
    
    def _extract_entity_name(self, entity_urn: str) -> str:
        """Extract entity name from URN for display."""
        if not entity_urn:
            return ''
        # Extract the last part of the URN as the entity name
        parts = entity_urn.split(',')
        if parts:
            return parts[-1].split(')')[-1] if ')' in parts[-1] else parts[-1]
        return entity_urn 