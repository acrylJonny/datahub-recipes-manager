"""
Tests-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class TestSyncOperations(BaseSyncOperations):
    """Test-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "tests"
    
    def get_sync_script_path(self) -> str:
        return "scripts/metadata_tests/entity_relationship_tests.py"
    
    def get_entity_display_name(self) -> str:
        return "Test"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate test-specific data."""
        required_fields = ['name', 'test_type', 'entity_urn']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform test data for sync."""
        return {
            'name': entity_data.get('name', ''),
            'description': entity_data.get('description', ''),
            'test_type': entity_data.get('test_type', 'UNIT'),
            'entity_urn': entity_data.get('entity_urn', ''),
            'test_definition': entity_data.get('test_definition', {}),
            'expected_result': entity_data.get('expected_result', ''),
            'test_category': entity_data.get('test_category', 'DATA_QUALITY'),
            'owners': entity_data.get('owners', []),
            'tags': entity_data.get('tags', []),
            'glossary_terms': entity_data.get('glossary_terms', []),
            'properties': entity_data.get('properties', {}),
        }


class TestStagingOperations(BaseStagingOperations):
    """Test-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "tests"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/metadata_tests/test_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Test"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate test staging data."""
        required_fields = ['name', 'test_type']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform test data for staging."""
        return {
            'name': staging_data.get('name', ''),
            'description': staging_data.get('description', ''),
            'test_type': staging_data.get('test_type', 'UNIT'),
            'entity_urn': staging_data.get('entity_urn', ''),
            'test_definition': staging_data.get('test_definition', {}),
            'expected_result': staging_data.get('expected_result', ''),
            'test_category': staging_data.get('test_category', 'DATA_QUALITY'),
            'owners': staging_data.get('owners', []),
            'tags': staging_data.get('tags', []),
            'glossary_terms': staging_data.get('glossary_terms', []),
            'properties': staging_data.get('properties', {}),
        }


class TestGitOperations(BaseGitOperations):
    """Test-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "tests"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/metadata_tests/{name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update test: {name}"
    
    def get_pr_title_template(self) -> str:
        return "Test Updates: {name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Test Changes

### Test: {name}

**Type:** {test_type}
**Category:** {test_category}
**Entity:** {entity_urn}
**Description:** {description}

### Changes Made:
- Updated test definition
- Modified test logic
- Updated expected results
- Updated entity associations
- Updated ownership information

### Review Checklist:
- [ ] Test name follows naming conventions
- [ ] Test type is appropriate
- [ ] Test category is correct
- [ ] Entity URN is valid (if applicable)
- [ ] Test definition is complete
- [ ] Expected results are realistic
- [ ] Owners are properly assigned
        """


class TestRemoteDataOperations(BaseRemoteDataOperations):
    """Test-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "tests"
    
    def get_remote_data_method(self) -> str:
        return "list_tests"
    
    def get_entity_display_name(self) -> str:
        return "Test"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote test data for display."""
        transformed = []
        for test in remote_data:
            transformed.append({
                'urn': test.get('urn', ''),
                'name': test.get('name', ''),
                'description': test.get('description', ''),
                'test_type': test.get('test_type', 'UNKNOWN'),
                'entity_urn': test.get('entity_urn', ''),
                'entity_name': self._extract_entity_name(test.get('entity_urn', '')),
                'test_definition': test.get('test_definition', {}),
                'expected_result': test.get('expected_result', ''),
                'test_category': test.get('test_category', 'DATA_QUALITY'),
                'owners': test.get('owners', []),
                'tags': test.get('tags', []),
                'glossary_terms': test.get('glossary_terms', []),
                'properties': test.get('properties', {}),
                'last_updated': test.get('last_updated', ''),
                'status': test.get('status', 'ACTIVE'),
                'last_run': test.get('last_run', ''),
                'last_result': test.get('last_result', 'UNKNOWN'),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for test."""
        return entity_data.get('urn', entity_data.get('name', ''))
    
    def _extract_entity_name(self, entity_urn: str) -> str:
        """Extract entity name from URN for display."""
        if not entity_urn:
            return ''
        # Extract the last part of the URN as the entity name
        parts = entity_urn.split(',')
        if parts:
            return parts[-1].split(')')[-1] if ')' in parts[-1] else parts[-1]
        return entity_urn 