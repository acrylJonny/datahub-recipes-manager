"""
Data Contracts-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class DataContractSyncOperations(BaseSyncOperations):
    """Data contract-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "data_contracts"
    
    def get_sync_script_path(self) -> str:
        return "scripts/mcps/cli_data_contract_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Data Contract"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate data contract-specific data."""
        required_fields = ['entity_urn', 'schema', 'status']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data contract data for sync."""
        return {
            'entity_urn': entity_data.get('entity_urn', ''),
            'schema': entity_data.get('schema', {}),
            'status': entity_data.get('status', 'DRAFT'),
            'properties': entity_data.get('properties', {}),
            'freshness': entity_data.get('freshness', {}),
            'data_quality': entity_data.get('data_quality', {}),
            'schema_metadata': entity_data.get('schema_metadata', {}),
            'owners': entity_data.get('owners', []),
            'tags': entity_data.get('tags', []),
            'glossary_terms': entity_data.get('glossary_terms', []),
        }


class DataContractStagingOperations(BaseStagingOperations):
    """Data contract-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "data_contracts"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/mcps/cli_data_contract_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Data Contract"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate data contract staging data."""
        required_fields = ['entity_urn', 'schema']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data contract data for staging."""
        return {
            'entity_urn': staging_data.get('entity_urn', ''),
            'schema': staging_data.get('schema', {}),
            'status': staging_data.get('status', 'DRAFT'),
            'properties': staging_data.get('properties', {}),
            'freshness': staging_data.get('freshness', {}),
            'data_quality': staging_data.get('data_quality', {}),
            'schema_metadata': staging_data.get('schema_metadata', {}),
            'owners': staging_data.get('owners', []),
            'tags': staging_data.get('tags', []),
            'glossary_terms': staging_data.get('glossary_terms', []),
        }


class DataContractGitOperations(BaseGitOperations):
    """Data contract-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "data_contracts"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/data_contracts/{entity_name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update data contract: {entity_name}"
    
    def get_pr_title_template(self) -> str:
        return "Data Contract Updates: {entity_name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Data Contract Changes

### Contract for: {entity_urn}

**Status:** {status}
**Schema Version:** {schema_version}

### Changes Made:
- Updated contract definition
- Modified schema specifications
- Updated data quality rules
- Updated freshness requirements
- Updated ownership information

### Review Checklist:
- [ ] Entity URN is valid and exists
- [ ] Schema definition is complete
- [ ] Data quality rules are appropriate
- [ ] Freshness requirements are realistic
- [ ] Status is correctly set
- [ ] Owners are properly assigned
        """


class DataContractRemoteDataOperations(BaseRemoteDataOperations):
    """Data contract-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "data_contracts"
    
    def get_remote_data_method(self) -> str:
        return "get_data_contracts"
    
    def get_entity_display_name(self) -> str:
        return "Data Contract"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote data contract data for display."""
        transformed = []
        for contract in remote_data:
            transformed.append({
                'urn': contract.get('urn', ''),
                'entity_urn': contract.get('entity_urn', ''),
                'entity_name': self._extract_entity_name(contract.get('entity_urn', '')),
                'schema': contract.get('schema', {}),
                'status': contract.get('status', 'UNKNOWN'),
                'properties': contract.get('properties', {}),
                'freshness': contract.get('freshness', {}),
                'data_quality': contract.get('data_quality', {}),
                'schema_metadata': contract.get('schema_metadata', {}),
                'owners': contract.get('owners', []),
                'tags': contract.get('tags', []),
                'glossary_terms': contract.get('glossary_terms', []),
                'last_updated': contract.get('last_updated', ''),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for data contract."""
        return entity_data.get('urn', entity_data.get('entity_urn', ''))
    
    def _extract_entity_name(self, entity_urn: str) -> str:
        """Extract entity name from URN for display."""
        if not entity_urn:
            return ''
        # Extract the last part of the URN as the entity name
        parts = entity_urn.split(',')
        if parts:
            return parts[-1].split(')')[-1] if ')' in parts[-1] else parts[-1]
        return entity_urn 