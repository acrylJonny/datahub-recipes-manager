"""
Data Products-specific operations for the metadata manager.
"""
from typing import Dict, Any, List, Optional
from ..common.sync_operations import BaseSyncOperations
from ..common.staging_operations import BaseStagingOperations
from ..common.git_operations import BaseGitOperations
from ..common.remote_data_operations import BaseRemoteDataOperations


class DataProductSyncOperations(BaseSyncOperations):
    """Data product-specific sync operations."""
    
    def get_entity_type(self) -> str:
        return "data_products"
    
    def get_sync_script_path(self) -> str:
        return "scripts/mcps/cli_data_product_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Data Product"
    
    def validate_entity_data(self, entity_data: Dict[str, Any]) -> bool:
        """Validate data product-specific data."""
        required_fields = ['name', 'description']
        return all(field in entity_data for field in required_fields)
    
    def transform_entity_data(self, entity_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data product data for sync."""
        return {
            'name': entity_data.get('name', ''),
            'description': entity_data.get('description', ''),
            'display_name': entity_data.get('display_name', ''),
            'external_url': entity_data.get('external_url', ''),
            'domain': entity_data.get('domain', ''),
            'owners': entity_data.get('owners', []),
            'tags': entity_data.get('tags', []),
            'glossary_terms': entity_data.get('glossary_terms', []),
            'properties': entity_data.get('properties', {}),
            'assets': entity_data.get('assets', []),
        }


class DataProductStagingOperations(BaseStagingOperations):
    """Data product-specific staging operations."""
    
    def get_entity_type(self) -> str:
        return "data_products"
    
    def get_mcp_script_path(self) -> str:
        return "scripts/mcps/cli_data_product_actions.py"
    
    def get_entity_display_name(self) -> str:
        return "Data Product"
    
    def validate_staging_data(self, staging_data: Dict[str, Any]) -> bool:
        """Validate data product staging data."""
        required_fields = ['name', 'description']
        return all(field in staging_data for field in required_fields)
    
    def transform_staging_data(self, staging_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data product data for staging."""
        return {
            'name': staging_data.get('name', ''),
            'description': staging_data.get('description', ''),
            'display_name': staging_data.get('display_name', ''),
            'external_url': staging_data.get('external_url', ''),
            'domain': staging_data.get('domain', ''),
            'owners': staging_data.get('owners', []),
            'tags': staging_data.get('tags', []),
            'glossary_terms': staging_data.get('glossary_terms', []),
            'properties': staging_data.get('properties', {}),
            'assets': staging_data.get('assets', []),
        }


class DataProductGitOperations(BaseGitOperations):
    """Data product-specific Git operations."""
    
    def get_entity_type(self) -> str:
        return "data_products"
    
    def get_file_path_pattern(self) -> str:
        return "metadata-manager/{environment}/data_products/{name}.json"
    
    def get_commit_message_template(self) -> str:
        return "Update data product: {name}"
    
    def get_pr_title_template(self) -> str:
        return "Data Product Updates: {name}"
    
    def get_pr_body_template(self) -> str:
        return """
## Data Product Changes

### Product: {name}

**Display Name:** {display_name}
**Description:** {description}
**Domain:** {domain}
**External URL:** {external_url}

### Changes Made:
- Updated product definition
- Modified asset associations
- Updated ownership information
- Updated tags and glossary terms
- Updated structured properties

### Review Checklist:
- [ ] Product name follows naming conventions
- [ ] Description is clear and comprehensive
- [ ] Domain assignment is appropriate
- [ ] Asset associations are correct
- [ ] Owners are properly assigned
- [ ] Tags and glossary terms are relevant
- [ ] Structured properties are valid
        """


class DataProductRemoteDataOperations(BaseRemoteDataOperations):
    """Data product-specific remote data operations."""
    
    def get_entity_type(self) -> str:
        return "data_products"
    
    def get_remote_data_method(self) -> str:
        return "list_data_products"
    
    def get_entity_display_name(self) -> str:
        return "Data Product"
    
    def transform_remote_data(self, remote_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform remote data product data for display."""
        transformed = []
        for product in remote_data:
            transformed.append({
                'urn': product.get('urn', ''),
                'name': product.get('name', ''),
                'display_name': product.get('display_name', ''),
                'description': product.get('description', ''),
                'external_url': product.get('external_url', ''),
                'domain': product.get('domain', {}),
                'owners': product.get('owners', []),
                'tags': product.get('tags', []),
                'glossary_terms': product.get('glossary_terms', []),
                'properties': product.get('properties', {}),
                'assets': product.get('assets', []),
                'asset_count': len(product.get('assets', [])),
            })
        return transformed
    
    def get_entity_identifier(self, entity_data: Dict[str, Any]) -> str:
        """Get unique identifier for data product."""
        return entity_data.get('urn', entity_data.get('name', '')) 