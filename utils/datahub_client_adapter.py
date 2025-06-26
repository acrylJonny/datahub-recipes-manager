#!/usr/bin/env python3
"""
Compatibility adapter for the new DataHub CI/CD client package.
This provides the same interface as the old utils but uses the new modular package.
"""

import logging
from typing import Dict, Any, Optional, List

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.services.tags import TagService
from datahub_cicd_client.services.domains import DomainService
from datahub_cicd_client.services.properties import StructuredPropertiesService
from datahub_cicd_client.services.glossary import GlossaryService
from datahub_cicd_client.services.data_products import DataProductService
from datahub_cicd_client.services.users import UserService
from datahub_cicd_client.services.groups import GroupService
from datahub_cicd_client.services.ownership_types import OwnershipTypeService
from datahub_cicd_client.services.ingestion import IngestionService
from datahub_cicd_client.services.edited_data import EditedDataService
from datahub_cicd_client.services.assertions import AssertionService
from datahub_cicd_client.services.tests import MetadataTestService
from datahub_cicd_client.services.data_contracts import DataContractService
from datahub_cicd_client.services.schema import SchemaService
from datahub_cicd_client.services.analytics import AnalyticsService

logger = logging.getLogger(__name__)


class DataHubRestClient:
    """
    Adapter class that provides backward compatibility with the old DataHub REST client interface.
    This delegates to the new modular service architecture while maintaining the same API.
    """

    def __init__(self, server_url: str, token: str, verify_ssl: bool = True):
        if not server_url:
            raise ValueError("server_url cannot be None or empty")
        
        self.server_url = server_url
        self.token = token
        self.verify_ssl = verify_ssl
        
        # Initialize connection
        self.connection = DataHubConnection(
            server_url=server_url,
            token=token,
            verify_ssl=verify_ssl
        )
        
        # Initialize services (lazy-loaded)
        self._tag_service = None
        self._domain_service = None
        self._properties_service = None
        self._glossary_service = None
        self._data_product_service = None
        self._user_service = None
        self._group_service = None
        self._ownership_type_service = None
        self._ingestion_service = None
        self._edited_data_service = None
        self._assertion_service = None
        self._metadata_test_service = None
        self._data_contract_service = None
        self._schema_service = None
        self._analytics_service = None

    @property
    def tag_service(self) -> TagService:
        if self._tag_service is None:
            self._tag_service = TagService(self.connection)
        return self._tag_service

    @property
    def domain_service(self) -> DomainService:
        if self._domain_service is None:
            self._domain_service = DomainService(self.connection)
        return self._domain_service

    @property
    def properties_service(self) -> StructuredPropertiesService:
        if self._properties_service is None:
            self._properties_service = StructuredPropertiesService(self.connection)
        return self._properties_service

    @property
    def glossary_service(self) -> GlossaryService:
        if self._glossary_service is None:
            self._glossary_service = GlossaryService(self.connection)
        return self._glossary_service

    @property
    def data_product_service(self) -> DataProductService:
        if self._data_product_service is None:
            self._data_product_service = DataProductService(self.connection)
        return self._data_product_service

    @property
    def user_service(self) -> UserService:
        if self._user_service is None:
            self._user_service = UserService(self.connection)
        return self._user_service

    @property
    def group_service(self) -> GroupService:
        if self._group_service is None:
            self._group_service = GroupService(self.connection)
        return self._group_service

    @property
    def ownership_type_service(self) -> OwnershipTypeService:
        if self._ownership_type_service is None:
            self._ownership_type_service = OwnershipTypeService(self.connection)
        return self._ownership_type_service

    @property
    def ingestion_service(self) -> IngestionService:
        if self._ingestion_service is None:
            self._ingestion_service = IngestionService(self.connection)
        return self._ingestion_service

    @property
    def edited_data_service(self) -> EditedDataService:
        if self._edited_data_service is None:
            self._edited_data_service = EditedDataService(self.connection)
        return self._edited_data_service

    @property
    def assertion_service(self) -> AssertionService:
        if self._assertion_service is None:
            self._assertion_service = AssertionService(self.connection)
        return self._assertion_service

    @property
    def metadata_test_service(self) -> MetadataTestService:
        if self._metadata_test_service is None:
            self._metadata_test_service = MetadataTestService(self.connection)
        return self._metadata_test_service

    @property
    def data_contract_service(self) -> DataContractService:
        if self._data_contract_service is None:
            self._data_contract_service = DataContractService(self.connection)
        return self._data_contract_service

    @property
    def schema_service(self) -> SchemaService:
        if self._schema_service is None:
            self._schema_service = SchemaService(self.connection)
        return self._schema_service

    @property
    def analytics_service(self) -> AnalyticsService:
        if self._analytics_service is None:
            self._analytics_service = AnalyticsService(self.connection)
        return self._analytics_service

    def test_connection(self) -> bool:
        return self.connection.test_connection()

    def execute_graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        return self.connection.execute_graphql(query, variables)
    
    def safe_execute_graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a GraphQL query with safe error handling.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            GraphQL response data or empty dict if failed
            
        Note:
            This method never returns None, making it safe to call .get() on the result.
        """
        try:
            result = self.execute_graphql(query, variables)
            return result if result is not None else {}
        except Exception as e:
            logger.error(f"GraphQL query failed: {str(e)}")
            return {}

    # Editable entities methods
    def get_editable_entities(self, start: int = 0, count: int = 100, query: str = "*", 
                            entity_type: str = None, platform: str = None, 
                            use_platform_pagination: bool = False, sort_by: str = "name", 
                            editable_only: bool = True, **kwargs) -> Dict[str, Any]:
        """
        Get editable entities from DataHub.
        
        Args:
            start: Starting offset for pagination
            count: Maximum number of entities to return
            query: Search query to filter entities
            entity_type: Type of entity to filter by (optional)
            platform: Platform to filter by (optional)
            use_platform_pagination: Whether to use platform-specific pagination
            sort_by: Field to sort by
            editable_only: Whether to return only editable entities
            **kwargs: Additional filter parameters
            
        Returns:
            Dict containing entities and pagination info
        """
        try:
            # Use the EditedDataService to get editable entities
            result = self.edited_data_service.get_editable_entities(
                start=start,
                count=count,
                query=query,
                entity_type=entity_type,
                platform=platform,
                use_platform_pagination=use_platform_pagination,
                sort_by=sort_by,
                editable_only=editable_only,
                **kwargs
            )
            
            if result and result.success:
                return {
                    "success": True,
                    "data": {
                        "searchResults": result.entities,
                        "total": result.total,
                        "start": result.start,
                        "count": result.count
                    }
                }
            else:
                logger.warning("Failed to get editable entities from service")
                return {
                    "success": False,
                    "data": {
                        "searchResults": [],
                        "total": 0,
                        "start": start,
                        "count": 0
                    }
                }
                
        except Exception as e:
            logger.error(f"Error getting editable entities: {str(e)}")
            return {
                "success": False,
                "data": {
                    "searchResults": [],
                    "total": 0,
                    "start": start,
                    "count": 0
                },
                "error": str(e)
            }

    def get_entity(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single entity by URN.
        
        Args:
            entity_urn: The URN of the entity to retrieve
            
        Returns:
            Entity data or None if not found
        """
        try:
            # Use the EditedDataService to get entity details
            entity = self.edited_data_service.get_editable_entity(entity_urn)
            return entity
        except Exception as e:
            logger.error(f"Error getting entity {entity_urn}: {str(e)}")
            return None

    def get_schema(self, entity_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get schema details for an entity.
        
        Args:
            entity_urn: The URN of the entity to get schema for
            
        Returns:
            Schema data or None if not found
        """
        try:
            # Use the SchemaService to get schema information
            return self.schema_service.get_entity_schema(entity_urn)
        except Exception as e:
            logger.error(f"Error getting schema for entity {entity_urn}: {str(e)}")
            return None

    def sync_metadata(self) -> bool:
        """
        Sync metadata with DataHub.
        
        Returns:
            bool: True if sync was successful, False otherwise
        """
        try:
            # This is a placeholder - the actual implementation would depend on
            # what kind of sync operation is needed
            logger.info("Metadata sync requested - this is a placeholder implementation")
            return True
        except Exception as e:
            logger.error(f"Error syncing metadata: {str(e)}")
            return False

    def _count_entity_metadata(self, entity: Dict[str, Any]) -> Dict[str, int]:
        """
        Count metadata elements for an entity.
        
        Args:
            entity: Entity data dictionary
            
        Returns:
            Dict with counts of different metadata types
        """
        try:
            counts = {
                "tags": 0,
                "glossary_terms": 0,
                "domains": 0,
                "structured_properties": 0,
                "editable_properties": 0,
                "schema_fields": 0
            }
            
            # Count tags
            if entity.get("tags") and entity["tags"].get("tags"):
                counts["tags"] = len(entity["tags"]["tags"])
            
            # Count glossary terms
            if entity.get("glossaryTerms") and entity["glossaryTerms"].get("terms"):
                counts["glossary_terms"] = len(entity["glossaryTerms"]["terms"])
            
            # Count domains
            if entity.get("domain") and entity["domain"].get("domain"):
                counts["domains"] = 1
            
            # Count structured properties
            if entity.get("structuredProperties") and entity["structuredProperties"].get("properties"):
                counts["structured_properties"] = len(entity["structuredProperties"]["properties"])
            
            # Count editable properties
            if entity.get("editableProperties"):
                editable_props = entity["editableProperties"]
                counts["editable_properties"] = sum(1 for key, value in editable_props.items() if value is not None and value != "")
            
            # Count schema fields
            if entity.get("schemaMetadata") and entity["schemaMetadata"].get("fields"):
                counts["schema_fields"] = len(entity["schemaMetadata"]["fields"])
            
            return counts
            
        except Exception as e:
            logger.error(f"Error counting entity metadata: {str(e)}")
            return {"tags": 0, "glossary_terms": 0, "domains": 0, "structured_properties": 0, "editable_properties": 0, "schema_fields": 0}

    def update_entity_properties(self, entity_urn: str, properties: Dict[str, Any]) -> bool:
        """
        Update properties for an entity.
        
        Args:
            entity_urn: The URN of the entity to update
            properties: Dictionary of properties to update
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            # Use the EditedDataService to update entity properties
            if "description" in properties:
                result = self.edited_data_service.update_entity_description(
                    entity_urn, properties["description"]
                )
                return result.success
            
            # For other properties, use bulk update
            result = self.edited_data_service.bulk_update_entity_properties(
                entity_urn, 
                description=properties.get("description"),
                custom_properties=properties.get("custom_properties")
            )
            return result.success
            
        except Exception as e:
            logger.error(f"Error updating entity properties for {entity_urn}: {str(e)}")
            return False

    def aggregate_across_entities(self, query: str = "*", entity_types: List[str] = None, 
                                 aggregation_types: List[str] = None) -> Optional[Dict[str, Any]]:
        """
        Aggregate data across entities.
        
        Args:
            query: Search query
            entity_types: List of entity types to aggregate
            aggregation_types: List of aggregation types to perform
            
        Returns:
            Dict with aggregation results or None if failed
        """
        try:
            # Use the AnalyticsService to perform aggregation
            return self.analytics_service.aggregate_across_entities(query, entity_types, aggregation_types)
        except Exception as e:
            logger.error(f"Error aggregating across entities: {str(e)}")
            return None

    # Tag methods
    def list_tags(self, query="*", start=0, count=100):
        return self.tag_service.list_tags(query=query, start=start, count=count)

    def get_remote_tags_data(self, query="*", start=0, count=100):
        return self.tag_service.get_remote_tags_data(query=query, start=start, count=count)

    def get_tag(self, tag_urn: str):
        return self.tag_service.get_tag(tag_urn)

    def create_tag(self, tag_id: str, name: str, description: str = ""):
        return self.tag_service.create_tag(tag_id, name, description)

    def create_or_update_tag(self, tag_id: str, name: str, description: str = ""):
        return self.tag_service.create_or_update_tag(tag_id, name, description)

    def delete_tag(self, tag_urn: str):
        return self.tag_service.delete_tag(tag_urn)

    def set_tag_color(self, tag_urn: str, color_hex: str):
        return self.tag_service.set_tag_color(tag_urn, color_hex)

    def update_tag_description(self, tag_urn: str, description: str):
        return self.tag_service.update_tag_description(tag_urn, description)

    def add_tag_owner(self, tag_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.tag_service.add_tag_owner(tag_urn, owner_urn, ownership_type)

    def remove_tag_owner(self, tag_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.tag_service.remove_tag_owner(tag_urn, owner_urn, ownership_type)

    def add_tag_to_entity(self, entity_urn: str, tag_urn: str, color_hex: str = None):
        return self.tag_service.add_tag_to_entity(entity_urn, tag_urn, color_hex)

    def remove_tag_from_entity(self, entity_urn: str, tag_urn: str):
        return self.tag_service.remove_tag_from_entity(entity_urn, tag_urn)

    def find_entities_with_metadata(self, field_type: str, metadata_urn: str, start: int = 0, count: int = 10):
        if field_type.lower() == "tag":
            return self.tag_service.find_entities_with_tag(metadata_urn, start, count)
        else:
            logger.warning(f"Finding entities with {field_type} not yet implemented")
            return {"entities": [], "total": 0}

    # Domain methods
    def list_domains(self, query="*", start=0, count=100):
        return self.domain_service.list_domains(query=query, start=start, count=count)

    def get_domain(self, domain_urn: str):
        return self.domain_service.get_domain(domain_urn)

    def create_domain(self, domain_id: str, name: str, description: str = "", parent_domain_urn: str = None):
        return self.domain_service.create_domain(domain_id, name, description, parent_domain_urn)

    def update_domain_description(self, domain_urn: str, description: str):
        return self.domain_service.update_domain_description(domain_urn, description)

    def update_domain_display_properties(self, domain_urn: str, color_hex: str = None, icon: Dict[str, str] = None):
        return self.domain_service.update_domain_display_properties(domain_urn, color_hex, icon)

    def add_domain_owner(self, domain_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.domain_service.add_domain_owner(domain_urn, owner_urn, ownership_type)

    def remove_domain_owner(self, domain_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.domain_service.remove_domain_owner(domain_urn, owner_urn, ownership_type)

    def delete_domain(self, domain_urn: str):
        return self.domain_service.delete_domain(domain_urn)

    def find_entities_with_domain(self, domain_urn: str, start: int = 0, count: int = 50):
        return self.domain_service.find_entities_with_domain(domain_urn, start, count)

    # Structured Properties methods
    def list_structured_properties(self, query="*", start=0, count=100):
        return self.properties_service.list_structured_properties(query=query, start=start, count=count)

    def get_structured_properties(self, start=0, count=1000):
        return self.properties_service.list_structured_properties(start=start, count=count)

    def get_structured_property(self, property_urn: str):
        return self.properties_service.get_structured_property(property_urn)

    def create_structured_property(self, display_name: str, description: str = "", value_type: str = "STRING", cardinality: str = "SINGLE", entity_types: list = None, allowedValues: list = None, qualified_name: str = None, **kwargs):
        return self.properties_service.create_structured_property(
            display_name=display_name, description=description, value_type=value_type,
            cardinality=cardinality, entity_types=entity_types, allowed_values=allowedValues,
            qualified_name=qualified_name, **kwargs
        )

    def delete_structured_property(self, property_urn: str):
        return self.properties_service.delete_structured_property(property_urn)

    def upsert_structured_properties(self, entity_urn: str, structured_properties: list):
        return self.properties_service.upsert_structured_properties(entity_urn, structured_properties)

    def remove_structured_properties(self, entity_urn: str, property_urns: list):
        return self.properties_service.remove_structured_properties(entity_urn, property_urns)

    # Glossary methods
    def list_glossary_nodes(self, query=None, count=100, start=0):
        return self.glossary_service.list_glossary_nodes(query=query, count=count, start=start)

    def get_glossary_node(self, node_urn):
        return self.glossary_service.get_glossary_node(node_urn)

    def create_glossary_node(self, node_id, name, description="", parent_urn=None):
        return self.glossary_service.create_glossary_node(node_id, name, description, parent_urn)

    def delete_glossary_node(self, node_urn: str):
        return self.glossary_service.delete_glossary_node(node_urn)

    def list_glossary_terms(self, node_urn=None, query=None, count=100, start=0):
        return self.glossary_service.list_glossary_terms(node_urn=node_urn, query=query, count=count, start=start)

    def get_glossary_term(self, term_urn):
        return self.glossary_service.get_glossary_term(term_urn)

    def create_glossary_term(self, term_id, name, description="", parent_node_urn=None, term_source="INTERNAL"):
        return self.glossary_service.create_glossary_term(term_id, name, description, parent_node_urn, term_source)

    def delete_glossary_term(self, term_urn: str):
        return self.glossary_service.delete_glossary_term(term_urn)

    def get_comprehensive_glossary_data(self, query="*", start=0, count=100):
        return self.glossary_service.get_comprehensive_glossary_data(query=query, start=start, count=count)

    def add_glossary_term_to_entity(self, entity_urn: str, term_urn: str):
        return self.glossary_service.add_glossary_term_to_entity(entity_urn, term_urn)

    def remove_glossary_term_from_entity(self, entity_urn: str, term_urn: str):
        return self.glossary_service.remove_glossary_term_from_entity(entity_urn, term_urn)

    # Data Product methods
    def list_data_products(self, query="*", start=0, count=100):
        try:
            result = self.data_product_service.list_data_products(query=query, start=start, count=count)
            # Ensure we always return a list, never None
            if result is None:
                logger.warning("Data product service returned None, returning empty list")
                return []
            return result
        except Exception as e:
            logger.error(f"Error listing data products: {str(e)}")
            # Return empty list instead of letting exception propagate
            return []

    def get_data_product(self, data_product_urn: str):
        try:
            result = self.data_product_service.get_data_product(data_product_urn)
            return result
        except Exception as e:
            logger.error(f"Error getting data product {data_product_urn}: {str(e)}")
            return None

    def create_data_product(self, product_id: str, name: str, description: str = "", external_url: str = None):
        return self.data_product_service.create_data_product(product_id, name, description, external_url)

    def delete_data_product(self, product_urn: str):
        return self.data_product_service.delete_data_product(product_urn)

    def add_data_product_owner(self, product_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.data_product_service.add_data_product_owner(product_urn, owner_urn, ownership_type)

    def remove_data_product_owner(self, product_urn: str, owner_urn: str, ownership_type: str = "urn:li:ownershipType:__system__business_owner"):
        return self.data_product_service.remove_data_product_owner(product_urn, owner_urn, ownership_type)

    # Assertion methods
    def get_assertions(self, start: int = 0, count: int = 100, query: str = "*", 
                      status: str = None, entity_urn: str = None, run_events_limit: int = 10):
        """
        Get assertions from DataHub.
        
        Args:
            start: Starting offset for pagination
            count: Maximum number of assertions to return
            query: Search query to filter assertions
            status: Filter by assertion status (optional)
            entity_urn: Filter by entity URN (optional)
            run_events_limit: Limit for run events (optional)
            
        Returns:
            Dict containing search results and metadata
        """
        try:
            logger.info(f"Getting assertions with query='{query}', start={start}, count={count}")
            
            # Use the actual AssertionService to get assertions
            assertions = self.assertion_service.list_assertions(query=query, start=start, count=count)
            
            # Format the response to match the expected web UI format
            search_results = []
            for assertion in assertions:
                search_result = {
                    "entity": assertion
                }
                search_results.append(search_result)
            
            return {
                "success": True,
                "data": {
                    "start": start,
                    "count": len(search_results),
                    "total": len(search_results),  # Note: This is approximate since we don't have total count from the service
                    "searchResults": search_results
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting assertions: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "data": {
                    "start": start,
                    "count": 0,
                    "total": 0,
                    "searchResults": []
                }
            }

    # Ingestion methods
    def list_ingestion_sources(self):
        return self.ingestion_service.list_ingestion_sources()

    def create_ingestion_source(self, recipe, **kwargs):
        return self.ingestion_service.create_ingestion_source(recipe, **kwargs)

    def get_ingestion_source(self, source_id):
        return self.ingestion_service.get_ingestion_source(source_id)

    def delete_ingestion_source(self, source_id: str):
        return self.ingestion_service.delete_ingestion_source(source_id)

    def trigger_ingestion(self, ingestion_source_id: str):
        return self.ingestion_service.execute_ingestion_source(ingestion_source_id)

    def patch_ingestion_source(self, source_id: str, **kwargs):
        return self.ingestion_service.patch_ingestion_source(source_id, **kwargs)

    def run_ingestion_source(self, source_id: str):
        return self.ingestion_service.execute_ingestion_source(source_id)

    def get_ingestion_executions(self, source_id: str, start: int = 0, count: int = 10):
        return self.ingestion_service.get_ingestion_executions(source_id, start, count)

    def get_ingestion_source_stats(self, source_id: str):
        return self.ingestion_service.get_ingestion_source_stats(source_id)

    def count_ingestion_sources(self, filters=None):
        return self.ingestion_service.count_ingestion_sources(filters)

    def test_connection_with_permissions(self):
        return self.test_connection()

    # Placeholder methods for not yet implemented features
    def list_policies(self, limit=100, start=0):
        logger.warning("Policy management not yet implemented in new client")
        return []

    def get_policy(self, policy_id):
        logger.warning("Policy management not yet implemented in new client")
        return None

    def create_policy(self, policy_data):
        logger.warning("Policy management not yet implemented in new client")
        return None

    def update_policy(self, policy_id, policy_data):
        logger.warning("Policy management not yet implemented in new client")
        return None

    def delete_policy(self, policy_id):
        logger.warning("Policy management not yet implemented in new client")
        return None

    def list_secrets(self, start: int = 0, count: int = 100):
        logger.warning("Secret management not yet implemented in new client")
        return []

    def create_secret(self, name: str, value: str, description: str = None):
        logger.warning("Secret management not yet implemented in new client")
        return False

    def delete_secret(self, name_or_urn: str):
        logger.warning("Secret management not yet implemented in new client")
        return False

    def update_secret(self, name: str, value: str, description: str = None):
        logger.warning("Secret management not yet implemented in new client")
        return False

    def __getattr__(self, name):
        logger.warning(f"Method '{name}' not implemented in new client")
        return lambda *args, **kwargs: None

    # User and Group management methods
    def list_users(self, start: int = 0, count: int = 100):
        """List users from DataHub"""
        try:
            users_data = self.user_service.search_users("*", start=start, count=count)
            
            # Check if users_data is None (GraphQL query failed)
            if users_data is None:
                logger.warning("User service returned None - GraphQL query may have failed")
                return {"success": False, "data": {"searchResults": []}, "error": "No data returned from user service"}
            
            # New service returns a direct list, not a dict
            if isinstance(users_data, list):
                formatted_users = []
                for user in users_data:
                    formatted_user = self.user_service.format_user_for_display(user)
                    formatted_users.append(formatted_user)
                
                return {
                    "success": True,
                    "data": {
                        "searchResults": [{"entity": user} for user in formatted_users],
                        "total": len(formatted_users),
                        "start": start,
                        "count": len(formatted_users)
                    }
                }
            else:
                # Fallback for old format (dict with users key)
                formatted_users = []
                for user in users_data.get("users", []):
                    formatted_user = self.user_service.format_user_for_display(user)
                    formatted_users.append(formatted_user)
                
                return {
                    "success": True,
                    "data": {
                        "searchResults": [{"entity": user} for user in formatted_users],
                        "total": users_data.get("total", 0),
                        "start": users_data.get("start", start),
                        "count": users_data.get("count", len(formatted_users))
                    }
                }
        except Exception as e:
            logger.error(f"Error listing users: {str(e)}")
            return {"success": False, "data": {"searchResults": []}, "error": str(e)}

    def list_groups(self, start: int = 0, count: int = 100):
        """List groups from DataHub"""
        try:
            groups_data = self.group_service.search_groups("*", start=start, count=count)
            
            # New service returns a direct list, not a dict
            if isinstance(groups_data, list):
                formatted_groups = []
                for group in groups_data:
                    formatted_group = self.group_service.format_group_for_display(group)
                    formatted_groups.append(formatted_group)
                
                return {
                    "success": True,
                    "data": {
                        "searchResults": [{"entity": group} for group in formatted_groups],
                        "total": len(formatted_groups),
                        "start": start,
                        "count": len(formatted_groups)
                    }
                }
            else:
                # Fallback for old format (dict with groups key)
                formatted_groups = []
                for group in groups_data.get("groups", []):
                    formatted_group = self.group_service.format_group_for_display(group)
                    formatted_groups.append(formatted_group)
                
                return {
                    "success": True,
                    "data": {
                        "searchResults": [{"entity": group} for group in formatted_groups],
                        "total": groups_data.get("total", 0),
                        "start": groups_data.get("start", start),
                        "count": groups_data.get("count", len(formatted_groups))
                    }
                }
        except Exception as e:
            logger.error(f"Error listing groups: {str(e)}")
            return {"success": False, "data": {"searchResults": []}, "error": str(e)}

    def list_ownership_types(self, start: int = 0, count: int = 100):
        """List ownership types from DataHub"""
        try:
            ownership_types = self.ownership_type_service.list_ownership_types(start=start, count=count)
            
            # Format for backward compatibility
            formatted_types = []
            for ownership_type in ownership_types:
                formatted_type = self.ownership_type_service.format_ownership_type_for_display(ownership_type)
                formatted_types.append(formatted_type)
            
            return {
                "success": True,
                "data": {
                    "ownershipTypes": formatted_types,
                    "total": len(formatted_types),
                    "start": start,
                    "count": len(formatted_types)
                }
            }
        except Exception as e:
            logger.error(f"Error listing ownership types: {str(e)}")
            return {"success": False, "data": {"ownershipTypes": []}, "error": str(e)}

    # Data Contract methods
    def get_data_contracts(self, start: int = 0, count: int = 100, query: str = "*"):
        """
        Get data contracts from DataHub.
        
        Args:
            start: Starting offset for pagination
            count: Maximum number of data contracts to return
            query: Search query to filter data contracts
            
        Returns:
            Dictionary with success status and data contracts information
        """
        try:
            logger.info(f"Getting data contracts with query='{query}', start={start}, count={count}")
            
            # Use the actual DataContractService to get data contracts with proper structure
            result = self.data_contract_service.get_data_contracts(query=query, start=start, count=count)
            return result
            
        except Exception as e:
            logger.error(f"Error getting data contracts: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def get_data_contract(self, contract_urn: str):
        """Get a specific data contract by URN."""
        try:
            # Use the actual DataContractService to get data contract
            contract = self.data_contract_service.get_data_contract(contract_urn)
            return contract
        except Exception as e:
            logger.error(f"Error getting data contract {contract_urn}: {str(e)}")
            return None

    # Test methods
    def list_tests(self, query: str = "*", start: int = 0, count: int = 100):
        """
        List metadata tests from DataHub.
        
        Args:
            query: Search query to filter tests
            start: Starting offset for pagination
            count: Maximum number of tests to return
            
        Returns:
            List of tests
        """
        try:
            logger.info(f"Listing tests with query='{query}', start={start}, count={count}")
            
            # Use the actual MetadataTestService to get tests
            tests = self.metadata_test_service.list_metadata_tests(query=query, start=start, count=count)
            return tests
            
        except Exception as e:
            logger.error(f"Error listing tests: {str(e)}")
            return []

    def get_dataset_info(self, dataset_urn: str):
        """Get dataset information."""
        try:
            logger.warning(f"Dataset info service not yet implemented - returning None for {dataset_urn}")
            return None
        except Exception as e:
            logger.error(f"Error getting dataset info {dataset_urn}: {str(e)}")
            return None

    def create_test(self, test_data):
        """Create a new test."""
        try:
            logger.warning("Test creation not yet implemented")
            return None
        except Exception as e:
            logger.error(f"Error creating test: {str(e)}")
            return None

    def update_test(self, test_urn: str, test_data):
        """Update an existing test."""
        try:
            logger.warning(f"Test update not yet implemented for {test_urn}")
            return None
        except Exception as e:
            logger.error(f"Error updating test {test_urn}: {str(e)}")
            return None

    def delete_test(self, test_urn: str):
        """Delete a test."""
        try:
            logger.warning(f"Test deletion not yet implemented for {test_urn}")
            return False
        except Exception as e:
            logger.error(f"Error deleting test {test_urn}: {str(e)}")
            return False

    # User methods
    def get_current_user(self):
        """Get current user information."""
        try:
            logger.warning("Get current user not yet implemented")
            return None
        except Exception as e:
            logger.error(f"Error getting current user: {str(e)}")
            return None

    def get_datasets_by_urns(self, urns):
        """Get datasets by URNs."""
        try:
            logger.info(f"Getting datasets by URNs: {urns}")
            
            # Use the data contract service to get datasets by URNs
            result = self.data_contract_service.get_datasets_by_urns(urns)
            return result
            
        except Exception as e:
            logger.error(f"Error getting datasets by URNs: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }


# Compatibility functions for datahub_utils
def get_datahub_client(server_url: str = None, token: str = None, verify_ssl: bool = True) -> DataHubRestClient:
    return DataHubRestClient(server_url=server_url, token=token, verify_ssl=verify_ssl)

def get_datahub_client_from_request(request) -> DataHubRestClient:
    """Get DataHub client from request context with proper connection settings"""
    try:
        # Try to get connection settings from the web UI
        from web_ui.models import Connection
        
        # Get default connection
        default_connection = Connection.get_default()
        if default_connection and default_connection.datahub_url:
            return DataHubRestClient(
                server_url=default_connection.datahub_url,
                token=default_connection.datahub_token or "",
                verify_ssl=getattr(default_connection, 'verify_ssl', True)
            )
        else:
            # Fallback to AppSettings
            from web_ui.models import AppSettings
            datahub_url = AppSettings.get("datahub_url", "")
            datahub_token = AppSettings.get("datahub_token", "")
            
            if datahub_url:
                return DataHubRestClient(
                    server_url=datahub_url,
                    token=datahub_token,
                    verify_ssl=True
                )
            else:
                # No valid URL available
                raise ValueError("No DataHub URL configured")
                
    except Exception as e:
        logger.error(f"Error getting DataHub client from request: {e}")
        # Don't return a client with invalid settings
        raise ValueError(f"Cannot create DataHub client: {e}")

def test_datahub_connection(request=None, server_url: str = None, token: str = None, verify_ssl: bool = True):
    """
    Test DataHub connection and return (connected, client) tuple for backward compatibility.
    
    Args:
        request: Django request object (optional)
        server_url: DataHub server URL (optional)
        token: DataHub token (optional)
        verify_ssl: Whether to verify SSL (optional)
    
    Returns:
        tuple: (connected: bool, client: DataHubRestClient or None)
    """
    try:
        client = None
        
        if request is not None:
            # Get client from request context
            try:
                client = get_datahub_client_from_request(request)
            except (ValueError, Exception) as e:
                logger.error(f"Cannot get client from request: {e}")
                return False, None
        elif server_url is not None and server_url.strip():
            # Use provided parameters
            try:
                client = DataHubRestClient(server_url=server_url, token=token, verify_ssl=verify_ssl)
            except (ValueError, Exception) as e:
                logger.error(f"Cannot create client with provided parameters: {e}")
                return False, None
        else:
            # No arguments provided - try to get default connection
            try:
                from web_ui.models import Connection
                default_connection = Connection.get_default()
                if default_connection and default_connection.datahub_url:
                    client = DataHubRestClient(
                        server_url=default_connection.datahub_url,
                        token=default_connection.datahub_token or "",
                        verify_ssl=getattr(default_connection, 'verify_ssl', True)
                    )
                else:
                    # Try AppSettings as fallback
                    try:
                        from web_ui.models import AppSettings
                        datahub_url = AppSettings.get("datahub_url", "")
                        datahub_token = AppSettings.get("datahub_token", "")
                        
                        if datahub_url and datahub_url.strip():
                            client = DataHubRestClient(
                                server_url=datahub_url,
                                token=datahub_token,
                                verify_ssl=True
                            )
                        else:
                            logger.warning("No DataHub connection configured")
                            return False, None
                    except Exception as e:
                        logger.error(f"Cannot get settings: {e}")
                        return False, None
            except Exception as e:
                logger.error(f"Cannot get default connection: {e}")
                return False, None
        
        # Test the connection
        if client and hasattr(client, 'server_url') and client.server_url and client.server_url.strip():
            connected = client.test_connection()
            return connected, client if connected else None
        else:
            return False, None
            
    except Exception as e:
        logger.error(f"Failed to test DataHub connection: {e}")
        return False, None

# Cache placeholders - updated to accept force_refresh parameter
def get_cached_policies(force_refresh: bool = False):
    logger.warning("Policy caching not yet implemented")
    return []

def get_cached_recipes(force_refresh: bool = False):
    logger.warning("Recipe caching not yet implemented")
    return []

def invalidate_recipes_cache():
    logger.warning("Recipe cache invalidation not yet implemented")
    pass

def invalidate_policies_cache():
    logger.warning("Policy cache invalidation not yet implemented")
    pass
