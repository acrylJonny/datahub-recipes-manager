"""
Glossary service for DataHub operations.
Handles both glossary nodes and glossary terms.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.exceptions import DataHubError
from datahub_cicd_client.graphql.mutations.glossary import (
    ADD_GLOSSARY_NODE_OWNER_MUTATION,
    ADD_GLOSSARY_TERM_OWNER_MUTATION,
    ADD_GLOSSARY_TERM_TO_ENTITY_MUTATION,
    CREATE_GLOSSARY_NODE_MUTATION,
    CREATE_GLOSSARY_TERM_MUTATION,
    DELETE_GLOSSARY_NODE_MUTATION,
    DELETE_GLOSSARY_TERM_MUTATION,
    REMOVE_GLOSSARY_TERM_FROM_ENTITY_MUTATION,
    UPDATE_GLOSSARY_NODE_DESCRIPTION_MUTATION,
    UPDATE_GLOSSARY_TERM_DESCRIPTION_MUTATION,
    UPSERT_GLOSSARY_NODE_STRUCTURED_PROPERTIES_MUTATION,
    UPSERT_GLOSSARY_TERM_STRUCTURED_PROPERTIES_MUTATION,
)
from datahub_cicd_client.graphql.queries.glossary import (
    COUNT_GLOSSARY_NODES_QUERY,
    COUNT_GLOSSARY_TERMS_QUERY,
    FIND_ENTITIES_WITH_GLOSSARY_TERM_QUERY,
    GET_COMPREHENSIVE_GLOSSARY_QUERY,
    GET_GLOSSARY_NODE_QUERY,
    GET_GLOSSARY_TERM_QUERY,
    LIST_GLOSSARY_NODES_QUERY,
    LIST_GLOSSARY_TERMS_QUERY,
)


class GlossaryService(BaseDataHubClient):
    """Service for managing DataHub glossary nodes and terms."""

    def __init__(self, connection):
        super().__init__(connection)
        self.logger = logging.getLogger(__name__)

    # Glossary Node Operations
    def list_glossary_nodes(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List glossary nodes from DataHub.

        Args:
            query: Search query to filter nodes
            start: Starting offset for pagination
            count: Number of nodes to return

        Returns:
            List of glossary node dictionaries

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(
            f"Listing glossary nodes with query='{query}', start={start}, count={count}"
        )

        variables = {
            "input": {
                "query": query,
                "types": ["GLOSSARY_NODE"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_GLOSSARY_NODES_QUERY, variables)

            if not result:
                self.logger.warning("GraphQL query returned no data")
                return []

            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return []

            search_results = search_data.get("searchResults", [])
            nodes = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity.get("type") == "GLOSSARY_NODE":
                    processed_node = self._process_glossary_node(entity)
                    if processed_node:
                        nodes.append(processed_node)

            self.logger.info(f"Retrieved {len(nodes)} glossary nodes")
            return nodes

        except Exception as e:
            self.logger.error(f"Error listing glossary nodes: {str(e)}")
            raise DataHubError(f"Failed to list glossary nodes: {str(e)}")

    def get_glossary_node(self, node_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific glossary node by URN.

        Args:
            node_urn: URN of the glossary node

        Returns:
            Glossary node dictionary or None if not found

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Getting glossary node: {node_urn}")

        variables = {"urn": node_urn}

        try:
            result = self.safe_execute_graphql(GET_GLOSSARY_NODE_QUERY, variables)

            if not result:
                self.logger.warning(f"GraphQL query returned no data for glossary node: {node_urn}")
                return None

            node_data = result.get("glossaryNode")

            if not node_data:
                self.logger.warning(f"Glossary node not found: {node_urn}")
                return None

            processed_node = self._process_glossary_node(node_data)
            self.logger.info(f"Retrieved glossary node: {processed_node.get('name', 'Unknown')}")
            return processed_node

        except Exception as e:
            self.logger.error(f"Error getting glossary node {node_urn}: {str(e)}")
            raise DataHubError(f"Failed to get glossary node: {str(e)}")

    def create_glossary_node(
        self,
        node_id: str,
        name: str,
        description: str = "",
        parent_urn: Optional[str] = None,
    ) -> Optional[str]:
        """
        Create a new glossary node.

        Args:
            node_id: ID for the node (used in URN)
            name: Display name for the node
            description: Description for the node
            parent_urn: URN of the parent node (optional)

        Returns:
            URN of the created node or None if failed

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Creating glossary node: {name}")

        # Build input for creation
        input_data = {
            "id": node_id,
            "name": name,
            "description": description,
        }

        if parent_urn:
            input_data["parentNode"] = parent_urn

        variables = {"input": input_data}

        try:
            result = self._execute_mutation(CREATE_GLOSSARY_NODE_MUTATION, variables)
            created_node = result.get("createGlossaryNode", {})
            node_urn = created_node.get("urn")

            if node_urn:
                self.logger.info(f"Created glossary node: {node_urn}")
                return node_urn
            else:
                self.logger.error("Failed to create glossary node - no URN returned")
                return None

        except Exception as e:
            self.logger.error(f"Error creating glossary node: {str(e)}")
            raise DataHubError(f"Failed to create glossary node: {str(e)}")

    def delete_glossary_node(self, node_urn: str) -> bool:
        """
        Delete a glossary node.

        Args:
            node_urn: URN of the glossary node to delete

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Deleting glossary node: {node_urn}")

        variables = {"urn": node_urn}

        try:
            result = self._execute_mutation(DELETE_GLOSSARY_NODE_MUTATION, variables)
            success = result.get("deleteEntity", False)

            if success:
                self.logger.info(f"Deleted glossary node: {node_urn}")
            else:
                self.logger.warning(f"Failed to delete glossary node: {node_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error deleting glossary node {node_urn}: {str(e)}")
            raise DataHubError(f"Failed to delete glossary node: {str(e)}")

    def update_glossary_node_description(self, node_urn: str, description: str) -> bool:
        """
        Update the description of a glossary node.

        Args:
            node_urn: URN of the glossary node
            description: New description

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Updating glossary node description: {node_urn}")

        variables = {
            "urn": node_urn,
            "description": description,
        }

        try:
            result = self._execute_mutation(UPDATE_GLOSSARY_NODE_DESCRIPTION_MUTATION, variables)
            success = result.get("updateDescription", False)

            if success:
                self.logger.info(f"Updated glossary node description: {node_urn}")
            else:
                self.logger.warning(f"Failed to update glossary node description: {node_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error updating glossary node description {node_urn}: {str(e)}")
            raise DataHubError(f"Failed to update glossary node description: {str(e)}")

    # Glossary Term Operations
    def list_glossary_terms(
        self,
        query: str = "*",
        start: int = 0,
        count: int = 100,
        node_urn: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List glossary terms from DataHub.

        Args:
            query: Search query to filter terms
            start: Starting offset for pagination
            count: Number of terms to return
            node_urn: Filter terms by parent node URN (optional)

        Returns:
            List of glossary term dictionaries

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(
            f"Listing glossary terms with query='{query}', start={start}, count={count}, node_urn={node_urn}"
        )

        # Build search query string
        search_string = query
        if node_urn:
            search_string = f'parentNodes.urn:"{node_urn}"'

        variables = {
            "input": {
                "query": search_string,
                "types": ["GLOSSARY_TERM"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_GLOSSARY_TERMS_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning("GraphQL query returned None for list glossary terms")
                return []
                
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return []

            search_results = search_data.get("searchResults", [])
            terms = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity.get("type") == "GLOSSARY_TERM":
                    processed_term = self._process_glossary_term(entity)
                    if processed_term:
                        terms.append(processed_term)

            self.logger.info(f"Retrieved {len(terms)} glossary terms")
            return terms

        except Exception as e:
            self.logger.error(f"Error listing glossary terms: {str(e)}")
            raise DataHubError(f"Failed to list glossary terms: {str(e)}")

    def get_glossary_term(self, term_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific glossary term by URN.

        Args:
            term_urn: URN of the glossary term

        Returns:
            Glossary term dictionary or None if not found

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Getting glossary term: {term_urn}")

        variables = {"urn": term_urn}

        try:
            result = self.safe_execute_graphql(GET_GLOSSARY_TERM_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning(f"GraphQL query returned None for glossary term: {term_urn}")
                return None
                
            term_data = result.get("glossaryTerm")

            if not term_data:
                self.logger.warning(f"Glossary term not found: {term_urn}")
                return None

            processed_term = self._process_glossary_term(term_data)
            self.logger.info(f"Retrieved glossary term: {processed_term.get('name', 'Unknown')}")
            return processed_term

        except Exception as e:
            self.logger.error(f"Error getting glossary term {term_urn}: {str(e)}")
            raise DataHubError(f"Failed to get glossary term: {str(e)}")

    def create_glossary_term(
        self,
        term_id: str,
        name: str,
        description: str = "",
        parent_node_urn: Optional[str] = None,
        term_source: str = "INTERNAL",
    ) -> Optional[str]:
        """
        Create a new glossary term.

        Args:
            term_id: ID for the term (used in URN)
            name: Display name for the term
            description: Description for the term
            parent_node_urn: URN of the parent node (optional)
            term_source: Source of the term (default: INTERNAL)

        Returns:
            URN of the created term or None if failed

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Creating glossary term: {name}")

        # Build input for creation
        input_data = {
            "id": term_id,
            "name": name,
            "description": description,
            "termSource": term_source,
        }

        if parent_node_urn:
            input_data["parentNode"] = parent_node_urn

        variables = {"input": input_data}

        try:
            result = self._execute_mutation(CREATE_GLOSSARY_TERM_MUTATION, variables)
            created_term = result.get("createGlossaryTerm", {})
            term_urn = created_term.get("urn")

            if term_urn:
                self.logger.info(f"Created glossary term: {term_urn}")
                return term_urn
            else:
                self.logger.error("Failed to create glossary term - no URN returned")
                return None

        except Exception as e:
            self.logger.error(f"Error creating glossary term: {str(e)}")
            raise DataHubError(f"Failed to create glossary term: {str(e)}")

    def delete_glossary_term(self, term_urn: str) -> bool:
        """
        Delete a glossary term.

        Args:
            term_urn: URN of the glossary term to delete

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Deleting glossary term: {term_urn}")

        variables = {"urn": term_urn}

        try:
            result = self._execute_mutation(DELETE_GLOSSARY_TERM_MUTATION, variables)
            success = result.get("deleteEntity", False)

            if success:
                self.logger.info(f"Deleted glossary term: {term_urn}")
            else:
                self.logger.warning(f"Failed to delete glossary term: {term_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error deleting glossary term {term_urn}: {str(e)}")
            raise DataHubError(f"Failed to delete glossary term: {str(e)}")

    def update_glossary_term_description(self, term_urn: str, description: str) -> bool:
        """
        Update the description of a glossary term.

        Args:
            term_urn: URN of the glossary term
            description: New description

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Updating glossary term description: {term_urn}")

        variables = {
            "urn": term_urn,
            "description": description,
        }

        try:
            result = self._execute_mutation(UPDATE_GLOSSARY_TERM_DESCRIPTION_MUTATION, variables)
            success = result.get("updateDescription", False)

            if success:
                self.logger.info(f"Updated glossary term description: {term_urn}")
            else:
                self.logger.warning(f"Failed to update glossary term description: {term_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error updating glossary term description {term_urn}: {str(e)}")
            raise DataHubError(f"Failed to update glossary term description: {str(e)}")

    # Combined Operations
    def get_comprehensive_glossary_data(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """
        Get comprehensive glossary data including both nodes and terms.

        Args:
            query: Search query to filter results
            start: Starting offset for pagination
            count: Number of items to return

        Returns:
            Dictionary containing nodes and terms with metadata

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(
            f"Getting comprehensive glossary data with query='{query}', start={start}, count={count}"
        )

        variables = {
            "input": {
                "query": query,
                "types": ["GLOSSARY_NODE", "GLOSSARY_TERM"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(GET_COMPREHENSIVE_GLOSSARY_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning("GraphQL query returned None, returning empty glossary data")
                return {"nodes": [], "terms": [], "total": 0, "start": 0, "count": 0}
            
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return {"nodes": [], "terms": [], "total": 0, "start": 0, "count": 0}

            search_results = search_data.get("searchResults", [])
            nodes = []
            terms = []

            for result_item in search_results:
                # Add None check for result_item
                if result_item is None:
                    continue
                    
                entity = result_item.get("entity", {})
                
                # Add None check for entity
                if entity is None:
                    continue
                    
                entity_type = entity.get("type")

                if entity_type == "GLOSSARY_NODE":
                    processed_node = self._process_glossary_node(entity)
                    if processed_node:
                        nodes.append(processed_node)
                elif entity_type == "GLOSSARY_TERM":
                    processed_term = self._process_glossary_term(entity)
                    if processed_term:
                        terms.append(processed_term)

            # Resolve relationship names
            all_items = nodes + terms
            self._resolve_relationship_names(all_items)

            result_data = {
                "nodes": nodes,
                "terms": terms,
                "total": search_data.get("total", 0),
                "start": search_data.get("start", 0),
                "count": search_data.get("count", 0),
            }

            self.logger.info(f"Retrieved {len(nodes)} nodes and {len(terms)} terms")
            return result_data

        except Exception as e:
            self.logger.error(f"Error getting comprehensive glossary data: {str(e)}")
            raise DataHubError(f"Failed to get comprehensive glossary data: {str(e)}")

    # Owner Management
    def add_glossary_node_owner(
        self,
        node_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a glossary node.

        Args:
            node_urn: URN of the glossary node
            owner_urn: URN of the owner to add
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding owner {owner_urn} to glossary node {node_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": node_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self._execute_mutation(ADD_GLOSSARY_NODE_OWNER_MUTATION, variables)
            success = result.get("addOwner", False)

            if success:
                self.logger.info(f"Added owner to glossary node: {node_urn}")
            else:
                self.logger.warning(f"Failed to add owner to glossary node: {node_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding owner to glossary node: {str(e)}")
            raise DataHubError(f"Failed to add owner to glossary node: {str(e)}")

    def add_glossary_term_owner(
        self,
        term_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a glossary term.

        Args:
            term_urn: URN of the glossary term
            owner_urn: URN of the owner to add
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding owner {owner_urn} to glossary term {term_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": term_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self._execute_mutation(ADD_GLOSSARY_TERM_OWNER_MUTATION, variables)
            success = result.get("addOwner", False)

            if success:
                self.logger.info(f"Added owner to glossary term: {term_urn}")
            else:
                self.logger.warning(f"Failed to add owner to glossary term: {term_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding owner to glossary term: {str(e)}")
            raise DataHubError(f"Failed to add owner to glossary term: {str(e)}")

    # Term-Entity Relationships
    def add_glossary_term_to_entity(self, entity_urn: str, term_urn: str) -> bool:
        """
        Add a glossary term to an entity.

        Args:
            entity_urn: URN of the entity
            term_urn: URN of the glossary term

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding glossary term {term_urn} to entity {entity_urn}")

        variables = {
            "input": {
                "termUrn": term_urn,
                "resourceUrn": entity_urn,
            }
        }

        try:
            result = self._execute_mutation(ADD_GLOSSARY_TERM_TO_ENTITY_MUTATION, variables)
            success = result.get("addTerm", False)

            if success:
                self.logger.info(f"Added glossary term to entity: {entity_urn}")
            else:
                self.logger.warning(f"Failed to add glossary term to entity: {entity_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding glossary term to entity: {str(e)}")
            raise DataHubError(f"Failed to add glossary term to entity: {str(e)}")

    def remove_glossary_term_from_entity(self, entity_urn: str, term_urn: str) -> bool:
        """
        Remove a glossary term from an entity.

        Args:
            entity_urn: URN of the entity
            term_urn: URN of the glossary term

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Removing glossary term {term_urn} from entity {entity_urn}")

        variables = {
            "input": {
                "termUrn": term_urn,
                "resourceUrn": entity_urn,
            }
        }

        try:
            result = self._execute_mutation(REMOVE_GLOSSARY_TERM_FROM_ENTITY_MUTATION, variables)
            success = result.get("removeTerm", False)

            if success:
                self.logger.info(f"Removed glossary term from entity: {entity_urn}")
            else:
                self.logger.warning(f"Failed to remove glossary term from entity: {entity_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error removing glossary term from entity: {str(e)}")
            raise DataHubError(f"Failed to remove glossary term from entity: {str(e)}")

    def find_entities_with_glossary_term(
        self, term_urn: str, start: int = 0, count: int = 50
    ) -> Dict[str, Any]:
        """
        Find entities that have a specific glossary term.

        Args:
            term_urn: URN of the glossary term
            start: Starting offset for pagination
            count: Number of entities to return

        Returns:
            Dictionary with entities and metadata

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Finding entities with glossary term: {term_urn}")

        variables = {
            "input": {
                "query": f'glossaryTerms.urn:"{term_urn}"',
                "types": ["DATASET", "CHART", "DASHBOARD", "DATA_JOB", "DATA_FLOW"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_ENTITIES_WITH_GLOSSARY_TERM_QUERY, variables)
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return {"entities": [], "total": 0, "start": 0, "count": 0}

            search_results = search_data.get("searchResults", [])
            entities = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                entities.append(entity)

            result_data = {
                "entities": entities,
                "total": search_data.get("total", 0),
                "start": search_data.get("start", 0),
                "count": search_data.get("count", 0),
            }

            self.logger.info(f"Found {len(entities)} entities with glossary term")
            return result_data

        except Exception as e:
            self.logger.error(f"Error finding entities with glossary term: {str(e)}")
            raise DataHubError(f"Failed to find entities with glossary term: {str(e)}")

    # Structured Properties
    def upsert_glossary_node_structured_properties(
        self, node_urn: str, structured_properties: List[Dict[str, Any]]
    ) -> bool:
        """
        Upsert structured properties for a glossary node.

        Args:
            node_urn: URN of the glossary node
            structured_properties: List of structured property dictionaries

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Upserting structured properties for glossary node: {node_urn}")

        variables = {
            "input": {
                "entityUrn": node_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self._execute_mutation(
                UPSERT_GLOSSARY_NODE_STRUCTURED_PROPERTIES_MUTATION, variables
            )
            updated_entity = result.get("upsertStructuredProperties", {})
            success = bool(updated_entity.get("urn"))

            if success:
                self.logger.info(f"Upserted structured properties for glossary node: {node_urn}")
            else:
                self.logger.warning(
                    f"Failed to upsert structured properties for glossary node: {node_urn}"
                )

            return success

        except Exception as e:
            self.logger.error(f"Error upserting structured properties for glossary node: {str(e)}")
            raise DataHubError(
                f"Failed to upsert structured properties for glossary node: {str(e)}"
            )

    def upsert_glossary_term_structured_properties(
        self, term_urn: str, structured_properties: List[Dict[str, Any]]
    ) -> bool:
        """
        Upsert structured properties for a glossary term.

        Args:
            term_urn: URN of the glossary term
            structured_properties: List of structured property dictionaries

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Upserting structured properties for glossary term: {term_urn}")

        variables = {
            "input": {
                "entityUrn": term_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self._execute_mutation(
                UPSERT_GLOSSARY_TERM_STRUCTURED_PROPERTIES_MUTATION, variables
            )
            updated_entity = result.get("upsertStructuredProperties", {})
            success = bool(updated_entity.get("urn"))

            if success:
                self.logger.info(f"Upserted structured properties for glossary term: {term_urn}")
            else:
                self.logger.warning(
                    f"Failed to upsert structured properties for glossary term: {term_urn}"
                )

            return success

        except Exception as e:
            self.logger.error(f"Error upserting structured properties for glossary term: {str(e)}")
            raise DataHubError(
                f"Failed to upsert structured properties for glossary term: {str(e)}"
            )

    # Utility Methods
    def count_glossary_nodes(self, query: str = "*") -> int:
        """
        Count the total number of glossary nodes matching a query.

        Args:
            query: Search query to filter nodes

        Returns:
            Total count of matching nodes

        Raises:
            DataHubError: If the operation fails
        """
        variables = {
            "input": {
                "query": query,
                "types": ["GLOSSARY_NODE"],
                "start": 0,
                "count": 1,
            }
        }

        try:
            result = self.safe_execute_graphql(COUNT_GLOSSARY_NODES_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning("GraphQL query returned None for count glossary nodes")
                return 0
                
            search_data = result.get("searchAcrossEntities", {})
            return search_data.get("total", 0)

        except Exception as e:
            self.logger.error(f"Error counting glossary nodes: {str(e)}")
            raise DataHubError(f"Failed to count glossary nodes: {str(e)}")

    def count_glossary_terms(self, query: str = "*") -> int:
        """
        Count the total number of glossary terms matching a query.

        Args:
            query: Search query to filter terms

        Returns:
            Total count of matching terms

        Raises:
            DataHubError: If the operation fails
        """
        variables = {
            "input": {
                "query": query,
                "types": ["GLOSSARY_TERM"],
                "start": 0,
                "count": 1,
            }
        }

        try:
            result = self.safe_execute_graphql(COUNT_GLOSSARY_TERMS_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning("GraphQL query returned None for count glossary terms")
                return 0
                
            search_data = result.get("searchAcrossEntities", {})
            return search_data.get("total", 0)

        except Exception as e:
            self.logger.error(f"Error counting glossary terms: {str(e)}")
            raise DataHubError(f"Failed to count glossary terms: {str(e)}")

    # Private helper methods
    def _process_glossary_node(self, entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a glossary node entity into standardized format."""
        if not entity or not entity.get("urn"):
            return None

        properties = entity.get("properties", {}) or {}
        ownership = entity.get("ownership", {}) or {}
        structured_props = entity.get("structuredProperties", {}) or {}
        parent_nodes_data = entity.get("parentNodes", {}) or {}
        parent_nodes = parent_nodes_data.get("nodes", []) if parent_nodes_data else []

        # Process ownership
        owners = []
        ownership_owners = ownership.get("owners", []) if ownership else []
        for owner_data in ownership_owners:
            if owner_data:
                owner = owner_data.get("owner", {}) or {}
                ownership_type = owner_data.get("ownershipType", {}) or {}
                owner_properties = owner.get("properties", {}) or {}
                ownership_info = ownership_type.get("info", {}) or {}

                # Extract name from URN if not available in properties
                owner_name = owner.get("username") or owner.get("name")
                if not owner_name and owner.get("urn"):
                    urn_parts = owner.get("urn").split(":")
                    if len(urn_parts) >= 4:
                        owner_name = urn_parts[-1]

                owner_info = {
                    "urn": owner.get("urn"),
                    "type": owner.get("type"),
                    "name": owner_name or "Unknown",
                    "displayName": owner_properties.get("displayName", ""),
                    "email": owner_properties.get("email", ""),
                    "ownershipType": {
                        "urn": ownership_type.get("urn"),
                        "name": ownership_info.get("name", "Unknown"),
                    },
                }
                owners.append(owner_info)

        # Process structured properties
        structured_properties = []
        structured_props_list = structured_props.get("properties", []) if structured_props else []
        for prop_data in structured_props_list:
            if prop_data:
                structured_property = prop_data.get("structuredProperty", {}) or {}
                prop_def = structured_property.get("definition", {}) or {}
                values = prop_data.get("values", []) or []

                prop_info = {
                    "urn": structured_property.get("urn"),
                    "displayName": prop_def.get("displayName", ""),
                    "qualifiedName": prop_def.get("qualifiedName", ""),
                    "values": [v.get("stringValue") or v.get("numberValue") for v in values if v],
                }
                structured_properties.append(prop_info)

        return {
            "urn": entity.get("urn"),
            "type": entity.get("type"),
            "name": properties.get("name", "Unknown"),
            "description": properties.get("description", ""),
            "customProperties": properties.get("customProperties", []),
            "owners": owners,
            "structuredProperties": structured_properties,
            "parentNodes": [
                {
                    "urn": p.get("urn"),
                    "name": p.get("properties", {}).get("name", "") if p.get("properties") else "",
                }
                for p in parent_nodes
                if p
            ],
            "properties": properties,
        }

    def _process_glossary_term(self, entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a glossary term entity into standardized format."""
        if not entity or not entity.get("urn"):
            return None

        properties = entity.get("properties", {}) or {}
        ownership = entity.get("ownership", {}) or {}
        structured_props = entity.get("structuredProperties", {}) or {}
        parent_nodes_data = entity.get("parentNodes", {}) or {}
        parent_nodes = parent_nodes_data.get("nodes", []) if parent_nodes_data else []
        domain = entity.get("domain", {})
        is_related_terms = entity.get("isRelatedTerms", {})
        has_related_terms = entity.get("hasRelatedTerms", {})

        # Process ownership (same as node)
        owners = []
        ownership_owners = ownership.get("owners", []) if ownership else []
        for owner_data in ownership_owners:
            if owner_data:
                owner = owner_data.get("owner", {}) or {}
                ownership_type = owner_data.get("ownershipType", {}) or {}
                owner_properties = owner.get("properties", {}) or {}
                ownership_info = ownership_type.get("info", {}) or {}

                owner_name = owner.get("username") or owner.get("name")
                if not owner_name and owner.get("urn"):
                    urn_parts = owner.get("urn").split(":")
                    if len(urn_parts) >= 4:
                        owner_name = urn_parts[-1]

                owner_info = {
                    "urn": owner.get("urn"),
                    "type": owner.get("type"),
                    "name": owner_name or "Unknown",
                    "displayName": owner_properties.get("displayName", ""),
                    "email": owner_properties.get("email", ""),
                    "ownershipType": {
                        "urn": ownership_type.get("urn"),
                        "name": ownership_info.get("name", "Unknown"),
                    },
                }
                owners.append(owner_info)

        # Process structured properties (same as node)
        structured_properties = []
        structured_props_list = structured_props.get("properties", []) if structured_props else []
        for prop_data in structured_props_list:
            if prop_data:
                structured_property = prop_data.get("structuredProperty", {}) or {}
                prop_def = structured_property.get("definition", {}) or {}
                values = prop_data.get("values", []) or []

                prop_info = {
                    "urn": structured_property.get("urn"),
                    "displayName": prop_def.get("displayName", ""),
                    "qualifiedName": prop_def.get("qualifiedName", ""),
                    "values": [v.get("stringValue") or v.get("numberValue") for v in values if v],
                }
                structured_properties.append(prop_info)

        # Process relationships
        is_related = []
        if is_related_terms and is_related_terms.get("relationships"):
            for rel in is_related_terms["relationships"]:
                if rel and rel.get("entity"):
                    rel_entity = rel["entity"]
                    rel_props = rel_entity.get("properties", {})
                    is_related.append(
                        {
                            "urn": rel_entity.get("urn"),
                            "name": rel_props.get("name", "Unknown"),
                        }
                    )

        has_related = []
        if has_related_terms and has_related_terms.get("relationships"):
            for rel in has_related_terms["relationships"]:
                if rel and rel.get("entity"):
                    rel_entity = rel["entity"]
                    rel_props = rel_entity.get("properties", {})
                    has_related.append(
                        {
                            "urn": rel_entity.get("urn"),
                            "name": rel_props.get("name", "Unknown"),
                        }
                    )

        # Process domain
        domain_info = None
        if domain and domain.get("domain"):
            domain_entity = domain["domain"]
            domain_props = domain_entity.get("properties", {})
            domain_info = {
                "urn": domain_entity.get("urn"),
                "name": domain_props.get("name", "Unknown"),
                "description": domain_props.get("description", ""),
            }

        return {
            "urn": entity.get("urn"),
            "type": entity.get("type"),
            "name": properties.get("name", "Unknown"),
            "description": properties.get("description", ""),
            "termSource": properties.get("termSource", "INTERNAL"),
            "sourceRef": properties.get("sourceRef", ""),
            "sourceUrl": properties.get("sourceUrl", ""),
            "customProperties": properties.get("customProperties", []),
            "owners": owners,
            "structuredProperties": structured_properties,
            "parentNodes": [
                {
                    "urn": p.get("urn"),
                    "name": p.get("properties", {}).get("name", "") if p.get("properties") else "",
                }
                for p in parent_nodes
                if p
            ],
            "domain": domain_info,
            "isRelatedTerms": is_related,
            "hasRelatedTerms": has_related,
            "deprecated": entity.get("deprecation", {}).get("deprecated", False),
            "properties": properties,
        }

    def _resolve_relationship_names(self, items: List[Dict[str, Any]]) -> None:
        """Resolve relationship names for glossary items."""
        # This is a placeholder for relationship name resolution
        # In the original implementation, this method resolved URNs to names
        # For now, we'll leave it as a no-op since the processing methods
        # already handle name resolution
        pass
