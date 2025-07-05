"""
Domain service for DataHub operations.

This service handles all domain-related operations including:
- Listing and searching domains
- Creating and updating domains
- Managing domain ownership
- Domain hierarchy operations
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.domains import (
    ADD_DOMAIN_OWNER_MUTATION,
    CREATE_DOMAIN_MUTATION,
    DELETE_DOMAIN_MUTATION,
    REMOVE_DOMAIN_OWNER_MUTATION,
    UPDATE_DOMAIN_DESCRIPTION_MUTATION,
    UPDATE_DOMAIN_DISPLAY_PROPERTIES_MUTATION,
    UPDATE_DOMAIN_STRUCTURED_PROPERTIES_MUTATION,
)
from datahub_cicd_client.graphql.queries.domains import (
    COUNT_DOMAINS_QUERY,
    FIND_ENTITIES_WITH_DOMAIN_QUERY,
    GET_DOMAIN_QUERY,
    LIST_DOMAINS_QUERY,
)


class DomainService(BaseDataHubClient):
    """Service for managing DataHub domains."""

    def __init__(self, connection: DataHubConnection):
        """Initialize the Domain service."""
        super().__init__(connection)

    def list_domains(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List domains in DataHub.

        Args:
            query: Search query to filter domains
            start: Starting offset for pagination
            count: Maximum number of domains to return

        Returns:
            List of domain objects
        """
        self.logger.info(f"Listing domains with query: {query}, start: {start}, count: {count}")

        variables = {
            "input": {
                "types": ["DOMAIN"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_DOMAINS_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                search_results = result["searchAcrossEntities"]["searchResults"]
                domains = []

                for item in search_results:
                    if "entity" in item and item["entity"] is not None:
                        entity = item["entity"]
                        domain = self._process_domain_entity(entity)
                        domains.append(domain)

                return domains

            self._log_graphql_errors(result)
            return []

        except Exception as e:
            self.logger.error(f"Error listing domains: {str(e)}")
            return []

    def get_domain(self, domain_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific domain by URN.

        Args:
            domain_urn: Domain URN to fetch

        Returns:
            Domain data or None if not found
        """
        self.logger.info(f"Getting domain: {domain_urn}")

        variables = {"urn": domain_urn}

        try:
            result = self.safe_execute_graphql(GET_DOMAIN_QUERY, variables)

            if result and "domain" in result:
                domain_data = result["domain"]
                if not domain_data:
                    return None

                return self._process_domain_entity(domain_data)

            self._log_graphql_errors(result)
            return None

        except Exception as e:
            self.logger.error(f"Error getting domain {domain_urn}: {str(e)}")
            return None

    def create_domain(
        self, domain_id: str, name: str, description: str = "", parent_domain_urn: str = None
    ) -> Optional[str]:
        """
        Create a new domain.

        Args:
            domain_id: Domain ID
            name: Domain name
            description: Domain description
            parent_domain_urn: Parent domain URN (optional)

        Returns:
            Domain URN if successful, None otherwise
        """
        self.logger.info(f"Creating domain: {name} with ID: {domain_id}")

        input_data = {"id": domain_id, "name": name, "description": description}

        if parent_domain_urn:
            input_data["parentDomain"] = parent_domain_urn

        variables = {"input": input_data}

        try:
            result = self.safe_execute_graphql(CREATE_DOMAIN_MUTATION, variables)

            if result and "data" in result and "createDomain" in result["data"]:
                domain_urn = result["data"]["createDomain"]
                if domain_urn:
                    self.logger.info(f"Successfully created domain {name} with URN: {domain_urn}")
                    return domain_urn

            self._log_graphql_errors(result)
            return None

        except Exception as e:
            self.logger.error(f"Error creating domain: {str(e)}")
            return None

    def update_domain_description(self, domain_urn: str, description: str) -> bool:
        """
        Update the description of a domain.

        Args:
            domain_urn: Domain URN
            description: New description

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Updating description for domain {domain_urn}")

        variables = {"input": {"resourceUrn": domain_urn, "description": description}}

        try:
            result = self.safe_execute_graphql(UPDATE_DOMAIN_DESCRIPTION_MUTATION, variables)

            if result and "data" in result and "updateDescription" in result["data"]:
                success = result["data"]["updateDescription"]
                if success:
                    self.logger.info(f"Successfully updated description for domain {domain_urn}")
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error updating domain description: {str(e)}")
            return False

    def update_domain_display_properties(
        self, domain_urn: str, color_hex: str = None, icon: Dict[str, str] = None
    ) -> bool:
        """
        Update the display properties of a domain.

        Args:
            domain_urn: Domain URN
            color_hex: Hex color code (e.g., "#914b4b")
            icon: Icon configuration with name, style, iconLibrary

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Updating display properties for domain {domain_urn}")

        input_data = {}
        if color_hex:
            input_data["colorHex"] = color_hex
        if icon:
            input_data["icon"] = icon

        variables = {"urn": domain_urn, "input": input_data}

        try:
            result = self.safe_execute_graphql(UPDATE_DOMAIN_DISPLAY_PROPERTIES_MUTATION, variables)

            if result and "data" in result and "updateDisplayProperties" in result["data"]:
                success = result["data"]["updateDisplayProperties"]
                if success:
                    self.logger.info(
                        f"Successfully updated display properties for domain {domain_urn}"
                    )
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error updating domain display properties: {str(e)}")
            return False

    def update_domain_structured_properties(
        self, domain_urn: str, structured_properties: List[Dict[str, Any]]
    ) -> bool:
        """
        Update structured properties of a domain.

        Args:
            domain_urn: Domain URN
            structured_properties: List of structured property updates

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Updating structured properties for domain {domain_urn}")

        variables = {
            "input": {
                "entityUrn": domain_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self.safe_execute_graphql(
                UPDATE_DOMAIN_STRUCTURED_PROPERTIES_MUTATION, variables
            )

            if result and "data" in result and "upsertStructuredProperties" in result["data"]:
                self.logger.info(
                    f"Successfully updated structured properties for domain {domain_urn}"
                )
                return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error updating domain structured properties: {str(e)}")
            return False

    def add_domain_owner(
        self,
        domain_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a domain.

        Args:
            domain_urn: Domain URN
            owner_urn: Owner URN (user or group)
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Adding owner {owner_urn} to domain {domain_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": domain_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self.safe_execute_graphql(ADD_DOMAIN_OWNER_MUTATION, variables)

            if result and "data" in result and "addOwner" in result["data"]:
                success = result["data"]["addOwner"]
                if success:
                    self.logger.info(f"Successfully added owner to domain {domain_urn}")
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error adding domain owner: {str(e)}")
            return False

    def remove_domain_owner(
        self,
        domain_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Remove an owner from a domain.

        Args:
            domain_urn: Domain URN
            owner_urn: Owner URN (user or group)
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Removing owner {owner_urn} from domain {domain_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": domain_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self.safe_execute_graphql(REMOVE_DOMAIN_OWNER_MUTATION, variables)

            if result and "data" in result and "removeOwner" in result["data"]:
                success = result["data"]["removeOwner"]
                if success:
                    self.logger.info(f"Successfully removed owner from domain {domain_urn}")
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error removing domain owner: {str(e)}")
            return False

    def delete_domain(self, domain_urn: str) -> bool:
        """
        Delete a domain.

        Args:
            domain_urn: Domain URN to delete

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Deleting domain: {domain_urn}")

        variables = {"urn": domain_urn}

        try:
            result = self.safe_execute_graphql(DELETE_DOMAIN_MUTATION, variables)

            if result and "data" in result and "deleteDomain" in result["data"]:
                success = result["data"]["deleteDomain"]
                if success:
                    self.logger.info(f"Successfully deleted domain {domain_urn}")
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error deleting domain: {str(e)}")
            return False

    def find_entities_with_domain(
        self, domain_urn: str, start: int = 0, count: int = 50
    ) -> Dict[str, Any]:
        """
        Find entities within a domain.

        Args:
            domain_urn: Domain URN to search within
            start: Starting offset for pagination
            count: Maximum number of entities to return

        Returns:
            Dictionary with search results and metadata
        """
        self.logger.info(f"Finding entities in domain {domain_urn}")

        variables = {
            "input": {
                "types": ["DATASET", "DASHBOARD", "CHART", "DATA_PRODUCT"],
                "query": "*",
                "start": start,
                "count": count,
                "filters": [{"field": "domains", "values": [domain_urn]}],
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_ENTITIES_WITH_DOMAIN_QUERY, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                search_data = result["data"]["searchAcrossEntities"]
                entities = []

                for item in search_data.get("searchResults", []):
                    if "entity" in item and item["entity"] is not None:
                        entities.append(item["entity"])

                return {
                    "entities": entities,
                    "start": search_data.get("start", 0),
                    "count": search_data.get("count", 0),
                    "total": search_data.get("total", 0),
                }

            self._log_graphql_errors(result)
            return {"entities": [], "start": 0, "count": 0, "total": 0}

        except Exception as e:
            self.logger.error(f"Error finding entities with domain: {str(e)}")
            return {"entities": [], "start": 0, "count": 0, "total": 0}

    def count_domains(self, query: str = "*") -> int:
        """
        Count domains matching the query.

        Args:
            query: Search query to filter domains

        Returns:
            Number of domains matching the query
        """
        try:
            return self._get_domains_count(query)
        except Exception as e:
            self.logger.error(f"Error counting domains: {str(e)}")
            return 0

    def get_comprehensive_domains_data(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """
        Get comprehensive domain data with all metadata.

        Args:
            query: Search query to filter domains
            start: Starting offset for pagination
            count: Number of domains to return

        Returns:
            Dictionary containing domains with comprehensive metadata
        """
        self.logger.info(
            f"Getting comprehensive domain data with query='{query}', start={start}, count={count}"
        )

        variables = {
            "input": {
                "query": query,
                "types": ["DOMAIN"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_DOMAINS_QUERY, variables)
            
            # Add explicit None check to prevent 'NoneType' object has no attribute 'get' error
            if result is None:
                self.logger.warning("GraphQL query returned None, returning empty domain data")
                return {"domains": [], "total": 0, "start": 0, "count": 0}
            
            self.logger.debug(f"GraphQL result: {result}")
            
            search_data = result.get("searchAcrossEntities", {})
            self.logger.debug(f"search_data: {search_data}")

            if not search_data:
                return {"domains": [], "total": 0, "start": 0, "count": 0}

            search_results = search_data.get("searchResults", [])
            self.logger.debug(f"search_results count: {len(search_results)}")
            domains = []

            for i, result_item in enumerate(search_results):
                # Add None check for result_item
                if result_item is None:
                    self.logger.debug(f"Result item {i} is None, skipping")
                    continue
                    
                entity = result_item.get("entity", {})
                self.logger.debug(f"Result item {i} entity: {entity}")
                
                # Add None check for entity
                if entity is None:
                    self.logger.debug(f"Result item {i} entity is None, skipping")
                    continue
                    
                entity_type = entity.get("type")
                self.logger.debug(f"Result item {i} entity_type: {entity_type}")

                if entity_type == "DOMAIN":
                    processed_domain = self._process_domain_entity(entity)
                    self.logger.debug(f"Processed domain {i}: {processed_domain}")
                    if processed_domain:
                        domains.append(processed_domain)
                else:
                    self.logger.debug(f"Result item {i} is not a DOMAIN, skipping")

            result_data = {
                "domains": domains,
                "total": search_data.get("total", 0),
                "start": search_data.get("start", 0),
                "count": search_data.get("count", 0),
            }

            self.logger.info(f"Retrieved {len(domains)} domains")
            return result_data

        except Exception as e:
            self.logger.error(f"Error getting comprehensive domain data: {str(e)}")
            raise Exception(f"Failed to get comprehensive domain data: {str(e)}")

    def _get_domains_count(self, query: str = "*") -> int:
        """Get total count of domains matching query."""
        variables = {
            "input": {
                "types": ["DOMAIN"],
                "query": query,
                "start": 0,
                "count": 1,
                "filters": [],
            }
        }

        try:
            result = self.safe_execute_graphql(COUNT_DOMAINS_QUERY, variables)

            if result and "data" in result and "searchAcrossEntities" in result["data"]:
                return result["data"]["searchAcrossEntities"].get("total", 0)

            return 0

        except Exception as e:
            self.logger.error(f"Error getting domains count: {str(e)}")
            return 0

    def _process_domain_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Process domain entity data from GraphQL response."""
        properties = entity.get("properties", {})

        # Extract parent domain URN
        parent_urn = None
        parent_domains = entity.get("parentDomains")
        if parent_domains and parent_domains.get("domains"):
            domains_list = parent_domains["domains"]
            if domains_list and len(domains_list) > 0:
                parent_urn = domains_list[0].get("urn")

        # Extract entities count
        entities_info = entity.get("entities", {})
        entities_count = entities_info.get("total", 0) if entities_info else 0

        return {
            "urn": entity.get("urn"),
            "id": entity.get("id"),
            "name": properties.get("name"),
            "description": properties.get("description"),
            "properties": properties,
            "parentDomain": parent_urn,  # For backward compatibility
            "parentDomains": entity.get("parentDomains"),
            "ownership": entity.get("ownership"),
            "institutionalMemory": entity.get("institutionalMemory"),
            "displayProperties": entity.get("displayProperties"),
            "entities": entity.get("entities"),
            "entities_count": entities_count,
            "structuredProperties": entity.get("structuredProperties"),
        }
