"""
Structured Properties service for DataHub operations.

This service handles all structured properties-related operations including:
- Listing and searching structured properties
- Creating and updating structured properties
- Managing structured property values on entities
- Property definition management
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.graphql.mutations.properties import (
    CREATE_STRUCTURED_PROPERTY_MUTATION,
    DELETE_STRUCTURED_PROPERTY_MUTATION,
    REMOVE_STRUCTURED_PROPERTIES_MUTATION,
    UPSERT_STRUCTURED_PROPERTIES_MUTATION,
)
from datahub_cicd_client.graphql.queries.properties import (
    GET_STRUCTURED_PROPERTIES_URNS_QUERY,
    GET_STRUCTURED_PROPERTY_QUERY,
    LIST_STRUCTURED_PROPERTIES_QUERY,
    TEST_STRUCTURED_PROPERTY_SUPPORT_QUERY,
)


class StructuredPropertiesService(BaseDataHubClient):
    """Service for managing DataHub structured properties."""

    def __init__(self, connection: DataHubConnection):
        """Initialize the Structured Properties service."""
        super().__init__(connection)

    def list_structured_properties(
        self, query: str = "*", start: int = 0, count: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List structured properties in DataHub.

        Args:
            query: Search query to filter properties
            start: Starting offset for pagination
            count: Maximum number of properties to return

        Returns:
            List of structured property objects
        """
        self.logger.info(
            f"Listing structured properties with query: {query}, start: {start}, count: {count}"
        )

        # First test if structured properties are supported
        if not self._test_structured_property_support():
            self.logger.warning("StructuredProperty type not supported in this DataHub version")
            return []

        variables = {
            "input": {
                "types": ["STRUCTURED_PROPERTY"],
                "query": query,
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(LIST_STRUCTURED_PROPERTIES_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                search_results = result["searchAcrossEntities"].get("searchResults", [])

                properties = []
                for search_result in search_results:
                    entity = search_result.get("entity", {})
                    if entity.get("type") == "STRUCTURED_PROPERTY":
                        properties.append(entity)

                return properties

            self._log_graphql_errors(result)
            return []

        except Exception as e:
            error_str = str(e)
            if "Unknown type" in error_str or "argument of type 'NoneType'" in error_str:
                self.logger.warning("StructuredProperty type not supported in this DataHub version")
                return []
            else:
                self.logger.error(f"Error listing structured properties: {error_str}")
                return []

    def get_structured_property(self, property_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific structured property by URN.

        Args:
            property_urn: Structured property URN to fetch

        Returns:
            Structured property data or None if not found
        """
        self.logger.info(f"Getting structured property: {property_urn}")

        variables = {"urn": property_urn}

        try:
            result = self.safe_execute_graphql(GET_STRUCTURED_PROPERTY_QUERY, variables)

            if result and "structuredProperty" in result:
                return result["structuredProperty"]

            self._log_graphql_errors(result)
            return None

        except Exception as e:
            self.logger.error(f"Error getting structured property {property_urn}: {str(e)}")
            return None

    def get_structured_properties_urns(self, start: int = 0, count: int = 1000) -> Dict[str, Any]:
        """
        Get all structured property URNs for filtering.

        Args:
            start: Start index for pagination
            count: Number of entities to return

        Returns:
            Dictionary with structured property URNs and filter fields
        """
        self.logger.info(f"Getting structured property URNs, start: {start}, count: {count}")

        variables = {
            "input": {
                "start": start,
                "count": count,
                "query": "*",
                "types": ["STRUCTURED_PROPERTY"],
            }
        }

        try:
            result = self.safe_execute_graphql(GET_STRUCTURED_PROPERTIES_URNS_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                search_data = result["searchAcrossEntities"]
                structured_properties = []

                for item in search_data.get("searchResults", []):
                    entity = item.get("entity", {})
                    urn = entity.get("urn")

                    if urn and (
                        "urn:li:structuredProperty:" in urn or "urn:li:structuredproperty:" in urn
                    ):
                        # Extract the ID from the URN
                        if "urn:li:structuredProperty:" in urn:
                            property_id = urn.replace("urn:li:structuredProperty:", "")
                        else:
                            property_id = urn.replace("urn:li:structuredproperty:", "")

                        structured_properties.append(
                            {
                                "urn": urn,
                                "id": property_id,
                                "filter_field": f"structuredProperties.{property_id}",
                            }
                        )

                return {
                    "success": True,
                    "total": search_data.get("total", 0),
                    "structured_properties": structured_properties,
                }

            self._log_graphql_errors(result)
            return {"success": False, "error": "Failed to fetch structured properties"}

        except Exception as e:
            self.logger.error(f"Error getting structured property URNs: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_structured_property(
        self,
        display_name: str,
        description: str = "",
        value_type: str = "STRING",
        cardinality: str = "SINGLE",
        entity_types: List[str] = None,
        allowed_values: List[Any] = None,
        qualified_name: str = None,
        **kwargs,
    ) -> Optional[str]:
        """
        Create a new structured property.

        Args:
            display_name: Display name for the property
            description: Property description
            value_type: Type of values (STRING, NUMBER, etc.)
            cardinality: SINGLE or MULTIPLE
            entity_types: List of entity types this property applies to
            allowed_values: List of allowed values (optional)
            qualified_name: Qualified name (optional, generated if not provided)
            **kwargs: Additional property configuration

        Returns:
            Property URN if successful, None otherwise
        """
        self.logger.info(f"Creating structured property: {display_name}")

        if not qualified_name:
            # Generate qualified name from display name
            qualified_name = display_name.lower().replace(" ", "_").replace("-", "_")

        input_data = {
            "displayName": display_name,
            "description": description,
            "qualifiedName": qualified_name,
            "valueType": value_type,
            "cardinality": cardinality,
        }

        if entity_types:
            input_data["entityTypes"] = entity_types

        if allowed_values:
            input_data["allowedValues"] = allowed_values

        # Add any additional configuration from kwargs
        input_data.update(kwargs)

        variables = {"input": input_data}

        try:
            result = self.safe_execute_graphql(CREATE_STRUCTURED_PROPERTY_MUTATION, variables)

            if result and "createStructuredProperty" in result:
                property_data = result["createStructuredProperty"]
                property_urn = property_data.get("urn")
                if property_urn:
                    self.logger.info(
                        f"Successfully created structured property {display_name} with URN: {property_urn}"
                    )
                    return property_urn

            self._log_graphql_errors(result)
            return None

        except Exception as e:
            self.logger.error(f"Error creating structured property: {str(e)}")
            return None

    def delete_structured_property(self, property_urn: str) -> bool:
        """
        Delete a structured property.

        Args:
            property_urn: Structured property URN to delete

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Deleting structured property: {property_urn}")

        variables = {"urn": property_urn}

        try:
            result = self.safe_execute_graphql(DELETE_STRUCTURED_PROPERTY_MUTATION, variables)

            if result and "deleteStructuredProperty" in result:
                success = result["deleteStructuredProperty"]
                if success:
                    self.logger.info(f"Successfully deleted structured property {property_urn}")
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error deleting structured property: {str(e)}")
            return False

    def upsert_structured_properties(
        self, entity_urn: str, structured_properties: List[Dict[str, Any]]
    ) -> bool:
        """
        Upsert structured properties on an entity.

        Args:
            entity_urn: Entity URN to update
            structured_properties: List of structured property updates

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Upserting structured properties for entity {entity_urn}")

        variables = {
            "input": {
                "entityUrn": entity_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self.safe_execute_graphql(UPSERT_STRUCTURED_PROPERTIES_MUTATION, variables)

            if result and "upsertStructuredProperties" in result:
                self.logger.info(
                    f"Successfully upserted structured properties for entity {entity_urn}"
                )
                return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error upserting structured properties: {str(e)}")
            return False

    def remove_structured_properties(self, entity_urn: str, property_urns: List[str]) -> bool:
        """
        Remove structured properties from an entity.

        Args:
            entity_urn: Entity URN to update
            property_urns: List of structured property URNs to remove

        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Removing structured properties from entity {entity_urn}")

        variables = {"input": {"entityUrn": entity_urn, "structuredPropertyUrns": property_urns}}

        try:
            result = self.safe_execute_graphql(REMOVE_STRUCTURED_PROPERTIES_MUTATION, variables)

            if result and "removeStructuredProperties" in result:
                success = result["removeStructuredProperties"]
                if success:
                    self.logger.info(
                        f"Successfully removed structured properties from entity {entity_urn}"
                    )
                    return True

            self._log_graphql_errors(result)
            return False

        except Exception as e:
            self.logger.error(f"Error removing structured properties: {str(e)}")
            return False

    def _test_structured_property_support(self) -> bool:
        """Test if structured properties are supported in this DataHub version."""
        try:
            variables = {
                "input": {"types": ["STRUCTURED_PROPERTY"], "query": "*", "start": 0, "count": 1}
            }

            result = self.safe_execute_graphql(TEST_STRUCTURED_PROPERTY_SUPPORT_QUERY, variables)

            if result and "errors" in result:
                for error in result["errors"]:
                    if "Unknown type 'StructuredProperty'" in error.get("message", ""):
                        return False
                    elif "UnknownType" in error.get("message", ""):
                        return False

            return True

        except Exception:
            return False

    def _get_structured_properties_count(self, query: str = "*") -> int:
        """Get total count of structured properties matching query."""
        if not self._test_structured_property_support():
            return 0

        variables = {
            "input": {"types": ["STRUCTURED_PROPERTY"], "query": query, "start": 0, "count": 1}
        }

        try:
            result = self.safe_execute_graphql(TEST_STRUCTURED_PROPERTY_SUPPORT_QUERY, variables)

            if result and "searchAcrossEntities" in result:
                return result["searchAcrossEntities"].get("total", 0)

            return 0

        except Exception as e:
            self.logger.error(f"Error getting structured properties count: {str(e)}")
            return 0
