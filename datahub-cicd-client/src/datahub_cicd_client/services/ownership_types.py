"""
Ownership types service for DataHub operations.

This service handles all ownership type-related operations including:
- Listing and searching ownership types
- Getting ownership type details
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.queries.ownership_types import (
    GET_OWNERSHIP_TYPE_QUERY,
    LIST_OWNERSHIP_TYPES_QUERY,
    LIST_OWNERSHIP_TYPES_SEARCH_QUERY,
)


class OwnershipTypeService(BaseDataHubClient):
    """Service for managing DataHub ownership types."""

    def __init__(self, connection):
        super().__init__(connection)
        self.entity_type = "ownershiptype"

    # Input Operations (Read/Query)

    def list_ownership_types(self, start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List ownership types from DataHub.

        Args:
            start: Starting offset
            count: Number of results to return

        Returns:
            List of ownership type data
        """
        self.logger.info(f"Listing ownership types, start={start}, count={count}")

        # Try the GraphQL queries first, but fallback to defaults if schema doesn't support it
        try:
            variables = {
                "input": {
                    "start": start,
                    "count": count
                }
            }

            # Try the dedicated listOwnershipTypes query first
            data = self.safe_execute_graphql(LIST_OWNERSHIP_TYPES_QUERY, variables)

            if data and "listOwnershipTypes" in data:
                ownership_types_data = data["listOwnershipTypes"]
                ownership_types = ownership_types_data.get("ownershipTypes", [])
                self.logger.info(f"Retrieved {len(ownership_types)} ownership types using listOwnershipTypes")
                return ownership_types
            else:
                # Fallback to search-based approach
                self.logger.info("Falling back to search-based ownership types listing")
                return self._list_ownership_types_search(start, count)

        except Exception as e:
            self.logger.warning(f"GraphQL queries failed, using default ownership types: {str(e)}")
            # Return default ownership types since the schema doesn't support querying them
            return self.get_default_ownership_types()

    def _list_ownership_types_search(self, start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List ownership types using search query as fallback.

        Args:
            start: Starting offset
            count: Number of results to return

        Returns:
            List of ownership type data
        """
        variables = {
            "input": {
                "type": "OWNERSHIP_TYPE",
                "query": "*",
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_OWNERSHIP_TYPES_SEARCH_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                self.logger.warning("No searchAcrossEntities data in response, using default ownership types")
                return self.get_default_ownership_types()

            search_results = data["searchAcrossEntities"].get("searchResults", [])
            ownership_types = []

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    ownership_types.append(entity)

            if ownership_types:
                self.logger.info(f"Retrieved {len(ownership_types)} ownership types using search")
                return ownership_types
            else:
                self.logger.info("No ownership types found via search, using defaults")
                return self.get_default_ownership_types()

        except Exception as e:
            self.logger.error(f"Error listing ownership types with search, using defaults: {str(e)}")
            return self.get_default_ownership_types()

    def get_ownership_type(self, ownership_type_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single ownership type by URN.

        Args:
            ownership_type_urn: The URN of the ownership type to retrieve

        Returns:
            Ownership type data or None if not found
        """
        self.logger.info(f"Getting ownership type: {ownership_type_urn}")

        variables = {"urn": ownership_type_urn}

        try:
            data = self.safe_execute_graphql(GET_OWNERSHIP_TYPE_QUERY, variables)

            if not data or "ownershipType" not in data:
                self.logger.warning(f"Ownership type not found: {ownership_type_urn}")
                return None

            ownership_type = data["ownershipType"]
            self.logger.info(f"Retrieved ownership type: {ownership_type.get('name', 'Unknown')}")
            return ownership_type

        except Exception as e:
            self.logger.error(f"Error getting ownership type {ownership_type_urn}: {str(e)}")
            return None

    def count_ownership_types(self) -> int:
        """
        Count total ownership types.

        Returns:
            Number of ownership types
        """
        self.logger.info("Counting ownership types")

        variables = {
            "input": {
                "start": 0,
                "count": 1  # We only need the total count
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_OWNERSHIP_TYPES_QUERY, variables)

            if data and "listOwnershipTypes" in data:
                total = data["listOwnershipTypes"].get("total", 0)
                self.logger.info(f"Found {total} ownership types")
                return total
            else:
                # Fallback count using search
                return self._count_ownership_types_search()

        except Exception as e:
            self.logger.warning(f"Error counting ownership types, trying search fallback: {str(e)}")
            return self._count_ownership_types_search()

    def _count_ownership_types_search(self) -> int:
        """
        Count ownership types using search as fallback.

        Returns:
            Number of ownership types
        """
        variables = {
            "input": {
                "type": "OWNERSHIP_TYPE",
                "query": "*",
                "start": 0,
                "count": 1,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_OWNERSHIP_TYPES_SEARCH_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return 0

            total = data["searchAcrossEntities"].get("total", 0)
            self.logger.info(f"Found {total} ownership types using search")
            return total

        except Exception as e:
            self.logger.error(f"Error counting ownership types with search: {str(e)}")
            return 0

    def search_ownership_types(self, query: str, start: int = 0, count: int = 100) -> Dict[str, Any]:
        """
        Search for ownership types with detailed response.

        Args:
            query: Search query string
            start: Starting offset
            count: Number of results to return

        Returns:
            Search response with ownership types and metadata
        """
        self.logger.info(f"Searching ownership types with query '{query}'")

        variables = {
            "input": {
                "type": "OWNERSHIP_TYPE",
                "query": query,
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_OWNERSHIP_TYPES_SEARCH_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return {"ownership_types": [], "total": 0, "start": start, "count": 0}

            search_data = data["searchAcrossEntities"]
            ownership_types = []

            for result in search_data.get("searchResults", []):
                entity = result.get("entity")
                if entity:
                    ownership_types.append(entity)

            response = {
                "ownership_types": ownership_types,
                "total": search_data.get("total", 0),
                "start": search_data.get("start", start),
                "count": search_data.get("count", len(ownership_types))
            }

            self.logger.info(f"Search returned {len(ownership_types)} ownership types out of {response['total']} total")
            return response

        except Exception as e:
            self.logger.error(f"Error searching ownership types: {str(e)}")
            return {"ownership_types": [], "total": 0, "start": start, "count": 0}

    def get_default_ownership_types(self) -> List[Dict[str, Any]]:
        """
        Get the default/system ownership types.

        Returns:
            List of default ownership types
        """
        # Common default ownership types in DataHub
        default_types = [
            {
                "urn": "urn:li:ownershipType:__system__business_owner",
                "name": "__system__business_owner",
                "info": {
                    "name": "Business Owner",
                    "description": "Business owner of the entity"
                }
            },
            {
                "urn": "urn:li:ownershipType:__system__technical_owner",
                "name": "__system__technical_owner",
                "info": {
                    "name": "Technical Owner",
                    "description": "Technical owner of the entity"
                }
            },
            {
                "urn": "urn:li:ownershipType:__system__data_steward",
                "name": "__system__data_steward",
                "info": {
                    "name": "Data Steward",
                    "description": "Data steward of the entity"
                }
            }
        ]

        self.logger.info("Returning default ownership types")
        return default_types

    # Utility Methods

    def format_ownership_type_for_display(self, ownership_type_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format ownership type data for display in UI.

        Args:
            ownership_type_data: Raw ownership type data from GraphQL

        Returns:
            Formatted ownership type data
        """
        if not ownership_type_data:
            return {}

        info = ownership_type_data.get("info", {})

        return {
            "urn": ownership_type_data.get("urn", ""),
            "name": ownership_type_data.get("name", ""),
            "display_name": info.get("name", ownership_type_data.get("name", "Unknown")),
            "description": info.get("description", "")
        }

    def get_ownership_type_by_name(self, type_name: str) -> Optional[Dict[str, Any]]:
        """
        Get an ownership type by name.

        Args:
            type_name: The name of the ownership type

        Returns:
            Ownership type data or None if not found
        """
        # Try common URN patterns
        possible_urns = [
            f"urn:li:ownershipType:{type_name}",
            f"urn:li:ownershipType:__system__{type_name}",
            f"urn:li:ownershipType:__system__{type_name.lower()}",
        ]

        for urn in possible_urns:
            result = self.get_ownership_type(urn)
            if result:
                return result

        # If not found by URN, search by name
        search_result = self.search_ownership_types(type_name, start=0, count=10)
        ownership_types = search_result.get("ownership_types", [])

        for ownership_type in ownership_types:
            if (ownership_type.get("name") == type_name or
                ownership_type.get("info", {}).get("name") == type_name):
                return ownership_type

        return None
