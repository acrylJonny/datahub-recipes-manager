"""
Group service for DataHub operations.

This service handles all group-related operations including:
- Listing and searching groups
- Getting group details
- Group membership operations
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.queries.groups import (
    COUNT_GROUPS_QUERY,
    GET_GROUP_QUERY,
    LIST_GROUPS_QUERY,
)


class GroupService(BaseDataHubClient):
    """Service for managing DataHub groups."""

    def __init__(self, connection):
        super().__init__(connection)
        self.entity_type = "corpgroup"

    # Input Operations (Read/Query)

    def list_groups(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List groups from DataHub.

        Args:
            query: Search query string
            start: Starting offset
            count: Number of results to return

        Returns:
            List of group data
        """
        self.logger.info(f"Listing groups with query '{query}', start={start}, count={count}")

        variables = {
            "input": {
                "types": ["CORP_GROUP"],
                "query": query,
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_GROUPS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                self.logger.warning("No searchAcrossEntities data in response")
                return []

            search_results = data["searchAcrossEntities"].get("searchResults", [])
            groups = []

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    groups.append(entity)

            self.logger.info(f"Retrieved {len(groups)} groups")
            return groups

        except Exception as e:
            self.logger.error(f"Error listing groups: {str(e)}")
            return []

    def get_group(self, group_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single group by URN.

        Args:
            group_urn: The URN of the group to retrieve

        Returns:
            Group data or None if not found
        """
        self.logger.info(f"Getting group: {group_urn}")

        variables = {"urn": group_urn}

        try:
            data = self.safe_execute_graphql(GET_GROUP_QUERY, variables)

            if not data or "corpGroup" not in data:
                self.logger.warning(f"Group not found: {group_urn}")
                return None

            group = data["corpGroup"]
            self.logger.info(f"Retrieved group: {group.get('name', 'Unknown')}")
            return group

        except Exception as e:
            self.logger.error(f"Error getting group {group_urn}: {str(e)}")
            return None

    def count_groups(self, query: str = "*") -> int:
        """
        Count groups matching the query.

        Args:
            query: Search query string

        Returns:
            Number of matching groups
        """
        self.logger.info(f"Counting groups with query '{query}'")

        variables = {
            "input": {
                "types": ["CORP_GROUP"],
                "query": query,
                "start": 0,
                "count": 1,  # We only need the total count
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(COUNT_GROUPS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return 0

            total = data["searchAcrossEntities"].get("total", 0)
            self.logger.info(f"Found {total} groups")
            return total

        except Exception as e:
            self.logger.error(f"Error counting groups: {str(e)}")
            return 0

    def search_groups(self, query: str = "*", start: int = 0, count: int = 200) -> List[Dict[str, Any]]:
        """
        Search for groups.

        Args:
            query: Search query string
            start: Starting offset
            count: Number of results to return

        Returns:
            List of group data
        """
        self.logger.info(f"Searching groups with query '{query}'")

        variables = {
            "input": {
                "types": ["CORP_GROUP"],  # Use 'types' (plural) instead of 'type'
                "query": query,
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_GROUPS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                self.logger.warning("No searchAcrossEntities data in response")
                return []

            search_results = data["searchAcrossEntities"].get("searchResults", [])
            groups = []

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    groups.append(entity)

            self.logger.info(f"Retrieved {len(groups)} groups")
            return groups

        except Exception as e:
            self.logger.error(f"Error searching groups: {str(e)}")
            return []

    def get_group_by_name(self, group_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a group by name.

        Args:
            group_name: The group name to search for

        Returns:
            Group data or None if not found
        """
        # Construct URN from group name
        group_urn = f"urn:li:corpGroup:{group_name}"
        return self.get_group(group_urn)

    def get_group_members(self, group_urn: str) -> List[Dict[str, Any]]:
        """
        Get members of a group.

        Args:
            group_urn: The URN of the group

        Returns:
            List of group members
        """
        group_data = self.get_group(group_urn)
        if not group_data:
            return []

        members_data = group_data.get("members", {})
        if not members_data:
            return []

        relationships = members_data.get("relationships", [])
        members = []

        for relationship in relationships:
            entity = relationship.get("entity")
            if entity:
                members.append(entity)

        return members

    # Utility Methods

    def format_group_for_display(self, group_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format group data for display in UI.

        Args:
            group_data: Raw group data from GraphQL

        Returns:
            Formatted group data
        """
        if not group_data:
            return {}

        properties = group_data.get("properties", {})
        info = group_data.get("info", {})
        editable_properties = group_data.get("editableProperties", {})
        members = group_data.get("members", {})

        return {
            "urn": group_data.get("urn", ""),
            "name": group_data.get("name", ""),
            "display_name": (
                properties.get("displayName") or
                info.get("displayName") or
                editable_properties.get("displayName") or
                group_data.get("name", "Unknown")
            ),
            "description": (
                properties.get("description") or
                info.get("description") or
                editable_properties.get("description") or
                ""
            ),
            "email": (
                properties.get("email") or
                info.get("email") or
                ""
            ),
            "picture_link": editable_properties.get("pictureLink", ""),
            "member_count": members.get("total", 0),
            "status": group_data.get("status", {})
        }

    def is_group_active(self, group_data: Dict[str, Any]) -> bool:
        """
        Check if a group is active (not removed).

        Args:
            group_data: Group data from GraphQL

        Returns:
            True if group is active, False otherwise
        """
        status = group_data.get("status", {})
        return not status.get("removed", False)
