"""
User service for DataHub operations.

This service handles all user-related operations including:
- Listing and searching users
- Getting user details
- User management operations
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.queries.users import (
    COUNT_USERS_QUERY,
    GET_USER_QUERY,
    LIST_USERS_QUERY,
)


class UserService(BaseDataHubClient):
    """Service for managing DataHub users."""

    def __init__(self, connection):
        super().__init__(connection)
        self.entity_type = "corpuser"

    # Input Operations (Read/Query)

    def list_users(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List users from DataHub.

        Args:
            query: Search query string
            start: Starting offset
            count: Number of results to return

        Returns:
            List of user data
        """
        self.logger.info(f"Listing users with query '{query}', start={start}, count={count}")

        variables = {
            "input": {
                "types": ["CORP_USER"],
                "query": query,
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_USERS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                self.logger.warning("No searchAcrossEntities data in response")
                return []

            search_results = data["searchAcrossEntities"].get("searchResults", [])
            users = []

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    users.append(entity)

            self.logger.info(f"Retrieved {len(users)} users")
            return users

        except Exception as e:
            self.logger.error(f"Error listing users: {str(e)}")
            return []

    def get_user(self, user_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single user by URN.

        Args:
            user_urn: The URN of the user to retrieve

        Returns:
            User data or None if not found
        """
        self.logger.info(f"Getting user: {user_urn}")

        variables = {"urn": user_urn}

        try:
            data = self.safe_execute_graphql(GET_USER_QUERY, variables)

            if not data or "corpUser" not in data:
                self.logger.warning(f"User not found: {user_urn}")
                return None

            user = data["corpUser"]
            self.logger.info(f"Retrieved user: {user.get('username', 'Unknown')}")
            return user

        except Exception as e:
            self.logger.error(f"Error getting user {user_urn}: {str(e)}")
            return None

    def count_users(self, query: str = "*") -> int:
        """
        Count users matching the query.

        Args:
            query: Search query string

        Returns:
            Number of matching users
        """
        self.logger.info(f"Counting users with query '{query}'")

        variables = {
            "input": {
                "types": ["CORP_USER"],
                "query": query,
                "start": 0,
                "count": 1,  # We only need the total count
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(COUNT_USERS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return 0

            total = data["searchAcrossEntities"].get("total", 0)
            self.logger.info(f"Found {total} users")
            return total

        except Exception as e:
            self.logger.error(f"Error counting users: {str(e)}")
            return 0

    def search_users(self, query: str = "*", start: int = 0, count: int = 200) -> List[Dict[str, Any]]:
        """
        Search for users.

        Args:
            query: Search query string
            start: Starting offset
            count: Number of results to return

        Returns:
            List of user data
        """
        self.logger.info(f"Searching users with query '{query}'")

        variables = {
            "input": {
                "types": ["CORP_USER"],
                "query": query,
                "start": start,
                "count": count,
                "filters": []
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_USERS_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                self.logger.warning("No searchAcrossEntities data in response")
                return []

            search_results = data["searchAcrossEntities"].get("searchResults", [])
            users = []

            for result in search_results:
                entity = result.get("entity")
                if entity:
                    users.append(entity)

            self.logger.info(f"Retrieved {len(users)} users")
            return users

        except Exception as e:
            self.logger.error(f"Error searching users: {str(e)}")
            return []

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by username.

        Args:
            username: The username to search for

        Returns:
            User data or None if not found
        """
        # Construct URN from username
        user_urn = f"urn:li:corpuser:{username}"
        return self.get_user(user_urn)

    # Utility Methods

    def format_user_for_display(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format user data for display in UI.

        Args:
            user_data: Raw user data from GraphQL

        Returns:
            Formatted user data
        """
        if not user_data:
            return {}

        properties = user_data.get("properties", {})
        info = user_data.get("info", {})
        editable_properties = user_data.get("editableProperties", {})

        return {
            "urn": user_data.get("urn", ""),
            "username": user_data.get("username", ""),
            "display_name": (
                properties.get("displayName") or
                info.get("displayName") or
                editable_properties.get("displayName") or
                user_data.get("username", "Unknown")
            ),
            "full_name": (
                properties.get("fullName") or
                info.get("fullName") or
                ""
            ),
            "email": (
                properties.get("email") or
                info.get("email") or
                ""
            ),
            "title": properties.get("title", ""),
            "department": properties.get("departmentName", ""),
            "manager_urn": properties.get("managerUrn", ""),
            "teams": editable_properties.get("teams", []),
            "skills": editable_properties.get("skills", []),
            "picture_link": editable_properties.get("pictureLink", ""),
            "status": user_data.get("status", {})
        }

    def is_user_active(self, user_data: Dict[str, Any]) -> bool:
        """
        Check if a user is active (not removed).

        Args:
            user_data: User data from GraphQL

        Returns:
            True if user is active, False otherwise
        """
        status = user_data.get("status", {})
        return not status.get("removed", False)
