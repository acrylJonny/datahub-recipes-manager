"""
Base DataHub client class.
"""

import logging
from abc import ABC
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.connection import DataHubConnection
from datahub_cicd_client.core.exceptions import DataHubGraphQLError


class BaseDataHubClient(ABC):
    """
    Abstract base class for DataHub clients.

    Provides common functionality for GraphQL execution, error handling,
    and response processing.
    """

    def __init__(self, connection: DataHubConnection):
        """
        Initialize the base client.

        Args:
            connection: DataHub connection instance
        """
        self.connection = connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a GraphQL query.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            GraphQL response data or None if failed

        Raises:
            DataHubGraphQLError: If GraphQL execution fails
        """
        result = self.connection.execute_graphql(query, variables)

        if result is None:
            return None

        # Check for GraphQL errors
        if "errors" in result and result["errors"]:
            error_messages = [error.get("message", "Unknown error") for error in result["errors"]]
            raise DataHubGraphQLError(
                f"GraphQL query failed: {'; '.join(error_messages)}",
                errors=result["errors"]
            )

        # Return the data portion if it exists, otherwise return the full result
        # Some DataHub responses have data nested under 'data', others don't
        if "data" in result:
            return result["data"]
        else:
            return result

    def _extract_search_results(self, data: Dict[str, Any], search_key: str = "searchAcrossEntities") -> List[Dict[str, Any]]:
        """
        Extract search results from GraphQL response.

        Args:
            data: GraphQL response data
            search_key: Key to look for search results

        Returns:
            List of search results
        """
        if not data or search_key not in data:
            return []

        search_data = data[search_key]
        return search_data.get("searchResults", [])

    def _extract_list_results(self, data: Dict[str, Any], list_key: str) -> List[Dict[str, Any]]:
        """
        Extract list results from GraphQL response.

        Args:
            data: GraphQL response data
            list_key: Key to look for list results

        Returns:
            List of results
        """
        if not data or list_key not in data:
            return []

        list_data = data[list_key]
        return list_data.get("entities", []) or list_data.get("results", [])

    def _extract_pagination_info(self, data: Dict[str, Any], key: str) -> Dict[str, Any]:
        """
        Extract pagination information from GraphQL response.

        Args:
            data: GraphQL response data
            key: Key to look for pagination info

        Returns:
            Dictionary with pagination info (start, count, total)
        """
        if not data or key not in data:
            return {"start": 0, "count": 0, "total": 0}

        result_data = data[key]
        return {
            "start": result_data.get("start", 0),
            "count": result_data.get("count", 0),
            "total": result_data.get("total", 0),
        }

    def _log_graphql_errors(self, result: Dict[str, Any]):
        """Log GraphQL errors for debugging."""
        if result and "errors" in result and result["errors"]:
            error_count = len(result["errors"])
            self.logger.warning(f"GraphQL query returned {error_count} error(s)")

            for i, error in enumerate(result["errors"][:3]):  # Log first 3 errors
                message = error.get("message", "Unknown error")
                self.logger.debug(f"GraphQL error {i+1}: {message}")

    def test_connection(self) -> bool:
        """Test connection to DataHub."""
        return self.connection.test_connection()

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
            If the query fails, it returns an empty dict and logs the error.
        """
        try:
            result = self.execute_graphql(query, variables)
            return result if result is not None else {}
        except Exception as e:
            self.logger.error(f"GraphQL query failed: {str(e)}")
            return {}
