"""
GraphQL connection client for DataHub.
"""

import json
import logging
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class GraphQLConnection:
    """GraphQL connection to DataHub GMS."""

    def __init__(self, datahub_url: str, token: Optional[str] = None):
        """
        Initialize GraphQL connection.

        Args:
            datahub_url: DataHub GMS URL
            token: Authentication token (optional)
        """
        self.datahub_url = datahub_url.rstrip("/")
        self.token = token
        self.graphql_endpoint = f"{self.datahub_url}/api/graphql"

        self.session = requests.Session()
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        self.session.headers.update(
            {"Content-Type": "application/json", "User-Agent": "datahub-cicd-client/0.1.0"}
        )

    def execute_query(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            GraphQL response data

        Raises:
            Exception: If query fails
        """
        payload = {"query": query, "variables": variables or {}}

        try:
            logger.debug(f"Executing GraphQL query: {query[:100]}...")
            response = self.session.post(self.graphql_endpoint, json=payload, timeout=30)
            response.raise_for_status()

            result = response.json()

            if "errors" in result:
                error_messages = [error.get("message", str(error)) for error in result["errors"]]
                raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")

            return result.get("data", {})

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error executing GraphQL query: {e}")
            raise Exception(f"Failed to execute GraphQL query: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            raise Exception(f"Invalid JSON response from GraphQL endpoint: {e}")
        except Exception as e:
            logger.error(f"Unexpected error executing GraphQL query: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test the GraphQL connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Simple introspection query to test connection
            query = """
            query {
                __schema {
                    queryType {
                        name
                    }
                }
            }
            """
            result = self.execute_query(query)
            return bool(result.get("__schema"))
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
