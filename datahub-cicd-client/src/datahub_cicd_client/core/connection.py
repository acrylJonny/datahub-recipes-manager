"""
DataHub connection management.
"""

import logging
from typing import Any, Dict, Optional

import requests

from datahub_cicd_client.core.exceptions import DataHubAuthenticationError, DataHubConnectionError

logger = logging.getLogger(__name__)


class DataHubConnection:
    """
    Manages connection to DataHub server.

    Handles authentication, session management, and basic HTTP operations.
    """

    def __init__(self, server_url: str, token: Optional[str] = None,
                 verify_ssl: bool = True, timeout: int = 30):
        """
        Initialize DataHub connection.

        Args:
            server_url: DataHub GMS server URL
            token: Authentication token (optional)
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        self.server_url = server_url.rstrip("/")
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # Set up headers
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }

        if token:
            self.headers["Authorization"] = f"Bearer {token}"

        # Create session for connection reuse
        self._session = requests.Session()
        self._session.headers.update(self.headers)
        self._session.verify = self.verify_ssl

        self.logger = logging.getLogger(__name__)

        # Initialize DataHub SDK graph client if available
        self.graph = None
        self._init_graph_client()

    def _init_graph_client(self):
        """Initialize DataHub Graph client if SDK is available."""
        try:
            from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

            config = DatahubClientConfig(
                server=self.server_url,
                token=self.token,
            )

            if hasattr(config, "verify_ssl"):
                config.verify_ssl = self.verify_ssl

            self.graph = DataHubGraph(config=config)

            if hasattr(self.graph, "verify_ssl"):
                self.graph.verify_ssl = self.verify_ssl

            self.logger.info("DataHubGraph client initialized successfully")

        except ImportError:
            self.logger.debug("DataHub SDK not available, using REST API only")
        except Exception as e:
            self.logger.warning(f"Failed to initialize DataHubGraph client: {str(e)}")

    def test_connection(self) -> bool:
        """
        Test connection to DataHub server.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            response = self._session.get(
                f"{self.server_url}/config",
                timeout=self.timeout
            )

            if response.status_code == 401:
                raise DataHubAuthenticationError("Invalid authentication token")
            elif response.status_code != 200:
                raise DataHubConnectionError(f"HTTP {response.status_code}: {response.text}")

            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def execute_graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a GraphQL query.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            GraphQL response or None if failed
        """
        try:
            # Try with DataHubGraph client first if available
            if self.graph:
                return self.graph.execute_graphql(query, variables)

            # Fallback to direct HTTP request
            payload = {"query": query, "variables": variables or {}}
            graphql_url = f"{self.server_url}/api/graphql"

            response = self._session.post(
                graphql_url,
                json=payload,
                timeout=self.timeout
            )

            if response.status_code == 401:
                raise DataHubAuthenticationError("Invalid authentication token")
            elif response.status_code != 200:
                self.logger.error(f"GraphQL request failed with status {response.status_code}")
                return None

            return response.json()

        except Exception as e:
            self.logger.error(f"Error executing GraphQL query: {str(e)}")
            return None

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Execute GET request."""
        url = f"{self.server_url}{endpoint}"
        return self._session.get(url, params=params, timeout=self.timeout)

    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Execute POST request."""
        url = f"{self.server_url}{endpoint}"
        return self._session.post(url, json=data, timeout=self.timeout)

    def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Execute PUT request."""
        url = f"{self.server_url}{endpoint}"
        return self._session.put(url, json=data, timeout=self.timeout)

    def delete(self, endpoint: str) -> requests.Response:
        """Execute DELETE request."""
        url = f"{self.server_url}{endpoint}"
        return self._session.delete(url, timeout=self.timeout)
