#!/usr/bin/env python3
"""
Test script to demonstrate how to use the DataHubGraph client directly.
This file can be run directly or via pytest.
"""

import os
import sys
import json
import logging
from pathlib import Path

try:
    import pytest
except ImportError:
    pytest = None

from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Apply pytest marks if pytest is available
network_test = (
    pytest.mark.skipif(
        os.environ.get("PYTEST_SKIP_NETWORK", "false").lower() == "true",
        reason="Network tests are disabled",
    )
    if pytest
    else lambda f: f
)


@network_test
def test_list_secrets(client: DataHubRestClient):
    """Test listing secrets using GraphQL"""
    logger.info("Testing list_secrets method...")
    secrets = client.list_secrets()
    if secrets:
        logger.info(f"Found {len(secrets)} secrets:")
        for secret in secrets:
            logger.info(f"Secret: {secret['name']}, URN: {secret['urn']}")
    else:
        logger.info("No secrets found.")


@network_test
def test_update_ingestion_source(
    client: DataHubRestClient, source_id: str = "analytics-database-prod"
):
    """
    Test updating an ingestion source using the GraphQL mutation.
    Uses the exact format provided in the documentation.
    """
    logger.info(
        f"Testing updateIngestionSource GraphQL mutation for source: {source_id}"
    )

    # Get the current source to ensure it exists
    current_source = client.get_ingestion_source(source_id)
    if not current_source:
        logger.error(f"Source not found: {source_id}")
        return

    # Use the exact GraphQL mutation format
    mutation = """
    mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
      updateIngestionSource(urn: $urn, input: $input)
    }
    """

    # Prepare variables for the mutation
    source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

    # Prepare the recipe content - use the existing recipe if available
    recipe_content = current_source.get("recipe", {})
    if isinstance(recipe_content, dict):
        recipe_json = json.dumps(recipe_content)
    else:
        recipe_json = recipe_content

    # Prepare the input variables following the exact format
    variables = {
        "urn": source_urn,
        "input": {
            "type": current_source.get("type", "postgres"),
            "name": current_source.get("name", "DataHub Postgres Ingestion"),
            "config": {
                "recipe": recipe_json,
                "executorId": "default",
                "debugMode": False,
                "extraArgs": [],
            },
            "schedule": {"interval": "0 0 * * *", "timezone": "UTC"},
        },
    }

    # Log the variables for debugging
    logger.debug(f"GraphQL variables: {json.dumps(variables)}")

    # Execute the GraphQL mutation
    try:
        result = client.execute_graphql(mutation, variables)

        if "errors" in result:
            logger.error(f"GraphQL errors: {result['errors']}")
            return False
        else:
            logger.info(f"Successfully updated ingestion source: {source_id}")
            return True
    except Exception as e:
        logger.error(f"Error updating ingestion source: {str(e)}")
        return False


@network_test
def test_graph_query(client: DataHubRestClient):
    """Test direct GraphQL query execution"""
    logger.info("Testing direct GraphQL query...")

    # Basic introspection query that should work on any GraphQL endpoint
    query = """
    query {
      __schema {
        queryType {
          name
        }
      }
    }
    """

    result = client.execute_graphql(query)
    if "errors" in result:
        logger.error(f"GraphQL query failed: {result['errors']}")
    else:
        schema = result.get("data", {}).get("__schema", {})
        query_type = schema.get("queryType", {}).get("name", "unknown")
        logger.info(f"GraphQL query type: {query_type}")

    # Try a simple query to check if user auth is available
    logger.info("Testing me query...")
    query = """
    query {
      me {
        corpUser {
          username
          urn
        }
      }
    }
    """

    result = client.execute_graphql(query)
    if "errors" in result:
        logger.error(f"Me query failed: {result['errors']}")
    else:
        me = result.get("data", {}).get("me", {})
        corp_user = me.get("corpUser", {})
        username = corp_user.get("username", "unknown")
        urn = corp_user.get("urn", "unknown")
        logger.info(f"Authenticated as: {username} (URN: {urn})")


def main():
    # Load environment variables
    load_dotenv()

    # Get DataHub configuration from environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", ""),
    }

    if not datahub_config["server"]:
        logger.error("DATAHUB_GMS_URL environment variable must be set")
        return 1

    # Initialize DataHub client
    client = DataHubRestClient(datahub_config["server"], datahub_config["token"])

    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        return 1

    logger.info("Connected to DataHub successfully")

    # Run tests
    test_list_secrets(client)
    test_graph_query(client)

    # Test update ingestion source if source_id is provided
    source_id = os.environ.get("TEST_SOURCE_ID", "analytics-database-prod")
    test_update_ingestion_source(client, source_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())
