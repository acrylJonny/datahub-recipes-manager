#!/usr/bin/env python3
"""
Run a DataHub ingestion source immediately using GraphQL mutation.

This script triggers an immediate execution of a DataHub ingestion source
using the createIngestionExecutionRequest mutation.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any, Optional

# Add the parent directory to the path so we can import the utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for running an ingestion source."""
    parser = argparse.ArgumentParser(
        description="Run a DataHub ingestion source immediately"
    )
    parser.add_argument(
        "--source-id",
        required=True,
        help="ID of the ingestion source to run (without the 'urn:li:dataHubIngestionSource:' prefix)",
    )
    parser.add_argument(
        "--server",
        help="DataHub server URL (default: from DATAHUB_GMS_URL env var)",
    )
    parser.add_argument(
        "--token",
        help="DataHub API token (default: from DATAHUB_TOKEN env var)",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for running an ingestion source immediately."""
    # Load environment variables from .env file
    load_dotenv()

    # Parse command line arguments
    args = parse_args()

    # Get DataHub connection details
    server = args.server or os.environ.get("DATAHUB_GMS_URL")
    token = args.token or os.environ.get("DATAHUB_TOKEN")

    if not server:
        logger.error(
            "DataHub server URL not provided. Set DATAHUB_GMS_URL environment variable or use --server."
        )
        sys.exit(1)

    source_id = args.source_id.strip()
    source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

    # Create DataHub client
    client = DataHubRestClient(server, token)

    # Test connection
    logger.info(f"Testing connection to DataHub at {server}...")
    if not client.test_connection():
        logger.error("Failed to connect to DataHub server")
        sys.exit(1)
    logger.info("Successfully connected to DataHub")

    # Define the GraphQL mutation
    mutation = """
    mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
      createIngestionExecutionRequest(input: $input)
    }
    """

    # Define the variables for the mutation
    variables = {"input": {"ingestionSourceUrn": source_urn}}

    try:
        # First try the GraphQL approach
        logger.info(f"Triggering immediate run for ingestion source: {source_id}")
        response = client.execute_graphql(mutation, variables)

        if response and "errors" not in response:
            logger.info(f"Successfully triggered ingestion source: {source_id}")
            logger.info(f"Response: {response}")
            sys.exit(0)
        else:
            if response and "errors" in response:
                error_msg = response.get("errors", [{}])[0].get(
                    "message", "Unknown GraphQL error"
                )
                logger.warning(f"GraphQL error: {error_msg}")
            else:
                logger.warning("Empty or invalid GraphQL response")

            # If GraphQL fails, try REST API
            logger.info("Falling back to REST API approach...")
            success = client.trigger_ingestion(source_id)

            if success:
                logger.info(
                    f"Successfully triggered ingestion source using REST API: {source_id}"
                )
                sys.exit(0)
            else:
                logger.error(f"Failed to trigger ingestion source: {source_id}")
                sys.exit(1)
    except Exception as e:
        logger.error(f"Error triggering ingestion source: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
