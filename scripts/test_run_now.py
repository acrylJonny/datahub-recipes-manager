#!/usr/bin/env python3
"""
Test script to run an ingestion source immediately.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent.absolute()))
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """
    Test the run_ingestion_source method of the DataHubRestClient
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test running an ingestion source")
    parser.add_argument(
        "--source-id",
        type=str,
        default="analytics-database-prod",
        help="The ID of the ingestion source to run",
    )

    args = parser.parse_args()
    source_id = args.source_id

    # Load environment variables
    load_dotenv()

    # Get DataHub connection details
    datahub_url = os.environ.get("DATAHUB_GMS_URL")
    datahub_token = os.environ.get("DATAHUB_TOKEN", "")

    if not datahub_url:
        logger.error("DATAHUB_GMS_URL environment variable is not set")
        sys.exit(1)

    # Create the DataHub client
    client = DataHubRestClient(datahub_url, datahub_token)

    # Test connection to DataHub
    logger.info(f"Testing connection to DataHub at {datahub_url}")
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        sys.exit(1)

    logger.info("Connected to DataHub successfully")

    # Run the ingestion source
    logger.info(f"Running ingestion source: {source_id}")

    try:
        # Create the source URN
        source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        # Create GraphQL mutation for running ingestion source immediately
        mutation = """
        mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
          createIngestionExecutionRequest(input: $input)
        }
        """

        variables = {"input": {"ingestionSourceUrn": source_urn}}

        # Execute the mutation
        response = client.execute_graphql(mutation, variables)

        if (
            response
            and "data" in response
            and "createIngestionExecutionRequest" in response["data"]
        ):
            logger.info(f"Successfully triggered ingestion source: {source_id}")
            logger.info(f"Response: {response}")
            return True
        else:
            logger.error(f"Failed to trigger ingestion source: {source_id}")
            logger.error(f"Response: {response}")
            return False
    except Exception as e:
        logger.error(f"Error triggering ingestion source: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
