#!/usr/bin/env python3
"""
Delete an assertion from DataHub.
This script deletes a specific assertion from DataHub.
"""

import argparse
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """
    Set up logging configuration
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Delete an assertion from DataHub")

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--assertion-urn",
        "-a",
        required=True,
        help="URN of the assertion to delete",
    )

    parser.add_argument(
        "--token-file",
        help="File containing DataHub access token",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force delete without confirmation",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Get token from file or environment
    token = None
    if args.token_file:
        try:
            with open(args.token_file, "r") as f:
                token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading token file: {str(e)}")
            sys.exit(1)
    else:
        token = get_token_from_env()

    # Initialize the client
    try:
        client = DataHubMetadataApiClient(args.server_url, token)
    except Exception as e:
        logger.error(f"Error initializing client: {str(e)}")
        sys.exit(1)

    try:
        # Get the assertion details first
        assertion = client.get_assertion(args.assertion_urn)
        if not assertion:
            logger.error(f"Assertion not found: {args.assertion_urn}")
            sys.exit(1)

        # Show assertion details and confirm deletion
        if not args.force:
            # Extract important information for confirmation
            assertion_type = assertion.get("info", {}).get("type", "UNKNOWN")
            description = assertion.get("info", {}).get("description", "")
            entity_urn = None

            # Try to extract entity URN based on assertion type
            info = assertion.get("info", {})
            if "datasetAssertion" in info:
                entity_urn = info["datasetAssertion"].get("datasetUrn")
            elif "freshnessAssertion" in info:
                entity_urn = info["freshnessAssertion"].get("entityUrn")
            elif "sqlAssertion" in info:
                entity_urn = info["sqlAssertion"].get("entityUrn")
            elif "fieldAssertion" in info:
                entity_urn = info["fieldAssertion"].get("entityUrn")
            elif "volumeAssertion" in info:
                entity_urn = info["volumeAssertion"].get("entityUrn")
            elif "schemaAssertion" in info:
                entity_urn = info["schemaAssertion"].get("entityUrn")

            # Print assertion details
            print("You are about to delete the following assertion:")
            print(f"URN:         {args.assertion_urn}")
            print(f"Type:        {assertion_type}")
            print(f"Description: {description}")
            if entity_urn:
                print(f"Entity URN:  {entity_urn}")

            # Confirm deletion
            response = input("Are you sure you want to delete this assertion? (y/N): ")
            if response.lower() not in ["y", "yes"]:
                print("Deletion cancelled.")
                sys.exit(0)

        # Delete the assertion
        logger.info(f"Deleting assertion {args.assertion_urn}...")
        success = client.delete_assertion(args.assertion_urn)

        if success:
            logger.info(f"Successfully deleted assertion: {args.assertion_urn}")
        else:
            logger.error(f"Failed to delete assertion: {args.assertion_urn}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error deleting assertion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
