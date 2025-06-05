#!/usr/bin/env python3
"""
Create a schema assertion in DataHub.
This script creates a new schema assertion for a dataset in DataHub.
"""

import argparse
import json
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
    parser = argparse.ArgumentParser(description="Create a schema assertion in DataHub")

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--dataset-urn",
        "-d",
        required=True,
        help="DataHub dataset URN",
    )

    parser.add_argument(
        "--compatibility",
        choices=[
            "EXACT_MATCH",
            "FORWARD_COMPATIBLE",
            "BACKWARD_COMPATIBLE",
            "FULL_COMPATIBLE",
        ],
        default="EXACT_MATCH",
        help="Schema compatibility level (default: EXACT_MATCH)",
    )

    parser.add_argument(
        "--schema-file",
        required=True,
        help="JSON file containing the schema field definitions",
    )

    parser.add_argument(
        "--description",
        "-desc",
        default="",
        help="Description for the assertion",
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
        "--tags",
        nargs="*",
        help="List of tag URNs to apply to the assertion (e.g., urn:li:tag:critical)",
    )

    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the assertion immediately after creation",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)

    # Load schema fields from file
    try:
        with open(args.schema_file, "r") as f:
            schema_data = json.load(f)

        # Check if the file has the expected format
        if isinstance(schema_data, dict) and "fields" in schema_data:
            fields = schema_data["fields"]
        elif isinstance(schema_data, list):
            fields = schema_data
        else:
            logger.error(
                "Schema file must contain a list of fields or a dictionary with a 'fields' key"
            )
            sys.exit(1)

        # Validate that each field has required properties
        for field in fields:
            if "path" not in field or "type" not in field:
                logger.error("Each schema field must have 'path' and 'type' properties")
                sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading schema file: {str(e)}")
        sys.exit(1)

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
        # Create schema assertion
        logger.info(f"Creating schema assertion for dataset {args.dataset_urn}...")

        assertion_urn = client.create_schema_assertion(
            dataset_urn=args.dataset_urn,
            fields=fields,
            compatibility=args.compatibility,
            description=args.description,
        )

        if not assertion_urn:
            logger.error("Failed to create schema assertion")
            sys.exit(1)

        logger.info(f"Successfully created schema assertion: {assertion_urn}")

        # Apply tags if specified
        if args.tags:
            for tag_urn in args.tags:
                logger.info(f"Adding tag {tag_urn} to assertion...")
                success = client.add_tag_to_assertion(assertion_urn, tag_urn)
                if success:
                    logger.info(f"Successfully added tag {tag_urn}")
                else:
                    logger.warning(f"Failed to add tag {tag_urn}")

        # Run the assertion if requested
        if args.run:
            logger.info("Running assertion...")
            result = client.run_assertion(assertion_urn)
            if result:
                logger.info(f"Assertion run result: {json.dumps(result, indent=2)}")
            else:
                logger.warning("Failed to run assertion")

        # Print the created assertion URN
        print(assertion_urn)

    except Exception as e:
        logger.error(f"Error creating schema assertion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
