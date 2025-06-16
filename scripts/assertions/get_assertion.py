#!/usr/bin/env python3
"""
Get a specific assertion from DataHub.
This script retrieves details of a specific data quality assertion from DataHub.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.assertions.assertion_utils import (
    get_assertion_entity_urn,
    get_assertion_type,
    save_assertion_to_file,
)

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
    parser = argparse.ArgumentParser(description="Get assertion details from DataHub")

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
        help="URN of the assertion to retrieve",
    )

    parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the assertion details (optional)",
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
        "--pretty-print",
        action="store_true",
        help="Pretty print JSON output",
    )

    parser.add_argument(
        "--include-run-events",
        action="store_true",
        help="Include recent assertion run events",
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
        # Get assertion
        logger.info(f"Fetching assertion {args.assertion_urn} from DataHub...")
        assertion = client.get_assertion(args.assertion_urn)

        if not assertion:
            logger.error(f"Assertion not found: {args.assertion_urn}")
            sys.exit(1)

        # Remove run events if not requested
        if not args.include_run_events and "runEvents" in assertion:
            del assertion["runEvents"]

        # Add summary information
        summary = {
            "urn": assertion.get("urn", ""),
            "type": get_assertion_type(assertion),
            "description": assertion.get("info", {}).get("description", ""),
            "entityUrn": get_assertion_entity_urn(assertion),
        }

        result = {
            "version": "1.0",
            "exported_at": datetime.now().isoformat(),
            "assertion": {"summary": summary, "details": assertion},
        }

        # Output assertion
        if args.output_file:
            success = save_assertion_to_file(
                result, args.output_file, args.pretty_print
            )
            if success:
                logger.info(f"Successfully saved assertion to {args.output_file}")
            else:
                logger.error(f"Failed to save assertion to {args.output_file}")
                sys.exit(1)
        else:
            # Print to stdout
            print(json.dumps(result, indent=4 if args.pretty_print else None))

        logger.info(f"Retrieved assertion {args.assertion_urn} from DataHub")

    except Exception as e:
        logger.error(f"Error getting assertion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
