#!/usr/bin/env python3
"""
Get details of a specific domain from DataHub.
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
    parser = argparse.ArgumentParser(
        description="Get details of a specific domain from DataHub"
    )

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--domain-urn",
        "-u",
        required=True,
        help="URN of the domain to retrieve (e.g., urn:li:domain:engineering)",
    )

    parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the domain details (optional)",
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
        "--include-entities",
        action="store_true",
        help="Include entities associated with the domain",
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
        # Get domain details
        logger.info(f"Fetching details for domain: {args.domain_urn}")
        domain = client.export_domain(
            args.domain_urn, include_entities=args.include_entities
        )

        if not domain:
            logger.error(f"Domain not found: {args.domain_urn}")
            sys.exit(1)

        result = {
            "version": "1.0",
            "exported_at": datetime.now().isoformat(),
            "domain": domain,
        }

        # Output domain details
        if args.output_file:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(args.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            with open(args.output_file, "w") as f:
                json.dump(result, f, indent=4 if args.pretty_print else None)
            logger.info(f"Successfully saved domain details to {args.output_file}")
        else:
            # Print to stdout
            print(json.dumps(result, indent=4 if args.pretty_print else None))

        domain_name = domain.get("properties", {}).get("name", args.domain_urn)
        logger.info(f"Retrieved details for domain: {domain_name}")

    except Exception as e:
        logger.error(f"Error getting domain details: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
