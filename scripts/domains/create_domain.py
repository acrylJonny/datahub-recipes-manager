#!/usr/bin/env python3
"""
Create a new domain in DataHub.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any

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
    parser = argparse.ArgumentParser(description="Create a new domain in DataHub")

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--name",
        "-n",
        required=True,
        help="Name of the domain to create",
    )

    parser.add_argument(
        "--description",
        "-d",
        help="Description of the domain",
    )

    parser.add_argument(
        "--parent-domain",
        help="URN of the parent domain (if creating a sub-domain)",
    )

    parser.add_argument(
        "--color",
        help="Color hex code for the domain (e.g., #0077b6)",
    )

    parser.add_argument(
        "--icon",
        help="Icon name for the domain (e.g., domain)",
    )

    parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )

    parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing domain definition (alternative to command line arguments)",
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

    return parser.parse_args()


def load_domain_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load domain definition from a JSON file

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary containing domain definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        # Check if the file contains a domain directly or wrapped in a container
        if "domain" in data:
            return data["domain"]
        elif "properties" in data:
            return data
        else:
            logger.warning(
                "Domain definition not found in expected format, using entire file content"
            )
            return data
    except Exception as e:
        logger.error(f"Error loading domain from file: {str(e)}")
        raise


def create_domain_definition(args) -> Dict[str, Any]:
    """
    Create domain definition from command line arguments

    Args:
        args: Command line arguments

    Returns:
        Dictionary containing domain definition
    """
    domain = {"properties": {"name": args.name}}

    if args.description:
        domain["properties"]["description"] = args.description

    if args.parent_domain:
        domain["parentDomains"] = {"domains": [{"urn": args.parent_domain}]}

    if args.color or args.icon:
        display_props = {}
        if args.color:
            display_props["colorHex"] = args.color

        if args.icon:
            display_props["icon"] = {
                "name": args.icon,
                "style": "solid",
                "iconLibrary": "MATERIAL",
            }

        domain["displayProperties"] = display_props

    if args.owners:
        owner_urns = args.owners.split(",")
        owners = []

        for urn in owner_urns:
            urn = urn.strip()
            if urn.startswith("urn:li:corpuser:"):
                owner_type = "USER"
            elif urn.startswith("urn:li:corpgroup:"):
                owner_type = "GROUP"
            else:
                logger.warning(f"Unknown owner URN type: {urn}")
                continue

            owners.append(
                {
                    "owner": {"urn": urn},
                    "type": owner_type,
                    "ownershipType": {"urn": "urn:li:ownershipType:Technical"},
                }
            )

        if owners:
            domain["ownership"] = {"owners": owners}

    return domain


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
        # Determine domain definition source
        domain_data = None
        if args.input_file:
            domain_data = load_domain_from_file(args.input_file)
            logger.info(f"Loaded domain definition from {args.input_file}")
        else:
            domain_data = create_domain_definition(args)
            logger.info("Created domain definition from command line arguments")

        # Create domain
        logger.info(f"Creating domain: {domain_data.get('properties', {}).get('name')}")
        domain_urn = client.import_domain(domain_data)

        if domain_urn:
            logger.info(
                f"Successfully created domain: {domain_data.get('properties', {}).get('name')}"
            )
            logger.info(f"Domain URN: {domain_urn}")
        else:
            logger.error("Failed to create domain")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error creating domain: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
