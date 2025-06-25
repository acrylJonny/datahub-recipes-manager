#!/usr/bin/env python3
"""
Update an existing domain in DataHub.
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
    parser = argparse.ArgumentParser(description="Update an existing domain in DataHub")

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
        help="URN of the domain to update (e.g., urn:li:domain:engineering)",
    )

    parser.add_argument(
        "--name",
        "-n",
        help="New name of the domain",
    )

    parser.add_argument(
        "--description",
        "-d",
        help="New description of the domain",
    )

    parser.add_argument(
        "--parent-domain",
        help="URN of the new parent domain (if changing parent)",
    )

    parser.add_argument(
        "--remove-parent",
        action="store_true",
        help="Remove parent domain relationship",
    )

    parser.add_argument(
        "--color",
        help="New color hex code for the domain (e.g., #0077b6)",
    )

    parser.add_argument(
        "--icon",
        help="New icon name for the domain (e.g., domain)",
    )

    parser.add_argument(
        "--add-owners",
        help="List of owner URNs to add, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )

    parser.add_argument(
        "--remove-owners",
        help="List of owner URNs to remove, comma-separated",
    )

    parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing updated domain definition (alternative to command line arguments)",
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


def create_update_definition(args, existing_domain: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create domain update definition from command line arguments and existing domain

    Args:
        args: Command line arguments
        existing_domain: The existing domain data

    Returns:
        Dictionary containing domain update definition
    """
    # Start with a copy of the existing domain
    domain = existing_domain.copy()

    # Update properties
    if "properties" not in domain:
        domain["properties"] = {}

    if args.name:
        domain["properties"]["name"] = args.name

    if args.description:
        domain["properties"]["description"] = args.description

    # Handle parent domain changes
    if args.remove_parent:
        if "parentDomains" in domain:
            del domain["parentDomains"]
    elif args.parent_domain:
        domain["parentDomains"] = {"domains": [{"urn": args.parent_domain}]}

    # Handle display properties
    if args.color or args.icon:
        if "displayProperties" not in domain:
            domain["displayProperties"] = {}

        if args.color:
            domain["displayProperties"]["colorHex"] = args.color

        if args.icon:
            if "icon" not in domain["displayProperties"]:
                domain["displayProperties"]["icon"] = {
                    "style": "solid",
                    "iconLibrary": "MATERIAL",
                }
            domain["displayProperties"]["icon"]["name"] = args.icon

    # Handle ownership changes
    if args.add_owners or args.remove_owners:
        # Get existing owners
        existing_owners = []
        if "ownership" in domain and "owners" in domain["ownership"]:
            existing_owners = domain["ownership"]["owners"]

        # Process owners to remove
        if args.remove_owners:
            remove_urns = [urn.strip() for urn in args.remove_owners.split(",")]
            existing_owners = [
                owner
                for owner in existing_owners
                if "owner" in owner
                and "urn" in owner["owner"]
                and owner["owner"]["urn"] not in remove_urns
            ]

        # Process owners to add
        if args.add_owners:
            add_urns = [urn.strip() for urn in args.add_owners.split(",")]

            for urn in add_urns:
                # Check if owner already exists
                if any(
                    (
                        "owner" in owner
                        and "urn" in owner["owner"]
                        and owner["owner"]["urn"] == urn
                    )
                    for owner in existing_owners
                ):
                    continue

                if urn.startswith("urn:li:corpuser:"):
                    owner_type = "USER"
                elif urn.startswith("urn:li:corpgroup:"):
                    owner_type = "GROUP"
                else:
                    logger.warning(f"Unknown owner URN type: {urn}")
                    continue

                existing_owners.append(
                    {
                        "owner": {"urn": urn},
                        "type": owner_type,
                        "ownershipType": {"urn": "urn:li:ownershipType:Technical"},
                    }
                )

        # Update the domain with modified ownership
        if existing_owners:
            if "ownership" not in domain:
                domain["ownership"] = {}
            domain["ownership"]["owners"] = existing_owners

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
        # First, get the existing domain
        logger.info(f"Fetching current domain information for: {args.domain_urn}")
        existing_domain = client.export_domain(args.domain_urn)

        if not existing_domain:
            logger.error(f"Domain not found: {args.domain_urn}")
            sys.exit(1)

        logger.info(
            f"Retrieved existing domain: {existing_domain.get('properties', {}).get('name', args.domain_urn)}"
        )

        # Determine domain update source
        updated_domain = None
        if args.input_file:
            updated_domain = load_domain_from_file(args.input_file)
            logger.info(f"Loaded domain update from {args.input_file}")
        else:
            updated_domain = create_update_definition(args, existing_domain)
            logger.info("Created domain update from command line arguments")

        # Update domain
        logger.info(f"Updating domain: {args.domain_urn}")
        success = client.update_domain(args.domain_urn, updated_domain)

        if success:
            logger.info(
                f"Successfully updated domain: {updated_domain.get('properties', {}).get('name', args.domain_urn)}"
            )
        else:
            logger.error("Failed to update domain")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error updating domain: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
