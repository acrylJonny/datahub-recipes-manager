#!/usr/bin/env python3
"""
Create a new glossary node or term in DataHub.
"""

import argparse
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
from scripts.glossary.glossary_utils import (
    load_glossary_node_from_file,
    load_glossary_term_from_file,
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
    parser = argparse.ArgumentParser(
        description="Create a new glossary node or term in DataHub"
    )

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    # Create subparsers for node and term commands
    subparsers = parser.add_subparsers(dest="entity_type", help="Entity type to create")

    # Node subparser
    node_parser = subparsers.add_parser("node", help="Create a glossary node")

    node_parser.add_argument(
        "--name",
        "-n",
        required=True,
        help="Name of the glossary node to create",
    )

    node_parser.add_argument(
        "--description",
        "-d",
        help="Description of the glossary node",
    )

    node_parser.add_argument(
        "--parent-node",
        help="URN of the parent glossary node",
    )

    node_parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )

    node_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing glossary node definition (alternative to command line arguments)",
    )

    # Term subparser
    term_parser = subparsers.add_parser("term", help="Create a glossary term")

    term_parser.add_argument(
        "--name",
        "-n",
        required=True,
        help="Name of the glossary term to create",
    )

    term_parser.add_argument(
        "--description",
        "-d",
        help="Description of the glossary term",
    )

    term_parser.add_argument(
        "--definition",
        help="Definition of the glossary term",
    )

    term_parser.add_argument(
        "--parent-node",
        help="URN of the parent glossary node",
    )

    term_parser.add_argument(
        "--source",
        help="Source of the glossary term",
    )

    term_parser.add_argument(
        "--owners",
        help="List of owner URNs, comma-separated (e.g., urn:li:corpuser:alice,urn:li:corpgroup:engineering)",
    )

    term_parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing glossary term definition (alternative to command line arguments)",
    )

    # Common arguments
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


def create_node_definition(args) -> Dict[str, Any]:
    """
    Create glossary node definition from command line arguments

    Args:
        args: Command line arguments

    Returns:
        Dictionary containing glossary node definition
    """
    node = {"properties": {"name": args.name}}

    if args.description:
        node["properties"]["description"] = args.description

    if args.parent_node:
        node["parentNode"] = {"urn": args.parent_node}

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
            node["ownership"] = {"owners": owners}

    return node


def create_term_definition(args) -> Dict[str, Any]:
    """
    Create glossary term definition from command line arguments

    Args:
        args: Command line arguments

    Returns:
        Dictionary containing glossary term definition
    """
    term = {"properties": {"name": args.name}}

    if args.description:
        term["properties"]["description"] = args.description

    if args.definition:
        term["properties"]["definition"] = args.definition

    if args.source:
        term["properties"]["termSource"] = args.source

    if args.parent_node:
        term["parentNode"] = {"urn": args.parent_node}

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
            term["ownership"] = {"owners": owners}

    return term


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
        # Determine entity type and create accordingly
        if args.entity_type == "node":
            # Create glossary node
            if args.input_file:
                node_data = load_glossary_node_from_file(args.input_file)
                logger.info(f"Loaded glossary node definition from {args.input_file}")
            else:
                node_data = create_node_definition(args)
                logger.info(
                    "Created glossary node definition from command line arguments"
                )

            logger.info(
                f"Creating glossary node: {node_data.get('properties', {}).get('name')}"
            )
            node_urn = client.import_glossary_node(node_data)

            if node_urn:
                logger.info(
                    f"Successfully created glossary node: {node_data.get('properties', {}).get('name')}"
                )
                logger.info(f"Glossary node URN: {node_urn}")
            else:
                logger.error("Failed to create glossary node")
                sys.exit(1)

        elif args.entity_type == "term":
            # Create glossary term
            if args.input_file:
                term_data = load_glossary_term_from_file(args.input_file)
                logger.info(f"Loaded glossary term definition from {args.input_file}")
            else:
                term_data = create_term_definition(args)
                logger.info(
                    "Created glossary term definition from command line arguments"
                )

            logger.info(
                f"Creating glossary term: {term_data.get('properties', {}).get('name')}"
            )
            term_urn = client.import_glossary_term(term_data)

            if term_urn:
                logger.info(
                    f"Successfully created glossary term: {term_data.get('properties', {}).get('name')}"
                )
                logger.info(f"Glossary term URN: {term_urn}")
            else:
                logger.error("Failed to create glossary term")
                sys.exit(1)
        else:
            logger.error("Entity type must be specified (node or term)")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error creating glossary entity: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
