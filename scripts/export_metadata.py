#!/usr/bin/env python3
"""
Export metadata from DataHub including domains, business glossaries, tags,
structured properties, metadata tests and assertions.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils._datahub_metadata_client import DataHubMetadataClient
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
    parser = argparse.ArgumentParser(description="Export metadata from DataHub")

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--output-file",
        "-o",
        required=True,
        help="Output file to save the exported metadata",
    )

    parser.add_argument(
        "--export-type",
        "-t",
        choices=["all", "domains", "glossary", "tags", "properties", "tests"],
        default="all",
        help="Type of metadata to export (default: all)",
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
        help="Include entities associated with domains (can significantly increase size)",
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
        client = DataHubMetadataClient(args.server_url, token)
    except Exception as e:
        logger.error(f"Error initializing client: {str(e)}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Export metadata based on type
    try:
        if args.export_type == "all":
            result = client.export_all_metadata(args.output_file)
            if not result:
                logger.error("Failed to export metadata")
                sys.exit(1)
        else:
            metadata = {}
            metadata["version"] = "1.0"
            metadata["exported_at"] = datetime.now().isoformat()

            if args.export_type == "domains":
                domains = client.list_domains()

                # If include_entities is specified, get entities for each domain
                if args.include_entities:
                    enriched_domains = []
                    for domain in domains:
                        domain_urn = domain.get("urn")
                        if domain_urn:
                            detailed_domain = client.export_domain(
                                domain_urn, include_entities=True
                            )
                            enriched_domains.append(detailed_domain)
                        else:
                            enriched_domains.append(domain)
                    domains = enriched_domains

                metadata["domains"] = domains

            elif args.export_type == "glossary":
                # Get root nodes and terms
                root_nodes = client.get_root_glossary_nodes()
                root_terms = client.get_root_glossary_terms()

                # For each root node, get its children
                nodes_with_children = []
                for node in root_nodes:
                    node_urn = node.get("urn")
                    if node_urn:
                        detailed_node = client.get_glossary_node(node_urn)
                        if detailed_node:
                            nodes_with_children.append(detailed_node)
                    else:
                        nodes_with_children.append(node)

                metadata["glossary"] = {
                    "nodes": nodes_with_children,
                    "terms": root_terms,
                }

            elif args.export_type == "tags":
                metadata["tags"] = client.list_all_tags()

            elif args.export_type == "properties":
                metadata["structured_properties"] = client.list_structured_properties()

            elif args.export_type == "tests":
                metadata["tests"] = client.list_metadata_tests()

            # Write to file
            with open(args.output_file, "w") as f:
                json.dump(metadata, f, indent=4 if args.pretty_print else None)

            logger.info(
                f"Successfully exported {args.export_type} to {args.output_file}"
            )

    except Exception as e:
        logger.error(f"Error exporting metadata: {str(e)}")
        sys.exit(1)

    logger.info("Export completed successfully")


if __name__ == "__main__":
    main()
