#!/usr/bin/env python3
"""
List glossary nodes and terms from DataHub.
This script retrieves glossary nodes and terms from a DataHub instance.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, List

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
        description="List glossary nodes and terms from DataHub"
    )

    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )

    parser.add_argument(
        "--output-file",
        "-o",
        help="Output file to save the glossary data (optional)",
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
        "--nodes-only",
        action="store_true",
        help="List only glossary nodes (no terms)",
    )

    parser.add_argument(
        "--terms-only",
        action="store_true",
        help="List only glossary terms (no nodes)",
    )

    parser.add_argument(
        "--root-only",
        action="store_true",
        help="List only root nodes and terms (no children)",
    )

    parser.add_argument(
        "--include-children",
        action="store_true",
        help="Include child nodes and terms in the output",
    )

    return parser.parse_args()


def build_hierarchy(
    nodes: List[Dict[str, Any]], terms: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Build a hierarchical representation of glossary nodes and terms

    Args:
        nodes: List of glossary nodes
        terms: List of glossary terms

    Returns:
        Dictionary with hierarchical glossary structure
    """
    # Create a map of URNs to nodes for easy lookup
    {node["urn"]: node for node in nodes}

    # Find root nodes
    root_nodes = []
    for node in nodes:
        parent_nodes = node.get("parentNodes", {}).get("nodes", [])
        if not parent_nodes:
            root_nodes.append(node)

    # Find terms with no parent nodes
    root_terms = []
    for term in terms:
        parent_nodes = term.get("parentNodes", {}).get("nodes", [])
        if not parent_nodes:
            root_terms.append(term)

    # Build the hierarchy
    result = {
        "rootNodes": root_nodes,
        "rootTerms": root_terms,
        "allNodes": nodes,
        "allTerms": terms,
    }

    return result


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
        nodes = []
        terms = []

        # Get nodes if needed
        if not args.terms_only:
            if args.root_only:
                logger.info("Fetching root glossary nodes from DataHub...")
                nodes = client.list_root_glossary_nodes()
            else:
                logger.info("Fetching all glossary nodes from DataHub...")
                nodes = client.list_glossary_nodes()

            # Include children if requested
            if args.include_children:
                enriched_nodes = []
                for node in nodes:
                    node_urn = node.get("urn")
                    if node_urn:
                        detailed_node = client.export_glossary_node(
                            node_urn, include_children=True
                        )
                        if detailed_node:
                            enriched_nodes.append(detailed_node)
                        else:
                            enriched_nodes.append(node)
                    else:
                        enriched_nodes.append(node)
                nodes = enriched_nodes

        # Get terms if needed
        if not args.nodes_only:
            if args.root_only:
                logger.info("Fetching root glossary terms from DataHub...")
                terms = client.list_root_glossary_terms()
            else:
                logger.info("Fetching all glossary terms from DataHub...")
                terms = client.list_glossary_terms()

        # Build result
        result = {
            "version": "1.0",
            "exported_at": datetime.now().isoformat(),
        }

        if args.nodes_only:
            result["glossaryNodes"] = nodes
        elif args.terms_only:
            result["glossaryTerms"] = terms
        else:
            # Build hierarchy if we have both nodes and terms
            glossary = build_hierarchy(nodes, terms)
            result["glossary"] = glossary

        # Output results
        if args.output_file:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(args.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)

            with open(args.output_file, "w") as f:
                json.dump(result, f, indent=4 if args.pretty_print else None)
            logger.info(f"Successfully saved glossary data to {args.output_file}")
        else:
            # Print to stdout
            print(json.dumps(result, indent=4 if args.pretty_print else None))

        node_count = len(nodes)
        term_count = len(terms)

        if args.nodes_only:
            logger.info(f"Retrieved {node_count} glossary nodes from DataHub")
        elif args.terms_only:
            logger.info(f"Retrieved {term_count} glossary terms from DataHub")
        else:
            logger.info(
                f"Retrieved {node_count} glossary nodes and {term_count} glossary terms from DataHub"
            )

    except Exception as e:
        logger.error(f"Error listing glossary data: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
