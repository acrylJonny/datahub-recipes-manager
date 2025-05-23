#!/usr/bin/env python3
"""
Delete a glossary node or term from DataHub.
"""

import argparse
import logging
import os
import sys
from typing import Dict, Any, Optional

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.glossary.glossary_utils import (
    get_glossary_node_name,
    get_glossary_term_name,
    check_glossary_node_dependencies,
    check_glossary_term_dependencies
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
    parser = argparse.ArgumentParser(description="Delete a glossary node or term from DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    # Create subparsers for node and term commands
    subparsers = parser.add_subparsers(dest="entity_type", help="Entity type to delete")
    
    # Node subparser
    node_parser = subparsers.add_parser("node", help="Delete a glossary node")
    
    node_parser.add_argument(
        "--urn",
        required=True,
        help="URN of the glossary node to delete",
    )
    
    node_parser.add_argument(
        "--force",
        action="store_true",
        help="Force deletion even if node has child nodes or terms (will remove all children as well)",
    )
    
    # Term subparser
    term_parser = subparsers.add_parser("term", help="Delete a glossary term")
    
    term_parser.add_argument(
        "--urn",
        required=True,
        help="URN of the glossary term to delete",
    )
    
    term_parser.add_argument(
        "--force",
        action="store_true",
        help="Force deletion even if term has entity associations",
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
        # Determine entity type and delete accordingly
        if args.entity_type == "node":
            # Get node details for logging
            node = client.export_glossary_node(args.urn)
            if not node:
                logger.error(f"Glossary node not found: {args.urn}")
                sys.exit(1)
                
            node_name = get_glossary_node_name(node) or args.urn
            
            # Check for dependencies
            has_dependencies = check_glossary_node_dependencies(client, args.urn)
            
            if has_dependencies and not args.force:
                logger.error(f"Glossary node '{node_name}' has child nodes or terms. Use --force to delete it and all its children.")
                sys.exit(1)
            
            # Delete node
            logger.info(f"Deleting glossary node: {node_name}")
            success = client.delete_glossary_node(args.urn)
            
            if success:
                logger.info(f"Successfully deleted glossary node: {node_name}")
            else:
                logger.error(f"Failed to delete glossary node: {node_name}")
                sys.exit(1)
                
        elif args.entity_type == "term":
            # Get term details for logging
            term = client.export_glossary_term(args.urn)
            if not term:
                logger.error(f"Glossary term not found: {args.urn}")
                sys.exit(1)
                
            term_name = get_glossary_term_name(term) or args.urn
            
            # Check for dependencies
            has_dependencies = check_glossary_term_dependencies(client, args.urn)
            
            if has_dependencies and not args.force:
                logger.error(f"Glossary term '{term_name}' has entity associations. Use --force to delete it anyway.")
                sys.exit(1)
            
            # Delete term
            logger.info(f"Deleting glossary term: {term_name}")
            success = client.delete_glossary_term(args.urn)
            
            if success:
                logger.info(f"Successfully deleted glossary term: {term_name}")
            else:
                logger.error(f"Failed to delete glossary term: {term_name}")
                sys.exit(1)
                
        else:
            logger.error("Entity type must be specified (node or term)")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error deleting glossary entity: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 