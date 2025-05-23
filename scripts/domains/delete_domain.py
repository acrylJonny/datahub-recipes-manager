#!/usr/bin/env python3
"""
Delete a domain from DataHub.
"""

import argparse
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

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
    parser = argparse.ArgumentParser(description="Delete a domain from DataHub")
    
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
        help="URN of the domain to delete (e.g., urn:li:domain:engineering)",
    )
    
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force delete even if domain has children or entity associations",
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
        "--confirm",
        action="store_true",
        help="Automatically confirm deletion without prompting (use with caution!)",
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
        # First, get the domain to display info to the user
        logger.info(f"Fetching domain information for: {args.domain_urn}")
        domain = client.export_domain(args.domain_urn)
        
        if not domain:
            logger.error(f"Domain not found: {args.domain_urn}")
            sys.exit(1)
        
        domain_name = domain.get("properties", {}).get("name", args.domain_urn)
        logger.info(f"Found domain: {domain_name} ({args.domain_urn})")
        
        # Check for dependencies
        has_children = False
        has_entities = False
        
        if not args.force:
            logger.info("Checking domain dependencies...")
            has_children, has_entities = client.check_domain_dependencies(args.domain_urn)
            
            if has_children:
                logger.warning(f"Domain {domain_name} has child domains")
                if not args.force:
                    logger.error("Cannot delete domain with child domains unless --force is specified")
                    sys.exit(1)
                else:
                    logger.warning("Will force delete domain and potentially orphan child domains")
            
            if has_entities:
                logger.warning(f"Domain {domain_name} has associated entities")
                if not args.force:
                    logger.error("Cannot delete domain with associated entities unless --force is specified")
                    sys.exit(1)
                else:
                    logger.warning("Will force delete domain and remove domain associations from entities")
        
        # Confirm deletion with user unless --confirm is provided
        if not args.confirm:
            user_input = input(f"Are you sure you want to delete domain '{domain_name}'? [y/N]: ")
            if user_input.lower() not in ["y", "yes"]:
                logger.info("Deletion cancelled by user")
                return
        
        # Delete domain
        logger.info(f"Deleting domain: {domain_name}")
        success = client.delete_domain(args.domain_urn, force=args.force)
        
        if success:
            logger.info(f"Successfully deleted domain: {domain_name}")
        else:
            logger.error("Failed to delete domain")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error deleting domain: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 