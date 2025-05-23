#!/usr/bin/env python3
"""
Delete a tag from DataHub.
"""

import argparse
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.tags.tag_utils import check_tag_dependencies

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
    parser = argparse.ArgumentParser(description="Delete a tag from DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--tag-urn",
        "-u",
        required=True,
        help="URN of the tag to delete (e.g., urn:li:tag:pii)",
    )
    
    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force delete even if tag has entity associations",
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
        # First, get the tag to display info to the user
        logger.info(f"Fetching tag information for: {args.tag_urn}")
        tag = client.export_tag(args.tag_urn)
        
        if not tag:
            logger.error(f"Tag not found: {args.tag_urn}")
            sys.exit(1)
        
        tag_name = tag.get("properties", {}).get("name", args.tag_urn)
        logger.info(f"Found tag: {tag_name} ({args.tag_urn})")
        
        # Check for entities using this tag
        if not args.force:
            logger.info("Checking tag dependencies...")
            has_entities = check_tag_dependencies(client, args.tag_urn)
            
            if has_entities:
                logger.warning(f"Tag {tag_name} is associated with entities")
                if not args.force:
                    logger.error("Cannot delete tag with associated entities unless --force is specified")
                    sys.exit(1)
                else:
                    logger.warning("Will force delete tag and remove tag associations from entities")
        
        # Confirm deletion with user unless --confirm is provided
        if not args.confirm:
            user_input = input(f"Are you sure you want to delete tag '{tag_name}'? [y/N]: ")
            if user_input.lower() not in ["y", "yes"]:
                logger.info("Deletion cancelled by user")
                return
        
        # Delete tag
        logger.info(f"Deleting tag: {tag_name}")
        success = client.delete_tag(args.tag_urn, force=args.force)
        
        if success:
            logger.info(f"Successfully deleted tag: {tag_name}")
        else:
            logger.error("Failed to delete tag")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error deleting tag: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 