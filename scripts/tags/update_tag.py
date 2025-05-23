#!/usr/bin/env python3
"""
Update an existing tag in DataHub.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
from utils.token_utils import get_token_from_env
from scripts.tags.tag_utils import load_tag_from_file

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
    parser = argparse.ArgumentParser(description="Update an existing tag in DataHub")
    
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
        help="URN of the tag to update (e.g., urn:li:tag:pii)",
    )
    
    parser.add_argument(
        "--name",
        "-n",
        help="New name of the tag",
    )
    
    parser.add_argument(
        "--description",
        "-d",
        help="New description of the tag",
    )
    
    parser.add_argument(
        "--color",
        help="New color hex code for the tag (e.g., #0077b6)",
    )
    
    parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing updated tag definition (alternative to command line arguments)",
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


def create_update_definition(args, existing_tag: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create tag update definition from command line arguments and existing tag
    
    Args:
        args: Command line arguments
        existing_tag: The existing tag data
        
    Returns:
        Dictionary containing tag update definition
    """
    # Start with a copy of the existing tag
    tag = existing_tag.copy()
    
    # Update properties
    if "properties" not in tag:
        tag["properties"] = {}
    
    if args.name:
        tag["properties"]["name"] = args.name
    
    if args.description:
        tag["properties"]["description"] = args.description
    
    if args.color:
        tag["properties"]["colorHex"] = args.color
    
    return tag


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
        # First, get the existing tag
        logger.info(f"Fetching current tag information for: {args.tag_urn}")
        existing_tag = client.export_tag(args.tag_urn)
        
        if not existing_tag:
            logger.error(f"Tag not found: {args.tag_urn}")
            sys.exit(1)
        
        logger.info(f"Retrieved existing tag: {existing_tag.get('properties', {}).get('name', args.tag_urn)}")
        
        # Determine tag update source
        updated_tag = None
        if args.input_file:
            updated_tag = load_tag_from_file(args.input_file)
            logger.info(f"Loaded tag update from {args.input_file}")
        else:
            updated_tag = create_update_definition(args, existing_tag)
            logger.info("Created tag update from command line arguments")
        
        # Update tag
        logger.info(f"Updating tag: {args.tag_urn}")
        success = client.update_tag(args.tag_urn, updated_tag)
        
        if success:
            logger.info(f"Successfully updated tag: {updated_tag.get('properties', {}).get('name', args.tag_urn)}")
        else:
            logger.error("Failed to update tag")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error updating tag: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 