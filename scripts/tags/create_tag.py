#!/usr/bin/env python3
"""
Create a new tag in DataHub.
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
    parser = argparse.ArgumentParser(description="Create a new tag in DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--name",
        "-n",
        help="Name of the tag to create",
    )
    
    parser.add_argument(
        "--description",
        "-d",
        help="Description of the tag",
    )
    
    parser.add_argument(
        "--color",
        help="Color hex code for the tag (e.g., #0077b6)",
    )
    
    parser.add_argument(
        "--input-file",
        "-i",
        help="JSON file containing tag definition (alternative to command line arguments)",
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


def create_tag_definition(args) -> Dict[str, Any]:
    """
    Create tag definition from command line arguments
    
    Args:
        args: Command line arguments
        
    Returns:
        Dictionary containing tag definition
    """
    tag = {
        "properties": {
            "name": args.name
        }
    }
    
    if args.description:
        tag["properties"]["description"] = args.description
    
    if args.color:
        tag["properties"]["colorHex"] = args.color
    
    return tag


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    # Make sure either name or input file is provided
    if not args.name and not args.input_file:
        logger.error("Either --name or --input-file must be provided")
        sys.exit(1)
    
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
        # Determine tag definition source
        tag_data = None
        if args.input_file:
            tag_data = load_tag_from_file(args.input_file)
            logger.info(f"Loaded tag definition from {args.input_file}")
        else:
            tag_data = create_tag_definition(args)
            logger.info("Created tag definition from command line arguments")
        
        # Create tag
        logger.info(f"Creating tag: {tag_data.get('properties', {}).get('name')}")
        tag_urn = client.import_tag(tag_data)
        
        if tag_urn:
            logger.info(f"Successfully created tag: {tag_data.get('properties', {}).get('name')}")
            logger.info(f"Tag URN: {tag_urn}")
        else:
            logger.error("Failed to create tag")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error creating tag: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 