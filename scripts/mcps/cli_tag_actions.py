#!/usr/bin/env python3
"""
CLI script for tag actions using the new environment-based URN generation system.
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from scripts.mcps.tag_actions import (
    download_tag_json,
    sync_tag_to_local,
    add_tag_to_staged_changes,
    setup_logging
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="CLI for tag actions with environment-based URN generation"
    )

    parser.add_argument(
        "--action",
        required=True,
        choices=["download", "sync", "stage"],
        help="Action to perform: download, sync, or stage"
    )

    parser.add_argument(
        "--tag-data",
        help="Path to JSON file containing tag data, or JSON string"
    )

    parser.add_argument(
        "--tag-urn",
        help="Tag URN (alternative to --tag-data for simple operations)"
    )

    parser.add_argument(
        "--tag-name",
        help="Tag name (used with --tag-urn)"
    )

    parser.add_argument(
        "--description",
        help="Tag description (used with --tag-urn)"
    )

    parser.add_argument(
        "--color-hex",
        help="Tag color in hex format (used with --tag-urn)"
    )

    parser.add_argument(
        "--environment",
        default="dev",
        help="Environment name for URN generation (default: dev)"
    )

    parser.add_argument(
        "--owner",
        default="datahub",
        help="Owner username (default: datahub)"
    )

    parser.add_argument(
        "--output-path",
        help="Output path for download action"
    )

    parser.add_argument(
        "--local-db-path",
        help="Local database path for sync action"
    )

    parser.add_argument(
        "--base-dir",
        help="Base directory for staged changes"
    )

    parser.add_argument(
        "--mutation-name",
        help="Mutation name (deprecated, use --environment instead)"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)"
    )

    return parser.parse_args()


def load_tag_data(args) -> Dict[str, Any]:
    """Load tag data from arguments"""
    if args.tag_data:
        # Try to load as file first, then as JSON string
        if os.path.exists(args.tag_data):
            with open(args.tag_data, 'r') as f:
                return json.load(f)
        else:
            return json.loads(args.tag_data)
    elif args.tag_urn:
        # Create minimal tag data from URN and other arguments
        tag_data = {
            "urn": args.tag_urn,
            "properties": {}
        }
        
        if args.tag_name:
            tag_data["properties"]["name"] = args.tag_name
        if args.description:
            tag_data["properties"]["description"] = args.description
        if args.color_hex:
            tag_data["properties"]["colorHex"] = args.color_hex
            
        return tag_data
    else:
        raise ValueError("Either --tag-data or --tag-urn must be provided")


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    try:
        # Load tag data
        tag_data = load_tag_data(args)
        logger.info(f"Loaded tag data for URN: {tag_data.get('urn')}")
        
        # Perform the requested action
        if args.action == "download":
            result = download_tag_json(tag_data, args.output_path)
            if args.output_path:
                logger.info(f"Tag JSON downloaded to: {result}")
            else:
                print(result)
                
        elif args.action == "sync":
            success = sync_tag_to_local(tag_data, args.local_db_path)
            if success:
                logger.info("Tag synced to local database successfully")
            else:
                logger.error("Failed to sync tag to local database")
                sys.exit(1)
                
        elif args.action == "stage":
            # Use environment for URN generation
            environment = args.environment
            mutation_name = args.mutation_name  # For backward compatibility
            
            logger.info(f"Adding tag to staged changes for environment: {environment}")
            
            result = add_tag_to_staged_changes(
                tag_data=tag_data,
                environment=environment,
                owner=args.owner,
                base_dir=args.base_dir,
                mutation_name=mutation_name
            )
            
            if result:
                logger.info("Tag added to staged changes successfully")
                for file_type, file_path in result.items():
                    logger.info(f"Created {file_type}: {file_path}")
            else:
                logger.error("Failed to add tag to staged changes")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 