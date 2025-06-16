#!/usr/bin/env python3
"""
Command-line interface for tag actions
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
    parser = argparse.ArgumentParser(description="DataHub tag actions")
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Download tag JSON action
    download_parser = subparsers.add_parser("download", help="Download tag JSON")
    download_parser.add_argument("--tag-file", required=True, help="JSON file with tag data")
    download_parser.add_argument("--output", help="Output file path")
    
    # Sync to local action
    sync_parser = subparsers.add_parser("sync", help="Sync tag to local database")
    sync_parser.add_argument("--tag-file", required=True, help="JSON file with tag data")
    sync_parser.add_argument("--db-path", help="Path to local database")
    
    # Add to staged changes action
    stage_parser = subparsers.add_parser("stage", help="Add tag to staged changes")
    stage_parser.add_argument("--tag-file", required=True, help="JSON file with tag data")
    stage_parser.add_argument("--environment", required=True, help="Environment name (directory structure)")
    stage_parser.add_argument("--owner", required=True, help="Owner username")
    stage_parser.add_argument("--base-dir", help="Base directory for output")
    stage_parser.add_argument("--mutation-name", help="Mutation name for deterministic URN generation")
    
    # General options
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    
    args = parser.parse_args()
    
    # Ensure an action was specified
    if args.action is None:
        parser.print_help()
        sys.exit(1)
    
    return args


def load_tag_data(file_path: str) -> Dict[str, Any]:
    """Load tag data from a JSON file"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading tag data: {str(e)}")
        sys.exit(1)


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    try:
        if args.action == "download":
            # Load tag data
            tag_data = load_tag_data(args.tag_file)
            
            # Download as JSON
            result = download_tag_json(tag_data, args.output)
            
            if args.output:
                print(f"Tag JSON saved to: {result}")
            else:
                print(result)
                
        elif args.action == "sync":
            # Load tag data
            tag_data = load_tag_data(args.tag_file)
            
            # Sync to local database
            success = sync_tag_to_local(tag_data, args.db_path)
            
            if success:
                print("Tag synced to local database successfully")
            else:
                print("Failed to sync tag to local database")
                sys.exit(1)
                
        elif args.action == "stage":
            # Load tag data
            tag_data = load_tag_data(args.tag_file)
            
            # Add to staged changes
            result = add_tag_to_staged_changes(
                tag_data, 
                args.environment, 
                args.owner,
                args.base_dir,
                args.mutation_name
            )
            
            print("Tag added to staged changes successfully")
            print(f"Properties MCP: {result['properties_file']}")
            print(f"Ownership MCP: {result['ownership_file']}")
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 