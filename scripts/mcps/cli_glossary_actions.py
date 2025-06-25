#!/usr/bin/env python3
"""
CLI wrapper for glossary actions with environment parameter support
"""

import argparse
import json
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from scripts.mcps.glossary_actions import (
    add_glossary_to_staged_changes,
    add_glossary_node_to_staged_changes,
    add_glossary_term_to_staged_changes,
    download_glossary_json,
    setup_logging
)

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Glossary Actions CLI")
    
    # Common arguments
    parser.add_argument("--log-level", default="INFO", help="Log level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--environment", default="dev", help="Environment name (dev, staging, prod)")
    parser.add_argument("--owner", default="admin", help="Owner username")
    parser.add_argument("--base-dir", help="Base directory for output")
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="action", help="Available actions")
    
    # Add to staged changes
    stage_parser = subparsers.add_parser("stage", help="Add glossary entity to staged changes")
    stage_parser.add_argument("--entity-type", choices=["node", "term"], required=True, help="Entity type")
    stage_parser.add_argument("--entity-data", required=True, help="JSON string or file path containing entity data")
    stage_parser.add_argument("--mutation-name", help="Mutation name for deterministic URN generation")
    
    # Download JSON
    download_parser = subparsers.add_parser("download", help="Download glossary entity as JSON")
    download_parser.add_argument("--entity-data", required=True, help="JSON string or file path containing entity data")
    download_parser.add_argument("--output-path", help="Output file path")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    if not args.action:
        parser.print_help()
        return 1
    
    try:
        # Parse entity data
        if args.action in ["stage", "download"]:
            entity_data = None
            if os.path.isfile(args.entity_data):
                with open(args.entity_data, 'r') as f:
                    entity_data = json.load(f)
            else:
                entity_data = json.loads(args.entity_data)
        
        if args.action == "stage":
            result = add_glossary_to_staged_changes(
                entity_data=entity_data,
                entity_type=args.entity_type,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir,
                mutation_name=args.mutation_name
            )
            logger.info(f"Successfully added {args.entity_type} to staged changes")
            print(json.dumps(result, indent=2))
            
        elif args.action == "download":
            result = download_glossary_json(
                entity_data=entity_data,
                output_path=args.output_path
            )
            logger.info(f"Successfully downloaded glossary JSON")
            if not args.output_path:
                print(result)
            else:
                print(f"Saved to: {result}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error executing {args.action}: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 