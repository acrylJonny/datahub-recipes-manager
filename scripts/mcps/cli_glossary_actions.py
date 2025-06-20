#!/usr/bin/env python3
"""
Command-line interface for glossary actions (nodes and terms)
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

from scripts.mcps.glossary_actions import (
    add_glossary_to_staged_changes,
    setup_logging
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="DataHub glossary actions")
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Add glossary node to staged changes
    node_parser = subparsers.add_parser("stage-node", help="Add glossary node to staged changes")
    node_parser.add_argument("--node-file", required=True, help="JSON file with glossary node data")
    node_parser.add_argument("--environment", default="dev", help="Environment name")
    node_parser.add_argument("--owner", default="admin", help="Owner username")
    node_parser.add_argument("--base-dir", default="metadata", help="Base directory for output")
    node_parser.add_argument("--mutation-name", help="Mutation name for deterministic URN generation")
    
    # Add glossary term to staged changes
    term_parser = subparsers.add_parser("stage-term", help="Add glossary term to staged changes")
    term_parser.add_argument("--term-file", required=True, help="JSON file with glossary term data")
    term_parser.add_argument("--environment", default="dev", help="Environment name")
    term_parser.add_argument("--owner", default="admin", help="Owner username")
    term_parser.add_argument("--base-dir", default="metadata", help="Base directory for output")
    term_parser.add_argument("--mutation-name", help="Mutation name for deterministic URN generation")
    
    # Add generic glossary entity to staged changes
    entity_parser = subparsers.add_parser("stage-entity", help="Add glossary entity to staged changes")
    entity_parser.add_argument("--entity-file", required=True, help="JSON file with glossary entity data")
    entity_parser.add_argument("--entity-type", required=True, choices=["node", "term"], help="Type of entity")
    entity_parser.add_argument("--environment", default="dev", help="Environment name")
    entity_parser.add_argument("--owner", default="admin", help="Owner username")
    entity_parser.add_argument("--base-dir", default="metadata", help="Base directory for output")
    entity_parser.add_argument("--mutation-name", help="Mutation name for deterministic URN generation")
    
    return parser.parse_args()


def load_json_data(file_path: str) -> Dict[str, Any]:
    """Load JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {str(e)}")
        raise


def main():
    """Main function"""
    try:
        args = parse_args()
        
        if not args.action:
            print("Please specify an action. Use --help for more information.")
            sys.exit(1)
        
        setup_logging()
        
        if args.action == "stage-node":
            # Load glossary node data
            node_data = load_json_data(args.node_file)
            
            # Add to staged changes using comprehensive function
            result = add_glossary_to_staged_changes(
                entity_data=node_data,
                entity_type="node",
                environment=args.environment,
                owner=args.owner,
                base_dir=os.path.join(args.base_dir, "glossary"),
                mutation_name=args.mutation_name
            )
            
            if result:
                print(f"✅ Glossary node added to staged changes successfully")
                print(f"📁 Node ID: {node_data.get('id', 'Unknown')}")
                print(f"📄 Files created: {len(result)}")
                
                if result:
                    print("\n📁 Files created:")
                    for file_path in result.values():
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add glossary node to staged changes")
                sys.exit(1)
                
        elif args.action == "stage-term":
            # Load glossary term data
            term_data = load_json_data(args.term_file)
            
            # Add to staged changes using comprehensive function
            result = add_glossary_to_staged_changes(
                entity_data=term_data,
                entity_type="term",
                environment=args.environment,
                owner=args.owner,
                base_dir=os.path.join(args.base_dir, "glossary"),
                mutation_name=args.mutation_name
            )
            
            if result:
                print(f"✅ Glossary term added to staged changes successfully")
                print(f"📁 Term ID: {term_data.get('id', 'Unknown')}")
                print(f"📄 Files created: {len(result)}")
                
                if result:
                    print("\n📁 Files created:")
                    for file_path in result.values():
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add glossary term to staged changes")
                sys.exit(1)
                
        elif args.action == "stage-entity":
            # Load glossary entity data
            entity_data = load_json_data(args.entity_file)
            
            # Add to staged changes using comprehensive function
            result = add_glossary_to_staged_changes(
                entity_data=entity_data,
                entity_type=args.entity_type,
                environment=args.environment,
                owner=args.owner,
                base_dir=os.path.join(args.base_dir, "glossary"),
                mutation_name=args.mutation_name
            )
            
            if result:
                print(f"✅ Glossary {args.entity_type} added to staged changes successfully")
                print(f"📁 Entity ID: {entity_data.get('id', 'Unknown')}")
                print(f"📄 Files created: {len(result)}")
                
                if result:
                    print("\n📁 Files created:")
                    for file_path in result.values():
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add glossary {args.entity_type} to staged changes")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 