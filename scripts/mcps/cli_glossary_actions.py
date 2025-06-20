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
    add_glossary_node_to_staged_changes,
    add_glossary_term_to_staged_changes,
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
    node_parser.add_argument("--include-all-aspects", action="store_true", default=True, help="Include all supported aspects")
    node_parser.add_argument("--custom-aspects", help="JSON string with custom aspects")
    
    # Add glossary term to staged changes
    term_parser = subparsers.add_parser("stage-term", help="Add glossary term to staged changes")
    term_parser.add_argument("--term-file", required=True, help="JSON file with glossary term data")
    term_parser.add_argument("--environment", default="dev", help="Environment name")
    term_parser.add_argument("--owner", default="admin", help="Owner username")
    term_parser.add_argument("--base-dir", default="metadata", help="Base directory for output")
    term_parser.add_argument("--include-all-aspects", action="store_true", default=True, help="Include all supported aspects")
    term_parser.add_argument("--custom-aspects", help="JSON string with custom aspects")
    
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


def load_json_data(file_path: str) -> Dict[str, Any]:
    """Load data from a JSON file"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON data from {file_path}: {str(e)}")
        sys.exit(1)


def parse_custom_aspects(custom_aspects_str: Optional[str]) -> Optional[Dict[str, Any]]:
    """Parse custom aspects from JSON string"""
    if not custom_aspects_str:
        return None
    
    try:
        return json.loads(custom_aspects_str)
    except Exception as e:
        logger.error(f"Error parsing custom aspects JSON: {str(e)}")
        sys.exit(1)


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    try:
        if args.action == "stage-node":
            # Load glossary node data
            node_data = load_json_data(args.node_file)
            custom_aspects = parse_custom_aspects(args.custom_aspects)
            
            # Add to staged changes
            result = add_glossary_node_to_staged_changes(
                node_data=node_data,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir,
                include_all_aspects=args.include_all_aspects,
                custom_aspects=custom_aspects
            )
            
            if result.get("success"):
                print(f"✅ Glossary node added to staged changes successfully")
                print(f"📁 Node ID: {result.get('node_id')}")
                print(f"🔗 Node URN: {result.get('node_urn')}")
                print(f"📊 MCPs created: {result.get('mcps_created')}")
                print(f"📄 Files saved: {len(result.get('files_saved', []))}")
                print(f"🎯 Aspects included: {', '.join(result.get('aspects_included', []))}")
                
                if result.get('files_saved'):
                    print("\n📁 Files created:")
                    for file_path in result.get('files_saved', []):
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add glossary node to staged changes: {result.get('message')}")
                sys.exit(1)
                
        elif args.action == "stage-term":
            # Load glossary term data
            term_data = load_json_data(args.term_file)
            custom_aspects = parse_custom_aspects(args.custom_aspects)
            
            # Add to staged changes
            result = add_glossary_term_to_staged_changes(
                term_data=term_data,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir,
                include_all_aspects=args.include_all_aspects,
                custom_aspects=custom_aspects
            )
            
            if result.get("success"):
                print(f"✅ Glossary term added to staged changes successfully")
                print(f"📁 Term ID: {result.get('term_id')}")
                print(f"🔗 Term URN: {result.get('term_urn')}")
                print(f"📊 MCPs created: {result.get('mcps_created')}")
                print(f"📄 Files saved: {len(result.get('files_saved', []))}")
                print(f"🎯 Aspects included: {', '.join(result.get('aspects_included', []))}")
                
                if result.get('files_saved'):
                    print("\n📁 Files created:")
                    for file_path in result.get('files_saved', []):
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add glossary term to staged changes: {result.get('message')}")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 