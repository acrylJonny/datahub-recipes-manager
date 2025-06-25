#!/usr/bin/env python3
"""
Command-line interface for structured property actions
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, Any, Optional, List

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from scripts.mcps.structured_property_actions import (
    add_structured_property_to_staged_changes,
    setup_logging
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="DataHub structured property actions")
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Add structured property to staged changes
    stage_parser = subparsers.add_parser("stage-property", help="Add structured property to staged changes")
    stage_parser.add_argument("--property-file", required=True, help="JSON file with structured property data")
    stage_parser.add_argument("--environment", default="dev", help="Environment name (dev, staging, prod)")
    stage_parser.add_argument("--owner", default="admin", help="Owner username")
    stage_parser.add_argument("--base-dir", help="Base directory for output")
    stage_parser.add_argument("--include-all-aspects", action="store_true", default=True, help="Include all supported aspects")
    stage_parser.add_argument("--custom-aspects", help="JSON string with custom aspects")
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
        if args.action == "stage-property":
            # Load structured property data
            property_data = load_json_data(args.property_file)
            custom_aspects = parse_custom_aspects(args.custom_aspects)
            
            # Extract required fields from property data
            property_id = property_data.get("id") or property_data.get("property_id")
            qualified_name = property_data.get("qualified_name")
            
            if not property_id:
                print("❌ Error: property_id is required in the JSON file")
                sys.exit(1)
            
            if not qualified_name:
                print("❌ Error: qualified_name is required in the JSON file")
                sys.exit(1)
            
            # Add to staged changes
            result = add_structured_property_to_staged_changes(
                property_id=property_id,
                qualified_name=qualified_name,
                display_name=property_data.get("display_name"),
                description=property_data.get("description"),
                value_type=property_data.get("value_type", "STRING"),
                cardinality=property_data.get("cardinality", "SINGLE"),
                allowedValues=property_data.get("allowedValues"),
                entity_types=property_data.get("entity_types"),
                owners=property_data.get("owners"),
                tags=property_data.get("tags"),
                terms=property_data.get("terms"),
                links=property_data.get("links"),
                custom_properties=property_data.get("custom_properties"),
                include_all_aspects=args.include_all_aspects,
                custom_aspects=custom_aspects,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir,
                mutation_name=args.mutation_name
            )
            
            if result.get("success"):
                print(f"✅ Structured property added to staged changes successfully")
                print(f"📁 Property ID: {result.get('property_id')}")
                print(f"🔗 Property URN: {result.get('property_urn')}")
                print(f"📊 MCPs created: {result.get('mcps_created')}")
                print(f"📄 Files saved: {len(result.get('files_saved', []))}")
                print(f"🎯 Aspects included: {', '.join(result.get('aspects_included', []))}")
                
                if result.get('files_saved'):
                    print("\n📁 Files created:")
                    for file_path in result.get('files_saved', []):
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add structured property to staged changes: {result.get('message')}")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 