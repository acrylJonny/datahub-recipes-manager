#!/usr/bin/env python3
"""
Command-line interface for data product actions
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

from scripts.mcps.data_product_actions import (
    add_data_product_to_staged_changes,
    setup_logging
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="DataHub data product actions")
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Add data product to staged changes
    stage_parser = subparsers.add_parser("stage-product", help="Add data product to staged changes")
    stage_parser.add_argument("--product-file", required=True, help="JSON file with data product data")
    stage_parser.add_argument("--environment", default="dev", help="Environment name")
    stage_parser.add_argument("--owner", default="admin", help="Owner username")
    stage_parser.add_argument("--base-dir", default="metadata", help="Base directory for output")
    stage_parser.add_argument("--include-all-aspects", action="store_true", default=True, help="Include all supported aspects")
    stage_parser.add_argument("--custom-aspects", help="JSON string with custom aspects")
    
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
        if args.action == "stage-product":
            # Load data product data
            product_data = load_json_data(args.product_file)
            custom_aspects = parse_custom_aspects(args.custom_aspects)
            
            # Extract required fields from product data
            data_product_id = product_data.get("id") or product_data.get("data_product_id")
            name = product_data.get("name")
            
            if not data_product_id:
                print("❌ Error: data_product_id is required in the JSON file")
                sys.exit(1)
            
            if not name:
                print("❌ Error: name is required in the JSON file")
                sys.exit(1)
            
            # Add to staged changes
            result = add_data_product_to_staged_changes(
                data_product_id=data_product_id,
                name=name,
                description=product_data.get("description"),
                external_url=product_data.get("external_url"),
                owners=product_data.get("owners"),
                tags=product_data.get("tags"),
                terms=product_data.get("terms"),
                domains=product_data.get("domains"),
                links=product_data.get("links"),
                custom_properties=product_data.get("custom_properties"),
                structured_properties=product_data.get("structured_properties"),
                sub_types=product_data.get("sub_types"),
                deprecated=product_data.get("deprecated", False),
                deprecation_note=product_data.get("deprecation_note", ""),
                include_all_aspects=args.include_all_aspects,
                custom_aspects=custom_aspects,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir
            )
            
            if result.get("success"):
                print(f"✅ Data product added to staged changes successfully")
                print(f"📁 Product ID: {result.get('data_product_id')}")
                print(f"🔗 Product URN: {result.get('data_product_urn')}")
                print(f"📊 MCPs created: {result.get('mcps_created')}")
                print(f"📄 Files saved: {len(result.get('files_saved', []))}")
                print(f"🎯 Aspects included: {', '.join(result.get('aspects_included', []))}")
                
                if result.get('files_saved'):
                    print("\n📁 Files created:")
                    for file_path in result.get('files_saved', []):
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add data product to staged changes: {result.get('message')}")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 