#!/usr/bin/env python3
"""
Command-line interface for test actions
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

from scripts.mcps.test_actions import (
    add_test_to_staged_changes,
    setup_logging
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="DataHub test actions")
    
    # Create subparsers for different actions
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    
    # Add test to staged changes
    stage_parser = subparsers.add_parser("stage-test", help="Add test to staged changes")
    stage_parser.add_argument("--test-id", required=True, help="Test ID")
    stage_parser.add_argument("--test-urn", required=True, help="Test URN")
    stage_parser.add_argument("--test-name", required=True, help="Test name")
    stage_parser.add_argument("--test-type", default="CUSTOM", help="Test type")
    stage_parser.add_argument("--description", help="Test description")
    stage_parser.add_argument("--category", help="Test category")
    stage_parser.add_argument("--entity-urn", help="Entity URN this test applies to")
    stage_parser.add_argument("--platform", help="Platform name")
    stage_parser.add_argument("--platform-instance", help="Platform instance")
    stage_parser.add_argument("--definition-file", help="JSON file with test definition containing URNs")
    stage_parser.add_argument("--environment", default="dev", help="Environment name (dev, staging, prod)")
    stage_parser.add_argument("--owner", default="admin", help="Owner username")
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


def main():
    """Main function"""
    args = parse_args()
    setup_logging(args.log_level)
    
    try:
        if args.action == "stage-test":
            # Load definition from file if provided
            definition = {}
            if args.definition_file:
                try:
                    with open(args.definition_file, 'r') as f:
                        definition = json.load(f)
                except Exception as e:
                    print(f"❌ Error loading definition file: {e}")
                    sys.exit(1)
            
            # Add to staged changes
            result = add_test_to_staged_changes(
                test_id=args.test_id,
                test_urn=args.test_urn,
                test_name=args.test_name,
                test_type=args.test_type,
                description=args.description,
                category=args.category,
                entity_urn=args.entity_urn,
                definition=definition,
                platform=args.platform,
                platform_instance=args.platform_instance,
                environment=args.environment,
                owner=args.owner,
                base_dir=args.base_dir,
                mutation_name=args.mutation_name
            )
            
            if result.get("success"):
                print(f"✅ Test added to staged changes successfully")
                print(f"📁 Test ID: {result.get('test_id')}")
                print(f"🔗 Test URN: {result.get('test_urn')}")
                print(f"📊 MCPs created: {result.get('mcps_created')}")
                print(f"📄 Files saved: {len(result.get('files_saved', []))}")
                print(f"🎯 Aspects included: {', '.join(result.get('aspects_included', []))}")
                
                if result.get('files_saved'):
                    print("\n📁 Files created:")
                    for file_path in result.get('files_saved', []):
                        print(f"  - {file_path}")
            else:
                print(f"❌ Failed to add test to staged changes: {result.get('message')}")
                sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 