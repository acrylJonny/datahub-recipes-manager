#!/usr/bin/env python3
"""
Script to manage DataHub policies (create, get, update, delete, list).

Example usage:
    # List all policies
    python manage_policy.py list
    
    # Get a specific policy 
    python manage_policy.py get my-policy-id
    
    # Create a new policy
    python manage_policy.py create --name "Test Policy" --description "Test policy description" --type METADATA_POLICY

    # Update a policy
    python manage_policy.py update my-policy-id --name "Updated Policy" --description "Updated description"
    
    # Delete a policy
    python manage_policy.py delete my-policy-id
"""

import os
import sys
import argparse
import logging
import json
from typing import Dict, Any, Optional, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient
from utils.env_loader import load_env_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("manage_policy")

def parse_args():
    parser = argparse.ArgumentParser(description="Manage DataHub policies")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    subparsers.required = True
    
    # List policies
    list_parser = subparsers.add_parser("list", help="List all policies")
    list_parser.add_argument("--limit", type=int, default=100, help="Maximum number of policies to list")
    list_parser.add_argument("--start", type=int, default=0, help="Starting offset for pagination")
    
    # Get policy
    get_parser = subparsers.add_parser("get", help="Get a specific policy")
    get_parser.add_argument("policy_id", help="Policy ID or URN to get")
    
    # Create policy
    create_parser = subparsers.add_parser("create", help="Create a new policy")
    create_parser.add_argument("--name", required=True, help="Name of the policy")
    create_parser.add_argument("--description", default="", help="Description of the policy")
    create_parser.add_argument("--type", default="METADATA_POLICY", choices=["METADATA_POLICY", "PLATFORM_POLICY"], 
                             help="Type of policy")
    create_parser.add_argument("--state", default="ACTIVE", choices=["ACTIVE", "INACTIVE"], 
                             help="State of the policy")
    create_parser.add_argument("--resources", help="JSON list of resources (e.g., '[{\"type\":\"dataset\",\"resource\":\"*\"}]')")
    create_parser.add_argument("--privileges", help="JSON list of privileges (e.g., '[\"VIEW_ENTITY_PAGE\"]')")
    create_parser.add_argument("--actors", help="JSON object of actors (e.g., '{\"allUsers\":true}')")
    
    # Update policy
    update_parser = subparsers.add_parser("update", help="Update an existing policy")
    update_parser.add_argument("policy_id", help="Policy ID or URN to update")
    update_parser.add_argument("--name", help="New name for the policy")
    update_parser.add_argument("--description", help="New description for the policy")
    update_parser.add_argument("--state", choices=["ACTIVE", "INACTIVE"], help="New state for the policy")
    update_parser.add_argument("--resources", help="JSON list of resources (e.g., '[{\"type\":\"dataset\",\"resource\":\"*\"}]')")
    update_parser.add_argument("--privileges", help="JSON list of privileges (e.g., '[\"VIEW_ENTITY_PAGE\"]')")
    update_parser.add_argument("--actors", help="JSON object of actors (e.g., '{\"allUsers\":true}')")
    
    # Delete policy
    delete_parser = subparsers.add_parser("delete", help="Delete a policy")
    delete_parser.add_argument("policy_id", help="Policy ID or URN to delete")
    
    return parser.parse_args()

def load_json_arg(arg_value):
    """Load a JSON string from an argument, or return None if the argument is None."""
    if arg_value is None:
        return None
    try:
        return json.loads(arg_value)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON argument: {e}")
        return None

def main():
    # Load environment variables
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_file):
        load_env_file(env_file)
    
    # Check for DATAHUB_SERVER
    datahub_server = os.environ.get("DATAHUB_SERVER")
    if not datahub_server:
        logger.error("DATAHUB_SERVER environment variable is not set")
        sys.exit(1)
    
    # Initialize client
    client = DataHubRestClient(datahub_server, os.environ.get("DATAHUB_TOKEN"))
    
    # Test connection
    if not client.test_connection():
        logger.error(f"Failed to connect to DataHub at {datahub_server}")
        sys.exit(1)
    
    args = parse_args()
    
    if args.action == "list":
        policies = client.list_policies(limit=args.limit, start=args.start)
        if not policies:
            logger.info("No policies found")
        else:
            logger.info(f"Found {len(policies)} policies:")
            for policy in policies:
                print(f"ID: {policy.get('id')}, Name: {policy.get('name')}, "
                      f"Type: {policy.get('type')}, State: {policy.get('state')}")
    
    elif args.action == "get":
        policy = client.get_policy(args.policy_id)
        if policy:
            print(json.dumps(policy, indent=2))
        else:
            logger.error(f"Policy not found: {args.policy_id}")
            sys.exit(1)
    
    elif args.action == "create":
        policy_data = {
            "name": args.name,
            "description": args.description,
            "type": args.type,
            "state": args.state,
        }
        
        # Parse JSON arguments
        if args.resources:
            policy_data["resources"] = load_json_arg(args.resources)
        else:
            policy_data["resources"] = [{"type": "dataset", "resource": "*"}]
            
        if args.privileges:
            policy_data["privileges"] = load_json_arg(args.privileges)
        else:
            policy_data["privileges"] = ["VIEW_ENTITY_PAGE"]
            
        if args.actors:
            policy_data["actors"] = load_json_arg(args.actors)
        else:
            policy_data["actors"] = {"allUsers": True}
        
        policy = client.create_policy(policy_data)
        if policy:
            logger.info(f"Policy created successfully with ID: {policy.get('id')}")
            print(json.dumps(policy, indent=2))
        else:
            logger.error("Failed to create policy")
            sys.exit(1)
    
    elif args.action == "update":
        policy_data = {}
        
        if args.name:
            policy_data["name"] = args.name
        if args.description is not None:
            policy_data["description"] = args.description
        if args.state:
            policy_data["state"] = args.state
            
        # Parse JSON arguments
        if args.resources:
            policy_data["resources"] = load_json_arg(args.resources)
        if args.privileges:
            policy_data["privileges"] = load_json_arg(args.privileges)
        if args.actors:
            policy_data["actors"] = load_json_arg(args.actors)
        
        if not policy_data:
            logger.error("No update parameters provided")
            sys.exit(1)
            
        policy = client.update_policy(args.policy_id, policy_data)
        if policy:
            logger.info(f"Policy updated successfully: {args.policy_id}")
            print(json.dumps(policy, indent=2))
        else:
            logger.error(f"Failed to update policy: {args.policy_id}")
            sys.exit(1)
    
    elif args.action == "delete":
        success = client.delete_policy(args.policy_id)
        if success:
            logger.info(f"Policy deleted successfully: {args.policy_id}")
        else:
            logger.error(f"Failed to delete policy: {args.policy_id}")
            sys.exit(1)

if __name__ == "__main__":
    main() 