#!/usr/bin/env python3
"""
Script to manage DataHub policies.

Commands:
- list: List all policies in DataHub
- get: Get details of a specific policy
- create: Create a new policy from a JSON configuration file
- update: Update an existing policy
- delete: Delete a policy

Example usage:
  # List all policies
  python manage_policies.py list

  # Get details of a specific policy
  python manage_policies.py get --id my-policy-id

  # Create a new policy 
  python manage_policies.py create --config-file policy.json

  # Update a policy
  python manage_policies.py update --id my-policy-id --config-file policy_updates.json

  # Delete a policy
  python manage_policies.py delete --id my-policy-id
"""

import os
import sys
import json
import argparse
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("manage_policies")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Manage DataHub policies")
    
    # Command subparsers
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # List policies command
    list_parser = subparsers.add_parser("list", help="List all policies")
    list_parser.add_argument("--state", choices=["ACTIVE", "INACTIVE"], 
                          help="Filter policies by state")
    list_parser.add_argument("--limit", type=int, default=100, 
                          help="Maximum number of policies to return")
    
    # Get policy command
    get_parser = subparsers.add_parser("get", help="Get details of a specific policy")
    get_parser.add_argument("--id", required=True, 
                        help="Policy ID or URN")
    
    # Create policy command
    create_parser = subparsers.add_parser("create", help="Create a new policy")
    create_parser.add_argument("--config-file", required=True, 
                           help="Path to JSON file containing policy configuration")
    
    # Update policy command
    update_parser = subparsers.add_parser("update", help="Update an existing policy")
    update_parser.add_argument("--id", required=True, 
                           help="Policy ID or URN to update")
    update_parser.add_argument("--config-file", required=True, 
                           help="Path to JSON file containing policy update configuration")
    
    # Delete policy command
    delete_parser = subparsers.add_parser("delete", help="Delete a policy")
    delete_parser.add_argument("--id", required=True, 
                           help="Policy ID or URN to delete")
    
    return parser.parse_args()


def list_policies(client: DataHubRestClient, limit: int, state: Optional[str]) -> None:
    """List all policies in DataHub"""
    logger.info(f"Listing policies (limit={limit}, state={state})")
    
    policies = client.list_policies(limit=limit, state=state)
    
    if not policies:
        logger.info("No policies found")
        return
    
    print(f"Found {len(policies)} policies:")
    for policy in policies:
        print(f"ID: {policy.get('id')}")
        print(f"  Name: {policy.get('name')}")
        print(f"  Type: {policy.get('type')}")
        print(f"  State: {policy.get('state')}")
        print(f"  Description: {policy.get('description')}")
        print()


def get_policy(client: DataHubRestClient, policy_id: str) -> None:
    """Get details of a specific policy"""
    logger.info(f"Getting policy details for '{policy_id}'")
    
    policy = client.get_policy(policy_id)
    
    if not policy:
        logger.error(f"Policy '{policy_id}' not found")
        return
    
    print(f"Policy ID: {policy.get('id')}")
    print(f"URN: {policy.get('urn')}")
    print(f"Name: {policy.get('name')}")
    print(f"Type: {policy.get('type')}")
    print(f"State: {policy.get('state')}")
    print(f"Description: {policy.get('description')}")
    print(f"Editable: {policy.get('editable')}")
    print(f"Version: {policy.get('version')}")
    
    print("\nResources:")
    for resource in policy.get('resources', []):
        print(f"  - Type: {resource.get('type')}, Resource: {resource.get('resource')}")
    
    print("\nPrivileges:")
    for privilege in policy.get('privileges', []):
        print(f"  - {privilege}")
    
    print("\nActors:")
    actors = policy.get('actors', {})
    if actors.get('users'):
        print(f"  Users: {', '.join(actors.get('users', []))}")
    if actors.get('groups'):
        print(f"  Groups: {', '.join(actors.get('groups', []))}")
    print(f"  All Users: {actors.get('allUsers', False)}")
    print(f"  All Groups: {actors.get('allGroups', False)}")
    print(f"  Resource Owners: {actors.get('resourceOwners', False)}")


def create_policy(client: DataHubRestClient, config_file: str) -> None:
    """Create a new policy from a JSON configuration file"""
    logger.info(f"Creating policy from config file: {config_file}")
    
    # Load and validate policy configuration
    try:
        with open(config_file, 'r') as f:
            policy_config = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Error loading policy configuration: {str(e)}")
        return
    
    # Validate required fields
    required_fields = ['id', 'name', 'type']
    missing_fields = [field for field in required_fields if field not in policy_config]
    
    if missing_fields:
        logger.error(f"Policy configuration missing required fields: {', '.join(missing_fields)}")
        return
    
    # Create the policy
    success = client.create_policy(policy_config)
    
    if success:
        logger.info(f"Successfully created policy '{policy_config.get('id')}'")
    else:
        logger.error(f"Failed to create policy '{policy_config.get('id')}'")


def update_policy(client: DataHubRestClient, policy_id: str, config_file: str) -> None:
    """Update an existing policy"""
    logger.info(f"Updating policy '{policy_id}' from config file: {config_file}")
    
    # Load and validate policy update configuration
    try:
        with open(config_file, 'r') as f:
            update_config = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Error loading policy update configuration: {str(e)}")
        return
    
    # Update the policy
    success = client.update_policy(policy_id, update_config)
    
    if success:
        logger.info(f"Successfully updated policy '{policy_id}'")
    else:
        logger.error(f"Failed to update policy '{policy_id}'")


def delete_policy(client: DataHubRestClient, policy_id: str) -> None:
    """Delete a policy"""
    logger.info(f"Deleting policy '{policy_id}'")
    
    success = client.delete_policy(policy_id)
    
    if success:
        logger.info(f"Successfully deleted policy '{policy_id}'")
    else:
        logger.error(f"Failed to delete policy '{policy_id}'")


def main() -> None:
    """Main function to manage DataHub policies"""
    # Load environment variables
    load_dotenv()
    
    # Parse command line arguments
    args = parse_args()
    
    # Get DataHub server URL and token from environment variables
    datahub_gms_url = os.getenv("DATAHUB_GMS_URL") or os.getenv("DATAHUB_SERVER")
    datahub_token = os.getenv("DATAHUB_TOKEN")
    
    if not datahub_gms_url:
        logger.error("DATAHUB_GMS_URL or DATAHUB_SERVER environment variable not set")
        sys.exit(1)
    
    # Initialize DataHub client
    client = DataHubRestClient(datahub_gms_url, datahub_token)
    
    # Test connection to DataHub
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        sys.exit(1)
    
    # Execute the requested command
    if args.command == "list":
        list_policies(client, args.limit, args.state)
    elif args.command == "get":
        get_policy(client, args.id)
    elif args.command == "create":
        create_policy(client, args.config_file)
    elif args.command == "update":
        update_policy(client, args.id, args.config_file)
    elif args.command == "delete":
        delete_policy(client, args.id)
    else:
        logger.error(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main() 