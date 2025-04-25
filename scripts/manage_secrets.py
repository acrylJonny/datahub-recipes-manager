#!/usr/bin/env python3
"""
Script to manage DataHub secrets.
Allows creating, listing, and deleting secrets in DataHub.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient


def create_secret(client: DataHubRestClient, name: str, value: str) -> bool:
    """
    Create a secret in DataHub

    Args:
        client: DataHub REST client
        name: Secret name
        value: Secret value

    Returns:
        True if successful, False otherwise
    """
    try:
        result = client.create_secret(name, value)

        if result:
            print(f"Secret '{name}' created successfully")
        else:
            print(f"Failed to create secret '{name}'")

        return result
    except Exception as e:
        print(f"Error creating secret: {str(e)}")
        return False


def patch_secret(client: DataHubRestClient, name: str, value: str) -> bool:
    """
    Update an existing secret in DataHub

    Args:
        client: DataHub REST client
        name: Secret name
        value: New secret value

    Returns:
        True if successful, False otherwise
    """
    try:
        # Use the dedicated update_secret method which tries GraphQL first
        # and falls back to create_secret if needed
        result = client.update_secret(name, value)

        if result:
            print(f"Secret '{name}' updated successfully")
        else:
            print(f"Failed to update secret '{name}'")

        return result
    except Exception as e:
        print(f"Error updating secret: {str(e)}")
        return False


def list_secrets(client: DataHubRestClient, limit: int = 100) -> bool:
    """
    List all secrets in DataHub

    Args:
        client: DataHub REST client
        limit: Maximum number of secrets to return

    Returns:
        True if successful, False otherwise
    """
    try:
        secrets = client.list_secrets(start=0, count=limit)

        if not secrets:
            print("No secrets found in DataHub.")
            return True

        print(f"Found {len(secrets)} secrets in DataHub:")
        print("-" * 80)
        print(f"{'NAME':<30} {'URN':<40} {'DESCRIPTION'}")
        print("-" * 80)

        for secret in secrets:
            name = secret.get("name", "")
            urn = secret.get("urn", "")
            description = secret.get("description", "")
            print(f"{name:<30} {urn:<40} {description}")

        return True
    except Exception as e:
        print(f"Error listing secrets: {str(e)}")
        return False


def delete_secret(client: DataHubRestClient, name_or_urn: str) -> bool:
    """
    Delete a secret from DataHub

    Args:
        client: DataHub REST client
        name_or_urn: Secret name or URN

    Returns:
        True if successful, False otherwise
    """
    try:
        result = client.delete_secret(name_or_urn)

        if result:
            print(f"Secret '{name_or_urn}' deleted successfully")
        else:
            print(f"Failed to delete secret '{name_or_urn}'")

        return result
    except Exception as e:
        print(f"Error deleting secret: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Manage DataHub secrets")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Create secret parser
    create_parser = subparsers.add_parser("create", help="Create a new secret")
    create_parser.add_argument("--name", required=True, help="Secret name")
    create_parser.add_argument("--value", required=True, help="Secret value")

    # Patch secret parser
    patch_parser = subparsers.add_parser("patch", help="Update an existing secret")
    patch_parser.add_argument("--name", required=True, help="Secret name")
    patch_parser.add_argument("--value", required=True, help="New secret value")

    # List secrets parser
    list_parser = subparsers.add_parser("list", help="List all secrets")
    list_parser.add_argument(
        "--limit", type=int, default=100, help="Maximum number of secrets to return"
    )

    # Delete secret parser
    delete_parser = subparsers.add_parser("delete", help="Delete a secret")
    delete_parser.add_argument(
        "--name", required=True, help="Secret name or URN to delete"
    )

    # Common arguments
    parser.add_argument("--env-file", help="Path to .env file for DataHub connection")

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        # Try to load from .env in the current directory
        load_dotenv()

    # Check for required environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", ""),
    }

    if not datahub_config["server"]:
        raise ValueError("DATAHUB_GMS_URL environment variable must be set")

    # Empty or default token is treated as not provided
    if (
        not datahub_config["token"]
        or datahub_config["token"] == "your_datahub_pat_token_here"
    ):
        print(
            "Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication."
        )
        print(
            "This will only work if your DataHub instance doesn't require authentication."
        )
        datahub_config["token"] = None

    # Create DataHub client
    client = DataHubRestClient(datahub_config["server"], datahub_config["token"])

    # Handle actions
    if args.action == "create":
        success = create_secret(client, args.name, args.value)
        return 0 if success else 1
    elif args.action == "patch":
        success = patch_secret(client, args.name, args.value)
        return 0 if success else 1
    elif args.action == "list":
        success = list_secrets(client, args.limit)
        return 0 if success else 1
    elif args.action == "delete":
        success = delete_secret(client, args.name)
        return 0 if success else 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
