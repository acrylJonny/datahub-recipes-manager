#!/usr/bin/env python3
"""
Script to import DataHub policies from files.

Example usage:
    # Import all policies from a directory
    python import_policy.py --input-dir policies/

    # Import a specific policy file
    python import_policy.py --input-file policies/my_policy_123.json

    # Import using custom DataHub connection
    python import_policy.py --server http://datahub:8080 --token your-token --input-dir policies/

    # Skip existing policies
    python import_policy.py --input-dir policies/ --skip-existing

    # Force update existing policies
    python import_policy.py --input-dir policies/ --force-update
"""

import os
import sys
import argparse
import logging
import json
import glob
from typing import Dict, Any, Tuple

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient
from utils.env_loader import load_env_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("import_policy")


def load_policy_file(file_path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Load a policy from a JSON file.

    Args:
        file_path: Path to the policy file

    Returns:
        Tuple of (policy_data, metadata)
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        if isinstance(data, dict) and "policy" in data:
            # File is in our export format with metadata
            policy_data = data.get("policy", {})
            metadata = data.get("metadata", {})
        else:
            # File contains just the policy data
            policy_data = data
            metadata = {}

        return policy_data, metadata
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load policy file {file_path}: {str(e)}")
        return {}, {}


def import_policy(
    client: DataHubRestClient,
    policy_data: Dict[str, Any],
    skip_existing: bool = False,
    force_update: bool = False,
) -> bool:
    """
    Import a policy to DataHub.

    Args:
        client: DataHub REST client
        policy_data: Policy data to import
        skip_existing: Whether to skip if policy already exists
        force_update: Whether to update if policy already exists

    Returns:
        True if successful, False otherwise
    """
    policy_data.get("id")
    policy_name = policy_data.get("name", "Unknown policy")

    # Remove metadata fields that should not be sent to DataHub
    policy_data.pop("_source_server", None)
    policy_data.pop("id", None)  # ID is assigned by DataHub
    policy_data.pop("urn", None)  # URN is assigned by DataHub

    # Check if policy exists by name
    existing_policies = client.list_policies(limit=1000)
    existing_policy = None

    for policy in existing_policies:
        if policy.get("name") == policy_name:
            existing_policy = policy
            logger.info(
                f"Found existing policy with name '{policy_name}' (ID: {policy.get('id')})"
            )
            break

    if existing_policy:
        if skip_existing:
            logger.info(f"Skipping existing policy: {policy_name}")
            return True

        if force_update:
            logger.info(f"Updating existing policy: {policy_name}")
            result = client.update_policy(existing_policy.get("id"), policy_data)
            if result:
                logger.info(f"Successfully updated policy: {policy_name}")
                return True
            else:
                logger.error(f"Failed to update policy: {policy_name}")
                return False

        logger.warning(
            f"Policy already exists: {policy_name}. Use --skip-existing or --force-update."
        )
        return False

    # Create new policy
    logger.info(f"Creating new policy: {policy_name}")
    result = client.create_policy(policy_data)
    if result:
        logger.info(
            f"Successfully created policy: {policy_name} with ID: {result.get('id')}"
        )
        return True
    else:
        logger.error(f"Failed to create policy: {policy_name}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Import DataHub policies from files")
    parser.add_argument(
        "--input-dir", help="Directory containing policy files to import"
    )
    parser.add_argument("--input-file", help="Specific policy file to import")
    parser.add_argument(
        "--server", help="DataHub server URL (defaults to DATAHUB_GMS_URL env var)"
    )
    parser.add_argument(
        "--token", help="DataHub access token (defaults to DATAHUB_TOKEN env var)"
    )
    parser.add_argument("--env-file", help="Path to .env file")
    parser.add_argument(
        "--skip-existing", action="store_true", help="Skip policies that already exist"
    )
    parser.add_argument(
        "--force-update", action="store_true", help="Update policies that already exist"
    )

    args = parser.parse_args()

    if not args.input_dir and not args.input_file:
        logger.error("Either --input-dir or --input-file must be specified")
        sys.exit(1)

    if args.skip_existing and args.force_update:
        logger.error("Cannot use both --skip-existing and --force-update together")
        sys.exit(1)

    # Load environment variables
    if args.env_file:
        load_env_file(args.env_file)
    else:
        env_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
        )
        if os.path.exists(env_file):
            load_env_file(env_file)

    # Initialize client
    server = (
        args.server
        or os.environ.get("DATAHUB_GMS_URL")
        or os.environ.get("DATAHUB_SERVER")
    )
    token = args.token or os.environ.get("DATAHUB_TOKEN")

    if not server:
        logger.error(
            "DataHub server URL not provided. Set DATAHUB_GMS_URL environment variable or use --server."
        )
        sys.exit(1)

    client = DataHubRestClient(server, token)

    # Test connection
    if not client.test_connection():
        logger.error(f"Failed to connect to DataHub at {server}")
        sys.exit(1)

    # Get policy files
    policy_files = []
    if args.input_file:
        if not os.path.exists(args.input_file):
            logger.error(f"Policy file not found: {args.input_file}")
            sys.exit(1)
        policy_files = [args.input_file]
    elif args.input_dir:
        if not os.path.exists(args.input_dir):
            logger.error(f"Policy directory not found: {args.input_dir}")
            sys.exit(1)
        policy_files = glob.glob(os.path.join(args.input_dir, "*.json"))

    if not policy_files:
        logger.error("No policy files found")
        sys.exit(1)

    logger.info(f"Found {len(policy_files)} policy files to import")

    # Import policies
    success_count = 0
    for file_path in policy_files:
        logger.info(f"Processing policy file: {file_path}")
        policy_data, metadata = load_policy_file(file_path)

        if not policy_data:
            logger.warning(f"Skipping invalid policy file: {file_path}")
            continue

        if import_policy(client, policy_data, args.skip_existing, args.force_update):
            success_count += 1

    logger.info(
        f"Successfully imported {success_count} out of {len(policy_files)} policies"
    )


if __name__ == "__main__":
    main()
