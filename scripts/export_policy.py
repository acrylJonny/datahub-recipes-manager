#!/usr/bin/env python3
"""
Script to export DataHub policies to files.

Example usage:
    # Export all policies to a directory
    python export_policy.py --output-dir policies/
    
    # Export a specific policy
    python export_policy.py --policy-id my-policy-id --output-dir policies/
    
    # Export using custom DataHub connection
    python export_policy.py --server http://datahub:8080 --token your-token --output-dir policies/
"""

import os
import sys
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient
from utils.env_loader import load_env_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("export_policy")

def sanitize_filename(name: str) -> str:
    """Convert a policy name to a valid filename."""
    # Replace spaces and special characters
    sanitized = name.lower().replace(' ', '_')
    sanitized = ''.join(c if c.isalnum() or c in '-_' else '_' for c in sanitized)
    return sanitized

def export_policy(policy: Dict[str, Any], output_dir: str) -> str:
    """
    Export a policy to a JSON file.
    
    Args:
        policy: Policy data dictionary
        output_dir: Directory to save the policy file
        
    Returns:
        Path to the saved policy file
    """
    policy_id = policy.get('id', 'unknown')
    policy_name = policy.get('name', f'policy_{policy_id}')
    policy_type = policy.get('type', 'policy').lower()
    
    # Create sanitized filename
    filename = f"{sanitize_filename(policy_name)}_{policy_id}.json"
    file_path = os.path.join(output_dir, filename)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Add export metadata
    export_data = {
        "policy": policy,
        "metadata": {
            "exported_at": datetime.now().isoformat(),
            "exported_by": os.environ.get("USER", "unknown"),
            "source_server": policy.get("_source_server", "unknown")
        }
    }
    
    # Save the policy to file
    with open(file_path, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    logger.info(f"Exported policy '{policy_name}' to {file_path}")
    return file_path

def main():
    parser = argparse.ArgumentParser(description="Export DataHub policies to files")
    parser.add_argument("--output-dir", default="exported_policies", help="Directory to save exported policies")
    parser.add_argument("--policy-id", help="Specific policy ID to export (exports all if not specified)")
    parser.add_argument("--server", help="DataHub server URL (defaults to DATAHUB_GMS_URL env var)")
    parser.add_argument("--token", help="DataHub access token (defaults to DATAHUB_TOKEN env var)")
    parser.add_argument("--env-file", help="Path to .env file")
    
    args = parser.parse_args()
    
    # Load environment variables
    if args.env_file:
        load_env_file(args.env_file)
    else:
        env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        if os.path.exists(env_file):
            load_env_file(env_file)
    
    # Initialize client
    server = args.server or os.environ.get("DATAHUB_GMS_URL") or os.environ.get("DATAHUB_SERVER")
    token = args.token or os.environ.get("DATAHUB_TOKEN")
    
    if not server:
        logger.error("DataHub server URL not provided. Set DATAHUB_GMS_URL environment variable or use --server.")
        sys.exit(1)
    
    client = DataHubRestClient(server, token)
    
    # Test connection
    if not client.test_connection():
        logger.error(f"Failed to connect to DataHub at {server}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    exported_files = []
    
    if args.policy_id:
        # Export specific policy
        logger.info(f"Exporting policy: {args.policy_id}")
        policy = client.get_policy(args.policy_id)
        if not policy:
            logger.error(f"Policy not found: {args.policy_id}")
            sys.exit(1)
        
        # Add source server to policy data
        policy["_source_server"] = server
        
        # Export the policy
        file_path = export_policy(policy, args.output_dir)
        exported_files.append(file_path)
    else:
        # Export all policies
        logger.info("Exporting all policies")
        policies = client.list_policies(limit=1000)
        
        if not policies:
            logger.info("No policies found")
            sys.exit(0)
        
        logger.info(f"Found {len(policies)} policies to export")
        
        for policy in policies:
            # Get the full policy details
            full_policy = client.get_policy(policy["id"])
            if full_policy:
                # Add source server to policy data
                full_policy["_source_server"] = server
                
                # Export the policy
                file_path = export_policy(full_policy, args.output_dir)
                exported_files.append(file_path)
            else:
                logger.warning(f"Failed to get details for policy: {policy['id']}")
    
    logger.info(f"Successfully exported {len(exported_files)} policies to {args.output_dir}")

if __name__ == "__main__":
    main() 