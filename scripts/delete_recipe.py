#!/usr/bin/env python3
"""
Script to delete DataHub ingestion recipes from a DataHub instance.
Uses the DataHub SDK (acryl-datahub) to delete ingestion sources.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_api import DataHubClient


def delete_ingestion_source(server: str, token: str, source_id: str) -> bool:
    """
    Delete an ingestion source from DataHub

    Args:
        server: DataHub GMS server URL
        token: DataHub authentication token
        source_id: Source ID to delete

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create DataHub client
        client = DataHubClient(server, token)

        # Delete the source
        success = client.delete_ingestion_source(source_id)

        if success:
            print(f"Ingestion source {source_id} deleted successfully")
        else:
            print(f"Failed to delete ingestion source {source_id}")

        return success
    except Exception as e:
        print(f"Error deleting ingestion source: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Delete DataHub ingestion recipes")
    parser.add_argument('--source-id', required=True, help='Source ID to delete')
    parser.add_argument('--env-file', help='Path to .env file for secrets')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        # Try to load from .env in the current directory
        load_dotenv()

    # Get DataHub configuration from environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", "")
    }

    if not datahub_config["server"] or not datahub_config["token"]:
        raise ValueError("DATAHUB_GMS_URL and DATAHUB_TOKEN environment variables must be set")

    # Confirm deletion unless --force is specified
    if not args.force:
        confirm = input(f"Are you sure you want to delete ingestion source {args.source_id}? [y/N] ")
        if confirm.lower() not in ['y', 'yes']:
            print("Deletion cancelled")
            return 0

    # Delete the ingestion source
    success = delete_ingestion_source(
        server=datahub_config["server"],
        token=datahub_config["token"],
        source_id=args.source_id
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())