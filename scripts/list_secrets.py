#!/usr/bin/env python3
"""
List all DataHub secrets.
This script retrieves and displays all secrets defined in DataHub.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List

sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient
from utils.token_utils import get_token
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def list_secrets(
    client: DataHubRestClient, output_format: str = "text", limit: int = 100
) -> List[Dict[str, Any]]:
    """
    List all secrets in DataHub

    Args:
        client: DataHub REST client
        output_format: Output format (text or json)
        limit: Maximum number of secrets to return

    Returns:
        List of secrets
    """
    try:
        logger.info("Listing DataHub secrets...")

        # Call the list_secrets method
        secrets = client.list_secrets(0, limit)

        if not secrets:
            logger.info("No secrets found")
            return []

        logger.info(f"Found {len(secrets)} secrets")

        # Format output
        if output_format == "json":
            print(json.dumps(secrets, indent=2))
        else:
            # Text format
            print(f"\n{'NAME':<30} | {'DESCRIPTION':<50}")
            print("-" * 85)
            for secret in secrets:
                name = secret.get("name", "")
                description = secret.get("description", "")

                print(f"{name:<30} | {description:<50}")

        return secrets
    except Exception as e:
        logger.error(f"Error listing secrets: {str(e)}")
        return []


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="List DataHub secrets")

    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text or json (default: text)",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of secrets to return (default: 100)",
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    # Load environment variables
    load_dotenv()

    # Parse arguments
    args = parse_args()

    # Get DataHub server URL and token
    datahub_url = os.environ.get("DATAHUB_GMS_URL")
    if not datahub_url:
        logger.error("DATAHUB_GMS_URL environment variable is required")
        sys.exit(1)

    # Get token using token_utils
    token = get_token()

    # Create DataHub client
    client = DataHubRestClient(datahub_url, token)

    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        sys.exit(1)

    # List secrets
    secrets = list_secrets(client, args.format, args.limit)

    if not secrets and not args.format == "json":
        logger.warning(
            "No secrets were found. This could be normal if none have been created yet."
        )


if __name__ == "__main__":
    main()
