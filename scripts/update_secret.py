#!/usr/bin/env python3
"""
Update (patch) an existing DataHub secret.
This script updates a secret in DataHub with a new value.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient
from utils.token_utils import get_token
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def update_secret(client: DataHubRestClient, name: str, value: str) -> bool:
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
        logger.info(f"Updating secret: {name}")

        # Call the update_secret method
        success = client.update_secret(name, value)

        if success:
            logger.info(f"Successfully updated secret: {name}")
        else:
            logger.error(f"Failed to update secret: {name}")

        return success
    except Exception as e:
        logger.error(f"Error updating secret: {str(e)}")
        return False


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Update a DataHub secret")

    parser.add_argument("--name", required=True, help="Name of the secret to update")

    parser.add_argument("--value", required=True, help="New value for the secret")

    parser.add_argument("--description", help="Optional description for the secret")

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

    # Update secret
    success = update_secret(client, args.name, args.value)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
