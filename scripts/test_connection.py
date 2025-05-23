#!/usr/bin/env python3
"""
Test the connection to a DataHub server.
"""

import argparse
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils._datahub_metadata_client import DataHubMetadataClient
from utils.token_utils import get_token_from_env

logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """
    Set up logging configuration
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args():
    """
    Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Test the connection to a DataHub server")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--token-file",
        help="File containing DataHub access token",
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO)",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    # Get token from file or environment
    token = None
    if args.token_file:
        try:
            with open(args.token_file, "r") as f:
                token = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading token file: {str(e)}")
            sys.exit(1)
    else:
        token = get_token_from_env()
    
    # Initialize the client and test connection
    try:
        logger.info(f"Testing connection to DataHub server at {args.server_url}")
        client = DataHubMetadataClient(args.server_url, token)
        
        # Try a simple GraphQL query to verify connection
        if hasattr(client, "test_connection"):
            result = client.test_connection()
        else:
            # Fallback: just list domains which should be a lightweight operation
            result = len(client.list_domains()) >= 0
        
        if result:
            logger.info("Connection successful!")
            return 0
        else:
            logger.error("Connection failed")
            return 1
    
    except Exception as e:
        logger.error(f"Error connecting to DataHub: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
