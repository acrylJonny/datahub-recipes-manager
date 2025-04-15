#!/usr/bin/env python3
"""
Script to trigger an immediate run of a DataHub ingestion source
"""

import os
import sys
import logging
import argparse
from dotenv import load_dotenv

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.datahub_rest_client import DataHubRestClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a DataHub ingestion source immediately")
    parser.add_argument("--id", required=True, help="The ID of the ingestion source to run")
    parser.add_argument("--server", help="DataHub server URL (overrides DATAHUB_SERVER environment variable)")
    parser.add_argument("--token", help="DataHub token (overrides DATAHUB_TOKEN environment variable)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load environment variables from .env file if it exists
    load_dotenv()
    
    # Get DataHub server URL and token from environment variables or command line arguments
    server_url = args.server or os.environ.get("DATAHUB_SERVER")
    token = args.token or os.environ.get("DATAHUB_TOKEN")
    
    if not server_url:
        logger.error("DataHub server URL not provided. Use --server or set DATAHUB_SERVER environment variable.")
        return 1
    
    # Create DataHub REST client
    client = DataHubRestClient(server_url=server_url, token=token)
    
    # Test the connection to DataHub
    if not client.test_connection():
        logger.error("Failed to connect to DataHub server")
        return 1
    
    # Run the ingestion source
    source_id = args.id
    logger.info(f"Triggering immediate run for ingestion source: {source_id}")
    
    success = client.run_ingestion_source(source_id)
    
    if success:
        logger.info(f"Successfully triggered ingestion source: {source_id}")
        return 0
    else:
        logger.error(f"Failed to trigger ingestion source: {source_id}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 