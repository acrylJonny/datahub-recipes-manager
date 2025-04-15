#!/usr/bin/env python3
"""
Run an existing DataHub ingestion source immediately.
This script triggers an immediate execution of an ingestion source.
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def run_ingestion_source(client: DataHubRestClient, source_id: str) -> bool:
    """
    Trigger immediate execution of an ingestion source
    
    Args:
        client: DataHub REST client
        source_id: ID of the ingestion source to run
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Verify source exists first
        source = client.get_ingestion_source(source_id)
        if not source:
            logger.error(f"Ingestion source not found: {source_id}")
            return False
        
        logger.info(f"Triggering immediate execution of ingestion source: {source_id}")
        
        # Call the run method
        success = client.run_ingestion_source(source_id)
        
        if success:
            logger.info(f"Successfully triggered execution of ingestion source: {source_id}")
        else:
            logger.error(f"Failed to trigger execution of ingestion source: {source_id}")
        
        return success
    except Exception as e:
        logger.error(f"Error triggering ingestion source execution: {str(e)}")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run a DataHub ingestion source')
    parser.add_argument('--source-id', required=True, help='The ID of the ingestion source to run')
    parser.add_argument('--gms-url', help='DataHub GMS URL (default: value from DATAHUB_GMS_URL env var)')
    parser.add_argument('--token', help='DataHub access token (default: value from DATAHUB_TOKEN env var)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        
    # Load environment variables
    load_dotenv()
    
    # Get DataHub connection details
    datahub_gms_url = args.gms_url or os.getenv('DATAHUB_GMS_URL')
    datahub_token = args.token or os.getenv('DATAHUB_TOKEN')
    
    if not datahub_gms_url:
        logger.error("DataHub GMS URL not provided. Use --gms-url or set DATAHUB_GMS_URL environment variable")
        sys.exit(1)
    
    # Create client
    if datahub_token:
        client = DataHubRestClient(datahub_gms_url, datahub_token)
    else:
        logger.warning("DATAHUB_TOKEN not set. Connecting without authentication...")
        client = DataHubRestClient(datahub_gms_url)
    
    # Check connection
    try:
        if not client.test_connection():
            logger.error(f"Failed to connect to DataHub at {datahub_gms_url}")
            sys.exit(1)
        logger.info(f"Successfully connected to DataHub at {datahub_gms_url}")
    except Exception as e:
        logger.error(f"Error connecting to DataHub: {str(e)}")
        sys.exit(1)
    
    # Run the ingestion source
    source_id = args.source_id
    logger.info(f"Attempting to run ingestion source: {source_id}")
    
    try:
        success = client.run_ingestion_source(source_id)
        if success:
            logger.info(f"✅ Successfully triggered ingestion run for source: {source_id}")
        else:
            logger.error(f"❌ Failed to run ingestion source: {source_id}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error running ingestion source: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 