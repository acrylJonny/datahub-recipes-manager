#!/usr/bin/env python3
"""
List all DataHub ingestion sources.
This script retrieves and displays all ingestion sources defined in DataHub.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient
from utils.token_utils import get_token
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def list_sources(client: DataHubRestClient, output_format: str = "text") -> List[Dict[str, Any]]:
    """
    List all ingestion sources in DataHub
    
    Args:
        client: DataHub REST client
        output_format: Output format (text or json)
        
    Returns:
        List of ingestion sources
    """
    try:
        logger.info("Listing ingestion sources...")
        
        # Call the list_ingestion_sources method
        sources = client.list_ingestion_sources()
        
        if not sources:
            logger.info("No ingestion sources found")
            return []
        
        logger.info(f"Found {len(sources)} ingestion sources")
        
        # Format output
        if output_format == "json":
            print(json.dumps(sources, indent=2))
        else:
            # Text format
            print(f"\n{'ID':<30} | {'NAME':<30} | {'TYPE':<15} | {'SCHEDULE':<20}")
            print("-" * 100)
            
            if sources is None:
                logger.warning("Sources data is None, cannot display table")
                return []
                
            for source in sources:
                source_id = source.get("id", source.get("source_id", ""))
                name = source.get("name", "")
                type_info = source.get("type", "")
                
                schedule_info = "None"
                if "schedule" in source and source["schedule"] and "interval" in source["schedule"]:
                    schedule_info = source["schedule"]["interval"]
                
                print(f"{source_id:<30} | {name:<30} | {type_info:<15} | {schedule_info:<20}")
        
        return sources
    except Exception as e:
        logger.error(f"Error listing ingestion sources: {str(e)}")
        return []

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="List DataHub ingestion sources")
    
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text or json (default: text)"
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
    
    try:
        # List ingestion sources
        sources = list_sources(client, args.format)
        
        if not sources and not args.format == "json":
            logger.warning("No ingestion sources were found. This could be normal if none have been created yet.")
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 