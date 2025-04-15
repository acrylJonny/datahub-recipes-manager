#!/usr/bin/env python3
"""
Script to list all DataHub ingestion sources (recipes) with their configuration.
"""

import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Load environment variables
    load_dotenv()
    
    # Get DataHub connection details
    datahub_gms_url = os.environ.get('DATAHUB_GMS_URL', 'http://localhost:8080')
    datahub_token = os.environ.get('DATAHUB_TOKEN')
    
    # Initialize client
    if datahub_token and datahub_token != 'your_datahub_pat_token_here':
        client = DataHubRestClient(server_url=datahub_gms_url, token=datahub_token)
    else:
        print("Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication.")
        print("This will only work if your DataHub instance doesn't require authentication.")
        client = DataHubRestClient(server_url=datahub_gms_url)
    
    # Test connection
    if not client.test_connection():
        print(f"Error: Could not connect to DataHub at {datahub_gms_url}")
        sys.exit(1)
    
    print(f"Successfully connected to DataHub at {datahub_gms_url}")
    
    # Get ingestion sources
    print("Fetching ingestion sources...")
    sources = client.list_ingestion_sources()
    
    if not sources:
        print("No ingestion sources found.")
        sys.exit(0)
    
    print(f"Retrieved {len(sources)} ingestion sources:")
    
    # Display source information
    for i, source in enumerate(sources, 1):
        print(f"\n--- Ingestion Source #{i} ---")
        print(f"ID:     {source['id']}")
        print(f"Name:   {source['name']}")
        print(f"Type:   {source['type']}")
        
        schedule = source.get('schedule', {})
        print(f"Schedule: {schedule.get('interval', 'N/A')} (Timezone: {schedule.get('timezone', 'UTC')})")
        
        # Display platform information if available
        if 'platform' in source and source['platform']:
            print(f"Platform: {source['platform']}")
        
        # Display last execution status if available
        if source.get('last_execution'):
            last_exec = source['last_execution']
            print(f"Last Execution: {last_exec.get('status', 'N/A')}")
            if last_exec.get('start_time'):
                # Convert milliseconds to seconds for readability
                start_time = last_exec['start_time'] / 1000 if last_exec['start_time'] else 0
                duration = last_exec['duration'] / 1000 if last_exec['duration'] else 0
                print(f"Started: {start_time} (Duration: {duration:.2f}s)")
        
        # Show recipe source type and key configuration
        recipe = source.get('recipe', {})
        source_config = recipe.get('source', {}).get('config', {})
        if source_config:
            print("\nSource Configuration:")
            # Print important configuration items
            for key in ['host', 'port', 'database', 'username', 'include_tables', 'include_views']:
                if key in source_config:
                    print(f"  {key}: {source_config[key]}")
        
        print(f"URN: {source['urn']}")
    
    print(f"\nTotal: {len(sources)} ingestion sources")

if __name__ == "__main__":
    main() 