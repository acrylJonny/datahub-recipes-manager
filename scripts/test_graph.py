#!/usr/bin/env python3
"""
Test script to demonstrate how to use the DataHubGraph client directly.
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_list_secrets(client: DataHubRestClient):
    """Test listing secrets using GraphQL"""
    logger.info("Testing list_secrets method...")
    secrets = client.list_secrets()
    if secrets:
        logger.info(f"Found {len(secrets)} secrets:")
        for secret in secrets:
            logger.info(f"Secret: {secret['name']}, URN: {secret['urn']}")
    else:
        logger.info("No secrets found.")

def test_graph_query(client: DataHubRestClient):
    """Test direct GraphQL query execution"""
    logger.info("Testing direct GraphQL query...")
    
    # Basic introspection query that should work on any GraphQL endpoint
    query = """
    query {
      __schema {
        queryType {
          name
        }
      }
    }
    """
    
    result = client.execute_graphql(query)
    if "errors" in result:
        logger.error(f"GraphQL query failed: {result['errors']}")
    else:
        schema = result.get("data", {}).get("__schema", {})
        query_type = schema.get("queryType", {}).get("name", "unknown")
        logger.info(f"GraphQL query type: {query_type}")
    
    # Try a simple query to check if user auth is available
    logger.info("Testing me query...")
    query = """
    query {
      me {
        corpUser {
          username
          urn
        }
      }
    }
    """
    
    result = client.execute_graphql(query)
    if "errors" in result:
        logger.error(f"Me query failed: {result['errors']}")
    else:
        me = result.get("data", {}).get("me", {})
        corp_user = me.get("corpUser", {})
        username = corp_user.get("username", "unknown")
        urn = corp_user.get("urn", "unknown")
        logger.info(f"Authenticated as: {username} (URN: {urn})")

def main():
    # Load environment variables
    load_dotenv()
    
    # Get DataHub configuration from environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", "")
    }

    if not datahub_config["server"]:
        logger.error("DATAHUB_GMS_URL environment variable must be set")
        return 1
    
    # Initialize DataHub client
    client = DataHubRestClient(datahub_config["server"], datahub_config["token"])
    
    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        return 1
    
    logger.info("Connected to DataHub successfully")
    
    # Run tests
    test_list_secrets(client)
    test_graph_query(client)
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 