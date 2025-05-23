#!/usr/bin/env python3
"""
Initialize environment variables for the DataHub connection.
This script is used to set up the environment before starting the application.
"""

import os
import logging

logger = logging.getLogger(__name__)

def initialize_datahub_environment():
    """
    Set up environment variables for DataHub connection if not already set.
    """
    # DataHub Connection Settings
    if 'DATAHUB_GMS_URL' not in os.environ:
        os.environ['DATAHUB_GMS_URL'] = 'http://localhost:8080'
        logger.info("Set default DATAHUB_GMS_URL to http://localhost:8080")
    
    if 'DATAHUB_TOKEN' not in os.environ:
        # For testing, you should replace this with a valid token when testing locally
        os.environ['DATAHUB_TOKEN'] = 'your_datahub_token_here'
        logger.info("Set placeholder DATAHUB_TOKEN - you should replace this with a valid token")
    
    # Security Settings
    if 'VERIFY_SSL' not in os.environ:
        os.environ['VERIFY_SSL'] = 'true'
        logger.info("Set VERIFY_SSL to true")
    
    logger.info("DataHub environment variables initialized")
    return {
        'DATAHUB_GMS_URL': os.environ['DATAHUB_GMS_URL'],
        'DATAHUB_TOKEN': '***' if os.environ['DATAHUB_TOKEN'] else 'None',
        'VERIFY_SSL': os.environ['VERIFY_SSL']
    }

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Initialize environment
    env_vars = initialize_datahub_environment()
    
    # Print environment variables
    logger.info("DataHub environment initialized with:")
    for key, value in env_vars.items():
        if key == 'DATAHUB_TOKEN':
            logger.info(f"  {key}: {'Set' if value != 'None' else 'Not set'}")
        else:
            logger.info(f"  {key}: {value}") 