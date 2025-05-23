#!/usr/bin/env python3
"""
Create a freshness assertion in DataHub.
This script creates a new freshness assertion for a dataset in DataHub.
"""

import argparse
import json
import logging
import os
import sys

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient
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
    parser = argparse.ArgumentParser(description="Create a freshness assertion in DataHub")
    
    parser.add_argument(
        "--server-url",
        "-s",
        required=True,
        help="DataHub server URL (e.g., http://localhost:8080)",
    )
    
    parser.add_argument(
        "--dataset-urn",
        "-d",
        required=True,
        help="DataHub dataset URN",
    )
    
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        required=True,
        help="Freshness interval value (e.g., 24 for 24 hours)",
    )
    
    parser.add_argument(
        "--unit",
        "-u",
        choices=["MINUTE", "HOUR", "DAY", "WEEK"],
        default="HOUR",
        help="Freshness interval unit (default: HOUR)",
    )
    
    parser.add_argument(
        "--timezone",
        "-tz",
        default="America/Los_Angeles",
        help="Timezone for evaluation (default: America/Los_Angeles)",
    )
    
    parser.add_argument(
        "--cron",
        default="0 */8 * * *",
        help="Cron expression for evaluation schedule (default: 0 */8 * * *)",
    )
    
    parser.add_argument(
        "--source-type",
        choices=["INFORMATION_SCHEMA", "LOOKER", "DEEQU", "OTHER"],
        default="INFORMATION_SCHEMA",
        help="Source type for evaluation (default: INFORMATION_SCHEMA)",
    )
    
    parser.add_argument(
        "--description",
        "-desc",
        default="",
        help="Description for the assertion",
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
    
    parser.add_argument(
        "--tags",
        nargs="*",
        help="List of tag URNs to apply to the assertion (e.g., urn:li:tag:critical)",
    )
    
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the assertion immediately after creation",
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
    
    # Initialize the client
    try:
        client = DataHubMetadataApiClient(args.server_url, token)
    except Exception as e:
        logger.error(f"Error initializing client: {str(e)}")
        sys.exit(1)
    
    try:
        # Create freshness assertion
        logger.info(f"Creating freshness assertion for dataset {args.dataset_urn}...")
        assertion_urn = client.create_freshness_assertion(
            dataset_urn=args.dataset_urn,
            schedule_interval=args.interval,
            schedule_unit=args.unit,
            timezone=args.timezone,
            cron=args.cron,
            source_type=args.source_type,
            description=args.description
        )
        
        if not assertion_urn:
            logger.error("Failed to create freshness assertion")
            sys.exit(1)
        
        logger.info(f"Successfully created freshness assertion: {assertion_urn}")
        
        # Apply tags if specified
        if args.tags:
            for tag_urn in args.tags:
                logger.info(f"Adding tag {tag_urn} to assertion...")
                success = client.add_tag_to_assertion(assertion_urn, tag_urn)
                if success:
                    logger.info(f"Successfully added tag {tag_urn}")
                else:
                    logger.warning(f"Failed to add tag {tag_urn}")
        
        # Run the assertion if requested
        if args.run:
            logger.info("Running assertion...")
            result = client.run_assertion(assertion_urn)
            if result:
                logger.info(f"Assertion run result: {json.dumps(result, indent=2)}")
            else:
                logger.warning("Failed to run assertion")
        
        # Print the created assertion URN
        print(assertion_urn)
    
    except Exception as e:
        logger.error(f"Error creating freshness assertion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 