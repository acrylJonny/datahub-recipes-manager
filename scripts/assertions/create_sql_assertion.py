#!/usr/bin/env python3
"""
Create a SQL assertion in DataHub.
This script creates a new custom SQL assertion for a dataset in DataHub.
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
    parser = argparse.ArgumentParser(description="Create a SQL assertion in DataHub")
    
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
        "--sql-statement",
        "--sql",
        required=True,
        help="SQL query statement to be evaluated",
    )
    
    parser.add_argument(
        "--operator",
        "-op",
        required=True,
        choices=["EQUAL_TO", "GREATER_THAN", "GREATER_THAN_OR_EQUAL_TO", "LESS_THAN", "LESS_THAN_OR_EQUAL_TO"],
        help="Comparison operator for the assertion",
    )
    
    parser.add_argument(
        "--value",
        required=True,
        help="Value to compare the SQL result against",
    )
    
    parser.add_argument(
        "--value-type",
        choices=["NUMBER", "STRING", "BOOLEAN"],
        default="NUMBER",
        help="Type of the value (default: NUMBER)",
    )
    
    parser.add_argument(
        "--timezone",
        "-tz",
        default="America/Los_Angeles",
        help="Timezone for evaluation (default: America/Los_Angeles)",
    )
    
    parser.add_argument(
        "--cron",
        default="0 */6 * * *",
        help="Cron expression for evaluation schedule (default: 0 */6 * * *)",
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
    
    parser.add_argument(
        "--from-file",
        help="Load SQL statement from file instead of command line",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)
    
    # Handle SQL statement from file
    sql_statement = args.sql_statement
    if args.from_file:
        try:
            with open(args.from_file, "r") as f:
                sql_statement = f.read().strip()
        except Exception as e:
            logger.error(f"Error reading SQL from file: {str(e)}")
            sys.exit(1)
    
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
        # Create SQL assertion
        logger.info(f"Creating SQL assertion for dataset {args.dataset_urn}...")
        
        assertion_urn = client.create_sql_assertion(
            dataset_urn=args.dataset_urn,
            sql_statement=sql_statement,
            operator=args.operator,
            value=args.value,
            value_type=args.value_type,
            timezone=args.timezone,
            cron=args.cron,
            description=args.description
        )
        
        if not assertion_urn:
            logger.error("Failed to create SQL assertion")
            sys.exit(1)
        
        logger.info(f"Successfully created SQL assertion: {assertion_urn}")
        
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
        logger.error(f"Error creating SQL assertion: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 