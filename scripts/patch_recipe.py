#!/usr/bin/env python

import os
import sys
import logging
import argparse

# Add the parent directory to the path so we can import the utils package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.datahub_rest_client import DataHubRestClient
from utils.recipe_util import load_recipe_instance

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Patch an existing DataHub ingestion source"
    )
    parser.add_argument(
        "--id", required=True, help="ID of the ingestion source to patch"
    )
    parser.add_argument(
        "--instance",
        help="Path to recipe instance YAML file with updated configuration",
    )
    parser.add_argument(
        "--schedule", help="New schedule cron expression (e.g. '0 0 * * *')"
    )
    parser.add_argument(
        "--run", action="store_true", help="Trigger an immediate run after patching"
    )

    args = parser.parse_args()

    if not args.instance and not args.schedule and not args.run:
        parser.error(
            "At least one of --instance, --schedule, or --run must be specified"
        )

    # Load environment variables
    from dotenv import load_dotenv

    load_dotenv()

    # Initialize DataHub client
    server_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
    token = os.getenv("DATAHUB_TOKEN")

    client = DataHubRestClient(server_url, token)

    # Test connection
    if not client.test_connection():
        logger.error("Failed to connect to DataHub")
        sys.exit(1)

    # Get current configuration
    source = client.get_ingestion_source(args.id)
    if not source:
        logger.error(f"Ingestion source with ID '{args.id}' not found")
        sys.exit(1)

    logger.info(f"Found ingestion source: {args.id}")

    # Prepare parameters for patch operation
    patch_params = {}

    # Get source URN
    source_urn = source.get("urn")
    if not source_urn:
        source_urn = f"urn:li:dataHubIngestionSource:{args.id}"

    # Update with instance file if provided
    if args.instance:
        try:
            recipe_instance = load_recipe_instance(args.instance)
            if recipe_instance:
                patch_params["recipe"] = recipe_instance
                logger.info(f"Loaded recipe instance from {args.instance}")
        except Exception as e:
            logger.error(f"Failed to load recipe instance: {str(e)}")
            sys.exit(1)

    # Update schedule if provided
    if args.schedule:
        patch_params["schedule_interval"] = args.schedule
        logger.info(f"Updated schedule to: {args.schedule}")

    # Patch the ingestion source
    updated_source = client.patch_ingestion_source(source_urn, **patch_params)
    if not updated_source:
        logger.error("Failed to patch ingestion source")
        sys.exit(1)

    logger.info(f"Successfully patched ingestion source: {args.id}")

    # Trigger immediate run if requested
    if args.run:
        execution_id = client.run_ingestion_source(args.id)
        if execution_id:
            logger.info(f"Triggered ingestion run with execution ID: {execution_id}")
        else:
            logger.error("Failed to trigger ingestion run")
            sys.exit(1)

    logger.info("Operation completed successfully")


if __name__ == "__main__":
    main()
