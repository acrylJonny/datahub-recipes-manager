#!/usr/bin/env python3
"""
Script to update the schedule of an existing DataHub ingestion recipe.
Uses the DataHub SDK (acryl-datahub) to update the schedule.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_api import DataHubClient


def update_ingestion_schedule(
    server: str, token: str, source_id: str, cron: str, timezone: str
) -> bool:
    """
    Update the schedule of a DataHub ingestion source using the SDK

    Args:
        server: DataHub GMS server URL
        token: DataHub authentication token
        source_id: Source ID to update
        cron: New cron expression
        timezone: New timezone

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create DataHub client
        client = DataHubClient(server, token)

        # Update the schedule
        success = client.update_ingestion_schedule(source_id, cron, timezone)

        if success:
            print(f"Schedule updated successfully for source: {source_id}")
            print(f"New schedule: {cron} (timezone: {timezone})")
        else:
            print(f"Failed to update schedule for source: {source_id}")

        return success

    except Exception as e:
        print(f"Error updating schedule: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Update DataHub ingestion schedule")
    parser.add_argument("--source-id", required=True, help="Source ID to update")
    parser.add_argument(
        "--cron", required=True, help="Cron expression for the schedule"
    )
    parser.add_argument("--timezone", default="UTC", help="Timezone for the schedule")
    parser.add_argument("--env-file", help="Path to .env file for secrets")

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        load_dotenv(args.env_file)

    # Get DataHub configuration from environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", ""),
    }

    if not datahub_config["server"] or not datahub_config["token"]:
        raise ValueError(
            "DATAHUB_GMS_URL and DATAHUB_TOKEN environment variables must be set"
        )

    # Update schedule
    result = update_ingestion_schedule(
        server=datahub_config["server"],
        token=datahub_config["token"],
        source_id=args.source_id,
        cron=args.cron,
        timezone=args.timezone,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
