#!/usr/bin/env python3
"""
Script to pull DataHub ingestion recipes from a DataHub instance.
Uses direct REST API calls to retrieve ingestion sources and save them as YAML files.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging

import yaml
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient


def get_ingestion_sources(
    client: DataHubRestClient, repo_home: str, ids: List[str] = None
) -> Tuple[int, int]:
    """
    Get ingestion sources from DataHub

    Args:
        client: DataHub REST client
        repo_home: Path to repository home directory
        ids: List of specific ingestion source IDs to get

    Returns:
        Tuple of (success_count, total_sources)
    """
    if not ids:
        # Try to list all sources
        try:
            sources = client.list_ingestion_sources()
            if not sources:
                logging.warning("No ingestion sources found in DataHub")
                return 0, 0

            logging.info(f"Found {len(sources)} ingestion sources")

            # Process sources
            success_count = 0
            for source in sources:
                source_id = source.get("id", "unknown")

                # Convert source to YAML and save
                try:
                    logging.info(f"Converting source '{source_id}' to YAML format...")
                    yaml_data = convert_source_to_yaml(source, repo_home)

                    # Save to file
                    yaml_path = os.path.join(repo_home, f"{source_id}.yml")
                    save_yaml_file(yaml_data, yaml_path)
                    success_count += 1
                except Exception as e:
                    logging.error(f"Error processing source '{source_id}': {str(e)}")

            return success_count, len(sources)

        except Exception as e:
            logging.error(f"Failed to list ingestion sources: {str(e)}")
            logging.warning(
                "Will attempt to use fallback method if specific IDs are provided"
            )
            if not ids:
                return 0, 0

    # If we have specific IDs, try to get them directly
    if ids:
        # Filter out None values from the ids list
        ids = [id for id in ids if id is not None]
        if ids:
            logging.info(
                f"Will attempt to pull {len(ids)} specific ingestion source(s): {', '.join(ids)}"
            )
        else:
            logging.info("No valid source IDs provided")
            return 0, 0

    success_count = 0
    failed_ids = []

    for source_id in ids:
        try:
            logging.info(f"Fetching ingestion source '{source_id}'...")
            source = client.get_ingestion_source(source_id)

            if not source:
                logging.warning(
                    f"Source '{source_id}' not found using standard method, trying fallback..."
                )
                source = fallback_get_source(client, source_id)

            if not source:
                logging.error(
                    f"Failed to retrieve source '{source_id}' after all attempts."
                )
                failed_ids.append(source_id)
                continue

            # Convert source to YAML and save
            logging.info(f"Converting source '{source_id}' to YAML format...")
            yaml_data = convert_source_to_yaml(source, repo_home)

            # Save to file - use output directory directly
            yaml_path = os.path.join(repo_home, f"{source_id}.yml")
            save_yaml_file(yaml_data, yaml_path)
            logging.info(f"✅ Successfully saved recipe to {yaml_path}")
            success_count += 1

        except Exception as e:
            logging.error(f"Error processing source '{source_id}': {str(e)}")
            failed_ids.append(source_id)

    if failed_ids:
        logging.warning(
            f"Failed to pull {len(failed_ids)} sources: {', '.join(failed_ids)}"
        )

    return success_count, len(ids)


def fallback_get_source(client: DataHubRestClient, source_id: str) -> Dict:
    """
    Fallback method to get a source when the standard API methods fail.
    Tries various direct HTTP approaches.
    """
    logging.info(f"Starting fallback method to retrieve source '{source_id}'...")

    # Get the server URL from the client
    server_url = client.server_url.rstrip("/")
    headers = client.session.headers

    # Try different endpoint formats
    endpoints = [
        f"{server_url}/api/v2/ingestionsource/{source_id}",
        f"{server_url}/api/v2/ingestion/sources/{source_id}",
        f"{server_url}/api/v2/ingestion/source/{source_id}",
        f"{server_url}/openapi/v3/ingestion/sources/{source_id}",
        f"{server_url}/openapi/v3/ingestion/source/{source_id}",
    ]

    for endpoint in endpoints:
        try:
            logging.info(f"Trying fallback endpoint: {endpoint}")
            response = client.session.get(endpoint)

            if response.status_code == 200:
                logging.info(
                    f"✅ Successfully retrieved source using endpoint: {endpoint}"
                )
                try:
                    return response.json()
                except ValueError:
                    logging.warning(
                        f"Endpoint returned non-JSON response: {response.text[:100]}..."
                    )
                    continue
            else:
                logging.debug(
                    f"Endpoint {endpoint} returned status {response.status_code}: {response.text[:100]}..."
                )
        except Exception as e:
            logging.debug(f"Error accessing endpoint {endpoint}: {str(e)}")

    logging.error(f"All fallback methods failed to retrieve source '{source_id}'")
    return None


def convert_source_to_yaml(source: Dict[str, Any], repo_home: str) -> Dict[str, Any]:
    """
    Convert a DataHub ingestion source to YAML format

    Args:
        source: DataHub ingestion source
        repo_home: Path to the repository home directory

    Returns:
        Dictionary representation of the source that can be saved as YAML
    """
    # Extract source ID from URN
    source_id = source.get("id")
    if not source_id and "urn" in source:
        source_id = source["urn"].split(":")[-1]

    # Handle cases where config.recipe might be None or not a string
    recipe_json = {}
    if source.get("config", {}).get("recipe"):
        try:
            if isinstance(source["config"]["recipe"], dict):
                recipe_json = source["config"]["recipe"]
            else:
                recipe_json = json.loads(source["config"]["recipe"])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Warning: Could not parse recipe JSON for {source_id}: {str(e)}")
            recipe_json = {}
        except Exception as e:
            print(f"Error processing recipe for {source_id}: {str(e)}")
            recipe_json = {}
    # Also check for recipe directly on the source (for fallback case)
    elif source.get("recipe"):
        try:
            if isinstance(source["recipe"], dict):
                recipe_json = source["recipe"]
            else:
                recipe_json = json.loads(source["recipe"])
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Warning: Could not parse recipe JSON for {source_id}: {str(e)}")
            recipe_json = {}
        except Exception as e:
            print(f"Error processing recipe for {source_id}: {str(e)}")
            recipe_json = {}

    # Create YAML structure
    yaml_config = {
        "recipe_id": source_id,
        "recipe_type": source.get("type", "batch"),
        "description": f"Pulled from DataHub: {source.get('name', source_id)}",
        # Add source configuration from recipe
        "source": recipe_json.get("source", {}),
        # Add executor configuration
        "executor_id": source.get("config", {}).get("executorId", "default"),
        # Add schedule configuration
        "schedule": {
            "cron": source.get("schedule", {}).get("interval", "0 0 * * *"),
            "timezone": source.get("schedule", {}).get("timezone", "UTC"),
        },
        # Add debug mode
        "debug_mode": source.get("config", {}).get("debugMode", False),
    }

    return yaml_config


def save_yaml_file(data: Dict[str, Any], output_path: str):
    """
    Save data as YAML file

    Args:
        data: Data to save
        output_path: Path to save the file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Save YAML file
    with open(output_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"Saved YAML file: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Pull DataHub ingestion recipes")
    parser.add_argument(
        "--output-dir", required=True, help="Directory to save pulled recipes"
    )
    parser.add_argument("--source-id", help="Specific source ID to pull")
    parser.add_argument("--env-file", help="Path to .env file for secrets")

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        # Try to load from .env in the current directory
        load_dotenv()

    # Get DataHub configuration from environment variables
    datahub_config = {
        "server": os.environ.get("DATAHUB_GMS_URL", ""),
        "token": os.environ.get("DATAHUB_TOKEN", ""),
    }

    if not datahub_config["server"]:
        raise ValueError("DATAHUB_GMS_URL environment variable must be set")

    # Empty or default token is treated as not provided
    if (
        not datahub_config["token"]
        or datahub_config["token"] == "your_datahub_pat_token_here"
    ):
        datahub_config["token"] = None
        logging.warning(
            "DATAHUB_TOKEN is not set. Will attempt to connect without authentication."
        )
        logging.warning(
            "This will only work if your DataHub instance doesn't require authentication."
        )

    # Create DataHub client
    client = DataHubRestClient(
        server_url=datahub_config["server"], token=datahub_config["token"]
    )

    # Get source IDs based on arguments
    source_ids = None
    if args.source_id:
        source_ids = [args.source_id]

    # Pull sources
    success_count, total_sources = get_ingestion_sources(
        client, args.output_dir, source_ids
    )

    if success_count > 0:
        logging.info(
            f"Successfully pulled {success_count} of {total_sources} ingestion sources."
        )
        return 0
    elif total_sources > 0:
        logging.error(f"Failed to pull any of the {total_sources} ingestion sources.")
        return 1
    else:
        logging.warning(
            "No ingestion sources were pulled. This may be normal if no sources exist."
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())
