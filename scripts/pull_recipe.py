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
from typing import Dict, Any, List, Optional

import yaml
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.datahub_rest_client import DataHubRestClient


def get_ingestion_sources(server: str, token: Optional[str] = None, source_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve ingestion sources from DataHub using the SDK

    Args:
        server: DataHub GMS server URL
        token: DataHub authentication token (optional)
        source_id: Optional source ID to retrieve a specific source

    Returns:
        List of ingestion sources
    """
    try:
        # Create DataHub client with or without token
        client = DataHubRestClient(server, token)

        if source_id:
            # Get a specific source
            source = client.get_ingestion_source(source_id)
            return [source] if source else []
        else:
            # Get all sources
            return client.list_ingestion_sources()

    except Exception as e:
        print(f"Error retrieving ingestion sources: {str(e)}")
        raise


def convert_source_to_yaml(source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a DataHub ingestion source to YAML format

    Args:
        source: DataHub ingestion source

    Returns:
        Dictionary representation of the source that can be saved as YAML
    """
    # Extract source ID from URN
    source_id = source["urn"].split(":")[-1]

    # Parse recipe JSON
    recipe_json = json.loads(source["config"]["recipe"])

    # Create YAML structure
    yaml_config = {
        "recipe_id": source_id,
        "recipe_type": source["type"],
        "description": f"Pulled from DataHub: {source['name']}",

        # Add source configuration from recipe
        "source": recipe_json.get("source", {}),

        # Add executor configuration
        "executor_id": source["config"]["executorId"],

        # Add schedule configuration
        "schedule": {
            "cron": source["schedule"]["interval"],
            "timezone": source["schedule"]["timezone"]
        },

        # Add debug mode
        "debug_mode": source["config"]["debugMode"]
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
    with open(output_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"Saved YAML file: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Pull DataHub ingestion recipes")
    parser.add_argument('--output-dir', required=True, help='Directory to save pulled recipes')
    parser.add_argument('--source-id', help='Specific source ID to pull')
    parser.add_argument('--env-file', help='Path to .env file for secrets')

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
        "token": os.environ.get("DATAHUB_TOKEN", "")
    }

    if not datahub_config["server"]:
        raise ValueError("DATAHUB_GMS_URL environment variable must be set")
    
    # Empty or default token is treated as not provided
    if not datahub_config["token"] or datahub_config["token"] == "your_datahub_pat_token_here":
        print("Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication.")
        print("This will only work if your DataHub instance doesn't require authentication.")
        datahub_config["token"] = None

    # Get ingestion sources
    sources = get_ingestion_sources(
        server=datahub_config["server"],
        token=datahub_config["token"],
        source_id=args.source_id
    )

    print(f"Retrieved {len(sources)} ingestion sources")

    # Convert and save sources as YAML files
    output_dir = Path(args.output_dir)
    for source in sources:
        yaml_config = convert_source_to_yaml(source)

        # Generate output path
        source_id = yaml_config["recipe_id"]
        source_type = yaml_config["recipe_type"]
        output_path = output_dir / f"{source_type}-{source_id}.yml"

        # Save YAML file
        save_yaml_file(yaml_config, str(output_path))

    return 0


if __name__ == "__main__":
    sys.exit(main())