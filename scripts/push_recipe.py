#!/usr/bin/env python3
"""
Script to push DataHub ingestion recipes to a DataHub instance.
Uses direct REST API calls to create or update ingestion sources.
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml
from dotenv import load_dotenv

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.template_renderer import render_template
from utils.datahub_rest_client import DataHubRestClient
from utils.recipe_util import apply_docker_networking


def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """Load YAML file and return as dictionary."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def resolve_secrets(config: Dict[str, Any], env_vars: Dict[str, str]) -> Dict[str, Any]:
    """
    Resolve secrets referenced in the config from environment variables.
    """

    def _resolve_value(value):
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            if env_var in env_vars:
                return env_vars[env_var]
            else:
                raise ValueError(f"Environment variable {env_var} not found")
        elif isinstance(value, dict):
            return {k: _resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [_resolve_value(v) for v in value]
        else:
            return value

    return {k: _resolve_value(v) for k, v in config.items()}


def create_datahub_recipe(
    recipe_config: Dict[str, Any],
    datahub_config: Dict[str, str],
    source_id: Optional[str] = None,
    run_ingestion: bool = False,
    create_secrets: bool = False,
    secret_references: Optional[List[str]] = None,
) -> Dict:
    """
    Create a DataHub ingestion recipe

    Args:
        recipe_config: The ingestion recipe configuration
        datahub_config: DataHub connection configuration
        source_id: Optional source ID
        run_ingestion: Whether to trigger immediate ingestion
        create_secrets: Whether to create secrets in DataHub
        secret_references: List of secret references to create

    Returns:
        Dictionary with source information
    """
    # Extract recipe information
    source_type = recipe_config.get("source", {}).get("type", "unknown")

    # Extract recipe metadata
    pipeline_name = (
        recipe_config.get("source", {})
        .get("config", {})
        .get("name", f"DataHub {source_type.capitalize()} Ingestion")
    )

    # Extract scheduling information
    schedule_interval = recipe_config.get("schedule", {}).get("interval", "0 0 * * *")
    timezone = recipe_config.get("schedule", {}).get("timezone", "UTC")

    # Extract execution configuration
    executor_id = recipe_config.get("executorId", "default")
    debug_mode = recipe_config.get("debug_mode", False)
    extra_args = recipe_config.get("extraArgs", {})

    # Get the recipe content
    recipe = recipe_config

    # Apply Docker networking if needed (will only make changes in Docker environment)
    recipe = apply_docker_networking(recipe)

    try:
        # Create DataHub client with or without token
        if datahub_config["token"]:
            client = DataHubRestClient(
                datahub_config["server"], datahub_config["token"]
            )
        else:
            client = DataHubRestClient(datahub_config["server"])

        # Create secrets if requested
        if create_secrets and secret_references:
            print("Creating secrets in DataHub...")
            secret_created = False
            failed_secrets = []

            # Filter out DataHub connection credentials - these shouldn't be stored as secrets
            excluded_secrets = ["DATAHUB_GMS_URL", "DATAHUB_TOKEN"]
            filtered_secrets = [
                s for s in secret_references if s not in excluded_secrets
            ]

            if not filtered_secrets:
                print("⚠️ No valid secrets to create. Skipping secret creation.")
                print(
                    "   Note: DATAHUB_GMS_URL and DATAHUB_TOKEN are automatically excluded as they are"
                )
                print("   connection parameters, not ingestion secrets.")

            for secret_name in filtered_secrets:
                secret_value = os.environ.get(secret_name)
                if secret_value:
                    success = client.create_secret(secret_name, secret_value)
                    if success:
                        print(f"✅ Secret {secret_name} created successfully")
                        secret_created = True
                    else:
                        print(f"⚠️ Failed to create secret {secret_name} in DataHub")
                        failed_secrets.append(secret_name)
                else:
                    print(f"⚠️ Skipping secret {secret_name} - not found in environment")

            if not secret_created:
                print(
                    "\n⚠️ No secrets could be created in DataHub. This may be because:"
                )
                print("  - Your DataHub instance doesn't have the secrets API enabled")
                print(
                    "  - The API endpoints attempted are not available in your DataHub version"
                )
                print("  - You don't have sufficient permissions to create secrets\n")
                print("The recipe will still be pushed using environment variables.\n")

        # Create the ingestion source
        ingestion_source = {
            "id": source_id,
            "name": pipeline_name,
            "type": source_type,
            "config": {
                "recipe": json.dumps(recipe),
                "executorId": executor_id,
                "debugMode": debug_mode,
                "version": "0.8.42",
            },
            "schedule": {"interval": schedule_interval, "timezone": timezone},
        }

        if extra_args:
            ingestion_source["config"]["extraArgs"] = extra_args

        source_result = client.create_ingestion_source(ingestion_source)

        if not source_result:
            raise Exception(
                "Failed to create ingestion source. Check logs for details."
            )

        # Handle different possible response formats
        source_urn = None
        if isinstance(source_result, dict):
            source_urn = source_result.get("urn")
        elif (
            isinstance(source_result, list) and len(source_result) > 0 and isinstance(source_result[0], dict)
        ):
            source_urn = source_result[0].get("urn")

        if not source_urn:
            source_urn = f"urn:li:dataHubIngestionSource:{source_id}"

        print("Recipe created/updated successfully.")
        print(f"Source URN: {source_urn}")
        print(
            f"Scheduled to run with cron expression: {schedule_interval} in timezone: {timezone}"
        )
        print(f"Using executor: {executor_id}")

        # Trigger immediate ingestion if requested
        if run_ingestion:
            ingestion_success = client.trigger_ingestion(source_id)
            if ingestion_success:
                print(f"Ingestion triggered successfully for source: {source_urn}")
            else:
                print(f"Failed to trigger ingestion for source: {source_urn}")

            source_result["ingestion_triggered"] = ingestion_success

        return source_result

    except Exception as e:
        print(f"Error creating/updating recipe: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Push DataHub ingestion recipes")
    parser.add_argument(
        "--instance", required=True, help="Path to the instance YAML file"
    )
    parser.add_argument("--env-file", help="Path to .env file for secrets")
    parser.add_argument(
        "--run-ingestion", action="store_true", help="Trigger immediate ingestion"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update if recipe exists"
    )
    parser.add_argument(
        "--create-secrets", action="store_true", help="Create secrets in DataHub"
    )

    args = parser.parse_args()

    # Load environment variables
    if args.env_file:
        load_dotenv(args.env_file)
    else:
        # Try to load from .env in the current directory
        load_dotenv()

    # Load instance configuration
    instance_config = load_yaml_file(args.instance)

    # Get template path
    template_type = instance_config.get("recipe_type")
    if not template_type:
        raise ValueError("Recipe type not specified in instance configuration")

    base_dir = Path(__file__).parent.parent
    template_path = base_dir / "recipes" / "templates" / f"{template_type}.yml"

    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Render template with parameters
    parameters = instance_config.get("parameters", {})
    rendered_template = render_template(str(template_path), parameters)

    # Check for required environment variables
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
        print(
            "Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication."
        )
        print(
            "This will only work if your DataHub instance doesn't require authentication."
        )
        datahub_config["token"] = None

    # Check for secret references and ensure they're in the environment
    secret_refs = instance_config.get("secret_references", [])
    missing_secrets = []

    for secret in secret_refs:
        if not os.environ.get(secret):
            missing_secrets.append(secret)

    if missing_secrets:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_secrets)}"
        )

    # Create source ID based on recipe_id if available
    source_id = instance_config.get("recipe_id", str(uuid.uuid4()))

    # Push recipe to DataHub using the SDK
    create_datahub_recipe(
        recipe_config=rendered_template,
        datahub_config=datahub_config,
        source_id=source_id,
        run_ingestion=args.run_ingestion,
        create_secrets=args.create_secrets,
        secret_references=secret_refs if args.create_secrets else None,
    )

    print(f"Recipe pushed successfully with ID: {source_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
