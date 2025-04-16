#!/usr/bin/env python3
"""
Test script for pushing a real recipe with content.
"""

import sys
import json
import logging
import os
from utils.datahub_rest_client import DataHubRestClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables
DATAHUB_URL = os.environ.get("DATAHUB_URL", "http://localhost:8080")
DATAHUB_TOKEN = os.environ.get("DATAHUB_TOKEN", "")

def test_recipe_handling():
    # Connect to DataHub
    client = DataHubRestClient(
        server_url=DATAHUB_URL,
        token=DATAHUB_TOKEN
    )
    logger.info(f"Connected to DataHub at {DATAHUB_URL}")

    # Sample recipe for testing
    recipe = {
        "source": {
            "type": "postgres",
            "config": {
                "host": "localhost",
                "port": 5432,
                "database": "postgres",
                "username": "postgres",
                "password": "postgres",
                "table_pattern": {"allow": ["public.%"]},
                "schema_pattern": {"allow": ["public"]}
            }
        },
        "sink": {
            "type": "datahub-rest",
            "config": {
                "server": "${DATAHUB_GMS_URL}",
                "token": "${DATAHUB_GMS_TOKEN}"
            }
        },
        "pipeline_name": "test-postgres-pipeline",
        "pipeline_version": "0.1",
        "source_to_platform_map": {"postgres": "postgres"},
        "transformers": [],
        "pipeline_config": {
            "run_id": "test-run",
            "profiling": {
                "enabled": True
            }
        }
    }

    # Create or update a source
    source_id = "test-postgres-source"
    source_name = "Test Postgres Source"
    
    # Try to get the source first to check if it exists
    existing_source = client.get_ingestion_source(source_id)
    
    if existing_source:
        logger.info(f"Source '{source_id}' already exists, fetched existing source")
        logger.debug(f"Existing source: {json.dumps(existing_source, indent=2)}")
        
        # Save original recipe if available for later comparison
        try:
            original_recipe_str = existing_source.get("config", {}).get("recipe", "{}")
            original_recipe = json.loads(original_recipe_str) if isinstance(original_recipe_str, str) else original_recipe_str
            logger.info(f"Original recipe loaded: {json.dumps(original_recipe.get('pipeline_config', {}), indent=2)}")
        except Exception as e:
            logger.warning(f"Could not parse original recipe: {e}")
            original_recipe = None
    else:
        logger.info(f"Source '{source_id}' does not exist, will create")
        original_recipe = None
    
    # Create/update the source with the recipe
    result = client.create_ingestion_source(
        name=source_name,
        type="postgres",
        recipe=recipe,
        source_id=source_id,
        schedule={"interval": "0 0 * * *", "timezone": "UTC"},
        executor_id="default",
        debug_mode=False
    )
    logger.info(f"Create/update result: {result}")
    
    # Verify source creation by retrieving it
    source = client.get_ingestion_source(source_id)
    if not source:
        logger.error(f"Failed to retrieve source {source_id} after creation")
        return
    
    logger.info(f"Successfully retrieved source: {source_id}")
    
    # Get the current recipe to use for patching
    try:
        current_recipe_str = source.get("config", {}).get("recipe", "{}")
        current_recipe = json.loads(current_recipe_str) if isinstance(current_recipe_str, str) else current_recipe_str
        logger.info(f"Current recipe loaded for patching: {json.dumps(current_recipe.get('pipeline_config', {}), indent=2)}")
    except Exception as e:
        logger.error(f"Could not parse current recipe for patching: {e}")
        return
    
    # Now patch the recipe with some updates based on the current recipe
    logger.info("Patching the recipe with updates...")
    
    # Start with the current recipe and modify it
    patch_recipe = current_recipe.copy() if current_recipe else recipe.copy()
    
    # Ensure the necessary keys exist
    if "pipeline_config" not in patch_recipe:
        patch_recipe["pipeline_config"] = {}
    
    if "profiling" not in patch_recipe["pipeline_config"]:
        patch_recipe["pipeline_config"]["profiling"] = {"enabled": False}
    else:
        # Change profiling to the opposite of what it currently is
        current_profiling = patch_recipe["pipeline_config"]["profiling"].get("enabled", True)
        patch_recipe["pipeline_config"]["profiling"]["enabled"] = not current_profiling
    
    # Add stateful ingestion if it doesn't exist
    if "stateful_ingestion" not in patch_recipe["pipeline_config"]:
        patch_recipe["pipeline_config"]["stateful_ingestion"] = {
            "enabled": True,
            "state_provider": {
                "type": "datahub",
                "config": {}
            }
        }
    else:
        # Toggle stateful ingestion
        current_stateful = patch_recipe["pipeline_config"]["stateful_ingestion"].get("enabled", False)
        patch_recipe["pipeline_config"]["stateful_ingestion"]["enabled"] = not current_stateful
    
    # First try with source_id directly (not URN) to test that part of the code
    logger.info(f"Testing patch with source_id (not URN): {source_id}")
    patch_result = client.patch_ingestion_source(
        urn=source_id,  # Deliberately use source_id instead of URN to test the code
        name=source_name,
        recipe=patch_recipe
    )
    
    logger.info(f"Patch result with source_id: {json.dumps(patch_result, indent=2) if patch_result else 'None'}")
    
    # Verify the patch worked by retrieving the source again
    updated_source = client.get_ingestion_source(source_id)
    if not updated_source:
        logger.error(f"Failed to retrieve updated source {source_id}")
        return
    
    logger.info(f"Successfully retrieved updated source: {updated_source.get('name')}")
    
    # Now test forcing the REST API fallback by deliberately using a malformed GraphQL mutation
    # We'll do this by monkeypatching the execute_graphql method to raise an exception
    original_execute_graphql = client.execute_graphql
    
    def force_graphql_error(*args, **kwargs):
        logger.info("Forcing GraphQL error to test REST API fallback")
        raise Exception("Forced GraphQL error to test REST API fallback")
    
    # Replace the method to cause GraphQL to fail
    client.execute_graphql = force_graphql_error
    
    # Now patch again to force the REST API fallback
    logger.info("Testing REST API fallback by forcing GraphQL to fail")
    rest_patch_result = client.patch_ingestion_source(
        urn=source_id,
        name=f"{source_name} Updated",
        recipe=patch_recipe,
        debug_mode=True  # Change debug mode to test the update
    )
    
    # Restore the original method
    client.execute_graphql = original_execute_graphql
    
    logger.info(f"REST API fallback patch result: {json.dumps(rest_patch_result, indent=2) if rest_patch_result else 'None'}")
    
    # Verify the fallback patch worked
    final_source = client.get_ingestion_source(source_id)
    if not final_source:
        logger.error(f"Failed to retrieve source after REST fallback {source_id}")
        return
        
    logger.info(f"Final source name: {final_source.get('name')}")
    logger.info(f"Final debug_mode: {final_source.get('config', {}).get('debugMode')}")
    
    # Extract and log the updated recipe to verify changes
    try:
        updated_recipe_str = final_source.get("config", {}).get("recipe", "{}")
        updated_recipe = json.loads(updated_recipe_str) if isinstance(updated_recipe_str, str) else updated_recipe_str
        
        # Log the original and updated values to clearly see the changes
        logger.info("=== Recipe Change Verification ===")
        
        # Profiling change
        initial_profiling = recipe.get("pipeline_config", {}).get("profiling", {}).get("enabled", "N/A")
        updated_profiling = updated_recipe.get("pipeline_config", {}).get("profiling", {}).get("enabled", "N/A")
        logger.info(f"Profiling enabled: {initial_profiling} -> {updated_profiling}")
        
        # Stateful ingestion change
        initial_stateful = recipe.get("pipeline_config", {}).get("stateful_ingestion", {}).get("enabled", "N/A")
        updated_stateful = updated_recipe.get("pipeline_config", {}).get("stateful_ingestion", {}).get("enabled", "N/A")
        logger.info(f"Stateful ingestion enabled: {initial_stateful} -> {updated_stateful}")
        
    except json.JSONDecodeError:
        logger.error("Could not parse updated recipe")
    
    logger.info("Test completed successfully")

if __name__ == "__main__":
    test_recipe_handling() 