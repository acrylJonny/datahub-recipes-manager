import copy
import logging
import os
from typing import Any, Dict

import yaml

try:
    from datahub_cicd_client.integrations.docker_utils import (
        should_apply_docker_networking,
        update_connection_params,
    )

    DOCKER_UTILS_AVAILABLE = True
except ImportError:
    DOCKER_UTILS_AVAILABLE = False

logger = logging.getLogger(__name__)


def load_recipe_instance(file_path):
    """
    Load a recipe instance from a YAML file.

    Args:
        file_path (str): Path to the recipe instance YAML file

    Returns:
        dict: The recipe instance configuration

    Raises:
        FileNotFoundError: If the file doesn't exist
        yaml.YAMLError: If the file is not valid YAML
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Recipe instance file not found: {file_path}")

    try:
        with open(file_path) as f:
            instance = yaml.safe_load(f)
            return instance
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading recipe instance: {e}")
        raise


def save_recipe_instance(instance, file_path):
    """
    Save a recipe instance to a YAML file.

    Args:
        instance (dict): The recipe instance configuration
        file_path (str): Path where to save the recipe instance YAML file

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            yaml.dump(instance, f, default_flow_style=False, sort_keys=False)
        return True
    except Exception as e:
        logger.error(f"Error saving recipe instance: {e}")
        return False


def apply_docker_networking(recipe: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply Docker networking adjustments to recipe parameters
    to ensure database connections work in Docker environment.
    Only applies in testing environments.

    Args:
        recipe: Recipe configuration dictionary

    Returns:
        Updated recipe configuration
    """
    if not DOCKER_UTILS_AVAILABLE:
        logger.warning("Docker utilities not available. Network adaptations will not be applied.")
        return recipe

    # Make a deep copy to avoid modifying the original
    updated_recipe = copy.deepcopy(recipe)

    # Check if we need to apply Docker networking (only in testing environments)
    if not should_apply_docker_networking():
        logger.debug("Docker networking not applied (not in testing environment)")
        return recipe

    logger.info("Testing environment with Docker detected, applying networking adaptations...")

    # Handle source configuration based on source type
    if "source" in updated_recipe and "config" in updated_recipe["source"]:
        source_config = updated_recipe["source"]["config"]
        source_type = updated_recipe["source"].get("type", "").lower()

        # Database sources with host/port parameters
        if source_type in [
            "postgres",
            "postgresql",
            "mysql",
            "mssql",
            "oracle",
            "db2",
            "snowflake",
        ]:
            if isinstance(source_config, dict) and "host" in source_config:
                source_config = update_connection_params(source_config)
                updated_recipe["source"]["config"] = source_config
                logger.info(
                    f"Updated {source_type} connection parameters for Docker: {source_config.get('host')}:{source_config.get('port')}"
                )

    # Return the updated recipe
    return updated_recipe
