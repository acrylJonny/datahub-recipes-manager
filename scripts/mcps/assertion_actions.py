#!/usr/bin/env python3
"""
Assertion actions for DataHub assertions
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

# Set up logging
logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def add_assertion_to_staged_changes(
    assertion_id: str,
    assertion_urn: str,
    assertion_name: str,
    assertion_type: str = "CUSTOM",
    description: Optional[str] = None,
    entity_urn: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: Optional[str] = None,
    mutation_name: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Add an assertion to staged changes by creating individual JSON files
    
    Args:
        assertion_id: Assertion ID (used for filename)
        assertion_urn: Assertion URN
        assertion_name: Assertion name
        assertion_type: Assertion type (CUSTOM, FIELD, VOLUME, etc.)
        description: Assertion description
        entity_urn: Entity URN this assertion applies to (assertee)
        config: Assertion configuration/definition
        environment: Environment name
        owner: Owner username
        base_dir: Base directory for output files
        mutation_name: Mutation name for deterministic URN generation
        **kwargs: Additional arguments
    
    Returns:
        Dictionary with success status and file information
    """
    try:
        setup_logging()
        
        # Import URN utilities with graceful fallback
        try:
            from utils.urn_utils import (
                get_mutation_config_for_environment,
                generate_mutated_urn,
                apply_urn_mutations_to_associations,
                get_entity_type_from_urn
            )
            urn_utils_available = True
        except ImportError:
            urn_utils_available = False
            logger.warning("URN utilities not available - using original URNs")
        
        # Get mutation configuration if URN utilities are available
        mutation_config = None
        if urn_utils_available and (environment or mutation_name):
            env_name = environment or mutation_name
            mutation_config = get_mutation_config_for_environment(env_name)
        
        # Apply URN mutations to the entity_urn (assertee) if mutations are configured
        mutated_entity_urn = entity_urn
        if urn_utils_available and mutation_config and entity_urn and (environment or mutation_name):
            env_name = environment or mutation_name
            # Determine the entity type from the URN
            entity_type = get_entity_type_from_urn(entity_urn)
            if entity_type:
                mutated_entity_urn = generate_mutated_urn(entity_urn, env_name, entity_type, mutation_config)
                logger.info(f"Mutated entity URN from {entity_urn} to {mutated_entity_urn}")
            else:
                logger.warning(f"Could not determine entity type for URN: {entity_urn}")
        
        # Apply URN mutations to any URNs in the config as well
        mutated_config = config.copy() if config else {}
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            mutated_config = apply_urn_mutations_to_assertion_config(
                mutated_config, env_name, mutation_config
            )
        
        # Determine the assertions directory path
        if base_dir:
            assertions_dir = Path(base_dir) / environment / "assertions"
        else:
            # When running from web_ui/, we need to go up one level to get to repo root
            current_dir = Path(os.getcwd())
            if current_dir.name == "web_ui":
                repo_root = current_dir.parent
            else:
                # Try to find repo root by looking for characteristic files
                repo_root = current_dir
                for _ in range(5):  # Search up to 5 levels
                    if (repo_root / "README.md").exists() and (repo_root / "scripts").exists() and (repo_root / "web_ui").exists():
                        break
                    repo_root = repo_root.parent
                else:
                    # Fallback: assume current directory
                    repo_root = current_dir
            
            assertions_dir = repo_root / "metadata-manager" / environment / "assertions"
        
        # Create directory if it doesn't exist
        assertions_dir.mkdir(parents=True, exist_ok=True)
        
        # Create assertion file path using the assertion ID
        assertion_file_path = assertions_dir / f"{assertion_id}.json"
        
        # Extract platform instance and browse path from entity URN if available
        platform_instance = None
        browse_path = None
        if mutated_entity_urn:
            try:
                # Try to extract platform instance from URN structure
                from web_ui.metadata_manager.views_tests import extract_platform_instance_from_urn
                platform_name, platform_instance, environment_part = extract_platform_instance_from_urn(mutated_entity_urn)
                logger.debug(f"Extracted platform instance: {platform_instance} from URN: {mutated_entity_urn}")
            except Exception as e:
                logger.warning(f"Failed to extract platform instance from URN {mutated_entity_urn}: {str(e)}")
        
        # Create assertion JSON structure in the format expected by the workflow
        # The workflow expects 'operation', 'assertion_type', 'name', and 'graphql_input'
        assertion_data = {
            "id": assertion_id,
            "urn": assertion_urn,
            "name": assertion_name,
            "operation": "create",  # or "update" if updating existing
            "assertion_type": assertion_type,
            "description": description or "",
            "entity_urn": mutated_entity_urn,  # Use mutated entity URN
            "platform_instance": platform_instance,  # Store extracted platform instance
            "browse_path": browse_path,  # Store browse path (may be None for now)
            "config": mutated_config,  # Use mutated config with updated URNs
            "owner": owner,
            "environment": environment,
            "created_at": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
            "last_modified_by": owner,
            # GraphQL input structure expected by the workflow
            "graphql_input": {
                "mutation": _get_mutation_for_assertion_type(assertion_type),
                "input": _build_graphql_input(
                    assertion_urn=assertion_urn,
                    assertion_name=assertion_name,
                    assertion_type=assertion_type,
                    description=description,
                    entity_urn=mutated_entity_urn,
                    config=mutated_config
                )
            }
        }
        
        # Write assertion JSON file
        with open(assertion_file_path, 'w') as f:
            json.dump(assertion_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Created assertion file: {assertion_file_path}")
        
        return {
            "success": True,
            "message": f"Assertion '{assertion_name}' added to staged changes",
            "files_saved": [str(assertion_file_path)],
            "files_created": [str(assertion_file_path.name)],
            "files_created_count": 1,
            "mcps_created": 1,  # For compatibility with views expecting this field
            "assertion_id": assertion_id,
            "assertion_urn": assertion_urn,
            "mutated_entity_urn": mutated_entity_urn
        }
        
    except Exception as e:
        logger.error(f"Error adding assertion to staged changes: {str(e)}")
        return {
            "success": False,
            "message": f"Error adding assertion to staged changes: {str(e)}",
            "files_saved": [],
            "files_created": [],
            "files_created_count": 0,
            "mcps_created": 0  # For compatibility with views expecting this field
        }


def apply_urn_mutations_to_assertion_config(
    assertion_config: Dict[str, Any], 
    environment_name: str,
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Apply URN mutations to assertion configuration recursively
    
    Args:
        assertion_config: Assertion configuration dictionary
        environment_name: Environment name for mutations
        mutation_config: Mutation configuration
    
    Returns:
        Updated assertion configuration with mutated URNs
    """
    if not assertion_config or not mutation_config:
        return assertion_config
    
    return _apply_mutations_recursively(assertion_config, environment_name, mutation_config)


def _apply_mutations_recursively(
    obj: Any, 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Any:
    """
    Recursively apply URN mutations to any URNs found in the object
    
    Args:
        obj: Object to process (dict, list, or primitive)
        environment_name: Environment name for mutations
        mutation_config: Mutation configuration
    
    Returns:
        Object with mutated URNs
    """
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            # Check if this key typically contains URNs
            if key in ['entity_urn', 'dataset_urn', 'tag_urn', 'term_urn', 'domain_urn', 'urn'] and isinstance(value, str):
                result[key] = _mutate_urn_if_applicable(value, environment_name, mutation_config)
            else:
                result[key] = _apply_mutations_recursively(value, environment_name, mutation_config)
        return result
    elif isinstance(obj, list):
        return [_apply_mutations_recursively(item, environment_name, mutation_config) for item in obj]
    elif isinstance(obj, str) and obj.startswith('urn:li:'):
        # This is a URN, try to mutate it
        return _mutate_urn_if_applicable(obj, environment_name, mutation_config)
    else:
        # Primitive value, return as-is
        return obj


def _mutate_urn_if_applicable(
    urn: str, 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> str:
    """
    Mutate a URN if it matches the mutation configuration
    
    Args:
        urn: URN to potentially mutate
        environment_name: Environment name for mutations
        mutation_config: Mutation configuration
    
    Returns:
        Mutated URN or original URN if no mutation applies
    """
    try:
        from utils.urn_utils import generate_mutated_urn, get_entity_type_from_urn
        entity_type = get_entity_type_from_urn(urn)
        if entity_type:
            return generate_mutated_urn(urn, environment_name, entity_type, mutation_config)
        else:
            logger.warning(f"Could not determine entity type for URN: {urn}")
            return urn
    except ImportError:
        logger.warning("URN utilities not available for mutation")
        return urn
    except Exception as e:
        logger.warning(f"Could not mutate URN {urn}: {e}")
        return urn


def _get_mutation_for_assertion_type(assertion_type: str) -> str:
    """
    Get the appropriate GraphQL mutation name for the assertion type
    
    Args:
        assertion_type: Type of assertion (CUSTOM, FIELD, VOLUME, etc.)
    
    Returns:
        GraphQL mutation name
    """
    type_to_mutation = {
        "CUSTOM": "upsertCustomAssertion",
        "FIELD": "createFieldAssertion", 
        "VOLUME": "createVolumeAssertion",
        "FRESHNESS": "createFreshnessAssertion",
        "SQL": "createSqlAssertion",
        "DATASET": "createDatasetAssertion"
    }
    
    return type_to_mutation.get(assertion_type.upper(), "upsertCustomAssertion")


def _build_graphql_input(
    assertion_urn: str,
    assertion_name: str,
    assertion_type: str,
    description: Optional[str],
    entity_urn: str,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Build GraphQL input structure for assertion creation
    
    Args:
        assertion_urn: Assertion URN
        assertion_name: Assertion name
        assertion_type: Assertion type
        description: Assertion description
        entity_urn: Entity URN (assertee)
        config: Assertion configuration
    
    Returns:
        GraphQL input dictionary
    """
    base_input = {
        "entityUrn": entity_urn,
        "type": assertion_type.upper(),
        "description": description or "",
    }
    
    # For custom assertions, include the URN and custom input
    if assertion_type.upper() == "CUSTOM":
        base_input.update({
            "urn": assertion_urn,
            "input": {
                "entityUrn": entity_urn,
                "type": "CUSTOM",
                "description": description or "",
                "logic": config.get("logic", ""),
                "source": config.get("source", {}),
                "customProperties": config.get("customProperties", {}),
                "externalUrl": config.get("externalUrl", "")
            }
        })
    else:
        # For other assertion types, merge config into base input
        base_input.update(config)
    
    return base_input 