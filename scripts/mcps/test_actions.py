#!/usr/bin/env python3
"""
Test actions for DataHub metadata tests
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

def add_test_to_staged_changes(
    test_id: str,
    test_urn: str,
    test_name: str,
    test_type: str = "CUSTOM",
    description: Optional[str] = None,
    category: Optional[str] = None,
    entity_urn: Optional[str] = None,
    definition: Optional[Dict[str, Any]] = None,
    yaml_definition: Optional[str] = None,
    platform: Optional[str] = None,
    platform_instance: Optional[str] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: Optional[str] = None,
    mutation_name: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Add a test to staged changes by creating MCP files
    
    Args:
        test_id: Test ID
        test_urn: Test URN
        test_name: Test name
        test_type: Test type (CUSTOM, FRESHNESS, etc.)
        description: Test description
        category: Test category
        entity_urn: Entity URN this test applies to
        definition: Test definition as dict
        yaml_definition: Test definition as YAML string
        platform: Platform name
        platform_instance: Platform instance
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
                apply_urn_mutations_to_associations
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
        
        # Apply URN mutations to the test definition if mutations are configured
        mutated_definition = definition.copy() if definition else {}
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            mutated_definition = apply_urn_mutations_to_test_definition(
                mutated_definition, env_name, mutation_config
            )
        
        # Determine the metadata_tests directory path
        if base_dir:
            metadata_tests_dir = Path(base_dir) / environment / "metadata_tests"
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
            
            metadata_tests_dir = repo_root / "metadata-manager" / environment / "metadata_tests"
        
        # Create directory if it doesn't exist
        metadata_tests_dir.mkdir(parents=True, exist_ok=True)
        
        # Create MCP file path
        mcp_file_path = metadata_tests_dir / "mcp_file.json"
        
        # Load existing MCPs or create empty list
        existing_mcps = []
        if mcp_file_path.exists():
            try:
                with open(mcp_file_path, 'r') as f:
                    existing_mcps = json.load(f)
                if not isinstance(existing_mcps, list):
                    existing_mcps = []
            except Exception as e:
                logger.warning(f"Could not load existing MCP file: {e}")
                existing_mcps = []
        
        # Create test MCPs
        new_mcps = []
        
        # 1. Test Definition MCP
        test_definition_mcp = {
            "entityType": "test",
            "entityUrn": test_urn,
            "changeType": "UPSERT",
            "aspectName": "testDefinition",
            "aspect": {
                "value": json.dumps({
                    "name": test_name,
                    "description": description or "",
                    "category": category or test_type,
                    "type": test_type,
                    "definition": mutated_definition,  # Use mutated definition with updated URNs
                    "yaml_definition": yaml_definition or "",
                    "entity_urn": entity_urn,
                    "platform": platform,
                    "platform_instance": platform_instance,
                    "lastModified": {
                        "time": int(datetime.now().timestamp() * 1000),
                        "actor": f"urn:li:corpuser:{owner}"
                    }
                }),
                "contentType": "application/json"
            }
        }
        new_mcps.append(test_definition_mcp)
        
        # 2. Status MCP (active by default)
        status_mcp = {
            "entityType": "test",
            "entityUrn": test_urn,
            "changeType": "UPSERT",
            "aspectName": "status",
            "aspect": {
                "value": json.dumps({
                    "removed": False
                }),
                "contentType": "application/json"
            }
        }
        new_mcps.append(status_mcp)
        
        # 3. Ownership MCP
        ownership_mcp = {
            "entityType": "test",
            "entityUrn": test_urn,
            "changeType": "UPSERT",
            "aspectName": "ownership",
            "aspect": {
                "value": json.dumps({
                    "owners": [
                        {
                            "owner": f"urn:li:corpuser:{owner}",
                            "type": "TECHNICAL_OWNER"
                        }
                    ],
                    "lastModified": {
                        "time": int(datetime.now().timestamp() * 1000),
                        "actor": f"urn:li:corpuser:{owner}"
                    }
                }),
                "contentType": "application/json"
            }
        }
        new_mcps.append(ownership_mcp)
        
        if not new_mcps:
            return {
                "success": False,
                "message": "Failed to create test MCPs",
                "test_id": test_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Remove any existing MCPs for this test URN to avoid duplicates
        existing_mcps = [
            mcp for mcp in existing_mcps 
            if mcp.get("entityUrn") != test_urn
        ]
        
        # Add new MCPs to the list
        existing_mcps.extend(new_mcps)
        
        # Save updated MCP file
        try:
            with open(mcp_file_path, 'w') as f:
                json.dump(existing_mcps, f, indent=2)
            logger.info(f"Saved MCP file with {len(existing_mcps)} MCPs to: {mcp_file_path}")
            mcp_saved = True
        except Exception as e:
            logger.error(f"Failed to save MCP file: {e}")
            mcp_saved = False
        
        files_saved = []
        if mcp_saved:
            files_saved.append(str(mcp_file_path))
        
        logger.info(f"Successfully added test '{test_name}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcps)}")
        
        return {
            "success": True,
            "message": f"Successfully created {len(new_mcps)} MCPs for test {test_id}",
            "test_id": test_id,
            "test_urn": test_urn,
            "mcps_created": len(new_mcps),
            "files_saved": files_saved,
            "aspects_included": [mcp.get("aspectName", "unknown") for mcp in new_mcps]
        }
        
    except Exception as e:
        logger.error(f"Error adding test {test_id} to staged changes: {e}")
        return {
            "success": False,
            "message": f"Error adding test to staged changes: {str(e)}",
            "test_id": test_id,
            "mcps_created": 0,
            "files_saved": []
        }


def apply_urn_mutations_to_test_definition(
    test_definition: Dict[str, Any], 
    environment_name: str,
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Apply URN mutations to any glossary terms, domains, tags, or structured properties
    found within the test definition JSON.
    
    Args:
        test_definition: The test definition dictionary
        environment_name: The target environment name
        mutation_config: The mutation configuration
        
    Returns:
        Dict: The test definition with mutated URNs
    """
    if not test_definition or not mutation_config:
        return test_definition
    
    # Import URN utilities
    try:
        from utils.urn_utils import generate_mutated_urn
    except ImportError:
        logger.warning("URN utilities not available - returning original definition")
        return test_definition
    
    # Create a deep copy to avoid modifying the original
    mutated_definition = json.loads(json.dumps(test_definition))
    
    # Recursively apply mutations to all URNs in the definition
    mutated_definition = _apply_mutations_recursively(
        mutated_definition, environment_name, mutation_config
    )
    
    return mutated_definition


def _apply_mutations_recursively(
    obj: Any, 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Any:
    """
    Recursively apply URN mutations to any URN strings found in the object.
    
    Args:
        obj: The object to process (can be dict, list, string, etc.)
        environment_name: The target environment name
        mutation_config: The mutation configuration
        
    Returns:
        The object with mutated URNs
    """
    try:
        from utils.urn_utils import generate_mutated_urn
    except ImportError:
        return obj
    
    if isinstance(obj, dict):
        # Process dictionary recursively
        result = {}
        for key, value in obj.items():
            result[key] = _apply_mutations_recursively(value, environment_name, mutation_config)
        return result
    
    elif isinstance(obj, list):
        # Process list recursively
        return [_apply_mutations_recursively(item, environment_name, mutation_config) for item in obj]
    
    elif isinstance(obj, str):
        # Check if this string is a URN that should be mutated
        if obj.startswith("urn:li:"):
            return _mutate_urn_if_applicable(obj, environment_name, mutation_config)
        return obj
    
    else:
        # Return other types as-is
        return obj


def _mutate_urn_if_applicable(
    urn: str, 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> str:
    """
    Mutate a URN if it's of a type that should be mutated according to the configuration.
    
    Args:
        urn: The URN to potentially mutate
        environment_name: The target environment name
        mutation_config: The mutation configuration
        
    Returns:
        The mutated URN or original URN if no mutation should be applied
    """
    try:
        from utils.urn_utils import generate_mutated_urn
    except ImportError:
        return urn
    
    # Extract entity type from URN
    if not urn.startswith("urn:li:"):
        return urn
    
    parts = urn.split(":")
    if len(parts) < 3:
        return urn
    
    entity_type = parts[2]
    
    # Map URN entity types to mutation config flags
    type_mapping = {
        'tag': 'apply_to_tags',
        'glossaryTerm': 'apply_to_glossary_terms',
        'glossaryNode': 'apply_to_glossary_nodes',
        'structuredProperty': 'apply_to_structured_properties',
        'domain': 'apply_to_domains',
        'dataProduct': 'apply_to_data_products',
    }
    
    mutation_flag = type_mapping.get(entity_type)
    if not mutation_flag:
        # Unknown entity type, return original URN
        return urn
    
    # Check if mutations should be applied for this entity type
    should_apply_mutation = mutation_config.get(mutation_flag, False)
    if not should_apply_mutation:
        return urn
    
    # Apply the mutation
    try:
        mutated_urn = generate_mutated_urn(urn, environment_name, entity_type, mutation_config)
        logger.debug(f"Mutated URN: {urn} -> {mutated_urn}")
        return mutated_urn
    except Exception as e:
        logger.warning(f"Failed to mutate URN {urn}: {e}")
        return urn 