#!/usr/bin/env python3
"""
Module providing functions for glossary actions in the DataHub UI:
- Download glossary JSON
- Sync glossary to local database
- Add glossary to staged changes
"""

import json
import logging
import os
import sys
import time
from typing import Dict, Any, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import local utilities
from utils.urn_utils import generate_deterministic_urn, extract_name_from_properties
from scripts.mcps.create_glossary_mcps import (
    create_comprehensive_glossary_mcps,
    save_mcp_to_file
)

logger = logging.getLogger(__name__)


def setup_logging(log_level="INFO"):
    """Set up logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def download_glossary_json(entity_data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Download glossary entity data as raw JSON

    Args:
        entity_data: Glossary entity data as a dictionary
        output_path: Optional path to save the JSON file

    Returns:
        Path to the saved file or JSON string if no path provided
    """
    try:
        formatted_json = json.dumps(entity_data, indent=2)
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as f:
                f.write(formatted_json)
            logger.info(f"Glossary JSON saved to {output_path}")
            return output_path
        else:
            return formatted_json
    except Exception as e:
        logger.error(f"Error downloading glossary JSON: {str(e)}")
        raise


def extract_entity_id_from_urn(urn: str, entity_type: str) -> str:
    """
    Extract entity ID from a DataHub URN

    Args:
        urn: DataHub URN (e.g., 'urn:li:glossaryNode:1234' or 'urn:li:glossaryTerm:5678')
        entity_type: Type of entity ("glossaryNode" or "glossaryTerm")

    Returns:
        Entity ID/key
    """
    expected_prefix = f"urn:li:{entity_type}:"
    if not urn or not urn.startswith(expected_prefix):
        raise ValueError(f"Invalid {entity_type} URN: {urn}")
    
    return urn.split(":")[-1]


def add_glossary_to_staged_changes(
    entity_data: Dict[str, Any], 
    entity_type: str,  # "node" or "term"
    environment: str, 
    owner: str,
    base_dir: Optional[str] = None,
    mutation_name: Optional[str] = None
) -> Dict[str, str]:
    """
    Add glossary entity to staged changes by creating a single MCP file containing all MCPs

    Args:
        entity_data: Glossary entity data as a dictionary
        entity_type: Type of entity ("node" or "term")
        environment: Environment name (for directory structure)
        owner: Owner username
        base_dir: Optional base directory (defaults to metadata-manager/{environment} in repo root)
        mutation_name: Optional mutation name for deterministic URN generation

    Returns:
        Dictionary with path to created MCP file
    """
    try:
        # Extract basic entity information
        entity_id = entity_data.get("id")
        entity_name = entity_data.get("name", entity_id)
        description = entity_data.get("description", "")
        
        if not entity_id:
            raise ValueError(f"Entity ID is required for {entity_type}")
        
        logger.info(f"Adding {entity_type} '{entity_name}' to staged changes...")
        
        # Determine output directory - use repo root metadata-manager instead of web_ui/metadata-manager
        if base_dir:
            # If base_dir is already something like metadata-manager/dev, append glossary
            if base_dir.endswith("glossary"):
                output_dir = base_dir
            else:
                output_dir = os.path.join(base_dir, "glossary")
        else:
            # Find repository root by looking for characteristic files
            current_dir = os.path.abspath(os.getcwd())
            repo_root = None
            
            # Search upwards for the repository root
            search_dir = current_dir
            for _ in range(10):  # Limit search to avoid infinite loop
                if (os.path.exists(os.path.join(search_dir, "README.md")) and 
                    os.path.exists(os.path.join(search_dir, "scripts")) and
                    os.path.exists(os.path.join(search_dir, "web_ui"))):
                    repo_root = search_dir
                    break
                parent_dir = os.path.dirname(search_dir)
                if parent_dir == search_dir:  # Reached filesystem root
                    break
                search_dir = parent_dir
            
            # Fallback: try to calculate from __file__ path
            if not repo_root:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                repo_root = os.path.dirname(os.path.dirname(script_dir))
            
            # Create environment directory with glossary subfolder
            output_dir = os.path.join(repo_root, "metadata-manager", environment, "glossary")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Use constant filename
        mcp_file_path = os.path.join(output_dir, "mcp_file.json")
        
        # Load existing MCP file or create new list - should be a simple list of MCPs
        existing_mcps = []
        if os.path.exists(mcp_file_path):
            try:
                with open(mcp_file_path, "r") as f:
                    file_content = json.load(f)
                    # Handle both old format (with metadata wrapper) and new format (simple list)
                    if isinstance(file_content, list):
                        existing_mcps = file_content
                    elif isinstance(file_content, dict) and "mcps" in file_content:
                        # Migrate from old format - extract just the MCPs
                        existing_mcps = file_content["mcps"]
                        logger.info(f"Migrating from old format - extracted {len(existing_mcps)} MCPs")
                    else:
                        logger.warning(f"Unknown MCP file format, starting fresh")
                        existing_mcps = []
                logger.info(f"Loaded existing MCP file with {len(existing_mcps)} existing MCPs")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load existing MCP file: {e}. Creating new file.")
                existing_mcps = []
        
        # Create comprehensive MCPs using all backend data
        logger.info(f"Creating comprehensive MCPs for {entity_type} '{entity_name}'...")
        new_mcps = create_comprehensive_glossary_mcps(
            entity_data=entity_data,
            entity_type=entity_type,
            owner=owner,
            environment=environment,
            mutation_name=mutation_name
        )
        
        logger.info(f"Created {len(new_mcps)} MCPs for {entity_type} '{entity_name}'")
        
        # Remove any existing MCPs for this entity URN to avoid duplicates
        # Get the entity URN from the first MCP (info MCP)
        entity_entity_urn = None
        if new_mcps:
            entity_entity_urn = new_mcps[0].get("entityUrn")
        
        if entity_entity_urn:
            existing_mcps = [
                mcp for mcp in existing_mcps 
                if mcp.get("entityUrn") != entity_entity_urn
            ]
        
        # Add new MCPs to the list
        existing_mcps.extend(new_mcps)
        
        # Save updated MCP file as a simple list
        mcp_saved = save_mcp_to_file(existing_mcps, mcp_file_path)
        
        created_files = {}
        if mcp_saved:
            created_files["mcp_file"] = mcp_file_path
        
        logger.info(f"Successfully added {entity_type} '{entity_name}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcps)}")
        
        # Return the path to the created file
        return created_files
        
    except Exception as e:
        logger.error(f"Error adding {entity_type} to staged changes: {str(e)}")
        raise


def add_glossary_node_to_staged_changes(
    node_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Add a glossary node to staged changes by creating comprehensive MCP files
    
    Args:
        node_data: Dictionary containing node information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    # Use the unified function
    return add_glossary_to_staged_changes(
        entity_data=node_data,
        entity_type="node",
        environment=environment,
        owner=owner,
        base_dir=base_dir
    )


def add_glossary_term_to_staged_changes(
    term_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Add a glossary term to staged changes by creating comprehensive MCP files
    
    Args:
        term_data: Dictionary containing term information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    # Use the unified function
    return add_glossary_to_staged_changes(
        entity_data=term_data,
        entity_type="term",
        environment=environment,
        owner=owner,
        base_dir=base_dir
    )


 