#!/usr/bin/env python3
"""
Module providing functions for tag actions in the DataHub UI:
- Download tag JSON
- Sync tag to local database
- Add tag to staged changes
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
from scripts.mcps.create_tag_mcps import (
    create_tag_properties_mcp, 
    create_tag_ownership_mcp, 
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


def download_tag_json(tag_data: Dict[str, Any], output_path: Optional[str] = None) -> str:
    """
    Download tag data as raw JSON

    Args:
        tag_data: Tag data as a dictionary
        output_path: Optional path to save the JSON file

    Returns:
        Path to the saved file or JSON string if no path provided
    """
    try:
        formatted_json = json.dumps(tag_data, indent=2)
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as f:
                f.write(formatted_json)
            logger.info(f"Tag JSON saved to {output_path}")
            return output_path
        else:
            return formatted_json
    except Exception as e:
        logger.error(f"Error downloading tag JSON: {str(e)}")
        raise


def extract_tag_id_from_urn(urn: str) -> str:
    """
    Extract tag ID from a DataHub URN

    Args:
        urn: DataHub tag URN (e.g., 'urn:li:tag:1234')

    Returns:
        Tag ID/key
    """
    if not urn or not urn.startswith("urn:li:tag:"):
        raise ValueError(f"Invalid tag URN: {urn}")
    
    return urn.split(":")[-1]


def sync_tag_to_local(tag_data: Dict[str, Any], local_db_path: Optional[str] = None) -> bool:
    """
    Sync tag data to local database

    Args:
        tag_data: Tag data as a dictionary
        local_db_path: Optional path to local database (defaults to environment setting)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Determine local database path if not provided
        if not local_db_path:
            # TODO: Get from environment or config
            local_db_path = os.getenv("DATAHUB_LOCAL_DB_PATH", "local_db/tags.json")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_db_path), exist_ok=True)
        
        # Load existing database or create new one
        try:
            with open(local_db_path, "r") as f:
                local_db = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            local_db = {"tags": {}}
        
        # Extract tag ID from URN
        tag_urn = tag_data.get("urn")
        if not tag_urn:
            raise ValueError("Tag URN not found in tag data")
        
        tag_id = extract_tag_id_from_urn(tag_urn)
        
        # Add or update tag in database
        local_db["tags"][tag_id] = tag_data
        
        # Save updated database
        with open(local_db_path, "w") as f:
            json.dump(local_db, f, indent=2)
        
        logger.info(f"Tag {tag_id} synced to local database")
        return True
    except Exception as e:
        logger.error(f"Error syncing tag to local database: {str(e)}")
        return False


def add_tag_to_staged_changes(
    tag_data: Dict[str, Any], 
    environment: str, 
    owner: str,
    base_dir: Optional[str] = None,
    mutation_name: Optional[str] = None
) -> Dict[str, str]:
    """
    Add tag to staged changes by creating a single MCP file containing all MCPs

    Args:
        tag_data: Tag data as a dictionary
        environment: Environment name (for directory structure)
        owner: Owner username
        base_dir: Optional base directory (defaults to metadata-manager/{environment} in repo root)
        mutation_name: Optional mutation name for deterministic URN generation

    Returns:
        Dictionary with path to created MCP file
    """
    try:
        # Extract tag information
        tag_urn = tag_data.get("urn")
        if not tag_urn:
            raise ValueError("Tag URN not found in tag data")
        
        # Extract tag properties
        tag_name = extract_name_from_properties(tag_data)
        if not tag_name:
            raise ValueError("Tag name not found in tag data")
        
        # Extract tag ID from URN or properties
        tag_id = tag_data.get("key") or extract_tag_id_from_urn(tag_urn)
        
        # Get description if available
        description = None
        if "properties" in tag_data and "description" in tag_data["properties"]:
            description = tag_data["properties"]["description"]
        
        # Get color if available
        color_hex = None
        if "properties" in tag_data and "colorHex" in tag_data["properties"]:
            color_hex = tag_data["properties"]["colorHex"]
        
        # Determine output directory - use repo root metadata-manager instead of web_ui/metadata-manager
        if base_dir:
            output_dir = base_dir
        else:
            # Find repository root by looking for characteristic files
            current_dir = os.path.abspath(os.getcwd())
            repo_root = None
            
            # Search upwards for the repository root (look for README.md and scripts/ directory)
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
                if "scripts/mcps" in script_dir:
                    repo_root = os.path.dirname(os.path.dirname(script_dir))
                else:
                    # Last resort: assume we're in a subdirectory and go up
                    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
            
            output_dir = os.path.join(repo_root, "metadata-manager", environment, "tags")
            logger.debug(f"Calculated repo root: {repo_root}, output dir: {output_dir}")
        
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
        
        # Create new MCPs for this tag
        new_mcps = []
        
        # Create properties MCP
        properties_mcp = create_tag_properties_mcp(
            tag_id=tag_id,
            owner=owner,
            tag_name=tag_name,
            description=description,
            color_hex=color_hex,
            environment=environment,
            mutation_name=mutation_name
        )
        new_mcps.append(properties_mcp)
        
        # Create ownership MCP
        ownership_mcp = create_tag_ownership_mcp(
            tag_id=tag_id,
            owner=owner,
            environment=environment,
            mutation_name=mutation_name
        )
        new_mcps.append(ownership_mcp)
        
        # Remove any existing MCPs for this tag URN to avoid duplicates
        tag_entity_urn = properties_mcp.get("entityUrn")
        existing_mcps = [
            mcp for mcp in existing_mcps 
            if mcp.get("entityUrn") != tag_entity_urn
        ]
        
        # Add new MCPs to the list
        existing_mcps.extend(new_mcps)
        
        # Save updated MCP file as a simple list
        mcp_saved = save_mcp_to_file(existing_mcps, mcp_file_path)
        
        created_files = {}
        if mcp_saved:
            created_files["mcp_file"] = mcp_file_path
        
        logger.info(f"Successfully added tag '{tag_name}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcps)}")
        
        # Return the path to the created file
        return created_files
        
    except Exception as e:
        logger.error(f"Error adding tag to staged changes: {str(e)}")
        raise 