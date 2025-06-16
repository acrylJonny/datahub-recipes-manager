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
from typing import Dict, Any, Optional

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import local utilities
from utils.urn_utils import generate_deterministic_urn, extract_name_from_properties
from scripts.mcps.create_tag_mcps import create_tag_properties_mcp, create_tag_ownership_mcp, save_mcp_to_file

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
    Add tag to staged changes by creating MCP files

    Args:
        tag_data: Tag data as a dictionary
        environment: Environment name (for directory structure)
        owner: Owner username
        base_dir: Optional base directory (defaults to metadata-manager/{environment})
        mutation_name: Optional mutation name for deterministic URN generation

    Returns:
        Dictionary with paths to created MCP files
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
        
        # Determine output directory
        if base_dir:
            output_dir = os.path.join(base_dir, "tags")
        else:
            output_dir = os.path.join("metadata-manager", environment, "tags")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate a filename-safe version of the tag_id
        safe_tag_id = tag_id.replace(" ", "_").lower()
        
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
        
        # Save properties MCP
        properties_file = os.path.join(output_dir, f"{safe_tag_id}_properties.json")
        save_mcp_to_file(properties_mcp, properties_file)
        
        # Create ownership MCP
        ownership_mcp = create_tag_ownership_mcp(
            tag_id=tag_id,
            owner=owner,
            environment=environment,
            mutation_name=mutation_name
        )
        
        # Save ownership MCP
        ownership_file = os.path.join(output_dir, f"{safe_tag_id}_ownership.json")
        save_mcp_to_file(ownership_mcp, ownership_file)
        
        logger.info(f"Successfully added tag '{tag_name}' to staged changes")
        
        # Return the paths to the created files
        return {
            "properties_file": properties_file,
            "ownership_file": ownership_file
        }
        
    except Exception as e:
        logger.error(f"Error adding tag to staged changes: {str(e)}")
        raise 