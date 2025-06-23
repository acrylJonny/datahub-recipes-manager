#!/usr/bin/env python3
"""
Module providing functions for structured property actions in the DataHub UI:
- Add structured property to staged changes
- Create comprehensive structured property MCPs
"""

import json
import logging
import os
import sys
import time
from typing import Dict, Any, Optional, List

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import local utilities
from utils.urn_utils import generate_deterministic_urn, extract_name_from_properties
from scripts.mcps.create_structured_property_mcps import (
    create_structured_property_staged_changes,
    save_mcps_to_files
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


def add_structured_property_to_staged_changes(
    property_id: str,
    qualified_name: str,
    display_name: Optional[str] = None,
    description: Optional[str] = None,
    value_type: str = "STRING",
    cardinality: str = "SINGLE",
    allowedValues: Optional[List[Any]] = None,
    entity_types: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: Optional[str] = None,
    mutation_name: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Add a structured property to staged changes by creating a single MCP file containing all MCPs.
    This follows the same pattern as tags - creates metadata-manager/{environment}/structured_properties/mcp_file.json
    containing a simple list of MCPs.
    
    Args:
        property_id: Unique identifier for the structured property
        qualified_name: Qualified name for the property
        display_name: Display name for the property
        description: Property description
        value_type: Value type (STRING, NUMBER, etc.)
        cardinality: Cardinality (SINGLE, MULTIPLE)
        allowedValues: List of allowed values
        entity_types: List of entity types this property can be applied to
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        links: List of documentation links
        custom_properties: Custom properties dictionary
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspects dictionary
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Optional base directory (defaults to metadata-manager/{environment}/structured_properties in repo root)
        mutation_name: Optional mutation name for deterministic URN generation
        **kwargs: Additional arguments
    
    Returns:
        Dictionary containing operation results
    """
    setup_logging()
    
    try:
        # Generate structured property URN
        property_urn = f"urn:li:structuredProperty:{property_id}"
        
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
            
            output_dir = os.path.join(repo_root, "metadata-manager", environment, "structured_properties")
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
        
        # Create MCPs using the comprehensive function
        new_mcps = create_structured_property_staged_changes(
            property_urn=property_urn,
            qualified_name=qualified_name,
            display_name=display_name,
            description=description,
            value_type=value_type,
            cardinality=cardinality,
            allowedValues=allowedValues,
            entity_types=entity_types,
            owners=owners,
            tags=tags,
            terms=terms,
            links=links,
            custom_properties=custom_properties,
            include_all_aspects=include_all_aspects,
            custom_aspects=custom_aspects,
            **kwargs
        )
        
        if not new_mcps:
            return {
                "success": False,
                "message": "Failed to create structured property MCPs",
                "property_id": property_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Remove any existing MCPs for this property URN to avoid duplicates
        existing_mcps = [
            mcp for mcp in existing_mcps 
            if mcp.get("entityUrn") != property_urn
        ]
        
        # Add new MCPs to the list
        existing_mcps.extend(new_mcps)
        
        # Save updated MCP file as a simple list (like tags)
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
            files_saved.append(mcp_file_path)
        
        logger.info(f"Successfully added structured property '{display_name or property_id}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcps)}")
        
        return {
            "success": True,
            "message": f"Successfully created {len(new_mcps)} MCPs for structured property {property_id}",
            "property_id": property_id,
            "property_urn": property_urn,
            "mcps_created": len(new_mcps),
            "files_saved": files_saved,
            "aspects_included": [mcp.get("aspectName", "unknown") for mcp in new_mcps]
        }
        
    except Exception as e:
        logger.error(f"Error adding structured property {property_id} to staged changes: {e}")
        return {
            "success": False,
            "message": f"Error adding structured property to staged changes: {str(e)}",
            "property_id": property_id,
            "mcps_created": 0,
            "files_saved": []
        }


def add_structured_property_to_staged_changes_legacy(
    property_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata-manager",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Legacy function for backward compatibility.
    Add a structured property to staged changes by creating comprehensive MCP files
    
    Args:
        property_data: Dictionary containing structured property information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    property_id = property_data.get("id")
    if not property_id:
        raise ValueError("property_data must contain 'id' field")
    
    qualified_name = property_data.get("qualified_name", property_id)
    display_name = property_data.get("name")
    description = property_data.get("description")
    value_type = property_data.get("value_type", "STRING")
    cardinality = property_data.get("cardinality", "SINGLE")
    
    # Extract other properties from property_data
    allowedValues = property_data.get("allowedValues", [])
    entity_types = property_data.get("entity_types", [])
    owners = property_data.get("owners", [])
    tags = property_data.get("tags", [])
    terms = property_data.get("terms", [])
    links = property_data.get("links", [])
    custom_properties = property_data.get("custom_properties", {})
    
    # Use the new function
    result = add_structured_property_to_staged_changes(
        property_id=property_id,
        qualified_name=qualified_name,
        display_name=display_name,
        description=description,
        value_type=value_type,
        cardinality=cardinality,
        allowedValues=allowedValues,
        entity_types=entity_types,
        owners=owners,
        tags=tags,
        terms=terms,
        links=links,
        custom_properties=custom_properties,
        include_all_aspects=include_all_aspects,
        custom_aspects=custom_aspects,
        environment=environment,
        owner=owner,
        base_dir=None  # Let the function calculate the correct path
    )
    
    if result.get("success"):
        # Return dictionary mapping aspect names to file paths for compatibility
        return {f"property_{i}": path for i, path in enumerate(result.get("files_saved", []))}
    else:
        raise Exception(result.get("message", "Failed to create structured property MCPs")) 