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
    base_dir: str = "metadata",
    **kwargs
) -> Dict[str, Any]:
    """
    Add a structured property to staged changes with comprehensive MCP generation.
    
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
        base_dir: Base directory for metadata files
        **kwargs: Additional arguments
    
    Returns:
        Dictionary containing operation results
    """
    setup_logging()
    
    try:
        # Generate structured property URN
        property_urn = f"urn:li:structuredProperty:{property_id}"
        
        # Create MCPs using the comprehensive function
        mcps = create_structured_property_staged_changes(
            property_urn=property_urn,
            qualified_name=qualified_name,
            display_name=display_name,
            description=description,
            value_type=value_type,
            cardinality=cardinality,
            allowed_values=allowedValues,
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
        
        if not mcps:
            return {
                "success": False,
                "message": "Failed to create structured property MCPs",
                "property_id": property_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Save MCPs to single file
        from scripts.mcps.create_structured_property_mcps import save_structured_property_to_single_file
        saved_file = save_structured_property_to_single_file(
            mcps=mcps,
            base_directory=base_dir,
            entity_id=property_id
        )
        
        return {
            "success": True,
            "message": f"Successfully created {len(mcps)} MCPs for structured property {property_id}",
            "property_id": property_id,
            "property_urn": property_urn,
            "mcps_created": len(mcps),
            "files_saved": [saved_file] if saved_file else [],
            "aspects_included": [mcp.get("aspectName", "unknown") for mcp in mcps]
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
        base_dir=os.path.join(base_dir, environment, "structured_properties")
    )
    
    if result.get("success"):
        # Return dictionary mapping aspect names to file paths for compatibility
        return {f"property_{i}": path for i, path in enumerate(result.get("files_saved", []))}
    else:
        raise Exception(result.get("message", "Failed to create structured property MCPs")) 