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
    allowed_values: Optional[List[Any]] = None,
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
        allowed_values: List of allowed values
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
            allowed_values=allowed_values,
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
        
        # Save MCPs to files
        saved_files = save_mcps_to_files(
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
            "files_saved": saved_files,
            "aspects_included": [mcp.aspectName for mcp in mcps]
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