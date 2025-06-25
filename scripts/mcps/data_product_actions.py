#!/usr/bin/env python3
"""
Module providing functions for data product actions in the DataHub UI:
- Add data product to staged changes
- Create comprehensive data product MCPs
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
from scripts.mcps.create_data_product_mcps import (
    create_data_product_staged_changes,
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


def add_data_product_to_staged_changes(
    data_product_id: str,
    name: str,
    description: Optional[str] = None,
    external_url: Optional[str] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    domains: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    sub_types: Optional[List[str]] = None,
    deprecated: bool = False,
    deprecation_note: str = "",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata",
    **kwargs
) -> Dict[str, Any]:
    """
    Add a data product to staged changes with comprehensive MCP generation.
    
    Args:
        data_product_id: Unique identifier for the data product
        name: Data product name
        description: Data product description
        external_url: External URL for the data product
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        domains: List of domain URNs
        links: List of documentation links
        custom_properties: Custom properties dictionary
        structured_properties: List of structured properties
        sub_types: List of sub-types
        deprecated: Whether the data product is deprecated
        deprecation_note: Deprecation note
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
        # Generate data product URN
        data_product_urn = f"urn:li:dataProduct:{data_product_id}"
        
        # Create MCPs using the comprehensive function
        mcps = create_data_product_staged_changes(
            data_product_urn=data_product_urn,
            name=name,
            description=description,
            external_url=external_url,
            owners=owners,
            tags=tags,
            terms=terms,
            domains=domains,
            links=links,
            custom_properties=custom_properties,
            structured_properties=structured_properties,
            sub_types=sub_types,
            deprecated=deprecated,
            deprecation_note=deprecation_note,
            include_all_aspects=include_all_aspects,
            custom_aspects=custom_aspects,
            **kwargs
        )
        
        if not mcps:
            return {
                "success": False,
                "message": "Failed to create data product MCPs",
                "data_product_id": data_product_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Save MCPs to files
        saved_files = save_mcps_to_files(
            mcps=mcps,
            base_directory=base_dir,
            entity_id=data_product_id
        )
        
        return {
            "success": True,
            "message": f"Successfully created {len(mcps)} MCPs for data product {data_product_id}",
            "data_product_id": data_product_id,
            "data_product_urn": data_product_urn,
            "mcps_created": len(mcps),
            "files_saved": saved_files,
            "aspects_included": [mcp.aspectName if hasattr(mcp, 'aspectName') else mcp.get("aspectName", "unknown") for mcp in mcps]
        }
        
    except Exception as e:
        logger.error(f"Error adding data product {data_product_id} to staged changes: {e}")
        return {
            "success": False,
            "message": f"Error adding data product to staged changes: {str(e)}",
            "data_product_id": data_product_id,
            "mcps_created": 0,
            "files_saved": []
        }


def add_data_product_to_staged_changes_legacy(
    data_product_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata-manager",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Legacy function for backward compatibility.
    Add a data product to staged changes by creating comprehensive MCP files
    
    Args:
        data_product_data: Dictionary containing data product information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    data_product_id = data_product_data.get("id")
    if not data_product_id:
        raise ValueError("data_product_data must contain 'id' field")
    
    data_product_name = data_product_data.get("name", data_product_id)
    description = data_product_data.get("description")
    external_url = data_product_data.get("external_url")
    domain_urn = data_product_data.get("domain_urn")
    entity_urns = data_product_data.get("entity_urns", [])
    
    # Extract other properties from data_product_data
    owners = data_product_data.get("owners", [])
    tags = data_product_data.get("tags", [])
    terms = data_product_data.get("terms", [])
    domains = [domain_urn] if domain_urn else []
    links = data_product_data.get("links", [])
    custom_properties = data_product_data.get("custom_properties", {})
    structured_properties = data_product_data.get("structured_properties", [])
    sub_types = data_product_data.get("sub_types", [])
    deprecated = data_product_data.get("deprecated", False)
    deprecation_note = data_product_data.get("deprecation_note", "")
    
    # Use the new function
    result = add_data_product_to_staged_changes(
        data_product_id=data_product_id,
        name=data_product_name,
        description=description,
        external_url=external_url,
        owners=owners,
        tags=tags,
        terms=terms,
        domains=domains,
        links=links,
        custom_properties=custom_properties,
        structured_properties=structured_properties,
        sub_types=sub_types,
        deprecated=deprecated,
        deprecation_note=deprecation_note,
        include_all_aspects=include_all_aspects,
        custom_aspects=custom_aspects,
        environment=environment,
        owner=owner,
        base_dir=os.path.join(base_dir, environment, "data_products")
    )
    
    if result.get("success"):
        # Return dictionary mapping aspect names to file paths for compatibility
        return {f"data_product_{i}": path for i, path in enumerate(result.get("files_saved", []))}
    else:
        raise Exception(result.get("message", "Failed to create data product MCPs")) 