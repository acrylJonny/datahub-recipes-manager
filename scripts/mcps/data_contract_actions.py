#!/usr/bin/env python3
"""
Module providing functions for data contract actions in the DataHub UI:
- Add data contract to staged changes
- Create comprehensive data contract MCPs
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
from scripts.mcps.create_data_contract_mcps import (
    create_data_contract_staged_changes,
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


def add_data_contract_to_staged_changes(
    data_contract_id: str,
    entity_urn: str,
    schema_assertions: Optional[List[str]] = None,
    freshness_assertions: Optional[List[str]] = None,
    data_quality_assertions: Optional[List[str]] = None,
    raw_contract: Optional[str] = None,
    state: str = "ACTIVE",
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata",
    **kwargs
) -> Dict[str, Any]:
    """
    Add a data contract to staged changes with comprehensive MCP generation.
    
    Args:
        data_contract_id: Unique identifier for the data contract
        entity_urn: URN of the entity this contract is for
        schema_assertions: List of schema assertion URNs
        freshness_assertions: List of freshness assertion URNs
        data_quality_assertions: List of data quality assertion URNs
        raw_contract: Raw YAML contract definition
        state: Contract state (ACTIVE or PENDING)
        custom_properties: Custom properties dictionary
        structured_properties: List of structured properties
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
        # Generate data contract URN
        data_contract_urn = f"urn:li:dataContract:{data_contract_id}"
        
        # Create MCPs using the comprehensive function
        mcps = create_data_contract_staged_changes(
            data_contract_urn=data_contract_urn,
            entity_urn=entity_urn,
            schema_assertions=schema_assertions,
            freshness_assertions=freshness_assertions,
            data_quality_assertions=data_quality_assertions,
            raw_contract=raw_contract,
            state=state,
            custom_properties=custom_properties,
            structured_properties=structured_properties,
            include_all_aspects=include_all_aspects,
            custom_aspects=custom_aspects,
            **kwargs
        )
        
        if not mcps:
            return {
                "success": False,
                "message": "Failed to create data contract MCPs",
                "data_contract_id": data_contract_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Save MCPs to files
        saved_files = save_mcps_to_files(
            mcps=mcps,
            base_directory=base_dir,
            entity_id=data_contract_id
        )
        
        return {
            "success": True,
            "message": f"Successfully created {len(mcps)} MCPs for data contract {data_contract_id}",
            "data_contract_id": data_contract_id,
            "data_contract_urn": data_contract_urn,
            "entity_urn": entity_urn,
            "mcps_created": len(mcps),
            "files_saved": saved_files,
            "aspects_included": [mcp.aspectName if hasattr(mcp, 'aspectName') else mcp.get("aspectName", "unknown") for mcp in mcps]
        }
        
    except Exception as e:
        logger.error(f"Error adding data contract {data_contract_id} to staged changes: {e}")
        return {
            "success": False,
            "message": f"Error adding data contract to staged changes: {str(e)}",
            "data_contract_id": data_contract_id,
            "mcps_created": 0,
            "files_saved": []
        }


def add_data_contract_to_staged_changes_legacy(
    contract_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata-manager",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Legacy function for backward compatibility.
    Add a data contract to staged changes by creating comprehensive MCP files
    
    Args:
        contract_data: Dictionary containing data contract information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    contract_id = contract_data.get("id")
    if not contract_id:
        raise ValueError("contract_data must contain 'id' field")
    
    entity_urn = contract_data.get("entity_urn")
    if not entity_urn:
        raise ValueError("contract_data must contain 'entity_urn' field")
    
    # Extract properties from contract_data
    properties = contract_data.get("properties", {})
    schema_assertions = []
    freshness_assertions = []
    data_quality_assertions = []
    raw_contract = None
    state = "ACTIVE"
    
    # Extract assertion URNs if available
    if properties.get("schema"):
        schema_assertions = [assertion.get("urn") for assertion in properties.get("schema", {}).get("assertions", []) if assertion.get("urn")]
    
    if properties.get("freshness"):
        freshness_assertions = [assertion.get("urn") for assertion in properties.get("freshness", {}).get("assertions", []) if assertion.get("urn")]
    
    if properties.get("dataQuality"):
        data_quality_assertions = [assertion.get("urn") for assertion in properties.get("dataQuality", {}).get("assertions", []) if assertion.get("urn")]
    
    # Extract other properties
    custom_properties = contract_data.get("custom_properties", {})
    structured_properties = contract_data.get("structured_properties", [])
    
    # Use the new function
    result = add_data_contract_to_staged_changes(
        data_contract_id=contract_id,
        entity_urn=entity_urn,
        schema_assertions=schema_assertions,
        freshness_assertions=freshness_assertions,
        data_quality_assertions=data_quality_assertions,
        raw_contract=raw_contract,
        state=state,
        custom_properties=custom_properties,
        structured_properties=structured_properties,
        include_all_aspects=include_all_aspects,
        custom_aspects=custom_aspects,
        environment=environment,
        owner=owner,
        base_dir=os.path.join(base_dir, environment, "data_contracts")
    )
    
    if result.get("success"):
        # Return dictionary mapping aspect names to file paths for compatibility
        return {f"contract_{i}": path for i, path in enumerate(result.get("files_saved", []))}
    else:
        raise Exception(result.get("message", "Failed to create data contract MCPs")) 