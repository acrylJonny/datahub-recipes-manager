#!/usr/bin/env python3
"""
Module providing functions for domain actions in the DataHub UI:
- Add domain to staged changes
- Create comprehensive domain MCPs
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
from scripts.mcps.create_domain_mcps import (
    create_domain_staged_changes,
    save_mcps_to_files
)

# Try to import the new URN generation utilities
try:
    sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'web_ui'))
    from utils.urn_utils import (
        generate_domain_urn, 
        get_mutation_config_for_environment
    )
    HAS_NEW_URN_UTILS = True
except ImportError:
    HAS_NEW_URN_UTILS = False

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


def add_domain_to_staged_changes(
    domain_id: str,
    name: str,
    description: Optional[str] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    terms: Optional[List[str]] = None,
    links: Optional[List[Dict[str, str]]] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    structured_properties: Optional[List[Dict[str, Any]]] = None,
    forms: Optional[List[Dict[str, Any]]] = None,
    test_results: Optional[List[Dict[str, Any]]] = None,
    display_properties: Optional[Dict[str, Any]] = None,
    parent_domain: Optional[str] = None,
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata-manager",
    **kwargs
) -> Dict[str, Any]:
    """
    Add a domain to staged changes with comprehensive MCP generation.
    
    Args:
        domain_id: Unique identifier for the domain
        name: Domain name
        description: Domain description
        owners: List of owner URNs
        tags: List of tag URNs
        terms: List of glossary term URNs
        links: List of documentation links
        custom_properties: Custom properties dictionary
        structured_properties: List of structured properties
        forms: List of form associations
        test_results: List of test results
        display_properties: Display properties (color, icon, etc.)
        parent_domain: Parent domain URN
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
        # Generate domain URN with mutation support
        domain_urn = f"urn:li:domain:{domain_id}"
        custom_urn = None
        
        # Extract mutation_name from kwargs if provided, otherwise use environment
        mutation_name = kwargs.pop('mutation_name', environment)
        
        if HAS_NEW_URN_UTILS:
            try:
                mutation_config = get_mutation_config_for_environment(environment)
                if mutation_config:
                    mutated_urn = generate_domain_urn(domain_urn, environment, mutation_config)
                    if mutated_urn != domain_urn:
                        custom_urn = mutated_urn
                        domain_urn = mutated_urn
                        logger.info(f"Generated mutated URN for domain: {f'urn:li:domain:{domain_id}'} -> {mutated_urn}")
                logger.info(f"Using mutation config for environment '{environment}': {mutation_config is not None}")
            except Exception as e:
                logger.warning(f"Could not get mutation config for environment '{environment}': {e}")
        
        # Create MCPs using the new comprehensive function
        mcps = create_domain_staged_changes(
            domain_urn=domain_urn,
            name=name,
            description=description,
            owners=owners,
            tags=tags,
            terms=terms,
            links=links,
            custom_properties=custom_properties,
            structured_properties=structured_properties,
            forms=forms,
            test_results=test_results,
            display_properties=display_properties,
            parent_domain=parent_domain,
            include_all_aspects=include_all_aspects,
            custom_aspects=custom_aspects,
            custom_urn=custom_urn,
            environment=environment,
            mutation_name=mutation_name,
            **kwargs
        )
        
        if not mcps:
            return {
                "success": False,
                "message": "Failed to create domain MCPs",
                "domain_id": domain_id,
                "mcps_created": 0,
                "files_saved": []
            }
        
        # Determine output directory - use repo root metadata-manager instead of web_ui/metadata-manager
        if base_dir == "metadata-manager":
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
            
            output_dir = os.path.join(repo_root, "metadata-manager", environment, "domains")
            logger.debug(f"Calculated repo root: {repo_root}, output dir: {output_dir}")
        else:
            output_dir = os.path.join(base_dir, environment, "domains")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Use constant filename like tags and structured properties
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
        
        # Remove any existing MCPs for this domain URN to avoid duplicates
        existing_mcps = [
            mcp for mcp in existing_mcps 
            if mcp.get("entityUrn") != domain_urn
        ]
        
        # Add new MCPs to the list
        existing_mcps.extend(mcps)
        
        # Save updated MCP file as a simple list (like tags and structured properties)
        try:
            with open(mcp_file_path, 'w') as f:
                json.dump(existing_mcps, f, indent=2)
            logger.info(f"Saved MCP file with {len(existing_mcps)} MCPs to: {mcp_file_path}")
            saved_files = [mcp_file_path]
        except Exception as e:
            logger.error(f"Failed to save MCP file: {e}")
            saved_files = []
        
        return {
            "success": True,
            "message": f"Successfully created {len(mcps)} MCPs for domain {domain_id}",
            "domain_id": domain_id,
            "domain_urn": domain_urn,
            "mcps_created": len(mcps),
            "files_saved": saved_files,
            "aspects_included": [
                mcp.get("aspectName", "unknown") if isinstance(mcp, dict)
                else getattr(mcp, "aspectName", "unknown")
                for mcp in mcps
            ]
        }
        
    except Exception as e:
        logger.error(f"Error adding domain {domain_id} to staged changes: {e}")
        return {
            "success": False,
            "message": f"Error adding domain to staged changes: {str(e)}",
            "domain_id": domain_id,
            "mcps_created": 0,
            "files_saved": []
        }


def add_domain_to_staged_changes_legacy(
    domain_data: Dict[str, Any],
    environment: str = "dev",
    owner: str = "admin",
    base_dir: str = "metadata",
    include_all_aspects: bool = True,
    custom_aspects: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Legacy function for backward compatibility.
    Add a domain to staged changes by creating comprehensive MCP files
    
    Args:
        domain_data: Dictionary containing domain information
        environment: Environment name for URN generation
        owner: Owner username
        base_dir: Base directory for metadata files
        include_all_aspects: Whether to include all supported aspects
        custom_aspects: Custom aspect data to include
    
    Returns:
        Dictionary mapping aspect names to file paths
    """
    setup_logging()
    
    domain_id = domain_data.get("id")
    if not domain_id:
        raise ValueError("domain_data must contain 'id' field")
    
    domain_name = domain_data.get("name", domain_id)
    description = domain_data.get("description")
    parent_domain_urn = domain_data.get("parent_urn")
    custom_properties = domain_data.get("custom_properties", {})
    
    # Extract other properties from domain_data
    owners = domain_data.get("owners", [])
    tags = domain_data.get("tags", [])
    terms = domain_data.get("terms", [])
    links = domain_data.get("links", [])
    structured_properties = domain_data.get("structured_properties", [])
    forms = domain_data.get("forms", [])
    test_results = domain_data.get("test_results", [])
    display_properties = domain_data.get("display_properties", {})
    mutation_name = domain_data.get("mutation_name")
    
    # Use the new function
    result = add_domain_to_staged_changes(
        domain_id=domain_id,
        name=domain_name,
        description=description,
        owners=owners,
        tags=tags,
        terms=terms,
        links=links,
        custom_properties=custom_properties,
        structured_properties=structured_properties,
        forms=forms,
        test_results=test_results,
        display_properties=display_properties,
        parent_domain=parent_domain_urn,
        include_all_aspects=include_all_aspects,
        custom_aspects=custom_aspects,
        environment=environment,
        owner=owner,
        base_dir="metadata-manager",
        mutation_name=mutation_name
    )
    
    # Convert to legacy format (file paths only)
    if result.get("success"):
        return {f"aspect_{i}": path for i, path in enumerate(result.get("files_saved", []))}
    else:
        logger.error(f"Failed to create domain MCPs: {result.get('message')}")
        return {} 