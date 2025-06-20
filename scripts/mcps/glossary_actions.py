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
    create_glossary_node_info_mcp,
    create_glossary_term_info_mcp, 
    create_glossary_ownership_mcp,
    create_glossary_status_mcp,
    create_glossary_global_tags_mcp,
    create_glossary_terms_mcp,
    create_glossary_browse_paths_mcp,
    create_glossary_institutional_memory_mcp,
    create_glossary_related_terms_mcp,
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
        # Debug: Log the incoming entity data structure
        logger.debug(f"Entity data type: {type(entity_data)}")
        logger.debug(f"Entity data keys: {list(entity_data.keys()) if isinstance(entity_data, dict) else 'Not a dict'}")
        logger.debug(f"Entity data: {entity_data}")
        
        # Map entity type to DataHub entity type
        datahub_entity_type = "glossaryNode" if entity_type == "node" else "glossaryTerm"
        
        # Extract entity information
        entity_urn = entity_data.get("urn")
        if not entity_urn:
            raise ValueError(f"{entity_type.title()} URN not found in entity data")
        
        # Extract entity properties
        entity_name = entity_data.get("name")
        if not entity_name:
            raise ValueError(f"{entity_type.title()} name not found in entity data")
        
        # Extract entity ID from URN or use name as fallback
        try:
            entity_id = extract_entity_id_from_urn(entity_urn, datahub_entity_type)
        except ValueError:
            # Fallback to using name as ID
            entity_id = entity_name.replace(" ", "_").lower()
        
        # Get description if available
        description = entity_data.get("description")
        
        # Get parent node URN if available
        parent_node_urn = None
        if entity_data.get("parent_urn"):
            parent_node_urn = entity_data["parent_urn"]
        elif entity_data.get("parent_id"):
            # Generate parent URN from parent ID
            parent_node_urn = generate_deterministic_urn(
                "glossaryNode", 
                str(entity_data["parent_id"]), 
                environment=environment, 
                mutation_name=mutation_name
            )
        
        # Determine output directory - use repo root metadata-manager instead of web_ui/metadata-manager
        if base_dir:
            output_dir = base_dir
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
            
            output_dir = os.path.join(repo_root, "metadata-manager", environment)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Use constant filename
        mcp_file_path = os.path.join(output_dir, "mcp_file.json")
        
        # Load existing MCP file or create new structure
        existing_mcp_data = {"mcps": [], "metadata": {"entities": []}}
        if os.path.exists(mcp_file_path):
            try:
                with open(mcp_file_path, "r") as f:
                    existing_mcp_data = json.load(f)
                    # Ensure required structure exists
                    if "mcps" not in existing_mcp_data:
                        existing_mcp_data["mcps"] = []
                    if "metadata" not in existing_mcp_data:
                        existing_mcp_data["metadata"] = {"entities": []}
                    if "entities" not in existing_mcp_data["metadata"]:
                        existing_mcp_data["metadata"]["entities"] = []
                logger.info(f"Loaded existing MCP file with {len(existing_mcp_data['mcps'])} existing MCPs")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load existing MCP file: {e}. Creating new file.")
                existing_mcp_data = {"mcps": [], "metadata": {"entities": []}}
        
        # Create new MCPs for this entity
        new_mcps = []
        
        # Create info MCP
        logger.info(f"Creating info MCP for {entity_type} '{entity_name}'...")
        if entity_type == "node":
            info_mcp = create_glossary_node_info_mcp(
                node_id=entity_id,
                owner=owner,
                node_name=entity_name,
                description=description,
                parent_node_urn=parent_node_urn,
                environment=environment,
                mutation_name=mutation_name
            )
        else:  # term
            # Get additional term-specific fields
            source_ref = entity_data.get("source_ref")
            
            info_mcp = create_glossary_term_info_mcp(
                term_id=entity_id,
                owner=owner,
                term_name=entity_name,
                description=description,
                parent_node_urn=parent_node_urn,
                source_ref=source_ref,
                environment=environment,
                mutation_name=mutation_name
            )
        
        new_mcps.append(info_mcp)
        
        # Create ownership MCP if ownership data exists
        ownership_data = entity_data.get("ownership_data") or entity_data.get("ownership")
        owners = []
        if isinstance(ownership_data, list):
            owners = ownership_data
        elif isinstance(ownership_data, dict) and "owners" in ownership_data:
            owners = ownership_data["owners"]
        # else: leave owners as empty list
        if owners:
            logger.info(f"Creating ownership MCP for {entity_type} '{entity_name}'...")
            logger.debug(f"Owners type: {type(owners)}")
            logger.debug(f"Owners: {owners}")
            first_owner = owners[0]
            logger.debug(f"First owner type: {type(first_owner)}")
            logger.debug(f"First owner: {first_owner}")
            # Check if first_owner is a dictionary or a string
            if isinstance(first_owner, dict):
                owner_urn = first_owner.get("owner_urn") or first_owner.get("ownerUrn") or first_owner.get("urn") or f"urn:li:corpuser:{owner}"
                # Try to get ownership type from dict, fallback to type, fallback to default
                ownership_type_urn = (
                    first_owner.get("ownership_type_urn") or
                    first_owner.get("type") or
                    (first_owner.get("ownershipType", {}).get("urn") if isinstance(first_owner.get("ownershipType"), dict) else None) or
                    "urn:li:ownershipType:dataowner"
                )
            elif isinstance(first_owner, str):
                owner_urn = first_owner
                ownership_type_urn = "urn:li:ownershipType:dataowner"
            else:
                logger.warning(f"Unexpected owner type: {type(first_owner)}, using default owner")
                owner_urn = f"urn:li:corpuser:{owner}"
                ownership_type_urn = "urn:li:ownershipType:dataowner"
            # Extract username from owner URN
            if owner_urn.startswith("urn:li:corpuser:"):
                owner_username = owner_urn.split(":")[-1]
            else:
                owner_username = owner
            ownership_mcp = create_glossary_ownership_mcp(
                entity_id=entity_id,
                entity_type=datahub_entity_type,
                owner=owner_username,
                ownership_type=ownership_type_urn,
                environment=environment,
                mutation_name=mutation_name
            )
            new_mcps.append(ownership_mcp)
        
        # Remove any existing MCPs for this entity URN to avoid duplicates
        entity_entity_urn = info_mcp.get("entityUrn")
        existing_mcp_data["mcps"] = [
            mcp for mcp in existing_mcp_data["mcps"] 
            if mcp.get("entityUrn") != entity_entity_urn
        ]
        
        # Remove existing metadata entry for this entity
        existing_mcp_data["metadata"]["entities"] = [
            entity for entity in existing_mcp_data["metadata"]["entities"]
            if entity.get("entity_urn") != entity_urn
        ]
        
        # Add new MCPs
        existing_mcp_data["mcps"].extend(new_mcps)
        
        # Add metadata entry for this entity
        entity_metadata = {
            "entity_name": entity_name,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "entity_urn": entity_urn,
            "datahub_entity_urn": entity_entity_urn,
            "environment": environment,
            "owner": owner,
            "updated_at": int(time.time() * 1000),
            "mcp_count": len(new_mcps)
        }
        existing_mcp_data["metadata"]["entities"].append(entity_metadata)
        
        # Update global metadata
        existing_mcp_data["metadata"].update({
            "total_mcps": len(existing_mcp_data["mcps"]),
            "total_entities": len(existing_mcp_data["metadata"]["entities"]),
            "last_updated": int(time.time() * 1000),
            "environment": environment
        })
        
        # Save updated MCP file
        mcp_saved = save_mcp_to_file(existing_mcp_data, mcp_file_path)
        
        created_files = {}
        if mcp_saved:
            created_files["mcp_file"] = mcp_file_path
        
        logger.info(f"Successfully added {entity_type} '{entity_name}' to staged changes with {len(new_mcps)} MCPs. Total MCPs in file: {len(existing_mcp_data['mcps'])}")
        
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
    
    node_id = node_data.get("id")
    if not node_id:
        raise ValueError("node_data must contain 'id' field")
    
    node_name = node_data.get("name", node_id)
    description = node_data.get("description")
    parent_node_urn = node_data.get("parent_urn")
    
    # Create output directory
    output_dir = os.path.join(base_dir, "glossary")
    os.makedirs(output_dir, exist_ok=True)
    
    created_files = {}
    
    # Core node info MCP (always created)
    node_info_mcp = create_glossary_node_info_mcp(
        node_id=node_id,
        owner=owner,
        node_name=node_name,
        description=description,
        parent_node_urn=parent_node_urn,
        environment=environment
    )
    
    node_info_path = os.path.join(output_dir, f"{node_id}_node_info.json")
    if save_mcp_to_file(node_info_mcp, node_info_path):
        created_files["glossaryNodeInfo"] = node_info_path
    
    # Core ownership MCP (always created)
    ownership_mcp = create_glossary_ownership_mcp(
        entity_id=node_id,
        entity_type="glossaryNode",
        owner=owner,
        environment=environment
    )
    
    ownership_path = os.path.join(output_dir, f"{node_id}_ownership.json")
    if save_mcp_to_file(ownership_mcp, ownership_path):
        created_files["ownership"] = ownership_path
    
    if include_all_aspects:
        # Status MCP
        status_mcp = create_glossary_status_mcp(
            entity_id=node_id,
            entity_type="glossaryNode",
            removed=node_data.get("removed", False),
            environment=environment
        )
        
        status_path = os.path.join(output_dir, f"{node_id}_status.json")
        if save_mcp_to_file(status_mcp, status_path):
            created_files["status"] = status_path
        
        # Global Tags MCP (if tags are provided)
        tags = node_data.get("tags", [])
        if tags:
            global_tags_mcp = create_glossary_global_tags_mcp(
                entity_id=node_id,
                entity_type="glossaryNode",
                tags=tags,
                owner=owner,
                environment=environment
            )
            
            tags_path = os.path.join(output_dir, f"{node_id}_global_tags.json")
            if save_mcp_to_file(global_tags_mcp, tags_path):
                created_files["globalTags"] = tags_path
        
        # Glossary Terms MCP (if glossary terms are provided)
        glossary_terms = node_data.get("glossary_terms", [])
        if glossary_terms:
            terms_mcp = create_glossary_terms_mcp(
                entity_id=node_id,
                entity_type="glossaryNode",
                glossary_terms=glossary_terms,
                owner=owner,
                environment=environment
            )
            
            terms_path = os.path.join(output_dir, f"{node_id}_glossary_terms.json")
            if save_mcp_to_file(terms_mcp, terms_path):
                created_files["glossaryTerms"] = terms_path
        
        # Browse Paths MCP (if browse paths are provided)
        browse_paths = node_data.get("browse_paths", [])
        if browse_paths:
            browse_paths_mcp = create_glossary_browse_paths_mcp(
                entity_id=node_id,
                entity_type="glossaryNode",
                browse_paths=browse_paths,
                environment=environment
            )
            
            browse_path = os.path.join(output_dir, f"{node_id}_browse_paths.json")
            if save_mcp_to_file(browse_paths_mcp, browse_path):
                created_files["browsePaths"] = browse_path
        
        # Institutional Memory MCP (if memory elements are provided)
        memory_elements = node_data.get("institutional_memory", [])
        if memory_elements:
            memory_mcp = create_glossary_institutional_memory_mcp(
                entity_id=node_id,
                entity_type="glossaryNode",
                memory_elements=memory_elements,
                owner=owner,
                environment=environment
            )
            
            memory_path = os.path.join(output_dir, f"{node_id}_institutional_memory.json")
            if save_mcp_to_file(memory_mcp, memory_path):
                created_files["institutionalMemory"] = memory_path
    
    # Handle custom aspects
    if custom_aspects:
        for aspect_name, aspect_data in custom_aspects.items():
            custom_path = os.path.join(output_dir, f"{node_id}_{aspect_name}.json")
            if save_mcp_to_file(aspect_data, custom_path):
                created_files[aspect_name] = custom_path
    
    logger.info(f"Created {len(created_files)} MCP files for glossary node '{node_id}'")
    return created_files


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
    
    term_id = term_data.get("id")
    if not term_id:
        raise ValueError("term_data must contain 'id' field")
    
    term_name = term_data.get("name", term_id)
    description = term_data.get("description")
    parent_node_urn = term_data.get("parent_urn")
    source_ref = term_data.get("source_ref")
    
    # Create output directory
    output_dir = os.path.join(base_dir, "glossary")
    os.makedirs(output_dir, exist_ok=True)
    
    created_files = {}
    
    # Core term info MCP (always created)
    term_info_mcp = create_glossary_term_info_mcp(
        term_id=term_id,
        owner=owner,
        term_name=term_name,
        description=description,
        parent_node_urn=parent_node_urn,
        source_ref=source_ref,
        environment=environment
    )
    
    term_info_path = os.path.join(output_dir, f"{term_id}_term_info.json")
    if save_mcp_to_file(term_info_mcp, term_info_path):
        created_files["glossaryTermInfo"] = term_info_path
    
    # Core ownership MCP (always created)
    ownership_mcp = create_glossary_ownership_mcp(
        entity_id=term_id,
        entity_type="glossaryTerm",
        owner=owner,
        environment=environment
    )
    
    ownership_path = os.path.join(output_dir, f"{term_id}_ownership.json")
    if save_mcp_to_file(ownership_mcp, ownership_path):
        created_files["ownership"] = ownership_path
    
    if include_all_aspects:
        # Status MCP
        status_mcp = create_glossary_status_mcp(
            entity_id=term_id,
            entity_type="glossaryTerm",
            removed=term_data.get("removed", False),
            environment=environment
        )
        
        status_path = os.path.join(output_dir, f"{term_id}_status.json")
        if save_mcp_to_file(status_mcp, status_path):
            created_files["status"] = status_path
        
        # Global Tags MCP (if tags are provided)
        tags = term_data.get("tags", [])
        if tags:
            global_tags_mcp = create_glossary_global_tags_mcp(
                entity_id=term_id,
                entity_type="glossaryTerm",
                tags=tags,
                owner=owner,
                environment=environment
            )
            
            tags_path = os.path.join(output_dir, f"{term_id}_global_tags.json")
            if save_mcp_to_file(global_tags_mcp, tags_path):
                created_files["globalTags"] = tags_path
        
        # Glossary Terms MCP (if glossary terms are provided)
        glossary_terms = term_data.get("glossary_terms", [])
        if glossary_terms:
            terms_mcp = create_glossary_terms_mcp(
                entity_id=term_id,
                entity_type="glossaryTerm",
                glossary_terms=glossary_terms,
                owner=owner,
                environment=environment
            )
            
            terms_path = os.path.join(output_dir, f"{term_id}_glossary_terms.json")
            if save_mcp_to_file(terms_mcp, terms_path):
                created_files["glossaryTerms"] = terms_path
        
        # Browse Paths MCP (if browse paths are provided)
        browse_paths = term_data.get("browse_paths", [])
        if browse_paths:
            browse_paths_mcp = create_glossary_browse_paths_mcp(
                entity_id=term_id,
                entity_type="glossaryTerm",
                browse_paths=browse_paths,
                environment=environment
            )
            
            browse_path = os.path.join(output_dir, f"{term_id}_browse_paths.json")
            if save_mcp_to_file(browse_paths_mcp, browse_path):
                created_files["browsePaths"] = browse_path
        
        # Institutional Memory MCP (if memory elements are provided)
        memory_elements = term_data.get("institutional_memory", [])
        if memory_elements:
            memory_mcp = create_glossary_institutional_memory_mcp(
                entity_id=term_id,
                entity_type="glossaryTerm",
                memory_elements=memory_elements,
                owner=owner,
                environment=environment
            )
            
            memory_path = os.path.join(output_dir, f"{term_id}_institutional_memory.json")
            if save_mcp_to_file(memory_mcp, memory_path):
                created_files["institutionalMemory"] = memory_path
        
        # Related Terms MCP (if related terms are provided)
        related_terms = term_data.get("related_terms", [])
        if related_terms:
            related_terms_mcp = create_glossary_related_terms_mcp(
                term_id=term_id,
                related_terms=related_terms,
                environment=environment
            )
            
            related_path = os.path.join(output_dir, f"{term_id}_related_terms.json")
            if save_mcp_to_file(related_terms_mcp, related_path):
                created_files["glossaryRelatedTerms"] = related_path
    
    # Handle custom aspects
    if custom_aspects:
        for aspect_name, aspect_data in custom_aspects.items():
            custom_path = os.path.join(output_dir, f"{term_id}_{aspect_name}.json")
            if save_mcp_to_file(aspect_data, custom_path):
                created_files[aspect_name] = custom_path
    
    logger.info(f"Created {len(created_files)} MCP files for glossary term '{term_id}'")
    return created_files


 