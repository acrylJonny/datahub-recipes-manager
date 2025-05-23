#!/usr/bin/env python3
"""
Common utilities for glossary operations.
"""

import json
import logging
import os
import sys
from typing import Dict, Any, List, Optional, Tuple

# Add the parent directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.datahub_metadata_api import DataHubMetadataApiClient

logger = logging.getLogger(__name__)


# ============================
# Glossary Node Utilities
# ============================

def load_glossary_node_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load glossary node definition from a JSON file
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing glossary node definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Check if the file contains a node directly or wrapped in a container
        if "glossaryNode" in data:
            return data["glossaryNode"]
        elif "properties" in data:
            return data
        else:
            logger.warning("Glossary node definition not found in expected format, using entire file content")
            return data
    except Exception as e:
        logger.error(f"Error loading glossary node from file: {str(e)}")
        raise


def save_glossary_node_to_file(node: Dict[str, Any], file_path: str, pretty_print: bool = True) -> bool:
    """
    Save glossary node definition to a JSON file
    
    Args:
        node: Glossary node data to save
        file_path: Path to save the glossary node data
        pretty_print: Whether to format the JSON with indentation
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        with open(file_path, "w") as f:
            json.dump(node, f, indent=4 if pretty_print else None)
        return True
    except Exception as e:
        logger.error(f"Error saving glossary node to file: {str(e)}")
        return False


def get_glossary_node_name(node: Dict[str, Any]) -> str:
    """
    Get the name of a glossary node
    
    Args:
        node: Glossary node data
        
    Returns:
        Name of the glossary node or empty string if not found
    """
    if not node:
        return ""
    
    if "properties" in node and "name" in node["properties"]:
        return node["properties"]["name"]
    
    if "name" in node:
        return node["name"]
    
    if "urn" in node:
        # For glossary node URNs like urn:li:glossaryNode:example
        return node["urn"].split(":")[-1]
    
    return ""


def check_glossary_node_dependencies(client: DataHubMetadataApiClient, node_urn: str) -> bool:
    """
    Check if a glossary node has child nodes or terms
    
    Args:
        client: DataHub metadata client
        node_urn: URN of the glossary node
        
    Returns:
        True if node has children, False otherwise
    """
    node_with_children = client.export_glossary_node(node_urn, include_children=True)
    return node_with_children is not None and "children" in node_with_children and len(node_with_children["children"].get("relationships", [])) > 0


# ============================
# Glossary Term Utilities
# ============================

def load_glossary_term_from_file(file_path: str) -> Dict[str, Any]:
    """
    Load glossary term definition from a JSON file
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Dictionary containing glossary term definition
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Check if the file contains a term directly or wrapped in a container
        if "glossaryTerm" in data:
            return data["glossaryTerm"]
        elif "properties" in data:
            return data
        else:
            logger.warning("Glossary term definition not found in expected format, using entire file content")
            return data
    except Exception as e:
        logger.error(f"Error loading glossary term from file: {str(e)}")
        raise


def save_glossary_term_to_file(term: Dict[str, Any], file_path: str, pretty_print: bool = True) -> bool:
    """
    Save glossary term definition to a JSON file
    
    Args:
        term: Glossary term data to save
        file_path: Path to save the glossary term data
        pretty_print: Whether to format the JSON with indentation
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(file_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        with open(file_path, "w") as f:
            json.dump(term, f, indent=4 if pretty_print else None)
        return True
    except Exception as e:
        logger.error(f"Error saving glossary term to file: {str(e)}")
        return False


def get_glossary_term_name(term: Dict[str, Any]) -> str:
    """
    Get the name of a glossary term
    
    Args:
        term: Glossary term data
        
    Returns:
        Name of the glossary term or empty string if not found
    """
    if not term:
        return ""
    
    if "name" in term:
        return term["name"]
    
    if "properties" in term and "name" in term["properties"]:
        return term["properties"]["name"]
    
    if "urn" in term:
        # For glossary term URNs like urn:li:glossaryTerm:example
        return term["urn"].split(":")[-1]
    
    return ""


def get_glossary_term_hierarchical_name(term: Dict[str, Any]) -> str:
    """
    Get the hierarchical name of a glossary term
    
    Args:
        term: Glossary term data
        
    Returns:
        Hierarchical name of the glossary term or empty string if not found
    """
    if not term:
        return ""
    
    if "hierarchicalName" in term:
        return term["hierarchicalName"]
    
    return get_glossary_term_name(term)


def check_glossary_term_dependencies(client: DataHubMetadataApiClient, term_urn: str) -> bool:
    """
    Check if a glossary term has entity associations
    
    Args:
        client: DataHub metadata client
        term_urn: URN of the glossary term
        
    Returns:
        True if term has associated entities, False otherwise
    """
    term_with_entities = client.export_glossary_term(term_urn, include_related=True)
    return term_with_entities is not None and "relatedEntities" in term_with_entities and len(term_with_entities["relatedEntities"]) > 0 