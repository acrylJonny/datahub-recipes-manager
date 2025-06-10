#!/usr/bin/env python3
"""
Utility functions for working with DataHub URNs.

This module provides functions for generating deterministic URNs for DataHub entities
to ensure consistency across different environments.
"""

import hashlib
import json
import re
from typing import Dict, Any, Optional


def generate_deterministic_urn(
    entity_type: str, name: str, namespace: Optional[str] = None
) -> str:
    """
    Generate a deterministic URN for a DataHub entity using MD5 hashing.

    Args:
        entity_type: The type of entity (tag, glossaryTerm, glossaryNode, domain)
        name: The name of the entity
        namespace: Optional namespace for scoping (e.g., parent node for glossary terms)

    Returns:
        A deterministic URN for the entity
    """
    # Ensure inputs are strings
    entity_type = str(entity_type) if entity_type is not None else ""
    name = str(name) if name is not None else ""

    # Normalize inputs to ensure consistency
    entity_type = entity_type.lower().strip()
    name = name.lower().strip()

    # Combine the inputs to create a unique identifier string
    unique_id_string = f"{entity_type}:{name}"
    if namespace:
        namespace = str(namespace) if namespace is not None else ""
        namespace = namespace.lower().strip()
        unique_id_string = f"{namespace}:{unique_id_string}"

    # Generate MD5 hash of the combined string
    md5_hash = hashlib.md5(unique_id_string.encode("utf-8")).hexdigest()

    # Map entity type to the URN format
    type_map = {
        "tag": "tag",
        "glossaryterm": "glossaryTerm",
        "glossarynode": "glossaryNode",
        "domain": "domain",
        "assertion": "assertion",
        "dataproduct": "dataProduct",
    }

    urn_type = type_map.get(entity_type, entity_type)

    # Return the URN in the DataHub format
    return f"urn:li:{urn_type}:{md5_hash}"


def get_full_urn_from_name(
    entity_type: str, name: str, namespace: Optional[str] = None
) -> str:
    """
    Get a full URN for an entity based on its name and type.

    Args:
        entity_type: The type of entity (tag, glossaryTerm, glossaryNode, domain)
        name: The name of the entity
        namespace: Optional namespace for scoping

    Returns:
        The full URN for the entity
    """
    return generate_deterministic_urn(entity_type, name, namespace)


def extract_name_from_properties(entity_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract the name of an entity from its properties.

    Args:
        entity_data: The entity data dictionary

    Returns:
        The name of the entity, or None if not found
    """
    # Different entity types store their names in different places
    if "properties" in entity_data:
        if "name" in entity_data["properties"]:
            return entity_data["properties"]["name"]

    if "info" in entity_data and "name" in entity_data["info"]:
        return entity_data["info"]["name"]

    if "name" in entity_data:
        return entity_data["name"]

    return None


def get_parent_path(entity_data: Dict[str, Any]) -> Optional[str]:
    """
    Get the parent path of an entity (for glossary terms/nodes).

    Args:
        entity_data: The entity data dictionary

    Returns:
        The parent path of the entity, or None if not found
    """
    if "parentNodes" in entity_data and "nodes" in entity_data["parentNodes"]:
        parent_nodes = entity_data["parentNodes"]["nodes"]
        if parent_nodes and len(parent_nodes) > 0:
            # Use the first parent node as the namespace
            return parent_nodes[0].get("urn")

    return None


def generate_deterministic_structured_property_id(
    entity_urn: str, property_name: str
) -> str:
    """
    Generate a deterministic ID for a structured property.

    Args:
        entity_urn: The URN of the entity
        property_name: The name of the property

    Returns:
        A deterministic ID for the structured property
    """
    # Ensure inputs are strings
    entity_urn = str(entity_urn) if entity_urn is not None else ""
    property_name = str(property_name) if property_name is not None else ""

    # Normalize inputs
    entity_urn = entity_urn.strip()
    property_name = property_name.lower().strip()

    # Combine inputs for a unique string
    unique_string = f"{entity_urn}:{property_name}"

    # Generate a deterministic ID using MD5
    return hashlib.md5(unique_string.encode()).hexdigest()


def sanitize_name(name):
    """Sanitize a name to be used in a URN"""
    if not name:
        return ""
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9]", "_", name)
    # Remove duplicate underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    # Remove leading and trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized.lower()



    # Traverse up the tree to get the full path
    while current:
        path_parts.append(sanitize_name(current.name))
        current = current.parent

    # Reverse the path to get root->node order and join with dots
    path_parts.reverse()
    return ".".join(path_parts)


# New URN mutation and environment-aware functions


def datahub_guid(obj: dict) -> str:
    """
    Generate a DataHub GUID from a dictionary of parameters.
    Uses MD5 hash of alphabetically sorted JSON representation.

    Args:
        obj (dict): Parameters for GUID generation

    Returns:
        str: MD5 hash (hexadecimal)
    """
    json_key = json.dumps(obj, separators=(",", ":"), sort_keys=True)
    md5_hash = hashlib.md5(json_key.encode("utf-8"))
    return str(md5_hash.hexdigest())


def recreate_container_id(container_type: str, **params) -> str:
    """
    Recreate a container ID based on container type and parameters.
    Used for consistent URN generation across environments.

    Args:
        container_type (str): Type of container (database, schema, etc.)
        **params: Container parameters (platform, instance, etc.)

    Returns:
        str: Container ID (MD5 hash)
    """
    # Define required and optional parameters by container type
    if container_type == "database":
        required = ["platform", "database"]
        optional = ["instance"]
    elif container_type == "schema":
        required = ["platform", "database", "schema"]
        optional = ["instance"]
    elif container_type == "container":
        required = ["platform"]
        optional = ["instance", "database", "schema"]
    else:
        raise ValueError(f"Unsupported container type: {container_type}")

    # Check required parameters
    for param in required:
        if param not in params:
            raise ValueError(f"Missing required parameter: {param}")

    # Create parameter dictionary with only allowed parameters
    allowed_params = required + optional
    param_dict = {
        k: v for k, v in params.items() if k in allowed_params and v is not None
    }

    # Generate the GUID
    return datahub_guid(param_dict)


def get_environment_aware_urn(
    entity_type, name, environment_config=None, container_params=None, parent_path=None
):
    """
    Generate an environment-aware URN for an entity.
    Can use either standard name-based URN or container-based URN with MD5 hash.

    Args:
        entity_type (str): Type of entity (tag, glossaryTerm, etc.)
        name (str): Entity name
        environment_config (dict, optional): Environment configuration
        container_params (dict, optional): Container parameters for MD5-based URNs
        parent_path (str, optional): Parent path for nested entities

    Returns:
        str: Full URN for the entity
    """
    # If no environment config or container params, use standard URN generation
    if not environment_config or not container_params:
        return get_full_urn_from_name(entity_type, name, parent_path)

    # Generate MD5 hash for container-based entities
    if entity_type in ["container", "dataset", "dataFlow", "dataJob"]:
        container_type = container_params.get("type", "container")
        guid = recreate_container_id(container_type, **container_params)
        return f"urn:li:{entity_type}:{guid}"

    # For other entity types, use standard URN generation but include environment info
    # in the sanitized name if available
    if environment_config and environment_config.get("name"):
        env_prefix = sanitize_name(environment_config.get("name"))
        sanitized_name = f"{env_prefix}_{sanitize_name(name)}"

        if parent_path:
            parent_path = f"{parent_path}"
            return f"urn:li:{entity_type}:{sanitized_name}.{parent_path}"
        else:
            return f"urn:li:{entity_type}:{sanitized_name}"

    # Default to standard URN generation
    return get_full_urn_from_name(entity_type, name, parent_path)


def apply_entity_mutation(entity, entity_type, environment_config=None):
    """
    Apply mutations to an entity based on its type and environment configuration.
    Primarily affects the deterministic URN generation.

    Args:
        entity: The entity to mutate
        entity_type (str): Type of entity (tag, glossaryTerm, etc.)
        environment_config (dict, optional): Environment configuration

    Returns:
        entity: The mutated entity
    """
    # Skip if no entity or no environment
    if not entity or not environment_config:
        return entity

    # Prepare container parameters if this is a container-related entity
    container_params = None
    if hasattr(entity, "container_params") and entity.container_params:
        container_params = entity.container_params

    # Get parent path for nested entities
    parent_path = None
    if hasattr(entity, "parent") and entity.parent:
        parent_path = get_parent_path(entity.parent)
    elif hasattr(entity, "parent_node") and entity.parent_node:
        parent_path = get_parent_path(entity.parent_node)

    # Generate new URN based on environment
    new_urn = get_environment_aware_urn(
        entity_type=entity_type,
        name=entity.name,
        environment_config=environment_config,
        container_params=container_params,
        parent_path=parent_path,
    )

    # Update entity's deterministic URN
    entity.deterministic_urn = new_urn

    # Set the environment reference
    if hasattr(entity, "environment_id") and environment_config.get("id"):
        entity.environment_id = environment_config.get("id")

    return entity
