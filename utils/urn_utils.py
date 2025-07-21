#!/usr/bin/env python3
"""
Utility functions for working with DataHub URNs.

This module provides functions for generating deterministic URNs for DataHub entities
to ensure consistency across different environments.
"""

import hashlib
import json
import re
import logging
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)


def generate_deterministic_urn(
    entity_type: str, name: str, namespace: Optional[str] = None, environment: Optional[str] = None, mutation_name: Optional[str] = None
) -> str:
    """
    Generate a deterministic URN for a DataHub entity using MD5 hashing.
    If mutation_name is not specified, the original URN format will be preserved.

    Args:
        entity_type: The type of entity (tag, glossaryTerm, glossaryNode, domain)
        name: The name of the entity
        namespace: Optional namespace for scoping (e.g., parent node for glossary terms)
        environment: Optional environment name (deprecated, use mutation_name instead)
        mutation_name: Optional mutation name for environment-specific URNs

    Returns:
        A deterministic URN for the entity
    """
    # Ensure inputs are strings
    entity_type = str(entity_type) if entity_type is not None else ""
    name = str(name) if name is not None else ""

    # Normalize inputs to ensure consistency
    entity_type = entity_type.lower().strip()
    name = name.lower().strip()

    # Map entity type to the URN format
    type_map = {
        "tag": "tag",
        "glossaryterm": "glossaryTerm",
        "glossarynode": "glossaryNode",
        "domain": "domain",
        "assertion": "assertion",
        "dataproduct": "dataProduct",
        "structuredproperty": "structuredProperty",
    }

    # Use the proper entity type
    normalized_type = type_map.get(entity_type, entity_type)

    # Choose environment name
    env_name = mutation_name or environment or "dev"

    # Generate base URN
    if normalized_type == "glossaryTerm" and namespace:
        # For glossary terms with a parent node, include the namespace
        base_urn = f"urn:li:{normalized_type}:{namespace}.{name}"
    else:
        base_urn = f"urn:li:{normalized_type}:{name}"

    # If no environment/mutation specified, return the base URN
    if not env_name or env_name == "dev":
        return base_urn

    # Hash the combination of environment and entity details
    hash_input = f"{env_name}_{normalized_type}_{name}"
    if namespace:
        hash_input += f"_{namespace}"

    hash_object = hashlib.md5(hash_input.encode())
    hash_hex = hash_object.hexdigest()

    # Format as UUID with dashes (8-4-4-4-12 pattern)
    # Use full 32 characters like DataHub UUIDs
    uuid_formatted = f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}"

    # Return the hashed URN format
    return f"urn:li:{normalized_type}:{uuid_formatted}"


def generate_unified_urn(
    entity_type: str, 
    entity_name: str, 
    environment: str = "dev", 
    mutation_name: Optional[str] = None
) -> str:
    """
    **UNIFIED URN GENERATION METHOD**
    
    This is the single, consistent method that should be used by ALL MCP scripts 
    and export functions for generating URNs. It ensures consistency across
    all entity types and all parts of the system.
    
    Args:
        entity_type: The DataHub entity type (domain, tag, glossaryTerm, etc.)
        entity_name: The name/identifier of the entity
        environment: The environment name (default: "dev")
        mutation_name: Optional mutation name (defaults to environment)
        
    Returns:
        str: The properly mutated URN for the entity
        
    Example:
        urn = generate_unified_urn("domain", "Customer Data", "dev")
        # Returns: urn:li:domain:5d13a50b6fbdc1f9 (if mutations are enabled)
        # Or:      urn:li:domain:customer-data (if mutations are disabled)
    """
    try:
        # Use mutation_name if provided, otherwise use environment
        env_name = mutation_name or environment or "dev"
        
        # Generate base URN using existing function
        base_urn = get_full_urn_from_name(entity_type, entity_name)
        
        # Get mutation configuration for the environment
        mutation_config = get_mutation_config_for_environment(env_name)
        
        # Use the unified mutation system
        mutated_urn = generate_mutated_urn(base_urn, env_name, entity_type, mutation_config)
        
        return mutated_urn
        
    except Exception as e:
        logger.error(f"Error in generate_unified_urn for {entity_type}/{entity_name}: {str(e)}")
        # Fall back to basic URN if there's an error
        return get_full_urn_from_name(entity_type, entity_name)


def generate_urn_for_new_entity(
    entity_type: str, 
    entity_name: str, 
    environment: str = "dev", 
    mutation_name: Optional[str] = None
) -> str:
    """
    Generate a URN for a NEW entity that doesn't exist yet.
    
    This should be used when creating truly new entities in the web UI 
    where no existing URN is available.
    
    Args:
        entity_type: The DataHub entity type (domain, tag, glossaryTerm, etc.)
        entity_name: The name/identifier of the new entity
        environment: The environment name (default: "dev")
        mutation_name: Optional mutation name (defaults to environment)
        
    Returns:
        str: The URN for the new entity
    """
    return generate_deterministic_urn(entity_type, entity_name, environment=environment, mutation_name=mutation_name)


def apply_urn_mutations_for_existing_entity(
    existing_urn: str, 
    environment: str = "dev", 
    mutation_name: Optional[str] = None
) -> str:
    """
    Apply environment mutations to an EXISTING entity's URN.
    
    This should be used when you already have an entity's URN and want to 
    apply environment-specific mutations to it. This preserves entity identity
    even if the entity's name has changed.
    
    Args:
        existing_urn: The existing URN of the entity
        environment: The target environment name
        mutation_name: Optional mutation name (defaults to environment)
        
    Returns:
        str: The mutated URN for the target environment
        
    Example:
        # Entity exists with URN: urn:li:domain:customer-data
        # Apply dev environment mutations:
        mutated = apply_urn_mutations_for_existing_entity(
            "urn:li:domain:customer-data", "dev"
        )
        # Returns: urn:li:domain:5d13a50b6fbdc1f9 (if mutations enabled)
    """
    if not existing_urn:
        logger.warning("Cannot apply mutations to empty URN")
        return existing_urn
    
    try:
        # Extract entity type from URN
        entity_type = _extract_entity_type_from_urn(existing_urn)
        if not entity_type:
            logger.warning(f"Could not extract entity type from URN: {existing_urn}")
            return existing_urn
        
        # Use mutation_name if provided, otherwise use environment
        env_name = mutation_name or environment or "dev"
        
        # Get mutation configuration for the environment
        mutation_config = get_mutation_config_for_environment(env_name)
        
        # Apply mutations to the existing URN
        mutated_urn = generate_mutated_urn(existing_urn, env_name, entity_type, mutation_config)
        
        return mutated_urn
        
    except Exception as e:
        logger.error(f"Error applying mutations to URN {existing_urn}: {str(e)}")
        return existing_urn


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


def apply_entity_mutation(entity, entity_type, environment_config=None):
    """
    Apply mutations to an entity based on its type and environment configuration.
    Primarily affects the deterministic URN generation.

    Args:
        entity: The entity to mutate
        entity_type (str): Type of entity (tag, glossaryTerm, etc.)
        environment_config (dict, optional): Environment configuration

    Returns:
        The mutated entity
    """
    # This is a placeholder for future entity mutation logic
    # Currently, mutations are handled primarily through URN generation
    return entity


# Django-specific URN mutation functions
# These functions require Django models and should only be used within the Django app


def generate_mutated_urn(
    input_urn: str, 
    environment_name: str, 
    entity_type: str,
    mutation_config: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a mutated URN based on environment and mutation configuration.
    
    This function creates a consistent URN generation system that:
    1. Creates MD5-based URNs when mutation checkboxes are enabled for specific entity types
    2. Uses environment name + input URN to generate the MD5 hash
    3. Returns original URN when checkboxes are unchecked or no mutations exist
    
    Args:
        input_urn: The original URN to potentially transform
        environment_name: The target environment name
        entity_type: The type of entity (tag, glossaryTerm, glossaryNode, structuredProperty, domain, dataProduct)
        mutation_config: Dictionary containing mutation settings (optional)
        
    Returns:
        str: Either the transformed URN (if mutations apply) or the original URN
    """
    if not input_urn or not environment_name:
        return input_urn
    
    # If no mutation config provided, return original URN
    if not mutation_config:
        return input_urn
    
    # Map entity types to their corresponding mutation flags
    entity_type_mapping = {
        'tag': 'apply_to_tags',
        'glossaryTerm': 'apply_to_glossary_terms',
        'glossaryNode': 'apply_to_glossary_nodes',
        'structuredProperty': 'apply_to_structured_properties',
        'domain': 'apply_to_domains',
        'dataProduct': 'apply_to_data_products',
    }
    
    # Get the mutation flag for this entity type
    mutation_flag = entity_type_mapping.get(entity_type)
    if not mutation_flag:
        logger.warning(f"Unknown entity type for mutation: {entity_type}")
        return input_urn
    
    # Check if mutations should be applied for this entity type
    should_apply_mutation = mutation_config.get(mutation_flag, False)
    if not should_apply_mutation:
        return input_urn
    
    # Generate MD5-based URN
    try:
        # Create input string for MD5: environment_name + input_urn
        hash_input = f"{environment_name}_{input_urn}"
        
        # Generate MD5 hash
        md5_hash = hashlib.md5(hash_input.encode('utf-8')).hexdigest()
        
        # Create new URN based on entity type
        mutated_urn = _create_mutated_urn_by_type(entity_type, md5_hash, input_urn)
        
        logger.info(f"Generated mutated URN for {entity_type}: {input_urn} -> {mutated_urn}")
        return mutated_urn
        
    except Exception as e:
        logger.error(f"Error generating mutated URN for {input_urn}: {str(e)}")
        return input_urn


def _create_mutated_urn_by_type(entity_type: str, md5_hash: str, original_urn: str) -> str:
    """
    Create a mutated URN based on the entity type and MD5 hash.
    
    Args:
        entity_type: The type of entity
        md5_hash: The MD5 hash to use in the URN
        original_urn: The original URN for reference
        
    Returns:
        str: The mutated URN
    """
    # Format MD5 hash as UUID with dashes (8-4-4-4-12 pattern)
    # Use full 32 characters like DataHub UUIDs
    uuid_formatted = f"{md5_hash[:8]}-{md5_hash[8:12]}-{md5_hash[12:16]}-{md5_hash[16:20]}-{md5_hash[20:32]}"
    
    if entity_type == 'tag':
        return f"urn:li:tag:{uuid_formatted}"
    elif entity_type == 'glossaryTerm':
        return f"urn:li:glossaryTerm:{uuid_formatted}"
    elif entity_type == 'glossaryNode':
        return f"urn:li:glossaryNode:{uuid_formatted}"
    elif entity_type == 'structuredProperty':
        return f"urn:li:structuredProperty:{uuid_formatted}"
    elif entity_type == 'domain':
        return f"urn:li:domain:{uuid_formatted}"
    elif entity_type == 'dataProduct':
        return f"urn:li:dataProduct:{uuid_formatted}"
    else:
        # Fallback: try to preserve the original URN structure but replace the identifier
        if ':' in original_urn:
            parts = original_urn.split(':')
            if len(parts) >= 3:
                parts[-1] = uuid_formatted
                return ':'.join(parts)
        
        # Final fallback
        return f"urn:li:{entity_type}:{uuid_formatted}"


def get_mutation_config_for_environment(environment_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the mutation configuration for a specific environment.
    
    Args:
        environment_name: The environment name to get mutations for
        
    Returns:
        Dict containing mutation configuration or None if not found
    """
    try:
        from web_ui.models import Environment, Mutation
        
        # Get the environment
        environment = Environment.objects.filter(name=environment_name).first()
        if not environment:
            logger.warning(f"Environment '{environment_name}' not found")
            return None
        
        # Get the associated mutation (assuming one mutation per environment for now)
        # You might need to adjust this logic based on your specific requirements
        mutation = Mutation.objects.filter(
            name__icontains=environment_name
        ).first() or Mutation.objects.first()
        
        if not mutation:
            logger.warning(f"No mutation configuration found for environment '{environment_name}'")
            return None
        
        return {
            'apply_to_tags': mutation.apply_to_tags,
            'apply_to_glossary_terms': mutation.apply_to_glossary_terms,
            'apply_to_glossary_nodes': mutation.apply_to_glossary_nodes,
            'apply_to_structured_properties': mutation.apply_to_structured_properties,
            'apply_to_domains': mutation.apply_to_domains,
            'apply_to_data_products': mutation.apply_to_data_products,
            'platform_instance_mapping': mutation.platform_instance_mapping,
            'custom_properties': mutation.custom_properties,
        }
        
    except Exception as e:
        logger.error(f"Error getting mutation config for environment '{environment_name}': {str(e)}")
        return None


def apply_urn_mutations_to_entity(entity_data: Dict[str, Any], environment_name: str) -> Dict[str, Any]:
    """
    Apply URN mutations to an entity's data structure.
    
    This function will look for URN fields in the entity data and apply mutations
    to them based on the environment configuration.
    
    Args:
        entity_data: The entity data dictionary
        environment_name: The target environment name
        
    Returns:
        Dict: The entity data with mutated URNs
    """
    if not entity_data or not environment_name:
        return entity_data
    
    # Get mutation configuration
    mutation_config = get_mutation_config_for_environment(environment_name)
    if not mutation_config:
        return entity_data
    
    # Create a copy to avoid modifying the original
    mutated_data = entity_data.copy()
    
    # Apply mutations to common URN fields
    urn_fields = ['urn', 'tag_urn', 'term_urn', 'node_urn', 'property_urn', 'domain_urn']
    
    for field in urn_fields:
        if field in mutated_data and mutated_data[field]:
            original_urn = mutated_data[field]
            
            # Determine entity type from URN or field name
            entity_type = _determine_entity_type_from_urn_or_field(original_urn, field)
            
            # Apply mutation
            mutated_urn = generate_mutated_urn(
                original_urn, 
                environment_name, 
                entity_type, 
                mutation_config
            )
            
            mutated_data[field] = mutated_urn
    
    return mutated_data


def _determine_entity_type_from_urn_or_field(urn: str, field_name: str) -> str:
    """
    Determine the entity type from URN or field name.
    
    Args:
        urn: The URN string
        field_name: The field name containing the URN
        
    Returns:
        str: The entity type
    """
    # Try to extract from URN first
    if ':' in urn:
        parts = urn.split(':')
        if len(parts) >= 3:
            urn_type = parts[2]  # e.g., 'tag', 'glossaryTerm', etc.
            if urn_type in ['tag', 'glossaryTerm', 'glossaryNode', 'structuredProperty', 'domain', 'dataProduct']:
                return urn_type
    
    # Fallback to field name mapping
    field_to_type = {
        'tag_urn': 'tag',
        'term_urn': 'glossaryTerm',
        'node_urn': 'glossaryNode',
        'property_urn': 'structuredProperty',
        'domain_urn': 'domain',
    }
    
    return field_to_type.get(field_name, 'unknown')


# Convenience functions for specific entity types
def generate_tag_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated tag URN."""
    return generate_mutated_urn(input_urn, environment_name, 'tag', mutation_config)


def generate_glossary_term_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated glossary term URN."""
    return generate_mutated_urn(input_urn, environment_name, 'glossaryTerm', mutation_config)


def generate_glossary_node_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated glossary node URN."""
    return generate_mutated_urn(input_urn, environment_name, 'glossaryNode', mutation_config)


def generate_structured_property_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated structured property URN."""
    return generate_mutated_urn(input_urn, environment_name, 'structuredProperty', mutation_config)


def generate_domain_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated domain URN."""
    return generate_mutated_urn(input_urn, environment_name, 'domain', mutation_config)


def generate_data_product_urn(input_urn: str, environment_name: str, mutation_config: Optional[Dict[str, Any]] = None) -> str:
    """Generate a mutated data product URN."""
    return generate_mutated_urn(input_urn, environment_name, 'dataProduct', mutation_config)


def apply_urn_mutations_to_associations(
    entity_data: Dict[str, Any], 
    environment_name: str,
    mutation_config: Optional[Dict[str, Any]] = None,
    mutate_main_entity_urn: bool = True
) -> Dict[str, Any]:
    """
    Apply URN mutations to entity associations (tags, glossary terms, etc.).
    
    Args:
        entity_data: The entity data dictionary
        environment_name: The target environment name
        mutation_config: Optional mutation configuration (will be fetched if not provided)
        mutate_main_entity_urn: If False, skips mutations to the main entity URN (for editable entities)
        
    Returns:
        Dict: The entity data with mutated association URNs
    """
    if not entity_data or not environment_name:
        return entity_data
    
    # Get mutation configuration if not provided
    if not mutation_config:
        mutation_config = get_mutation_config_for_environment(environment_name)
        if not mutation_config:
            return entity_data
    
    # Create a copy to avoid modifying the original
    mutated_data = entity_data.copy()
    
    # Apply mutations to different types of associations
    mutated_data = _mutate_tag_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_glossary_term_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_structured_property_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_domain_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_parent_node_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_related_term_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_ownership_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_form_associations(mutated_data, environment_name, mutation_config)
    mutated_data = _mutate_test_associations(mutated_data, environment_name, mutation_config)
    # Only mutate platform instance in main entity URN if requested
    if mutate_main_entity_urn:
        mutated_data = _mutate_platform_instance(mutated_data, environment_name, mutation_config)
    else:
        # Apply platform instance mutations only to associations, not main URN
        mutated_data = _mutate_platform_instance_associations_only(mutated_data, environment_name, mutation_config)
    
    return mutated_data


def _extract_entity_name_from_urn(urn: str) -> Optional[str]:
    """
    Extract entity name from a DataHub URN.
    
    Args:
        urn: DataHub URN (e.g., "urn:li:tag:PII" or "urn:li:domain:customer-data")
        
    Returns:
        Entity name if extractable, None otherwise
    """
    if not urn or not isinstance(urn, str):
        return None
    
    try:
        # Standard DataHub URN format: urn:li:entityType:name
        parts = urn.split(':')
        if len(parts) >= 4 and parts[0] == 'urn' and parts[1] == 'li':
            # Join remaining parts in case the name contains colons
            entity_name = ':'.join(parts[3:])
            return entity_name
    except Exception:
        pass
    
    return None


def _extract_domain_name_from_entity_data(entity_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract human-readable domain name from entity data.
    
    Args:
        entity_data: Entity data dictionary
        
    Returns:
        Domain name if extractable, None otherwise
    """
    # Try to extract domain name from browse path
    if 'browsePathV2' in entity_data and 'path' in entity_data['browsePathV2']:
        path = entity_data['browsePathV2']['path']
        if len(path) > 1:  # Skip first empty entity, look for domain info
            for path_component in path[1:]:
                if 'entity' in path_component and 'properties' in path_component['entity']:
                    properties = path_component['entity']['properties']
                    if 'name' in properties:
                        domain_name = properties['name']
                        logger.debug(f"Extracted domain name from browse path: {domain_name}")
                        return domain_name
    
    # Try to extract from browsePaths (legacy format)
    if 'browsePaths' in entity_data and entity_data['browsePaths']:
        # Take the first browse path and extract potential domain name
        browse_path = entity_data['browsePaths'][0]
        if browse_path.startswith('/'):
            path_parts = browse_path.strip('/').split('/')
            if path_parts and path_parts[0]:
                domain_name = path_parts[0]
                logger.debug(f"Extracted domain name from legacy browse path: {domain_name}")
                return domain_name
    
    # If we can't extract domain name from entity data, return None
    logger.debug("Could not extract domain name from entity data")
    return None


def _generate_consistent_mutated_urn(
    original_urn: str, 
    entity_type: str, 
    environment_name: str, 
    mutation_config: Dict[str, Any],
    entity_data: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate a consistent mutated URN by extracting the entity name and generating a base URN.
    
    Args:
        original_urn: Original URN from DataHub
        entity_type: Type of entity (tag, glossaryTerm, etc.)
        environment_name: Target environment name
        mutation_config: Mutation configuration
        entity_data: Optional entity data for extracting human-readable names
        
    Returns:
        Consistently mutated URN
    """
    # For consistent mutations, always use the entity ID from the URN 
    # Don't use human-readable names from entity data (like browse paths) as they can vary
    entity_name = _extract_entity_name_from_urn(original_urn)
    
    if entity_name:
        # Generate base URN using the same system as the pages
        base_urn = get_full_urn_from_name(entity_type, entity_name)
        mutated_urn = generate_mutated_urn(base_urn, environment_name, entity_type, mutation_config)
        logger.debug(f"Generated consistent {entity_type} URN: {entity_name} -> {base_urn} -> {mutated_urn}")
        return mutated_urn
    else:
        # Fallback to original behavior if we can't extract entity name
        mutated_urn = generate_mutated_urn(original_urn, environment_name, entity_type, mutation_config)
        logger.debug(f"Used original {entity_type} URN: {original_urn} -> {mutated_urn}")
        return mutated_urn


def _mutate_tag_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate tag associations in entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_tags', False):
        return data
    
    # Handle globalTags
    if 'globalTags' in data and 'tags' in data['globalTags']:
        for tag in data['globalTags']['tags']:
            if 'tag' in tag:
                original_urn = tag['tag']
                mutated_urn = _generate_consistent_mutated_urn(original_urn, 'tag', environment_name, mutation_config)
                tag['tag'] = mutated_urn
    
    # Handle tags in other formats
    if 'tags' in data:
        if isinstance(data['tags'], list):
            for i, tag_item in enumerate(data['tags']):
                if isinstance(tag_item, dict):
                    if 'urn' in tag_item:
                        original_urn = tag_item['urn']
                        mutated_urn = _generate_consistent_mutated_urn(original_urn, 'tag', environment_name, mutation_config)
                        tag_item['urn'] = mutated_urn
                    elif 'tag' in tag_item:
                        original_urn = tag_item['tag']
                        mutated_urn = _generate_consistent_mutated_urn(original_urn, 'tag', environment_name, mutation_config)
                        tag_item['tag'] = mutated_urn
                elif isinstance(tag_item, str):
                    # Direct URN string
                    mutated_urn = _generate_consistent_mutated_urn(tag_item, 'tag', environment_name, mutation_config)
                    data['tags'][i] = mutated_urn
    
    return data


def _mutate_glossary_term_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate glossary term associations in entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_glossary_terms', False):
        return data
    
    # Handle glossaryTerms
    if 'glossaryTerms' in data and 'terms' in data['glossaryTerms']:
        for term in data['glossaryTerms']['terms']:
            if 'urn' in term:
                original_urn = term['urn']
                mutated_urn = _generate_consistent_mutated_urn(original_urn, 'glossaryTerm', environment_name, mutation_config)
                term['urn'] = mutated_urn
    
    # Handle terms in other formats
    if 'terms' in data:
        if isinstance(data['terms'], list):
            for term_item in data['terms']:
                if isinstance(term_item, dict) and 'urn' in term_item:
                    original_urn = term_item['urn']
                    mutated_urn = _generate_consistent_mutated_urn(original_urn, 'glossaryTerm', environment_name, mutation_config)
                    term_item['urn'] = mutated_urn
    
    return data


def _mutate_structured_property_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate structured property associations in entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_structured_properties', False):
        return data
    
    # Handle structuredProperties
    if 'structuredProperties' in data and 'properties' in data['structuredProperties']:
        for prop in data['structuredProperties']['properties']:
            if 'propertyUrn' in prop:
                original_urn = prop['propertyUrn']
                mutated_urn = _generate_consistent_mutated_urn(original_urn, 'structuredProperty', environment_name, mutation_config)
                prop['propertyUrn'] = mutated_urn
    
    return data


def _mutate_domain_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate domain associations in entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_domains', False):
        return data
    
    # Handle nested domain structure: data['domain']['domain']['urn']
    if 'domain' in data and isinstance(data['domain'], dict) and 'domain' in data['domain']:
        domain_obj = data['domain']['domain']
        if isinstance(domain_obj, dict) and 'urn' in domain_obj:
            original_urn = domain_obj['urn']
            mutated_urn = _generate_consistent_mutated_urn(original_urn, 'domain', environment_name, mutation_config, data)
            domain_obj['urn'] = mutated_urn
            logger.debug(f"Mutated domain association URN: {original_urn} -> {mutated_urn}")
        elif isinstance(domain_obj, str):
            # Handle case where domain_obj is directly a URN string
            original_urn = domain_obj
            mutated_urn = _generate_consistent_mutated_urn(original_urn, 'domain', environment_name, mutation_config, data)
            data['domain']['domain'] = mutated_urn
            logger.debug(f"Mutated domain association URN: {original_urn} -> {mutated_urn}")
    
    # Handle direct domain URN
    if 'domainUrn' in data:
        original_urn = data['domainUrn']
        mutated_urn = _generate_consistent_mutated_urn(original_urn, 'domain', environment_name, mutation_config, data)
        data['domainUrn'] = mutated_urn
        logger.debug(f"Mutated domain URN: {original_urn} -> {mutated_urn}")
    
    return data


def _mutate_parent_node_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate parent node associations in glossary entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_glossary_nodes', False):
        return data
    
    # Handle parentNodes
    if 'parentNodes' in data and 'nodes' in data['parentNodes']:
        for node in data['parentNodes']['nodes']:
            if 'urn' in node:
                original_urn = node['urn']
                mutated_urn = _generate_consistent_mutated_urn(original_urn, 'glossaryNode', environment_name, mutation_config)
                node['urn'] = mutated_urn
    
    # Handle direct parentNode URN
    if 'parentNode' in data:
        original_urn = data['parentNode']
        mutated_urn = _generate_consistent_mutated_urn(original_urn, 'glossaryNode', environment_name, mutation_config)
        data['parentNode'] = mutated_urn
    
    return data


def _mutate_related_term_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate related term associations in glossary entity data using consistent name-based URN generation."""
    if not mutation_config.get('apply_to_glossary_terms', False):
        return data
    
    # Handle relatedTerms
    if 'relatedTerms' in data and 'terms' in data['relatedTerms']:
        for term in data['relatedTerms']['terms']:
            if 'urn' in term:
                original_urn = term['urn']
                mutated_urn = _generate_consistent_mutated_urn(original_urn, 'glossaryTerm', environment_name, mutation_config)
                term['urn'] = mutated_urn
    
    return data


def _mutate_ownership_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate ownership associations in entity data."""
    # Ownership URNs typically reference users/groups, not metadata entities
    # So we generally don't mutate these, but this function is here for completeness
    return data


def _mutate_form_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate form associations in entity data using consistent name-based URN generation."""
    # Handle forms if they reference structured properties or other entities
    if 'forms' in data:
        for form in data['forms']:
            if 'formUrn' in form:
                # Forms might reference structured properties
                original_urn = form['formUrn']
                if 'structuredProperty' in original_urn and mutation_config.get('apply_to_structured_properties', False):
                    mutated_urn = _generate_consistent_mutated_urn(original_urn, 'structuredProperty', environment_name, mutation_config)
                    form['formUrn'] = mutated_urn
    
    return data


def _mutate_test_associations(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate test associations in entity data using consistent name-based URN generation."""
    # Handle test definitions that might reference tags, terms, etc.
    if 'tests' in data:
        for test in data['tests']:
            # Apply mutations to any URNs within test definitions
            if isinstance(test, dict):
                test = _mutate_tag_associations(test, environment_name, mutation_config)
                test = _mutate_glossary_term_associations(test, environment_name, mutation_config)
    
    return data


def mutate_mcp_associations(
    mcp_data: Dict[str, Any], 
    environment_name: str,
    mutation_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Apply URN mutations to MCP (Metadata Change Proposal) data.
    
    Args:
        mcp_data: The MCP data dictionary
        environment_name: The target environment name
        mutation_config: Optional mutation configuration
        
    Returns:
        Dict: The MCP data with mutated URNs
    """
    if not mcp_data or not environment_name:
        return mcp_data
    
    # Get mutation configuration if not provided
    if not mutation_config:
        mutation_config = get_mutation_config_for_environment(environment_name)
        if not mutation_config:
            return mcp_data
    
    # Create a copy to avoid modifying the original
    mutated_data = mcp_data.copy()
    
    # Handle the entity URN in the MCP
    if 'entityUrn' in mutated_data:
        original_urn = mutated_data['entityUrn']
        entity_type = get_entity_type_from_urn(original_urn)
        if entity_type:
            mutated_urn = generate_mutated_urn(original_urn, environment_name, entity_type, mutation_config)
            mutated_data['entityUrn'] = mutated_urn
    
    # Handle aspect data
    if 'aspect' in mutated_data:
        mutated_data['aspect'] = apply_urn_mutations_to_associations(
            mutated_data['aspect'], 
            environment_name, 
            mutation_config
        )
    
    return mutated_data


def get_entity_type_from_urn(urn: str) -> Optional[str]:
    """
    Extract the entity type from a URN.
    
    Args:
        urn: The URN to analyze
        
    Returns:
        str: The entity type or None if not determinable
    """
    if not urn or not isinstance(urn, str):
        return None
    
    try:
        # Standard DataHub URN format: urn:li:entityType:identifier
        parts = urn.split(':')
        if len(parts) >= 3 and parts[0] == 'urn' and parts[1] == 'li':
            return parts[2]
    except Exception:
        pass
    
    return None


def _mutate_platform_instance(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate platform instance data based on platform_instance_mapping."""
    platform_instance_mapping = mutation_config.get('platform_instance_mapping', {})
    
    if not platform_instance_mapping:
        return data
    
    # Handle dataPlatformInstance.instanceId mutations
    if 'dataPlatformInstance' in data:
        platform_instance_data = data['dataPlatformInstance']
        if 'instanceId' in platform_instance_data:
            original_instance_id = platform_instance_data['instanceId']
            
            # Apply platform instance mapping
            for from_instance, to_instance in platform_instance_mapping.items():
                if original_instance_id == from_instance:
                    platform_instance_data['instanceId'] = to_instance
                    logger.info(f"Applied platform instance mutation: {from_instance} -> {to_instance}")
                    break
    
    # Handle entity URNs that contain platform instance references
    if 'urn' in data:
        original_urn = data['urn']
        mutated_urn = original_urn
        
        # Apply platform instance mapping to URN
        for from_instance, to_instance in platform_instance_mapping.items():
            if from_instance in mutated_urn:
                mutated_urn = mutated_urn.replace(from_instance, to_instance)
                logger.info(f"Applied platform instance mutation to URN: {original_urn} -> {mutated_urn}")
        
        data['urn'] = mutated_urn
    
    return data


def _mutate_platform_instance_associations_only(
    data: Dict[str, Any], 
    environment_name: str, 
    mutation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Mutate platform instance data in associations only, skip main entity URN."""
    platform_instance_mapping = mutation_config.get('platform_instance_mapping', {})
    
    if not platform_instance_mapping:
        return data
    
    # Handle dataPlatformInstance.instanceId mutations
    if 'dataPlatformInstance' in data:
        platform_instance_data = data['dataPlatformInstance']
        if 'instanceId' in platform_instance_data:
            original_instance_id = platform_instance_data['instanceId']
            
            # Apply platform instance mapping
            for from_instance, to_instance in platform_instance_mapping.items():
                if original_instance_id == from_instance:
                    platform_instance_data['instanceId'] = to_instance
                    logger.info(f"Applied platform instance mutation: {from_instance} -> {to_instance}")
                    break
    
    # NOTE: Skip main entity URN mutations (data['urn']) for editable entities
    # Only associations should be mutated, not the main dataset/entity URN
    
    return data


def _extract_entity_type_from_urn(urn: str) -> Optional[str]:
    """
    Extract the entity type from a DataHub URN.
    
    Args:
        urn: DataHub URN (e.g., "urn:li:domain:customer-data")
        
    Returns:
        str: The entity type (e.g., "domain") or None if invalid URN
    """
    if not urn or not urn.startswith("urn:li:"):
        return None
    
    try:
        parts = urn.split(":")
        if len(parts) >= 3:
            return parts[2]  # urn:li:ENTITY_TYPE:identifier
    except Exception:
        pass
    
    return None
