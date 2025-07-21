#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for glossary nodes and terms in DataHub
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Dict, Optional, Any, List

# Add parent directory to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

try:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import (
        # Glossary-specific classes
        GlossaryNodeInfoClass,
        GlossaryTermInfoClass,
        GlossaryRelatedTermsClass,
        GlossaryTermAssociationClass,
        
        # Common aspect classes
        OwnershipClass,
        OwnerClass,
        OwnershipTypeClass,
        OwnershipSourceClass,
        OwnershipSourceTypeClass,
        AuditStampClass,
        ChangeTypeClass,
        StatusClass,
        GlobalTagsClass,
        TagAssociationClass,
        GlossaryTermsClass,
        BrowsePathsClass,
        InstitutionalMemoryClass,
        InstitutionalMemoryMetadataClass,
        DeprecationClass,
        SubTypesClass,
        StructuredPropertiesClass,
        StructuredPropertyValueAssignmentClass,
        FormsClass,
        FormAssociationClass,
        FormPromptAssociationClass,
        TestResultsClass,
        TestResultClass,
        TestResultTypeClass,
        DisplayPropertiesClass,
        IconPropertiesClass,
        IconLibraryClass,
        
        # Domain-specific classes
        DomainPropertiesClass,
        DomainsClass,
        
        # Data Platform Instance
        DataPlatformInstanceClass,
        
        # Browse Paths V2
        BrowsePathsV2Class,
        BrowsePathEntryClass,
        
        # URL and other utility classes
        TimeStampClass,
    )
except ImportError:
    print("DataHub SDK not found. Please install with: pip install 'acryl-datahub>=0.10.0'")
    sys.exit(1)

# Import local utilities
from utils.urn_utils import generate_unified_urn, extract_name_from_properties

logger = logging.getLogger(__name__)


def create_glossary_node_info_mcp(
    node_data: Dict[str, Any],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary node info using comprehensive backend data

    Args:
        node_data: Complete node data from backend model
        owner: The owner of the node
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Extract data from node_data
    node_id = node_data.get("id")
    node_name = node_data.get("name", node_id)
    description = node_data.get("description", "")
    parent_urn = node_data.get("parent_urn")
    
    # Create node URN - use custom URN if provided, otherwise generate deterministic URN
    if custom_urn:
        node_urn = custom_urn
    else:
        node_urn = generate_unified_urn("glossaryNode", node_id, environment, mutation_name
        )

    # Create glossary node info with all available data
    node_info = GlossaryNodeInfoClass(
        definition=description or f"Glossary Node: {node_name}",
        name=node_name,
        parentNode=parent_urn
    )

    # Create the MCP
    mcp = MetadataChangeProposalWrapper(
        entityUrn=node_urn,
        entityType="glossaryNode",
        aspectName="glossaryNodeInfo",
        aspect=node_info,
        changeType=ChangeTypeClass.UPSERT
    )

    # Convert to dictionary for JSON serialization
    return mcp.to_obj()


def create_glossary_term_info_mcp(
    term_data: Dict[str, Any],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary term info using comprehensive backend data

    Args:
        term_data: Complete term data from backend model
        owner: The owner of the term
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Extract data from term_data
    term_id = term_data.get("id")
    term_name = term_data.get("name", term_id)
    description = term_data.get("description", "")
    parent_urn = term_data.get("parent_urn")
    term_source = term_data.get("term_source", "INTERNAL")
    
    # Create term URN - use custom URN if provided, otherwise generate deterministic URN
    if custom_urn:
        term_urn = custom_urn
    else:
        term_urn = generate_unified_urn("glossaryTerm", term_id, environment, mutation_name
        )

    # Create glossary term info with all available data
    term_info = GlossaryTermInfoClass(
        definition=description or f"Glossary Term: {term_name}",
        termSource=term_source,
        name=term_name,
        sourceRef=term_source,
        sourceUrl=None,  # Could be added if available in backend
        parentNode=parent_urn
    )

    # Create the MCP
    mcp = MetadataChangeProposalWrapper(
        entityUrn=term_urn,
        entityType="glossaryTerm",
        aspectName="glossaryTermInfo",
        aspect=term_info,
        changeType=ChangeTypeClass.UPSERT
    )

    # Convert to dictionary for JSON serialization
    return mcp.to_obj()


def create_glossary_ownership_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity ownership using comprehensive backend data

    Args:
        entity_data: Complete entity data from backend model
        entity_type: Type of entity ("glossaryNode" or "glossaryTerm")
        owner: The owner of the entity
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Extract entity ID
    entity_id = entity_data.get("id")
    
    # Create entity URN - use custom URN if provided, otherwise generate deterministic URN
    if custom_urn:
        entity_urn = custom_urn
    else:
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

    # Create audit stamp
    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Extract ownership data from backend
    ownership_data = entity_data.get("ownership_data")
    owners = []
    
    if ownership_data:
        # Handle different ownership data formats
        if isinstance(ownership_data, list):
            owners = ownership_data
        elif isinstance(ownership_data, dict) and "owners" in ownership_data:
            owners = ownership_data["owners"]
        elif isinstance(ownership_data, dict):
            # Single owner object
            owners = [ownership_data]
        else:
            # Unexpected format, log and skip
            logger.warning(f"Unexpected ownership_data format: {type(ownership_data)}: {ownership_data}")
            owners = []

    # If no ownership data from backend, create default ownership
    if not owners:
        owners = [{
            "owner": f"urn:li:corpuser:{owner}",
            "type": "urn:li:ownershipType:dataowner",
            "source": None
        }]

    # Create owner objects
    owner_objects = []
    for owner_info in owners:
        if isinstance(owner_info, dict):
            owner_urn = owner_info.get("owner", f"urn:li:corpuser:{owner}")
            ownership_type_urn = owner_info.get("type", "urn:li:ownershipType:dataowner")
            source = owner_info.get("source")
            # Extract enum from URN (last part after colon)
            if ownership_type_urn and ":" in ownership_type_urn:
                ownership_type_enum = ownership_type_urn.split(":")[-1].upper()
            else:
                ownership_type_enum = "DATAOWNER"
            owner_objects.append(OwnerClass(
                owner=owner_urn,
                type=ownership_type_enum,
                typeUrn=ownership_type_urn,
                source=source
            ))
        else:
            # Handle string format
            owner_objects.append(OwnerClass(
                owner=str(owner_info),
                type="DATAOWNER",
                typeUrn="urn:li:ownershipType:dataowner",
                source=None
            ))

    # Create ownership
    ownership = OwnershipClass(
        owners=owner_objects,
        lastModified=audit_stamp
    )

    # Create the MCP
    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="ownership",
        aspect=ownership,
        changeType=ChangeTypeClass.UPSERT
    )

    # Convert to dictionary for JSON serialization
    return mcp.to_obj()


def create_glossary_status_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity status using comprehensive backend data
    """
    # Extract entity ID and deprecated status
    entity_id = entity_data.get("id")
    deprecated = entity_data.get("deprecated", False)
    
    entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
    )

    # Create status aspect
    status = StatusClass(removed=deprecated)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_deprecation_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity deprecation using comprehensive backend data
    """
    try:
        # Extract entity ID and deprecated status
        entity_id = entity_data.get("id")
        deprecated = entity_data.get("deprecated", False)
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        if deprecated:
            # Create deprecation aspect
            deprecation = DeprecationClass(
                deprecated=deprecated,
                decommissionTime=None,
                note=entity_data.get("deprecation_note", "This entity has been deprecated"),
                actor=f"urn:li:corpuser:{entity_data.get('deprecated_by', 'admin')}"
            )

            mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                entityType=entity_type,
                aspectName="deprecation",
                aspect=deprecation,
                changeType=ChangeTypeClass.UPSERT
            )

            return mcp.to_obj()
        else:
            # Return None if not deprecated
            return None
    except Exception as e:
        logger.warning(f"Failed to create deprecation MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
        return None


def create_glossary_global_tags_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity global tags using comprehensive backend data
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    # Extract entity ID and tags
    entity_id = entity_data.get("id")
    tags = entity_data.get("tags", [])
    
    # If no tags, return None
    if not tags:
        return None
    
    entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
    )

    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)

    current_time = int(time.time() * 1000)
    
    tag_associations = []
    for tag in tags:
        if isinstance(tag, dict):
            tag_name = tag.get("name", tag.get("tag", str(tag)))
        else:
            tag_name = str(tag)
        
        tag_urn = f"urn:li:tag:{tag_name}"
        
        # Apply URN mutation to the tag URN if utilities are available
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            tag_urn = generate_mutated_urn(
                tag_urn, env_name, 'tag', mutation_config
            )
        
        tag_associations.append(TagAssociationClass(
            tag=tag_urn,  # Use mutated URN
            context=None,
            attribution=None
        ))

    global_tags = GlobalTagsClass(tags=tag_associations)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="globalTags",
        aspect=global_tags,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_terms_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm" 
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity terms associations using comprehensive backend data
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    # Extract entity ID and glossary terms
    entity_id = entity_data.get("id")
    glossary_terms = entity_data.get("glossary_terms", [])
    
    # If no glossary terms, return None
    if not glossary_terms:
        return None
    
    entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
    )

    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    term_associations = []
    for term in glossary_terms:
        if isinstance(term, dict):
            term_name = term.get("name", term.get("term", str(term)))
        else:
            term_name = str(term)
        
        term_urn = f"urn:li:glossaryTerm:{term_name}"
        
        # Apply URN mutation to the glossary term URN if utilities are available
        if urn_utils_available and mutation_config and (environment or mutation_name):
            env_name = environment or mutation_name
            term_urn = generate_mutated_urn(
                term_urn, env_name, 'glossaryTerm', mutation_config
            )
        
        term_associations.append(GlossaryTermAssociationClass(
            urn=term_urn,  # Use mutated URN
            actor=f"urn:li:corpuser:{owner}",
            context=None,
            attribution=None
        ))

    glossary_terms_aspect = GlossaryTermsClass(
        terms=term_associations,
        auditStamp=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="glossaryTerms",
        aspect=glossary_terms_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_browse_paths_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity browse paths using comprehensive backend data
    """
    # Extract entity ID and browse paths
    entity_id = entity_data.get("id")
    browse_paths = entity_data.get("browse_paths", [])
    
    # If no browse paths, return None
    if not browse_paths:
        return None
    
    entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
    )

    browse_paths_aspect = BrowsePathsClass(paths=browse_paths)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="browsePaths",
        aspect=browse_paths_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_institutional_memory_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity institutional memory using comprehensive backend data
    """
    # Extract entity ID and institutional memory
    entity_id = entity_data.get("id")
    memory_elements = entity_data.get("institutional_memory", [])
    
    # If no memory elements, return None
    if not memory_elements:
        return None
    
    entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    memory_metadata = []
    for element in memory_elements:
        if isinstance(element, dict):
            url = element.get("url", "")
            description = element.get("description", "")
        else:
            url = str(element)
            description = ""
        
        memory_metadata.append(InstitutionalMemoryMetadataClass(
            url=url,
            description=description,
            createStamp=audit_stamp
        ))

    institutional_memory = InstitutionalMemoryClass(elements=memory_metadata)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="institutionalMemory",
        aspect=institutional_memory,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_related_terms_mcp(
    term_data: Dict[str, Any],
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary term related terms using comprehensive backend data
    """
    try:
        # Extract term ID and related terms
        term_id = term_data.get("id")
        related_terms = term_data.get("related_terms", [])
        relationships_data = term_data.get("relationships_data", {})
        
        # If no related terms, return None
        if not related_terms and not relationships_data:
            return None
        
        entity_urn = generate_unified_urn("glossaryTerm", term_id, environment, mutation_name
        )
        
        # Extract URNs from related terms
        related_term_urns = []
        
        # Import URN utilities with graceful fallback
        try:
            from utils.urn_utils import (
                get_mutation_config_for_environment,
                generate_mutated_urn
            )
            urn_utils_available = True
        except ImportError:
            urn_utils_available = False
            logger.warning("URN utilities not available - using original URNs")

        # Get mutation configuration if URN utilities are available
        mutation_config = None
        if urn_utils_available and (environment or mutation_name):
            env_name = environment or mutation_name
            mutation_config = get_mutation_config_for_environment(env_name)

        # Handle related_terms field
        if related_terms:
            for term in related_terms:
                if isinstance(term, dict):
                    term_urn = term.get("urn", term.get("term", str(term)))
                else:
                    term_urn = str(term)
                
                # Apply URN mutation to related term URN if utilities are available
                if urn_utils_available and mutation_config and (environment or mutation_name):
                    env_name = environment or mutation_name
                    # Determine entity type from URN
                    if term_urn.startswith('urn:li:glossaryTerm:'):
                        term_urn = generate_mutated_urn(
                            term_urn, env_name, 'glossaryTerm', mutation_config
                        )
                    elif term_urn.startswith('urn:li:glossaryNode:'):
                        term_urn = generate_mutated_urn(
                            term_urn, env_name, 'glossaryNode', mutation_config
                        )
                
                related_term_urns.append(term_urn)
        
        # Handle relationships_data field
        if relationships_data:
            # If it's a list, treat as list of relationships
            if isinstance(relationships_data, list):
                for relationship in relationships_data:
                    if isinstance(relationship, dict):
                        related_urn = relationship.get("urn", relationship.get("entity", ""))
                        if related_urn:
                            # Apply URN mutation to related URN if utilities are available
                            if urn_utils_available and mutation_config and (environment or mutation_name):
                                env_name = environment or mutation_name
                                if related_urn.startswith('urn:li:glossaryTerm:'):
                                    related_urn = generate_mutated_urn(
                                        related_urn, env_name, 'glossaryTerm', mutation_config
                                    )
                                elif related_urn.startswith('urn:li:glossaryNode:'):
                                    related_urn = generate_mutated_urn(
                                        related_urn, env_name, 'glossaryNode', mutation_config
                                    )
                            related_term_urns.append(related_urn)
            # If it's a dict, look for 'relationships' key
            elif isinstance(relationships_data, dict):
                for relationship in relationships_data.get("relationships", []):
                    if isinstance(relationship, dict):
                        related_urn = relationship.get("urn", relationship.get("entity", ""))
                        if related_urn:
                            # Apply URN mutation to related URN if utilities are available
                            if urn_utils_available and mutation_config and (environment or mutation_name):
                                env_name = environment or mutation_name
                                if related_urn.startswith('urn:li:glossaryTerm:'):
                                    related_urn = generate_mutated_urn(
                                        related_urn, env_name, 'glossaryTerm', mutation_config
                                    )
                                elif related_urn.startswith('urn:li:glossaryNode:'):
                                    related_urn = generate_mutated_urn(
                                        related_urn, env_name, 'glossaryNode', mutation_config
                                    )
                            related_term_urns.append(related_urn)
            else:
                logger.warning(f"Unexpected relationships_data format: {type(relationships_data)}: {relationships_data}")

        # Create related terms aspect
        related_terms_aspect = GlossaryRelatedTermsClass(
            relatedTerms=related_term_urns,  # Use mutated URNs
            isRelatedTerms=[],  # Can be used for "Is A" relationships if needed
            hasRelatedTerms=[],  # Can be used for "Has A" relationships if needed
            values=[]  # Can be used for "Has Value" relationships if needed
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType="glossaryTerm",
            aspectName="glossaryRelatedTerms",
            aspect=related_terms_aspect,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
    except Exception as e:
        logger.warning(f"Failed to create related terms MCP for term {term_data.get('id', 'unknown')}: {str(e)}")
        return None


def create_glossary_domains_mcp(
    term_data: Dict[str, Any],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary term domains using comprehensive backend data
    """
    # Import URN utilities with graceful fallback
    try:
        from utils.urn_utils import (
            get_mutation_config_for_environment,
            generate_mutated_urn
        )
        urn_utils_available = True
    except ImportError:
        urn_utils_available = False
        logger.warning("URN utilities not available - using original URNs")
    
    # Extract term ID and domain information
    term_id = term_data.get("id")
    domain_urn = term_data.get("domain_urn")
    domain_data = term_data.get("domain")
    
    # If no domain information, return None
    if not domain_urn and not domain_data:
        return None
    
    entity_urn = generate_unified_urn("glossaryTerm", term_id, environment, mutation_name
    )
    
    # Get mutation configuration if URN utilities are available
    mutation_config = None
    if urn_utils_available and (environment or mutation_name):
        env_name = environment or mutation_name
        mutation_config = get_mutation_config_for_environment(env_name)
    
    # Determine domain URN
    final_domain_urn = domain_urn
    if not final_domain_urn and domain_data:
        if isinstance(domain_data, dict):
            final_domain_urn = domain_data.get("urn", domain_data.get("name", ""))
        else:
            final_domain_urn = str(domain_data)
    
    if not final_domain_urn:
        return None
    
    # Apply URN mutation to the domain URN if utilities are available
    if urn_utils_available and mutation_config and (environment or mutation_name):
        env_name = environment or mutation_name
        final_domain_urn = generate_mutated_urn(
            final_domain_urn, env_name, 'domain', mutation_config
        )
    
    # Create domains aspect (just domains list)
    domains_aspect = DomainsClass(
        domains=[final_domain_urn]  # Use mutated URN
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType="glossaryTerm",
        aspectName="domains",
        aspect=domains_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_display_properties_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity display properties using comprehensive backend data
    """
    try:
        # Extract entity ID and display properties
        entity_id = entity_data.get("id")
        color_hex = entity_data.get("color_hex")
        
        # Only create if color_hex exists
        if not color_hex:
            return None
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        # Create display properties with icon information
        display_properties = DisplayPropertiesClass(
            colorHex=color_hex,
            icon=None  # Could add icon support later if needed
        )

        # Create the MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType=entity_type,
            aspectName="displayProperties",
            aspect=display_properties,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
        
    except Exception as e:
        logger.error(f"Error creating display properties MCP: {str(e)}")
        return None


def create_glossary_data_platform_instance_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity data platform instance
    """
    try:
        # Extract entity ID and platform information
        entity_id = entity_data.get("id")
        platform = entity_data.get("platform", "datahub")
        platform_instance = entity_data.get("platform_instance")
        
        # Only create if platform instance exists
        if not platform_instance:
            return None
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        # Create data platform instance
        data_platform_instance = DataPlatformInstanceClass(
            platform=f"urn:li:dataPlatform:{platform}",
            instance=f"urn:li:dataPlatformInstance:({platform},{platform_instance})" if platform_instance else None
        )

        # Create the MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType=entity_type,
            aspectName="dataPlatformInstance",
            aspect=data_platform_instance,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
        
    except Exception as e:
        logger.error(f"Error creating data platform instance MCP: {str(e)}")
        return None


def create_glossary_subtypes_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity sub types
    """
    try:
        # Extract entity ID and sub types
        entity_id = entity_data.get("id")
        sub_types = entity_data.get("sub_types", [])
        
        # Only create if sub types exist
        if not sub_types:
            return None
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        # Create sub types
        subtypes = SubTypesClass(
            typeNames=sub_types if isinstance(sub_types, list) else [sub_types]
        )

        # Create the MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType=entity_type,
            aspectName="subTypes",
            aspect=subtypes,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
        
    except Exception as e:
        logger.error(f"Error creating sub types MCP: {str(e)}")
        return None


def create_glossary_forms_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity forms
    """
    try:
        # Extract entity ID and forms data
        entity_id = entity_data.get("id")
        forms_data = entity_data.get("forms", [])
        
        # Only create if forms exist
        if not forms_data:
            return None
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        # Create audit stamp
        current_time = int(time.time() * 1000)
        audit_stamp = AuditStampClass(
            time=current_time,
            actor=f"urn:li:corpuser:{owner}"
        )

        # Create form associations
        form_associations = []
        for form_data in forms_data:
            if isinstance(form_data, dict):
                form_urn = form_data.get("urn")
                if form_urn:
                    form_association = FormAssociationClass(
                        urn=form_urn,
                        incompletePrompts=[],
                        completedPrompts=[]
                    )
                    form_associations.append(form_association)

        if not form_associations:
            return None

        # Create forms
        forms = FormsClass(
            incompleteForms=form_associations,
            completedForms=[],
            verifications=[]
        )

        # Create the MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType=entity_type,
            aspectName="forms",
            aspect=forms,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
        
    except Exception as e:
        logger.error(f"Error creating forms MCP: {str(e)}")
        return None


def create_glossary_structured_properties_mcp(
    entity_data: Dict[str, Any],
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity structured properties
    """
    try:
        # Import URN utilities with graceful fallback
        try:
            from utils.urn_utils import (
                get_mutation_config_for_environment,
                generate_mutated_urn
            )
            urn_utils_available = True
        except ImportError:
            urn_utils_available = False
            logger.warning("URN utilities not available - using original URNs")
        
        # Extract entity ID and structured properties
        entity_id = entity_data.get("id")
        structured_props_data = entity_data.get("structured_properties", {})
        
        # Only create if structured properties exist
        if not structured_props_data:
            return None
        
        entity_urn = generate_unified_urn(entity_type, entity_id, environment, mutation_name
        )

        # Get mutation configuration if URN utilities are available
        mutation_config = None
        if urn_utils_available and (environment or mutation_name):
            env_name = environment or mutation_name
            mutation_config = get_mutation_config_for_environment(env_name)

        # Create audit stamp
        current_time = int(time.time() * 1000)
        audit_stamp = AuditStampClass(
            time=current_time,
            actor=f"urn:li:corpuser:{owner}"
        )

        # Create structured property value assignments
        property_assignments = []
        for prop_urn, prop_values in structured_props_data.items():
            if not isinstance(prop_values, list):
                prop_values = [prop_values]
            
            # Apply URN mutation to the property URN if utilities are available
            mutated_prop_urn = prop_urn
            if urn_utils_available and mutation_config and (environment or mutation_name):
                env_name = environment or mutation_name
                mutated_prop_urn = generate_mutated_urn(
                    prop_urn, env_name, 'structuredProperty', mutation_config
                )
            
            # Convert values to appropriate types
            converted_values = []
            for value in prop_values:
                if isinstance(value, (str, int, float)):
                    converted_values.append(value)
                else:
                    converted_values.append(str(value))
            
            if converted_values:
                assignment = StructuredPropertyValueAssignmentClass(
                    propertyUrn=mutated_prop_urn,  # Use mutated URN
                    values=converted_values,
                    created=audit_stamp,
                    lastModified=audit_stamp
                )
                property_assignments.append(assignment)

        if not property_assignments:
            return None

        # Create structured properties
        structured_properties = StructuredPropertiesClass(
            properties=property_assignments
        )

        # Create the MCP
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            entityType=entity_type,
            aspectName="structuredProperties",
            aspect=structured_properties,
            changeType=ChangeTypeClass.UPSERT
        )

        return mcp.to_obj()
        
    except Exception as e:
        logger.error(f"Error creating structured properties MCP: {str(e)}")
        return None


def save_mcp_to_file(mcp_data: Dict[str, Any], file_path: str) -> bool:
    """
    Save MCP data to a JSON file
    
    Args:
        mcp_data: MCP data to save
        file_path: Path to save the file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write to file
        with open(file_path, 'w') as f:
            json.dump(mcp_data, f, indent=2)
        
        logger.info(f"Saved MCP to {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving MCP to {file_path}: {str(e)}")
        return False


def create_comprehensive_glossary_mcps(
    entity_data: Dict[str, Any],
    entity_type: str,  # "node" or "term"
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    custom_urn: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Create comprehensive MCPs for a glossary entity using all available backend data
    
    Args:
        entity_data: Complete entity data from backend model
        entity_type: Type of entity ("node" or "term")
        owner: Owner username
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)
    
    Returns:
        List of MCP dictionaries
    """
    mcps = []
    
    # Determine DataHub entity type
    datahub_entity_type = "glossaryNode" if entity_type == "node" else "glossaryTerm"
    
    # 1. Core info MCP (always created) - KEY ASPECT
    try:
        if entity_type == "node":
            info_mcp = create_glossary_node_info_mcp(
                entity_data, owner, environment, mutation_name, custom_urn
            )
        else:
            info_mcp = create_glossary_term_info_mcp(
                entity_data, owner, environment, mutation_name, custom_urn
            )
        mcps.append(info_mcp)
        logger.info(f"Created info MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.error(f"Failed to create info MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
        # Info MCP is critical, so we should still try to continue but log the error
    
    # 2. Ownership MCP (always created) - CORE ASPECT
    try:
        ownership_mcp = create_glossary_ownership_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name, custom_urn
        )
        mcps.append(ownership_mcp)
        logger.info(f"Created ownership MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create ownership MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 3. Status MCP (always created) - CORE ASPECT
    try:
        status_mcp = create_glossary_status_mcp(
            entity_data, datahub_entity_type, environment, mutation_name
        )
        mcps.append(status_mcp)
        logger.info(f"Created status MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create status MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 4. Global Tags MCP (if tags exist) - COMMON ASPECT
    try:
        tags_mcp = create_glossary_global_tags_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name
        )
        if tags_mcp:
            mcps.append(tags_mcp)
            logger.info(f"Created global tags MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create global tags MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 5. Glossary Terms MCP (if glossary terms exist) - COMMON ASPECT
    try:
        terms_mcp = create_glossary_terms_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name
        )
        if terms_mcp:
            mcps.append(terms_mcp)
            logger.info(f"Created glossary terms MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create glossary terms MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 6. Browse Paths MCP (if browse paths exist) - SUPPORTED FOR TERMS
    try:
        browse_mcp = create_glossary_browse_paths_mcp(
            entity_data, datahub_entity_type, environment, mutation_name
        )
        if browse_mcp:
            mcps.append(browse_mcp)
            logger.info(f"Created browse paths MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create browse paths MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 7. Institutional Memory MCP (if memory elements exist) - COMMON ASPECT
    try:
        memory_mcp = create_glossary_institutional_memory_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name
        )
        if memory_mcp:
            mcps.append(memory_mcp)
            logger.info(f"Created institutional memory MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create institutional memory MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 8. Display Properties MCP (if color_hex exists) - VISUAL ASPECT
    try:
        display_mcp = create_glossary_display_properties_mcp(
            entity_data, datahub_entity_type, environment, mutation_name
        )
        if display_mcp:
            mcps.append(display_mcp)
            logger.info(f"Created display properties MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create display properties MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 9. Deprecation MCP (if deprecated) - LIFECYCLE ASPECT
    try:
        if entity_data.get("deprecated", False):
            deprecation_mcp = create_glossary_deprecation_mcp(
                entity_data, datahub_entity_type, environment, mutation_name
            )
            if deprecation_mcp:
                mcps.append(deprecation_mcp)
                logger.info(f"Created deprecation MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create deprecation MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # TERM-SPECIFIC ASPECTS
    if entity_type == "term":
        # 10. Related Terms MCP (only for terms, if related terms exist) - TERM-SPECIFIC
        try:
            related_mcp = create_glossary_related_terms_mcp(
                entity_data, environment, mutation_name
            )
            if related_mcp:
                mcps.append(related_mcp)
                logger.info(f"Created related terms MCP for term {entity_data.get('id', 'unknown')}")
        except Exception as e:
            logger.warning(f"Failed to create related terms MCP for term {entity_data.get('id', 'unknown')}: {str(e)}")
        
        # 11. Domains MCP (only for terms, if domain exists) - TERM-SPECIFIC
        try:
            domains_mcp = create_glossary_domains_mcp(
                entity_data, owner, environment, mutation_name
            )
            if domains_mcp:
                mcps.append(domains_mcp)
                logger.info(f"Created domains MCP for term {entity_data.get('id', 'unknown')}")
        except Exception as e:
            logger.warning(f"Failed to create domains MCP for term {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # NEW ASPECTS - Additional common aspects that could be supported
    
    # 12. Data Platform Instance MCP (if platform instance exists)
    try:
        platform_instance_mcp = create_glossary_data_platform_instance_mcp(
            entity_data, datahub_entity_type, environment, mutation_name
        )
        if platform_instance_mcp:
            mcps.append(platform_instance_mcp)
            logger.info(f"Created data platform instance MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create data platform instance MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 13. Sub Types MCP (if sub types exist)
    try:
        subtypes_mcp = create_glossary_subtypes_mcp(
            entity_data, datahub_entity_type, environment, mutation_name
        )
        if subtypes_mcp:
            mcps.append(subtypes_mcp)
            logger.info(f"Created sub types MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create sub types MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 14. Forms MCP (if forms exist)
    try:
        forms_mcp = create_glossary_forms_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name
        )
        if forms_mcp:
            mcps.append(forms_mcp)
            logger.info(f"Created forms MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create forms MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    # 15. Structured Properties MCP (if structured properties exist)
    try:
        structured_props_mcp = create_glossary_structured_properties_mcp(
            entity_data, datahub_entity_type, owner, environment, mutation_name
        )
        if structured_props_mcp:
            mcps.append(structured_props_mcp)
            logger.info(f"Created structured properties MCP for {entity_type} {entity_data.get('id', 'unknown')}")
    except Exception as e:
        logger.warning(f"Failed to create structured properties MCP for {entity_type} {entity_data.get('id', 'unknown')}: {str(e)}")
    
    logger.info(f"Created {len(mcps)} MCPs total for {entity_type} {entity_data.get('id', 'unknown')}")
    return mcps


# Legacy functions for backward compatibility
def create_glossary_node_info_mcp_legacy(
    node_id: str,
    owner: str,
    node_name: Optional[str] = None,
    description: Optional[str] = None,
    parent_node_urn: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Legacy function for backward compatibility"""
    node_data = {
        "id": node_id,
        "name": node_name or node_id,
        "description": description,
        "parent_urn": parent_node_urn
    }
    return create_glossary_node_info_mcp(node_data, owner, environment, mutation_name)


def create_glossary_term_info_mcp_legacy(
    term_id: str,
    owner: str,
    term_name: Optional[str] = None,
    description: Optional[str] = None,
    parent_node_urn: Optional[str] = None,
    source_ref: Optional[str] = None,
    source_url: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Legacy function for backward compatibility"""
    term_data = {
        "id": term_id,
        "name": term_name or term_id,
        "description": description,
        "parent_urn": parent_node_urn,
        "term_source": source_ref
    }
    return create_glossary_term_info_mcp(term_data, owner, environment, mutation_name)


def create_glossary_ownership_mcp_legacy(
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    ownership_type: str = "urn:li:ownershipType:dataowner",
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Legacy function for backward compatibility"""
    entity_data = {
        "id": entity_id,
        "ownership_data": [{
            "owner": f"urn:li:corpuser:{owner}",
            "type": ownership_type,
            "source": None
        }]
    }
    return create_glossary_ownership_mcp(entity_data, entity_type, owner, environment, mutation_name) 