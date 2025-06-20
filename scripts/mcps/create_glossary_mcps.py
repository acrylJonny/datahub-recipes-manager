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

from utils.urn_utils import generate_deterministic_urn


logger = logging.getLogger(__name__)


def setup_logging(log_level: str):
    """Set up logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def create_glossary_node_info_mcp(
    node_id: str,
    owner: str,
    node_name: Optional[str] = None,
    description: Optional[str] = None,
    parent_node_urn: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary node info

    Args:
        node_id: The node identifier
        owner: The owner of the node
        node_name: Display name for the node (optional, defaults to node_id)
        description: Description of the node (optional)
        parent_node_urn: URN of parent node (optional)
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Set defaults
    if node_name is None:
        node_name = node_id

    # Create node URN using deterministic generation
    node_urn = generate_deterministic_urn(
        "glossaryNode", node_id, environment=environment, mutation_name=mutation_name
    )

    # Create glossary node info
    node_info = GlossaryNodeInfoClass(
        definition=description or f"Glossary Node: {node_name}",
        name=node_name,
        parentNode=parent_node_urn
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
    """
    Create an MCP for glossary term info

    Args:
        term_id: The term identifier
        owner: The owner of the term
        term_name: Display name for the term (optional, defaults to term_id)
        description: Description of the term (optional)
        parent_node_urn: URN of parent node (optional)
        source_ref: Source reference (optional)
        source_url: Source URL (optional)
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Set defaults
    if term_name is None:
        term_name = term_id

    # Create term URN using deterministic generation
    term_urn = generate_deterministic_urn(
        "glossaryTerm", term_id, environment=environment, mutation_name=mutation_name
    )

    # Create glossary term info
    term_info = GlossaryTermInfoClass(
        definition=description or f"Glossary Term: {term_name}",
        termSource=source_ref or "MANUAL",  # termSource is required
        name=term_name,
        sourceRef=source_ref,
        sourceUrl=source_url,
        parentNode=parent_node_urn
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
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    owner: str,
    ownership_type: str = "urn:li:ownershipType:dataowner",
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity ownership

    Args:
        entity_id: The entity identifier
        entity_type: Type of entity ("glossaryNode" or "glossaryTerm")
        owner: The owner of the entity
        ownership_type: Type of ownership (defaults to dataowner)
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)

    Returns:
        Dictionary representation of the MCP
    """
    # Create entity URN using deterministic generation
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
    )

    # Create audit stamp
    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    # Create ownership
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner}",
                type=OwnershipTypeClass.from_string(ownership_type),
                source=None
            )
        ],
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
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    removed: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity status (soft delete)
    """
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
    )

    status = StatusClass(removed=removed)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        entityType=entity_type,
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_glossary_global_tags_mcp(
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    tags: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity global tags
    """
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    
    tag_associations = []
    for tag in tags:
        tag_urn = f"urn:li:tag:{tag}"
        tag_associations.append(TagAssociationClass(
            tag=tag_urn,
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
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm" 
    glossary_terms: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity terms associations
    """
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    term_associations = []
    for term in glossary_terms:
        term_urn = f"urn:li:glossaryTerm:{term}"
        term_associations.append(GlossaryTermAssociationClass(
            urn=term_urn,
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
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    browse_paths: List[str],
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity browse paths
    """
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
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
    entity_id: str,
    entity_type: str,  # "glossaryNode" or "glossaryTerm"
    memory_elements: List[Dict[str, str]],  # [{"url": "...", "description": "..."}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary entity institutional memory
    """
    entity_urn = generate_deterministic_urn(
        entity_type, entity_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    memory_metadata = []
    for element in memory_elements:
        memory_metadata.append(InstitutionalMemoryMetadataClass(
            url=element["url"],
            description=element["description"],
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
    term_id: str,
    related_terms: List[Dict[str, str]],  # [{"urn": "...", "relationshipType": "..."}, ...]
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for glossary term related terms (only for terms)
    
    Args:
        term_id: The term identifier
        related_terms: List of related term URNs and their relationship types
        environment: Environment name (deprecated, use mutation_name instead)
        mutation_name: Mutation name for deterministic URN (optional)
    
    Returns:
        Dictionary representation of the MCP
    """
    entity_urn = generate_deterministic_urn(
        "glossaryTerm", term_id, environment=environment, mutation_name=mutation_name
    )
    
    # Extract URNs from related terms - DataHub uses simple string lists
    related_term_urns = [term["urn"] for term in related_terms]

    # Create related terms aspect using the correct structure
    # Based on the DataHub SDK, GlossaryRelatedTermsClass expects string lists
    related_terms_aspect = GlossaryRelatedTermsClass(
        relatedTerms=related_term_urns,  # Use the relatedTerms field for general relationships
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


def create_domain_properties_mcp(
    domain_id: str,
    domain_name: str,
    owner: str,
    description: Optional[str] = None,
    parent_domain_urn: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain properties
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    domain_properties = DomainPropertiesClass(
        name=domain_name,
        description=description,
        customProperties=custom_properties or {},
        created=audit_stamp,
        parentDomain=parent_domain_urn
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="domainProperties",
        aspect=domain_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_ownership_mcp(
    domain_id: str,
    owner: str,
    ownership_type: str = "urn:li:ownershipType:dataowner",
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain ownership
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner}",
                type=OwnershipTypeClass.from_string(ownership_type),
                source=None
            )
        ],
        lastModified=audit_stamp
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="ownership",
        aspect=ownership,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_institutional_memory_mcp(
    domain_id: str,
    memory_elements: List[Dict[str, str]],  # [{"url": "...", "description": "..."}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain institutional memory
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    memory_metadata = []
    for element in memory_elements:
        memory_metadata.append(InstitutionalMemoryMetadataClass(
            url=element["url"],
            description=element["description"],
            createStamp=audit_stamp
        ))

    institutional_memory = InstitutionalMemoryClass(elements=memory_metadata)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="institutionalMemory",
        aspect=institutional_memory,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_structured_properties_mcp(
    domain_id: str,
    properties: List[Dict[str, Any]],  # [{"propertyUrn": "...", "values": [...]}, ...]
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain structured properties
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )
    
    property_assignments = []
    for prop in properties:
        property_assignments.append(StructuredPropertyValueAssignmentClass(
            propertyUrn=prop["propertyUrn"],
            values=prop["values"],
            created=audit_stamp,
            lastModified=audit_stamp
        ))

    structured_properties = StructuredPropertiesClass(properties=property_assignments)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="structuredProperties",
        aspect=structured_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_forms_mcp(
    domain_id: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    incomplete_forms: List[str] = None,
    completed_forms: List[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain forms
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    incomplete_form_associations = []
    if incomplete_forms:
        for form_urn in incomplete_forms:
            incomplete_form_associations.append(FormAssociationClass(
                urn=form_urn,
                incompletePrompts=[],
                completedPrompts=[]
            ))

    completed_form_associations = []
    if completed_forms:
        for form_urn in completed_forms:
            completed_form_associations.append(FormAssociationClass(
                urn=form_urn,
                incompletePrompts=[],
                completedPrompts=[]
            ))

    forms = FormsClass(
        incompleteForms=incomplete_form_associations,
        completedForms=completed_form_associations,
        verifications=[]
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="forms",
        aspect=forms,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_test_results_mcp(
    domain_id: str,
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
    failing_tests: List[str] = None,
    passing_tests: List[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain test results
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    current_time = int(time.time() * 1000)
    audit_stamp = AuditStampClass(
        time=current_time,
        actor=f"urn:li:corpuser:{owner}"
    )

    failing_test_results = []
    if failing_tests:
        for test_urn in failing_tests:
            failing_test_results.append(TestResultClass(
                test=test_urn,
                type=TestResultTypeClass.FAILURE,
                testDefinitionMd5=None,
                lastComputed=audit_stamp
            ))

    passing_test_results = []
    if passing_tests:
        for test_urn in passing_tests:
            passing_test_results.append(TestResultClass(
                test=test_urn,
                type=TestResultTypeClass.SUCCESS,
                testDefinitionMd5=None,
                lastComputed=audit_stamp
            ))

    test_results = TestResultsClass(
        failing=failing_test_results,
        passing=passing_test_results
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="testResults",
        aspect=test_results,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_display_properties_mcp(
    domain_id: str,
    color_hex: Optional[str] = None,
    icon_library: Optional[str] = None,
    icon_name: Optional[str] = None,
    icon_style: Optional[str] = None,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain display properties
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    icon_properties = None
    if icon_library and icon_name and icon_style:
        icon_properties = IconPropertiesClass(
            iconLibrary=IconLibraryClass.from_string(icon_library),
            name=icon_name,
            style=icon_style
        )

    display_properties = DisplayPropertiesClass(
        colorHex=color_hex,
        icon=icon_properties
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="displayProperties",
        aspect=display_properties,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def save_mcp_to_file(mcp: Dict[str, Any], output_path: str, enable_dedup: bool = True) -> bool:
    """
    Save an MCP dictionary to a JSON file with optional deduplication

    Args:
        mcp: The MCP dictionary
        output_path: File path to save to
        enable_dedup: Whether to enable deduplication (default: True)

    Returns:
        True if file was saved, False if skipped due to deduplication
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Check for deduplication if enabled
        if enable_dedup and os.path.exists(output_path):
            try:
                with open(output_path, 'r') as f:
                    existing_mcp = json.load(f)
                
                # Compare the MCPs for equality
                if _mcps_are_equal(existing_mcp, mcp):
                    logger.info(f"MCP file unchanged, skipping: {output_path}")
                    return False
                else:
                    logger.info(f"MCP file changed, updating: {output_path}")
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not read existing MCP file for comparison: {e}")
                # Continue with saving if we can't read the existing file
        
        # Write to file
        with open(output_path, 'w') as f:
            json.dump(mcp, f, indent=2, default=str)
            
        logger.info(f"Saved MCP to: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save MCP: {str(e)}")
        raise


def _mcps_are_equal(mcp1: Dict[str, Any], mcp2: Dict[str, Any]) -> bool:
    """
    Compare two MCP dictionaries for equality, ignoring timestamp fields
    
    Args:
        mcp1: First MCP dictionary
        mcp2: Second MCP dictionary
    
    Returns:
        True if MCPs are functionally equivalent, False otherwise
    """
    try:
        # Create deep copies to avoid modifying originals
        import copy
        mcp1_copy = copy.deepcopy(mcp1)
        mcp2_copy = copy.deepcopy(mcp2)
        
        # Remove timestamp fields that change on every run
        _remove_timestamp_fields(mcp1_copy)
        _remove_timestamp_fields(mcp2_copy)
        
        # Compare the sanitized MCPs
        return mcp1_copy == mcp2_copy
    except Exception as e:
        logger.warning(f"Error comparing MCPs: {e}")
        return False


def _remove_timestamp_fields(mcp: Dict[str, Any]) -> None:
    """
    Remove timestamp fields from MCP dictionary (modifies in place)
    
    Args:
        mcp: MCP dictionary to modify
    """
    try:
        # Remove common timestamp fields
        if isinstance(mcp, dict):
            # Remove auditStamp timestamps
            if "aspect" in mcp and isinstance(mcp["aspect"], dict):
                aspect = mcp["aspect"]
                
                # Remove timestamps from ownership aspect
                if "lastModified" in aspect and isinstance(aspect["lastModified"], dict):
                    if "time" in aspect["lastModified"]:
                        del aspect["lastModified"]["time"]
                
                # Remove timestamps from any other audit stamps
                for key, value in aspect.items():
                    if isinstance(value, dict) and "time" in value:
                        del value["time"]
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and "time" in item:
                                del item["time"]
            
            # Remove any top-level timestamp fields
            if "systemMetadata" in mcp and isinstance(mcp["systemMetadata"], dict):
                if "lastObserved" in mcp["systemMetadata"]:
                    del mcp["systemMetadata"]["lastObserved"]
                if "runId" in mcp["systemMetadata"]:
                    del mcp["systemMetadata"]["runId"]
    except Exception as e:
        logger.warning(f"Error removing timestamp fields: {e}") 