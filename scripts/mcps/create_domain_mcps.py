#!/usr/bin/env python3
"""
Script to create MCP (Metadata Change Proposal) files for domains in DataHub
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
        # Domain-specific classes
        DomainPropertiesClass,
        
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
        GlossaryTermAssociationClass,
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


def create_domain_status_mcp(
    domain_id: str,
    removed: bool = False,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain status (soft delete)
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    status = StatusClass(removed=removed)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="status",
        aspect=status,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_global_tags_mcp(
    domain_id: str,
    tags: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain global tags
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )
    
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
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="globalTags",
        aspect=global_tags,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_glossary_terms_mcp(
    domain_id: str,
    glossary_terms: List[str],
    owner: str,
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain glossary terms associations
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
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
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="glossaryTerms",
        aspect=glossary_terms_aspect,
        changeType=ChangeTypeClass.UPSERT
    )

    return mcp.to_obj()


def create_domain_browse_paths_mcp(
    domain_id: str,
    browse_paths: List[str],
    environment: Optional[str] = None,
    mutation_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create an MCP for domain browse paths
    """
    domain_urn = generate_deterministic_urn(
        "domain", domain_id, environment=environment, mutation_name=mutation_name
    )

    browse_paths_aspect = BrowsePathsClass(paths=browse_paths)

    mcp = MetadataChangeProposalWrapper(
        entityUrn=domain_urn,
        entityType="domain",
        aspectName="browsePaths",
        aspect=browse_paths_aspect,
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
    Save an MCP to a JSON file with optional deduplication
    
    Args:
        mcp: The MCP dictionary to save
        output_path: Path where to save the file
        enable_dedup: Whether to enable deduplication (skip if file exists with same content)
    
    Returns:
        True if file was created/updated, False if skipped due to deduplication
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if enable_dedup and os.path.exists(output_path):
        # Check if the content is the same
        try:
            with open(output_path, 'r') as f:
                existing_mcp = json.load(f)
            
            # Remove timestamp fields for comparison
            mcp_copy = json.loads(json.dumps(mcp))
            existing_copy = json.loads(json.dumps(existing_mcp))
            _remove_timestamp_fields(mcp_copy)
            _remove_timestamp_fields(existing_copy)
            
            if _mcps_are_equal(mcp_copy, existing_copy):
                logger.debug(f"Skipping {output_path} - content unchanged")
                return False
        except (json.JSONDecodeError, KeyError):
            # If we can't read/parse the existing file, overwrite it
            pass
    
    # Write the file
    with open(output_path, 'w') as f:
        json.dump(mcp, f, indent=2)
    
    logger.info(f"Created MCP file: {output_path}")
    return True


def _mcps_are_equal(mcp1: Dict[str, Any], mcp2: Dict[str, Any]) -> bool:
    """
    Compare two MCPs for equality, ignoring timestamp fields
    """
    try:
        return json.dumps(mcp1, sort_keys=True) == json.dumps(mcp2, sort_keys=True)
    except Exception:
        return False


def _remove_timestamp_fields(mcp: Dict[str, Any]) -> None:
    """
    Remove timestamp fields from MCP for comparison purposes
    """
    if isinstance(mcp, dict):
        # Remove common timestamp fields
        timestamp_fields = ['time', 'created', 'lastModified', 'lastComputed']
        for field in timestamp_fields:
            mcp.pop(field, None)
        
        # Recursively process nested objects
        for value in mcp.values():
            if isinstance(value, dict):
                _remove_timestamp_fields(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        _remove_timestamp_fields(item)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Domain MCPs")
    parser.add_argument("--domain-id", required=True, help="Domain ID")
    parser.add_argument("--domain-name", required=True, help="Domain name")
    parser.add_argument("--owner", required=True, help="Owner username")
    parser.add_argument("--description", help="Domain description")
    parser.add_argument("--environment", help="Environment name")
    parser.add_argument("--mutation-name", help="Mutation name")
    parser.add_argument("--output-dir", default="metadata/domains", help="Output directory")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # Create domain properties MCP
    domain_properties_mcp = create_domain_properties_mcp(
        domain_id=args.domain_id,
        domain_name=args.domain_name,
        owner=args.owner,
        description=args.description,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Create ownership MCP
    ownership_mcp = create_domain_ownership_mcp(
        domain_id=args.domain_id,
        owner=args.owner,
        environment=args.environment,
        mutation_name=args.mutation_name
    )
    
    # Save MCPs
    os.makedirs(args.output_dir, exist_ok=True)
    
    properties_path = os.path.join(args.output_dir, f"{args.domain_id}_domain_properties.json")
    ownership_path = os.path.join(args.output_dir, f"{args.domain_id}_ownership.json")
    
    save_mcp_to_file(domain_properties_mcp, properties_path)
    save_mcp_to_file(ownership_mcp, ownership_path)
    
    print(f"Created domain MCPs:")
    print(f"  - Properties: {properties_path}")
    print(f"  - Ownership: {ownership_path}") 